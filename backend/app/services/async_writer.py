"""SendLog 队列驱动异步批量写入：独立数据库连接、asyncpg 批量 INSERT，不阻塞发送主循环。"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from time import monotonic
from typing import Any

from aiolimiter import AsyncLimiter
from sqlalchemy.ext.asyncio import AsyncConnection

from app.core.config import settings
from app.core.database import writer_engine
from app.services.ring_buffer import SendLogRingBuffer

logger = logging.getLogger(__name__)

CRITICAL_WRITER_QUEUE_MAXSIZE = max(1, int(getattr(settings, "critical_writer_queue_maxsize", 100_000) or 100_000))
DETAIL_WRITER_QUEUE_MAXSIZE = max(1, int(getattr(settings, "detail_writer_queue_maxsize", 500_000) or 500_000))
DETAIL_DROP_WARNING_INTERVAL_SECONDS = 60.0
ASYNC_WRITER_MAX_FLUSHES_PER_SECOND = 80
# 小批量直接幂等 INSERT；大批量 COPY 到会话临时表后再用 ON CONFLICT 合并，禁止直接 COPY 主表。
ASYNC_WRITER_COPY_BATCH_THRESHOLD = max(100, int(getattr(settings, "writer_copy_batch_threshold", 1000) or 1000))
ASYNC_WRITER_BATCH_SIZE_DEFAULT = 5000
SEND_LOG_COPY_COLUMNS = (
    "id", "task_id", "recipient_id", "email", "sender_email", "status", "final_status", "retry_state", "bounce_type",
    "abort_retry", "error", "error_message", "response_text", "attempt_count", "monitor_trigger_count", "provider_message_id", "log_type", "sent_at",
    "egress_ip", "proxy_info", "outbound_ip", "sg_event_id", "extra_data", "created_at",
)
SEND_LOG_INSERT_SQL = f"""
INSERT INTO send_logs ({', '.join(SEND_LOG_COPY_COLUMNS)})
VALUES ({', '.join(f'${index}' for index in range(1, len(SEND_LOG_COPY_COLUMNS) + 1))})
ON CONFLICT DO NOTHING
"""
SEND_LOG_TEMP_TABLE = "mercury_send_log_copy_stage"
SEND_LOG_TEMP_INSERT_SQL = f"""
INSERT INTO send_logs ({', '.join(SEND_LOG_COPY_COLUMNS)})
SELECT {', '.join(SEND_LOG_COPY_COLUMNS)} FROM {SEND_LOG_TEMP_TABLE}
ON CONFLICT DO NOTHING
RETURNING id
"""
ASYNC_WRITER_MAX_FLUSH_INTERVAL_DEFAULT = 0.1
ASYNC_WRITER_FLUSH_INTERVAL_NORMAL = 0.2
ASYNC_WRITER_TURBO_THRESHOLD = 1000
ASYNC_WRITER_NORMAL_THRESHOLD = 200
ASYNC_WRITER_FLUSH_RETRY_ATTEMPTS = 3
ASYNC_WRITER_STOP_SENTINEL = object()
LOG_TYPE_MONITOR = "monitor"

TASK_COUNTER_DELTA_UPDATE_SQL = """
UPDATE tasks
SET sent_count = GREATEST(0, COALESCE(sent_count, 0) + $2::int),
    failed_count = GREATEST(0, COALESCE(failed_count, 0) + $3::int),
    updated_at = $4
WHERE id = $1
"""
WRITER_MAX_CONCURRENT_FLUSHES = max(1, int(getattr(settings, "writer_max_concurrent_flushes", 8) or 8))
TASK_COUNTER_FLUSH_INTERVAL_SECONDS = max(0.2, float(getattr(settings, "task_counter_flush_interval_seconds", 1.0) or 1.0))
_writer_flush_semaphore: asyncio.Semaphore | None = None
_writer_flush_semaphore_lock = asyncio.Lock()


async def _get_writer_flush_semaphore() -> asyncio.Semaphore:
    global _writer_flush_semaphore
    async with _writer_flush_semaphore_lock:
        if _writer_flush_semaphore is None:
            _writer_flush_semaphore = asyncio.Semaphore(WRITER_MAX_CONCURRENT_FLUSHES)
        return _writer_flush_semaphore


@dataclass(frozen=True, slots=True)
class CompactOutcome:
    task_id: int
    recipient_id: int | None
    status: str
    provider_message_id: str = ""
    attempt_count: int = 0
    created_at: datetime | None = None
    log_type: str = "regular"


@dataclass(frozen=True, slots=True)
class DetailLog:
    record: dict[str, Any]


@dataclass
class SendLogFlushStats:
    batch_size: int
    inserted_success: int
    inserted_failed: int
    flush_ms: float
    queue_size: int


OnBatchFlushedCallback = Callable[[list[dict[str, Any]], SendLogFlushStats], Awaitable[None]]


def get_writer_batch_size_default() -> int:
    return max(500, int(getattr(settings, "writer_batch_size", ASYNC_WRITER_BATCH_SIZE_DEFAULT) or ASYNC_WRITER_BATCH_SIZE_DEFAULT))


def get_writer_batch_size_turbo() -> int:
    configured = getattr(settings, "writer_turbo_batch_size", None)
    if configured is None:
        configured = getattr(settings, "writer_batch_size_turbo", ASYNC_WRITER_BATCH_SIZE_DEFAULT)
    return max(1000, int(configured or ASYNC_WRITER_BATCH_SIZE_DEFAULT))


def _is_monitor_send_log_record(record: dict[str, Any]) -> bool:
    return str(record.get("log_type") or "regular").strip().lower() == LOG_TYPE_MONITOR


def normalize_send_log_record(record: dict[str, Any]) -> dict[str, Any]:
    recipient_id = record.get("recipient_id")
    normalized_recipient_id = int(recipient_id) if recipient_id not in (None, "", 0) else None
    sent_at = record.get("sent_at")
    created_at = record.get("created_at") or datetime.utcnow()
    if not isinstance(created_at, datetime):
        created_at = datetime.utcnow()
    if sent_at is not None and not isinstance(sent_at, datetime):
        sent_at = None
    extra_data_raw = record.get("extra_data")
    extra_data = None
    if extra_data_raw not in (None, ""):
        try:
            if isinstance(extra_data_raw, (dict, list)):
                extra_data = json.dumps(extra_data_raw, ensure_ascii=False, default=str)
            else:
                parsed_extra_data = json.loads(str(extra_data_raw))
                extra_data = json.dumps(parsed_extra_data, ensure_ascii=False, default=str)
        except (TypeError, ValueError, json.JSONDecodeError):
            extra_data = json.dumps({"raw": str(extra_data_raw)[:500]}, ensure_ascii=False)
    normalized_email = str(record.get("email") or "").strip().lower()
    return {
        "id": int(record.get("id") or 0) or None,
        "task_id": int(record.get("task_id") or 0),
        "recipient_id": normalized_recipient_id,
        "email": normalized_email,
        "sender_email": str(record.get("sender_email") or "").strip()[:255] or None,
        "status": str(record.get("status") or "failed")[:50],
        "final_status": str(record.get("final_status") or "").strip().lower()[:50],
        "retry_state": str(record.get("retry_state") or "none").strip().lower()[:20] or "none",
        "bounce_type": str(record.get("bounce_type") or "").strip().lower()[:20],
        "abort_retry": bool(record.get("abort_retry") or False),
        "error": str(record.get("error") or "")[:4000],
        "error_message": str(record.get("error_message") or "")[:4000],
        "response_text": str(record.get("response_text") or "")[:4000],
        "attempt_count": int(record.get("attempt_count") or 0),
        "monitor_trigger_count": int(record.get("monitor_trigger_count") or 0) or None,
        "provider_message_id": str(record.get("provider_message_id") or ""),
        "log_type": str(record.get("log_type") or "regular"),
        "sent_at": sent_at,
        "egress_ip": str(record.get("egress_ip") or "")[:64],
        "proxy_info": str(record.get("proxy_info") or "")[:255],
        "outbound_ip": str(record.get("outbound_ip")).strip()[:45] if record.get("outbound_ip") not in (None, "") else None,
        "sg_event_id": str(record.get("sg_event_id") or "").strip()[:100] or None,
        "extra_data": extra_data,
        "created_at": created_at,
    }


def _normalized_records_to_copy_rows(records: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for row in records:
        if not row["email"] or row["task_id"] <= 0 or not row.get("id"):
            continue
        rows.append(
            (
                row["id"],
                row["task_id"],
                row["recipient_id"],
                row["email"],
                row["sender_email"],
                row["status"],
                row["final_status"],
                row["retry_state"],
                row["bounce_type"],
                row["abort_retry"],
                row["error"],
                row["error_message"],
                row["response_text"],
                row["attempt_count"],
                row["monitor_trigger_count"],
                row["provider_message_id"],
                row["log_type"] or "regular",
                row["sent_at"],
                row["egress_ip"],
                row["proxy_info"],
                row["outbound_ip"],
                row["sg_event_id"],
                row["extra_data"],
                row["created_at"],
            )
        )
    return rows


def _count_flushed_regular_statuses(batch: list[dict[str, Any]]) -> tuple[int, int]:
    success_count = 0
    failed_count = 0
    latest_by_email: dict[tuple[int, str], dict[str, Any]] = {}
    for record in batch:
        if _is_monitor_send_log_record(record):
            continue
        email_key = str(record.get("email") or "").strip().lower()
        task_key = int(record.get("task_id") or 0)
        if not email_key or task_key <= 0:
            continue
        latest_by_email[(task_key, email_key)] = record
    for record in latest_by_email.values():
        status_value = str(record.get("status") or "").strip().lower()
        if status_value in {"sent", "success", "delivered"}:
            success_count += 1
        elif status_value in {"failed", "bounced", "blocked", "dropped"}:
            failed_count += 1
        elif status_value in {"unknown", "delivery_unknown"}:
            failed_count += 0
    return success_count, failed_count


class AsyncBatchWriter:
    """SendLog 异步批量写入器：环形缓冲 + 队列双路径，限速刷盘，不阻塞发送主循环。"""

    def __init__(
        self,
        task_id: int,
        *,
        batch_size: int | None = None,
        max_flush_interval: float = ASYNC_WRITER_MAX_FLUSH_INTERVAL_DEFAULT,
        queue_maxsize: int = DETAIL_WRITER_QUEUE_MAXSIZE,
        critical_queue_maxsize: int = CRITICAL_WRITER_QUEUE_MAXSIZE,
        on_batch_flushed: OnBatchFlushedCallback | None = None,
        ring_buffer: SendLogRingBuffer | None = None,
        max_flushes_per_second: int | None = None,
    ) -> None:
        self.task_id = int(task_id)
        self._configured_batch_size = max(1, int(batch_size or get_writer_batch_size_turbo()))
        self._max_flush_interval = max(0.05, float(max_flush_interval))
        self._queue_maxsize = max(1, int(queue_maxsize))
        self._queue: deque[Any] = deque()
        self._queue_event = asyncio.Event()
        self._queue_space_event = asyncio.Event()
        self._queue_space_event.set()
        self._queue_lock = asyncio.Lock()
        self._ring_buffer = ring_buffer or SendLogRingBuffer(capacity=self._queue_maxsize)
        self._on_batch_flushed = on_batch_flushed
        self._critical_queue: asyncio.Queue[CompactOutcome] = asyncio.Queue(maxsize=max(1, int(critical_queue_maxsize)))
        self._critical_writer_task: asyncio.Task | None = None
        self._writer_task: asyncio.Task | None = None
        self._stop_requested = False
        self._turbo_active = False
        self._restart_count = 0
        self._write_paused = False
        flush_rate = max(1, int(max_flushes_per_second or getattr(settings, "writer_max_flushes_per_second", ASYNC_WRITER_MAX_FLUSHES_PER_SECOND) or ASYNC_WRITER_MAX_FLUSHES_PER_SECOND))
        self._flush_limiter = AsyncLimiter(max_rate=flush_rate, time_period=1.0)
        self._flush_latency_samples: list[float] = []
        self._pending_sent_delta = 0
        self._pending_failed_delta = 0
        self._last_counter_flush_at = monotonic()
        self._dropped_count = 0
        self._detail_dropped_since_warning = 0
        self._last_detail_drop_warning_at = 0.0
        self._detail_flush_inflight = 0
        self._critical_flush_inflight = 0

    @property
    def queue_size(self) -> int:
        return len(self._queue) + int(self._ring_buffer.size)

    @property
    def critical_queue_size(self) -> int:
        return int(self._critical_queue.qsize())

    @property
    def pending_count(self) -> int:
        return (
            self.queue_size
            + self.critical_queue_size
            + max(0, int(self._detail_flush_inflight))
            + max(0, int(self._critical_flush_inflight))
        )

    @property
    def dropped_count(self) -> int:
        return int(self._dropped_count) + int(self._ring_buffer.dropped_total)

    @property
    def ring_buffer_dropped(self) -> int:
        return int(self._ring_buffer.dropped_total)

    def set_write_paused(self, paused: bool) -> None:
        self._write_paused = bool(paused)

    def flush_latency_percentiles(self) -> dict[str, float]:
        samples = sorted(self._flush_latency_samples[-200:])
        if not samples:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}

        def percentile(p: float) -> float:
            index = min(len(samples) - 1, max(0, int(round((len(samples) - 1) * p))))
            return round(samples[index], 2)

        return {"p50": percentile(0.5), "p95": percentile(0.95), "p99": percentile(0.99)}

    def is_alive(self) -> bool:
        return (
            self._writer_task is not None
            and not self._writer_task.done()
            and self._critical_writer_task is not None
            and not self._critical_writer_task.done()
        )

    async def start(self) -> None:
        if self.is_alive():
            return
        self._stop_requested = False
        self._writer_task = asyncio.create_task(self._writer_loop(), name=f"detail-log-writer-{self.task_id}")
        self._critical_writer_task = asyncio.create_task(self._critical_writer_loop(), name=f"compact-outcome-writer-{self.task_id}")
        logger.info(
            "任务 %s Writer 已启动：最小结果账本队列容量=%s，详细日志队列容量=%s",
            self.task_id,
            self._critical_queue.maxsize,
            self._queue_maxsize,
        )

    async def restart(self) -> None:
        self._restart_count += 1
        previous = self._writer_task
        if previous is not None and not previous.done():
            return
        writer_error = None
        if previous is not None:
            try:
                writer_error = previous.exception()
            except asyncio.CancelledError:
                writer_error = "cancelled"
        logger.error("任务 %s SendLog 写入协程重启 attempt=%s previous_error=%s", self.task_id, self._restart_count, writer_error)
        await self.start()

    async def enqueue_outcome(self, outcome: CompactOutcome) -> None:
        """关键账本使用有界 asyncio.Queue；队列满时阻塞，绝不丢弃。"""
        await self._critical_queue.put(outcome)

    async def enqueue_outcomes(self, outcomes: list[CompactOutcome]) -> int:
        for outcome in outcomes:
            await self.enqueue_outcome(outcome)
        return len(outcomes)

    async def enqueue(self, record: dict[str, Any], *, allow_drop_callback: bool = True) -> bool:
        """兼容入口：regular/monitor 先写关键账本，再尝试写可丢弃详情。"""
        normalized = normalize_send_log_record(record)
        status = str(normalized.get("status") or "failed").lower()
        compact_status = "success" if status in {"sent", "success", "delivered", "monitor_sent"} else "bounced" if status == "bounced" else "unknown" if status in {"unknown", "delivery_unknown"} else "failed"
        await self.enqueue_outcome(CompactOutcome(
            task_id=int(normalized["task_id"]),
            recipient_id=normalized.get("recipient_id"),
            status=compact_status,
            provider_message_id=str(normalized.get("provider_message_id") or ""),
            attempt_count=int(normalized.get("attempt_count") or 0),
            created_at=normalized.get("created_at"),
            log_type=str(normalized.get("log_type") or "regular"),
        ))
        return await self.enqueue_detail(DetailLog(record))

    async def enqueue_detail(self, detail: DetailLog) -> bool:
        record = detail.record
        async with self._queue_lock:
            if len(self._queue) >= self._queue_maxsize:
                self._dropped_count += 1
                self._detail_dropped_since_warning += 1
                now_value = monotonic()
                if now_value - self._last_detail_drop_warning_at >= DETAIL_DROP_WARNING_INTERVAL_SECONDS:
                    logger.warning("任务 %s 详细日志队列已满，已丢弃 %s 条记录", self.task_id, self._detail_dropped_since_warning)
                    self._detail_dropped_since_warning = 0
                    self._last_detail_drop_warning_at = now_value
                return False
            self._queue.append(record)
            self._queue_event.set()
        return True

    async def enqueue_many(self, records: list[dict[str, Any]], *, allow_drop_callback: bool = True) -> int:
        enqueued = 0
        for record in records:
            if await self.enqueue(record, allow_drop_callback=allow_drop_callback):
                enqueued += 1
        return enqueued

    async def flush_and_stop(self, timeout: float = 15.0) -> None:
        """任务收尾屏障：停止接收阶段结束后，等待全部内存结果异步落库。"""
        self._stop_requested = True
        self._queue_event.set()
        tasks = [task for task in (self._writer_task, self._critical_writer_task) if task is not None]
        if not tasks:
            return
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        except asyncio.TimeoutError as exc:
            logger.error(
                "任务 %s SendLog 写入器在 %s 秒内未完成排空，停止无限等待并由任务收尾记录持久化失败 queue_depth=%s",
                self.task_id,
                timeout,
                self.queue_size,
            )
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise TimeoutError(f"SendLog 写入器排空超时，剩余队列={self.queue_size}") from exc
        except Exception as exc:
            logger.error("任务 %s SendLog 写入器停止异常：%s", self.task_id, exc)
            raise

    async def flush(self, timeout: float = 30.0) -> None:
        """等待当前批次和两个写入队列排空，但不停止 Writer。"""
        deadline = monotonic() + max(0.1, float(timeout))
        while True:
            writer_task = self._writer_task
            critical_task = self._critical_writer_task
            for task in (writer_task, critical_task):
                if task is not None and task.done() and not task.cancelled():
                    error = task.exception()
                    if error is not None:
                        raise RuntimeError(f"SendLog Writer 已异常退出：{error}") from error
            if self.pending_count <= 0:
                # 两个 Writer 都可能已经把最后一批移出队列但尚未提交事务；pending_count
                # 同时覆盖队列与正在刷盘的批次，只有全部归零才允许完成审计。
                await asyncio.sleep(0)
                if self.pending_count <= 0:
                    return
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(f"SendLog Writer flush 超时，剩余队列={self.queue_size}")
            await asyncio.sleep(min(0.02, remaining))

    def _resolve_flush_settings(self) -> tuple[int, float]:
        queue_size = self.queue_size
        if queue_size > ASYNC_WRITER_TURBO_THRESHOLD:
            self._turbo_active = True
        elif queue_size < ASYNC_WRITER_NORMAL_THRESHOLD:
            self._turbo_active = False
        if self._turbo_active:
            return get_writer_batch_size_turbo(), self._max_flush_interval
        return get_writer_batch_size_default(), ASYNC_WRITER_FLUSH_INTERVAL_NORMAL

    async def _pop_queue_record(self, timeout: float) -> Any:
        async with self._queue_lock:
            if self._queue:
                return self._queue.popleft()
            self._queue_event.clear()
        try:
            await asyncio.wait_for(self._queue_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        async with self._queue_lock:
            if not self._queue:
                self._queue_event.clear()
                return None
            record = self._queue.popleft()
            if not self._queue:
                self._queue_event.clear()
            return record

    async def _critical_writer_loop(self) -> None:
        """关键账本独立连接循环；失败时无限退避重试，记录绝不丢弃。"""
        pending: list[CompactOutcome] = []
        try:
            while not self._stop_requested or not self._critical_queue.empty() or pending:
                try:
                    item = await asyncio.wait_for(self._critical_queue.get(), timeout=self._max_flush_interval)
                    pending.append(item)
                    while len(pending) < self._configured_batch_size:
                        pending.append(self._critical_queue.get_nowait())
                except asyncio.QueueEmpty:
                    pass
                except asyncio.TimeoutError:
                    pass
                if not pending:
                    continue
                batch = list(pending)
                self._critical_flush_inflight = len(batch)
                try:
                    async with writer_engine.connect() as conn:
                        raw_connection = await conn.get_raw_connection()
                        asyncpg_conn = raw_connection.driver_connection
                        rows = [(
                            item.task_id,
                            item.recipient_id,
                            item.status,
                            item.provider_message_id,
                            item.attempt_count,
                            item.log_type,
                            item.created_at or datetime.utcnow(),
                        ) for item in batch]
                        await asyncpg_conn.executemany(
                            """
                            INSERT INTO compact_outcomes
                                (task_id, recipient_id, status, provider_message_id, attempt_count, log_type, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT DO NOTHING
                            """,
                            rows,
                        )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "最小结果账本批量写入成功 task_id=%s count=%s",
                            self.task_id,
                            len(batch),
                        )
                    pending.clear()
                except Exception as exc:
                    logger.error("任务 %s 最小结果账本写入失败，将保留并重试：%s", self.task_id, exc)
                    await asyncio.sleep(1.0)
                finally:
                    self._critical_flush_inflight = 0
        finally:
            self._critical_flush_inflight = 0
            self._critical_writer_task = None

    async def _writer_loop(self) -> None:
        pending_batch: list[dict[str, Any]] = []
        last_flush_at = monotonic()
        try:
            while True:
                flush_batch_size, flush_interval = self._resolve_flush_settings()
                wait_timeout = flush_interval
                if pending_batch:
                    wait_timeout = max(0.01, flush_interval - (monotonic() - last_flush_at))

                ring_batch = await self._ring_buffer.drain(flush_batch_size)
                if ring_batch:
                    pending_batch.extend(ring_batch)

                record = await self._pop_queue_record(wait_timeout)
                if record is not None:
                    pending_batch.append(record)

                should_flush = bool(pending_batch) and (
                    len(pending_batch) >= flush_batch_size or (monotonic() - last_flush_at) >= flush_interval
                )
                if should_flush:
                    working_batch = list(pending_batch)
                    pending_batch.clear()
                    await self._safe_flush_batch(working_batch, requeue_on_failure=True)
                    last_flush_at = monotonic()

                if self._stop_requested and not self._queue and self._ring_buffer.size <= 0 and not pending_batch:
                    break

            final_ring_batch = await self._ring_buffer.drain(self._ring_buffer.size)
            if final_ring_batch:
                pending_batch.extend(final_ring_batch)
            if pending_batch:
                await self._safe_flush_batch(pending_batch, requeue_on_failure=False, force_counter_flush=True)
            await self._flush_task_counter_deltas(force=True)
        except Exception as exc:
            logger.exception("任务 %s SendLog 写入协程异常退出：%s", self.task_id, exc)
            raise
        finally:
            await self._flush_leftover_records()
            self._writer_task = None

    async def _requeue_failed_batch(self, batch: list[dict[str, Any]]) -> None:
        for record in batch:
            retry_count = int(record.get("_writer_retry_count") or 0) + 1
            if retry_count > ASYNC_WRITER_FLUSH_RETRY_ATTEMPTS:
                # 关键 CompactOutcome 已先成功落库；详情失败不能再阻塞任务完成，保留明确计数供监控诊断。
                self._dropped_detail_records = int(getattr(self, "_dropped_detail_records", 0) or 0) + 1
                logger.error(
                    "任务 %s 详细日志写入重试耗尽，已保留最小结果账本但丢弃详情记录 email=%s dropped=%s",
                    self.task_id,
                    str(record.get("email") or ""),
                    self._dropped_detail_records,
                )
                continue
            retry_record = dict(record)
            retry_record["_writer_retry_count"] = retry_count
            if not self._ring_buffer.put_nowait(retry_record):
                async with self._queue_lock:
                    if len(self._queue) < self._queue_maxsize:
                        self._queue.append(retry_record)
                        self._queue_event.set()
                    else:
                        logger.error(
                            "任务 %s SendLog 写入失败后重入队失败 email=%s retry=%s",
                            self.task_id,
                            str(record.get("email") or ""),
                            retry_count,
                        )

    async def _flush_leftover_records(self) -> None:
        leftover: list[dict[str, Any]] = []
        async with self._queue_lock:
            while self._queue:
                item = self._queue.popleft()
                if item is not ASYNC_WRITER_STOP_SENTINEL:
                    leftover.append(item)
            self._queue_event.clear()
        ring_leftover = await self._ring_buffer.drain(self._ring_buffer.size)
        if ring_leftover:
            leftover.extend(ring_leftover)
        if leftover:
            logger.warning("任务 %s SendLog 写入协程退出时仍有 %s 条未刷盘日志，尝试最终写入", self.task_id, len(leftover))
            try:
                await self._safe_flush_batch(leftover, requeue_on_failure=False, force_counter_flush=True)
            except Exception as exc:
                logger.error("任务 %s SendLog 最终刷盘失败：%s", self.task_id, exc)

    async def _safe_flush_batch(self, batch: list[dict[str, Any]], *, requeue_on_failure: bool, force_counter_flush: bool = False) -> None:
        if not batch:
            return
        flush_started_at = monotonic()
        flushed_records: list[dict[str, Any]] = []
        inserted_success = 0
        inserted_failed = 0
        self._detail_flush_inflight += len(batch)
        try:
            inserted_success, inserted_failed, flushed_records = await self._flush_batch_atomic(batch)
        except Exception as exc:
            logger.exception("任务 %s SendLog 批量写入失败 size=%s error=%s", self.task_id, len(batch), exc)
            if requeue_on_failure and not self._stop_requested:
                await self._requeue_failed_batch(batch)
            else:
                logger.error("任务 %s SendLog 最终刷盘失败，未重入队以避免写入器退出后积压 size=%s", self.task_id, len(batch))
        finally:
            self._detail_flush_inflight = max(0, self._detail_flush_inflight - len(batch))
        flush_ms = round((monotonic() - flush_started_at) * 1000, 2)
        self._flush_latency_samples.append(flush_ms)
        if len(self._flush_latency_samples) > 500:
            self._flush_latency_samples = self._flush_latency_samples[-300:]
        if inserted_success or inserted_failed:
            self._pending_sent_delta += inserted_success
            self._pending_failed_delta += inserted_failed
        await self._flush_task_counter_deltas(force=force_counter_flush)
        stats = SendLogFlushStats(len(batch), inserted_success, inserted_failed, flush_ms, self.queue_size)
        if self._on_batch_flushed is not None:
            try:
                # ON CONFLICT DO NOTHING 的幂等重复同样已从队列消费，必须释放对应的
                # inflight/outstanding 计数，否则任务发送完成后会永久等待而不能自动结束。
                await self._on_batch_flushed(batch, stats)
            except Exception as exc:
                logger.error("任务 %s SendLog 刷盘回调失败：%s", self.task_id, exc)

    async def _flush_task_counter_deltas(self, *, force: bool = False) -> None:
        sent_delta = int(self._pending_sent_delta or 0)
        failed_delta = int(self._pending_failed_delta or 0)
        if sent_delta <= 0 and failed_delta <= 0:
            return
        now_value = monotonic()
        if not force and now_value - float(self._last_counter_flush_at or 0.0) < TASK_COUNTER_FLUSH_INTERVAL_SECONDS:
            return
        self._pending_sent_delta = 0
        self._pending_failed_delta = 0
        self._last_counter_flush_at = now_value
        try:
            async with writer_engine.connect() as conn:
                raw_connection = await conn.get_raw_connection()
                asyncpg_conn = raw_connection.driver_connection
                async with conn.begin():
                    await asyncpg_conn.execute(
                        TASK_COUNTER_DELTA_UPDATE_SQL,
                        self.task_id,
                        sent_delta,
                        failed_delta,
                        datetime.utcnow(),
                    )
        except Exception:
            self._pending_sent_delta += sent_delta
            self._pending_failed_delta += failed_delta
            logger.warning("任务 %s 计数聚合更新失败，将在下一轮重试 sent_delta=%s failed_delta=%s", self.task_id, sent_delta, failed_delta, exc_info=True)

    async def _flush_batch_atomic(self, batch: list[dict[str, Any]]) -> tuple[int, int, list[dict[str, Any]]]:
        await self._flush_limiter.acquire(1)
        semaphore = await _get_writer_flush_semaphore()
        async with semaphore:
            async with writer_engine.connect() as conn:
                raw_connection = await conn.get_raw_connection()
                asyncpg_conn = raw_connection.driver_connection
                async with conn.begin():
                    flushed_records: list[dict[str, Any]] = []
                    normalized_records = [normalize_send_log_record(record) for record in batch]
                    valid_records = [record for record in normalized_records if record["email"] and int(record["task_id"] or 0) > 0]
                    missing_id_count = sum(1 for record in valid_records if not record.get("id"))
                    if missing_id_count:
                        id_rows = await asyncpg_conn.fetch(
                            "SELECT nextval(pg_get_serial_sequence('send_logs', 'id'))::int AS id FROM generate_series(1, $1::int)",
                            missing_id_count,
                        )
                        allocated_ids = [int(row["id"] or 0) for row in id_rows]
                        allocated_iter = iter(allocated_ids)
                        for record in valid_records:
                            if not record.get("id"):
                                record["id"] = next(allocated_iter, None)
                    copy_rows = _normalized_records_to_copy_rows(valid_records)
                    if copy_rows:
                        if len(copy_rows) >= ASYNC_WRITER_COPY_BATCH_THRESHOLD:
                            await asyncpg_conn.execute(
                                f"CREATE TEMP TABLE IF NOT EXISTS {SEND_LOG_TEMP_TABLE} "
                                "(LIKE send_logs INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
                            )
                            await asyncpg_conn.execute(f"TRUNCATE TABLE {SEND_LOG_TEMP_TABLE}")
                            await asyncpg_conn.copy_records_to_table(
                                SEND_LOG_TEMP_TABLE,
                                records=copy_rows,
                                columns=SEND_LOG_COPY_COLUMNS,
                            )
                            inserted_ids = {
                                int(row["id"])
                                for row in await asyncpg_conn.fetch(SEND_LOG_TEMP_INSERT_SQL)
                            }
                        else:
                            await asyncpg_conn.executemany(SEND_LOG_INSERT_SQL, copy_rows)
                            candidate_ids = [int(record["id"]) for record in valid_records if record.get("id")]
                            inserted_ids = {
                                int(row["id"])
                                for row in await asyncpg_conn.fetch(
                                    "SELECT id FROM send_logs WHERE id = ANY($1::bigint[])",
                                    candidate_ids,
                                )
                            }
                        flushed_records = [
                            record for record in valid_records if int(record.get("id") or 0) in inserted_ids
                        ]
                    inserted_success, inserted_failed = _count_flushed_regular_statuses(flushed_records)
                    return inserted_success, inserted_failed, flushed_records


