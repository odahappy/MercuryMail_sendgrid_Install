"""发送前预分配 SendLog ID，供 SendGrid custom_args 与 Events API 精确匹配。

热发送路径不得逐批访问数据库分配序列；任务启动时使用一次 SQL 批量预热 ID 池，
后续在内存中无锁领取。池耗尽时再按大块补货，避免 batch=50、RTT=1.5s 场景下
每秒额外产生一次数据库往返。
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque

from app.core.database import writer_engine
from app.models.models import Recipient

logger = logging.getLogger(__name__)

DEFAULT_SEND_LOG_ID_BLOCK_SIZE = 10_000
MAX_SEND_LOG_ID_BLOCK_SIZE = 200_000


class SendLogIdAllocator:
    """任务级 SendLog ID 内存分配器。

    单个任务通常只需要一个分配器，内部用 asyncio.Lock 保护补货；正常领取只从 deque
    弹出，CPU 开销远低于每批 `SELECT nextval(...)`。
    """

    def __init__(self, task_id: int, *, block_size: int = DEFAULT_SEND_LOG_ID_BLOCK_SIZE) -> None:
        self.task_id = int(task_id)
        self.block_size = max(1, min(int(block_size or DEFAULT_SEND_LOG_ID_BLOCK_SIZE), MAX_SEND_LOG_ID_BLOCK_SIZE))
        self._ids: deque[int] = deque()
        self._lock = asyncio.Lock()
        self.allocated_total = 0
        self.refill_count = 0

    @property
    def cached_count(self) -> int:
        return len(self._ids)

    async def warmup(self, expected_count: int | None = None) -> None:
        target = self.block_size
        if expected_count is not None and int(expected_count or 0) > 0:
            target = min(MAX_SEND_LOG_ID_BLOCK_SIZE, max(self.block_size, int(expected_count or 0)))
        await self._ensure_available(target)

    async def allocate_for_recipients(self, recipients: list[Recipient]) -> dict[str, int]:
        recipient_keys: list[str] = []
        seen: set[str] = set()
        for recipient in recipients:
            recipient_id = int(getattr(recipient, "id", 0) or 0)
            email = str(getattr(recipient, "email", "") or "").strip().lower()
            key = str(recipient_id) if recipient_id > 0 else f"email:{email}"
            if not email or key in seen:
                continue
            seen.add(key)
            recipient_keys.append(key)
        if not recipient_keys:
            return {}
        try:
            await self._ensure_available(len(recipient_keys))
        except Exception as exc:
            logger.error(
                "任务 %s SendLog ID 补货失败，将仅分配缓存中的可用 ID requested=%s cached=%s error_type=%s error=%s",
                self.task_id,
                len(recipient_keys),
                len(self._ids),
                type(exc).__name__,
                exc,
            )
        mapping: dict[str, int] = {}
        for key in recipient_keys:
            if not self._ids:
                break
            mapping[key] = int(self._ids.popleft())
        if len(mapping) != len(recipient_keys):
            logger.error(
                "任务 %s SendLog ID 内存池领取不足 expected=%s actual=%s cached=%s",
                self.task_id,
                len(recipient_keys),
                len(mapping),
                len(self._ids),
            )
        return mapping

    async def _ensure_available(self, required: int) -> None:
        normalized_required = max(1, int(required or 1))
        if len(self._ids) >= normalized_required:
            return
        async with self._lock:
            if len(self._ids) >= normalized_required:
                return
            fetch_count = max(self.block_size, normalized_required - len(self._ids))
            rows = await _fetch_send_log_ids(fetch_count)
            self._ids.extend(rows)
            self.allocated_total += len(rows)
            self.refill_count += 1
            logger.debug(
                "任务 %s SendLog ID 池补货 count=%s cached=%s refill_count=%s",
                self.task_id,
                len(rows),
                len(self._ids),
                self.refill_count,
            )


async def _fetch_send_log_ids(count: int) -> list[int]:
    normalized_count = max(1, min(int(count or 1), MAX_SEND_LOG_ID_BLOCK_SIZE))
    async with writer_engine.connect() as conn:
        raw_connection = await conn.get_raw_connection()
        asyncpg_conn = raw_connection.driver_connection
        rows = await asyncpg_conn.fetch(
            "SELECT nextval(pg_get_serial_sequence('send_logs', 'id'))::int AS id FROM generate_series(1, $1::int)",
            normalized_count,
        )
    return [int(row["id"] or 0) for row in rows if int(row["id"] or 0) > 0]


async def allocate_send_log_ids(task_id: int, recipients: list[Recipient]) -> dict[str, int]:
    """兼容旧调用：为批量发送预分配 send_log_id，返回 email(lower) -> id。"""
    allocator = SendLogIdAllocator(task_id, block_size=max(DEFAULT_SEND_LOG_ID_BLOCK_SIZE, len(recipients)))
    try:
        return await allocator.allocate_for_recipients(recipients)
    except Exception:
        logger.exception("预分配 SendLog ID 失败 task_id=%s recipients=%s", task_id, len(recipients))
        return {}
