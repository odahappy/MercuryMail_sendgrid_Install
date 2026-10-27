"""SenderPool：常驻事件循环线程池，专责执行 SendGrid SDK 同步调用。"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Callable

logger = logging.getLogger(__name__)

try:
    from app.core.config import settings

    SENDER_POOL_MAX_WORKERS = max(64, min(int(getattr(settings, "sender_pool_max_workers", 256) or 256), 1024))
    SENDER_POOL_HEADROOM = max(8, int(getattr(settings, "sender_pool_headroom", 32) or 32))
except Exception:
    SENDER_POOL_MAX_WORKERS = 256
    SENDER_POOL_HEADROOM = 32
SENDER_POOL_QUEUE_MAXSIZE = 4096
SENDER_POOL_SUBMIT_TIMEOUT_SECONDS = 120.0
_total_active_sdk_inflight = 0
_total_active_sdk_inflight_lock = threading.Lock()
_delivery_unknown_futures: set[int] = set()
_delivery_unknown_lock = threading.Lock()
_delivery_unknown_total = 0


class SenderQueueTimeoutError(TimeoutError):
    """SenderPool 队列等待超时，请求尚未提交到 HTTP 发送阶段。"""


class DeliveryUnknownError(TimeoutError):
    """HTTP 未收到响应；不得按普通失败自动重试。"""

    def __init__(self, message: str, *, future_id: int) -> None:
        super().__init__(message)
        self.future_id = int(future_id)
        self.delivery_status = "delivery_unknown"


def _default_sender_pool_workers() -> int:
    try:
        from app.core.config import settings

        configured_workers = getattr(settings, "sender_pool_workers", None)
        if configured_workers not in (None, "", 0, "0"):
            return max(4, min(int(configured_workers), SENDER_POOL_MAX_WORKERS))
    except Exception:
        pass
    cpu_floor = (os.cpu_count() or 4) * 2
    return min(SENDER_POOL_MAX_WORKERS, max(64, cpu_floor))


def get_total_active_sdk_inflight() -> int:
    with _total_active_sdk_inflight_lock:
        return max(0, int(_total_active_sdk_inflight or 0))


def get_delivery_unknown_stats() -> dict[str, int]:
    with _delivery_unknown_lock:
        return {
            "delivery_unknown_active": len(_delivery_unknown_futures),
            "delivery_unknown_total": int(_delivery_unknown_total or 0),
        }


@contextmanager
def track_sdk_inflight():
    """兼容保留：并发槽位改由 SenderPool.run_sync 在底层完成后释放。"""
    yield


@dataclass
class _SenderWorkItem:
    fn: Callable[..., Any]
    args: tuple
    kwargs: dict
    future: Future
    submitted_at: float


class _SenderLoopWorker:
    def __init__(self, index: int) -> None:
        self.index = index
        self._queue: asyncio.Queue[_SenderWorkItem | None] = asyncio.Queue(maxsize=SENDER_POOL_QUEUE_MAXSIZE)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = threading.Event()
        self._stopped = threading.Event()
        self._inflight = 0
        self._completed = 0
        self._dropped = 0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"mercury-sender-{self.index}",
            daemon=True,
        )
        self._thread.start()
        if not self._started.wait(timeout=10.0):
            raise RuntimeError(f"SenderPool worker {self.index} 启动超时")

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        try:
            loop.run_until_complete(self._worker_loop())
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()
            self._stopped.set()

    async def _worker_loop(self) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            if item.future.cancelled():
                self._completed += 1
                self._queue.task_done()
                continue
            self._inflight += 1
            try:
                result = item.fn(*item.args, **item.kwargs)
                if not item.future.done():
                    item.future.set_result(result)
            except Exception as exc:
                if not item.future.done():
                    item.future.set_exception(exc)
            finally:
                self._inflight = max(0, self._inflight - 1)
                self._completed += 1
                self._queue.task_done()

    def submit(self, fn: Callable[..., Any], *args, **kwargs) -> Future:
        if self._loop is None or self._thread is None or not self._thread.is_alive():
            raise RuntimeError(f"SenderPool worker {self.index} 未运行")
        future: Future = Future()
        item = _SenderWorkItem(fn=fn, args=args, kwargs=kwargs, future=future, submitted_at=monotonic())
        try:
            self._loop.call_soon_threadsafe(self._enqueue_item, item)
        except RuntimeError as exc:
            future.set_exception(exc)
        return future

    def _enqueue_item(self, item: _SenderWorkItem) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._dropped += 1
            if not item.future.done():
                item.future.set_exception(RuntimeError("SenderPool 工作队列已满，发送请求被拒绝"))

    def load(self) -> int:
        return int(self._queue.qsize()) + int(self._inflight)

    def stats(self) -> dict[str, int]:
        return {
            "index": self.index,
            "queue_size": int(self._queue.qsize()),
            "inflight": int(self._inflight),
            "completed": int(self._completed),
            "dropped": int(self._dropped),
            "alive": int(self._thread.is_alive() if self._thread else False),
        }

    def shutdown(self) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._enqueue_shutdown_sentinel)
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning("SenderPool worker %s 关闭超时，线程仍存活 queue_size=%s inflight=%s", self.index, self._queue.qsize(), self._inflight)

    def _enqueue_shutdown_sentinel(self) -> None:
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            dropped = 0
            while self._queue.full():
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is not None and not item.future.done():
                    item.future.set_exception(RuntimeError("SenderPool 正在关闭，未执行的发送请求已取消"))
                self._queue.task_done()
                dropped += 1
            if dropped:
                self._dropped += dropped
                logger.warning("SenderPool worker %s 关闭时丢弃未执行请求 count=%s", self.index, dropped)
            self._queue.put_nowait(None)


class SenderPool:
    """轮询分发 SendGrid SDK 同步调用到多个常驻事件循环线程。"""

    def __init__(self, workers: int | None = None) -> None:
        default_workers = _default_sender_pool_workers()
        chosen_workers = default_workers if workers is None else int(workers)
        self._workers_count = max(1, min(chosen_workers, SENDER_POOL_MAX_WORKERS))
        self._workers = [_SenderLoopWorker(index) for index in range(self._workers_count)]
        self._rr_index = 0
        self._lock = threading.Lock()
        self._started = False
        self._slot_semaphore: asyncio.Semaphore | None = None
        self._slot_semaphore_loop: asyncio.AbstractEventLoop | None = None

    def _get_slot_semaphore(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        slot_count = max(1, int(self._workers_count))
        if self._slot_semaphore is None or self._slot_semaphore_loop is not loop:
            self._slot_semaphore = asyncio.Semaphore(slot_count)
            self._slot_semaphore_loop = loop
        return self._slot_semaphore

    def _increment_pool_inflight(self) -> None:
        global _total_active_sdk_inflight
        with _total_active_sdk_inflight_lock:
            _total_active_sdk_inflight += 1

    def _decrement_pool_inflight(self) -> None:
        global _total_active_sdk_inflight
        with _total_active_sdk_inflight_lock:
            _total_active_sdk_inflight = max(0, _total_active_sdk_inflight - 1)

    def _record_delivery_unknown(self, future_id: int, *, timeout_seconds: float) -> None:
        global _delivery_unknown_total
        with _delivery_unknown_lock:
            normalized_future_id = int(future_id)
            is_new_unknown = normalized_future_id not in _delivery_unknown_futures
            _delivery_unknown_futures.add(normalized_future_id)
            if is_new_unknown:
                _delivery_unknown_total += 1
        logger.warning(
            "SenderPool 同步调用超时 %.1fs，投递结果未知，槽位保留至底层 SDK 完成 future_id=%s active_inflight=%s",
            float(timeout_seconds),
            future_id,
            get_total_active_sdk_inflight(),
        )

    def _finalize_slot_release(
        self,
        loop: asyncio.AbstractEventLoop,
        semaphore: asyncio.Semaphore,
        future_id: int,
        concurrent_future: Future,
    ) -> None:
        def _release_on_loop() -> None:
            try:
                semaphore.release()
            finally:
                self._decrement_pool_inflight()
                was_unknown = False
                with _delivery_unknown_lock:
                    was_unknown = int(future_id) in _delivery_unknown_futures
                    _delivery_unknown_futures.discard(int(future_id))
                if was_unknown:
                    late_error: BaseException | None = None
                    if concurrent_future.done() and not concurrent_future.cancelled():
                        try:
                            concurrent_future.result()
                        except Exception as exc:
                            late_error = exc
                    logger.warning(
                        "SenderPool 投递结果未知请求已在底层线程结束，槽位已释放 future_id=%s late_error=%s",
                        future_id,
                        late_error,
                    )

        try:
            loop.call_soon_threadsafe(_release_on_loop)
        except RuntimeError:
            self._decrement_pool_inflight()
            with _delivery_unknown_lock:
                _delivery_unknown_futures.discard(int(future_id))
            logger.warning(
                "SenderPool 所属事件循环已关闭，已直接修正底层完成计数 future_id=%s",
                future_id,
            )

    def _release_slot_immediately(self, loop: asyncio.AbstractEventLoop, semaphore: asyncio.Semaphore) -> None:
        def _release_on_loop() -> None:
            try:
                semaphore.release()
            finally:
                self._decrement_pool_inflight()

        loop.call_soon_threadsafe(_release_on_loop)

    def ensure_started(self) -> None:
        with self._lock:
            if self._started:
                return
            for worker in self._workers:
                worker.start()
            self._started = True
            logger.info("SenderPool 已启动 workers=%s", self._workers_count)

    def resize(self, workers: int) -> None:
        target = max(1, min(int(workers), SENDER_POOL_MAX_WORKERS))
        workers_to_start: list[_SenderLoopWorker] = []
        workers_to_shutdown: list[_SenderLoopWorker] = []
        previous_count = self._workers_count
        with self._lock:
            if target == self._workers_count and self._started:
                return
            if target > self._workers_count:
                start_index = len(self._workers)
                workers_to_start = [_SenderLoopWorker(index) for index in range(start_index, target)]
                self._workers.extend(workers_to_start)
                self._workers_count = target
            else:
                workers_to_shutdown = self._workers[target:]
                self._workers = self._workers[:target]
                self._workers_count = target
                self._rr_index = self._rr_index % max(1, target)
            started = self._started
        if started:
            for worker in workers_to_start:
                worker.start()
            for worker in workers_to_shutdown:
                worker.shutdown()
        else:
            self.ensure_started()
        if target > previous_count and self._slot_semaphore is not None:
            for _ in range(target - previous_count):
                self._slot_semaphore.release()
        elif target < previous_count and get_total_active_sdk_inflight() <= 0:
            self._slot_semaphore = None
            self._slot_semaphore_loop = None
        logger.info("SenderPool 已调整容量 workers=%s", target)

    def _pick_worker(self) -> _SenderLoopWorker:
        with self._lock:
            worker_count = len(self._workers)
            if worker_count <= 0:
                raise RuntimeError("SenderPool 未初始化 worker")
            for _ in range(worker_count):
                worker = self._workers[self._rr_index % worker_count]
                self._rr_index += 1
                if worker._thread is not None and worker._thread.is_alive():
                    return worker
            return self._workers[(self._rr_index - 1) % worker_count]

    async def run_sync(
        self,
        fn: Callable[..., Any],
        *args,
        timeout: float | None = None,
        queue_timeout: float | None = None,
        queue_wait_observer: Callable[[float], None] | None = None,
        **kwargs,
    ) -> Any:
        """执行同步 SDK 调用；队列预算与 HTTP 预算分别受控，HTTP 超时保留槽位。"""
        self.ensure_started()
        loop = asyncio.get_running_loop()
        semaphore = self._get_slot_semaphore()
        queue_started_at = monotonic()
        if queue_timeout is None:
            await semaphore.acquire()
        else:
            try:
                await asyncio.wait_for(semaphore.acquire(), timeout=float(queue_timeout))
            except asyncio.TimeoutError as exc:
                raise SenderQueueTimeoutError(
                    f"SenderPool 队列等待超时（超过 {float(queue_timeout):.1f} 秒），HTTP 请求尚未发出"
                ) from exc
        if queue_wait_observer is not None:
            queue_wait_observer(max(0.0, (monotonic() - queue_started_at) * 1000.0))
        self._increment_pool_inflight()
        worker = self._pick_worker()
        try:
            concurrent_future = worker.submit(fn, *args, **kwargs)
        except Exception:
            self._release_slot_immediately(loop, semaphore)
            raise
        future_id = id(concurrent_future)
        concurrent_future.add_done_callback(
            lambda completed_future: self._finalize_slot_release(loop, semaphore, future_id, completed_future)
        )
        async_future = asyncio.wrap_future(concurrent_future)
        shielded_future = asyncio.shield(async_future)
        if timeout is None:
            return await shielded_future
        try:
            return await asyncio.wait_for(shielded_future, timeout=float(timeout))
        except asyncio.TimeoutError as exc:
            # DeliveryUnknownError 继承 TimeoutError；若底层 Future 已结束，优先透传其真实结果，
            # 避免把传输层协议错误误记成 SenderPool 自身超时并留下过期 unknown 计数。
            if concurrent_future.done() and not concurrent_future.cancelled():
                return concurrent_future.result()
            self._record_delivery_unknown(future_id, timeout_seconds=float(timeout))
            raise DeliveryUnknownError(
                f"SenderPool HTTP 请求超时（超过 {float(timeout):.1f} 秒），投递状态未知，需人工确认，禁止自动重试",
                future_id=future_id,
            ) from exc
        except asyncio.CancelledError:
            if not concurrent_future.done():
                self._record_delivery_unknown(future_id, timeout_seconds=float(timeout))
                logger.warning(
                    "SenderPool 调用协程被取消但底层 HTTP 仍在执行，已标记投递状态未知并保留槽位 future_id=%s",
                    future_id,
                )
            raise

    def worker_stats(self) -> list[dict[str, int]]:
        return [worker.stats() for worker in self._workers]

    def aggregate_stats(self) -> dict[str, int | float]:
        stats = self.worker_stats()
        queue_total = sum(int(item.get("queue_size", 0)) for item in stats)
        inflight_total = sum(int(item.get("inflight", 0)) for item in stats)
        dropped_total = sum(int(item.get("dropped", 0)) for item in stats)
        alive_total = sum(int(item.get("alive", 0)) for item in stats)
        unknown_stats = get_delivery_unknown_stats()
        return {
            "workers": len(stats),
            "alive_workers": alive_total,
            "queue_depth": queue_total,
            "inflight": inflight_total,
            "active_sdk_inflight": get_total_active_sdk_inflight(),
            "dropped": dropped_total,
            **unknown_stats,
        }

    def shutdown(self) -> None:
        with self._lock:
            for worker in self._workers:
                worker.shutdown()
            self._started = False


_sender_pool: SenderPool | None = None
_task_sender_pools: dict[int, SenderPool] = {}
_sender_pool_lock = threading.Lock()


def get_sender_pool(task_id: int | None = None, *, workers: int | None = None) -> SenderPool:
    """返回任务独立的 SDK 执行池；无任务标识的兼容调用使用低容量公共池。"""
    global _sender_pool
    normalized_task_id = max(0, int(task_id or 0))
    with _sender_pool_lock:
        if normalized_task_id > 0:
            pool = _task_sender_pools.get(normalized_task_id)
            if pool is None:
                chosen_workers = max(1, min(int(workers or 1), SENDER_POOL_MAX_WORKERS))
                pool = SenderPool(workers=chosen_workers)
                _task_sender_pools[normalized_task_id] = pool
                logger.info("任务 %s 独立 SenderPool 已创建 workers=%s", normalized_task_id, chosen_workers)
            return pool
        if _sender_pool is None:
            _sender_pool = SenderPool(workers=max(1, min(int(workers or 4), SENDER_POOL_MAX_WORKERS)))
        return _sender_pool


def ensure_sender_pool_capacity(min_workers: int = 0, *, task_id: int | None = None) -> None:
    target = max(1, min(int(min_workers or 1), SENDER_POOL_MAX_WORKERS))
    pool = get_sender_pool(task_id, workers=target)
    if target > pool._workers_count:
        pool.resize(target)
    else:
        pool.ensure_started()


def aggregate_sender_pool_stats() -> dict[str, int | float]:
    with _sender_pool_lock:
        pools = list(_task_sender_pools.values()) + ([_sender_pool] if _sender_pool is not None else [])
    snapshots = [pool.aggregate_stats() for pool in pools]
    return {
        "pools": len(snapshots),
        "workers": sum(int(item.get("workers", 0)) for item in snapshots),
        "alive_workers": sum(int(item.get("alive_workers", 0)) for item in snapshots),
        "queue_depth": sum(int(item.get("queue_depth", 0)) for item in snapshots),
        "inflight": sum(int(item.get("inflight", 0)) for item in snapshots),
        "active_sdk_inflight": get_total_active_sdk_inflight(),
        "dropped": sum(int(item.get("dropped", 0)) for item in snapshots),
        **get_delivery_unknown_stats(),
    }


def shutdown_task_sender_pool(task_id: int) -> None:
    normalized_task_id = max(0, int(task_id or 0))
    if normalized_task_id <= 0:
        return
    with _sender_pool_lock:
        pool = _task_sender_pools.pop(normalized_task_id, None)
    if pool is not None:
        pool.shutdown()
        logger.info("任务 %s 独立 SenderPool 已关闭", normalized_task_id)


def shutdown_sender_pool() -> None:
    global _sender_pool
    with _sender_pool_lock:
        pools = list(_task_sender_pools.values())
        _task_sender_pools.clear()
        shared_pool = _sender_pool
        _sender_pool = None
    for pool in pools:
        pool.shutdown()
    if shared_pool is not None:
        shared_pool.shutdown()
