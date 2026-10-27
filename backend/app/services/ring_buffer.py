"""无锁环形缓冲区：SendLog 写入热路径快速入队，后台协程批量消费。"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any


class SendLogRingBuffer:
    """固定容量环形缓冲；满时拒绝新记录并计数，由上层决定是否走备用队列或告警。"""

    def __init__(self, capacity: int = 65536) -> None:
        self._capacity = max(1024, int(capacity))
        self._buffer: deque[dict[str, Any]] = deque(maxlen=self._capacity)
        self._dropped = 0
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._buffer)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def dropped_total(self) -> int:
        return int(self._dropped)

    def put_nowait(self, record: dict[str, Any]) -> bool:
        if len(self._buffer) >= self._capacity:
            self._dropped += 1
            return False
        self._buffer.append(record)
        return True

    async def put(self, record: dict[str, Any]) -> bool:
        async with self._lock:
            return self.put_nowait(record)

    async def drain(self, max_items: int) -> list[dict[str, Any]]:
        limit = max(1, int(max_items))
        async with self._lock:
            batch: list[dict[str, Any]] = []
            while self._buffer and len(batch) < limit:
                batch.append(self._buffer.popleft())
            return batch

    async def drain_all(self) -> list[dict[str, Any]]:
        async with self._lock:
            batch = list(self._buffer)
            self._buffer.clear()
            return batch
