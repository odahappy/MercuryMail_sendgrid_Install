from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from time import monotonic
from typing import Callable


@dataclass
class RealtimeRateTracker:
    """按 SDK 完成时间统计成功邮件，并输出每秒更新的平滑实时速率。"""

    window_seconds: float = 5.0
    half_life_seconds: float = 2.0
    retention_seconds: float = 15.0
    clock: Callable[[], float] = monotonic
    _success_events: deque[tuple[float, int]] = field(default_factory=deque, init=False, repr=False)
    _processed_events: deque[tuple[float, int]] = field(default_factory=deque, init=False, repr=False)
    _ema_success_rate: float = field(default=0.0, init=False, repr=False)
    _ema_processed_rate: float = field(default=0.0, init=False, repr=False)
    _last_sample_at: float | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.window_seconds = max(1.0, float(self.window_seconds))
        self.half_life_seconds = max(0.1, float(self.half_life_seconds))
        self.retention_seconds = max(self.window_seconds, float(self.retention_seconds))

    def record(self, *, success_count: int = 0, processed_count: int = 0, at: float | None = None) -> None:
        now_value = self._now(at)
        normalized_success = max(0, int(success_count or 0))
        normalized_processed = max(normalized_success, int(processed_count or 0))
        if normalized_success:
            self._success_events.append((now_value, normalized_success))
        if normalized_processed:
            self._processed_events.append((now_value, normalized_processed))
        self._prune(now_value)

    def sample(self, *, at: float | None = None) -> dict[str, float | int]:
        now_value = self._now(at)
        self._prune(now_value)
        success_count = self._count_since(self._success_events, now_value - self.window_seconds)
        processed_count = self._count_since(self._processed_events, now_value - self.window_seconds)
        raw_success_rate = success_count / self.window_seconds
        raw_processed_rate = processed_count / self.window_seconds
        if self._last_sample_at is None:
            self._ema_success_rate = raw_success_rate
            self._ema_processed_rate = raw_processed_rate
        else:
            elapsed = max(0.0, now_value - self._last_sample_at)
            alpha = 1.0 - math.exp(-math.log(2.0) * elapsed / self.half_life_seconds)
            self._ema_success_rate += alpha * (raw_success_rate - self._ema_success_rate)
            self._ema_processed_rate += alpha * (raw_processed_rate - self._ema_processed_rate)
        self._last_sample_at = now_value
        return {
            "success_rate": round(max(0.0, self._ema_success_rate), 2),
            "processed_rate": round(max(0.0, self._ema_processed_rate), 2),
            "raw_success_rate": round(max(0.0, raw_success_rate), 2),
            "raw_processed_rate": round(max(0.0, raw_processed_rate), 2),
            "success_count": success_count,
            "processed_count": processed_count,
            "window_seconds": round(self.window_seconds, 2),
            "sampled_at": round(now_value, 3),
        }

    def _now(self, at: float | None) -> float:
        return float(self.clock() if at is None else at)

    def _prune(self, now_value: float) -> None:
        cutoff = now_value - self.retention_seconds
        for events in (self._success_events, self._processed_events):
            while events and events[0][0] < cutoff:
                events.popleft()

    @staticmethod
    def _count_since(events: deque[tuple[float, int]], cutoff: float) -> int:
        return sum(count for timestamp, count in events if timestamp >= cutoff)
