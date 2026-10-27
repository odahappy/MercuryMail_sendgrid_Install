"""自适应背压：监控预取/写入队列深度与内存占用，动态调节预取批次与告警。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from time import monotonic

logger = logging.getLogger(__name__)

BACKPRESSURE_CHECK_INTERVAL_SECONDS = 10.0
BACKPRESSURE_PREFETCH_LOW_WATERMARK = 10
BACKPRESSURE_WRITE_HIGH_WATERMARK = 100_000
BACKPRESSURE_WRITE_PAUSE_WATERMARK = 300_000
BACKPRESSURE_MEMORY_WARN_PERCENT = 92.0
BACKPRESSURE_MEMORY_REDUCE_PERCENT = 88.0
BACKPRESSURE_PREFETCH_BOOST_RATIO = 1.5
BACKPRESSURE_PREFETCH_REDUCE_RATIO = 0.6


@dataclass
class BackpressureState:
    prefetch_queue_depth: int = 0
    write_queue_depth: int = 0
    memory_percent: float = 0.0
    prefetch_batch_multiplier: float = 1.0
    write_paused: bool = False
    alert_message: str = ""
    last_check_at: float = 0.0


def sample_memory_percent() -> float:
    try:
        import psutil

        return float(psutil.virtual_memory().percent)
    except Exception:
        return 0.0


def evaluate_backpressure(
    *,
    prefetch_depth: int,
    write_depth: int,
    memory_percent: float | None = None,
    base_prefetch_batch: int,
    previous: BackpressureState | None = None,
) -> BackpressureState:
    now_value = monotonic()
    mem = float(memory_percent if memory_percent is not None else sample_memory_percent())
    multiplier = float(previous.prefetch_batch_multiplier if previous else 1.0)
    write_paused = False
    alert = ""

    if write_depth >= BACKPRESSURE_WRITE_PAUSE_WATERMARK:
        write_paused = True
        alert = f"写入队列深度 {write_depth} 超过暂停阈值 {BACKPRESSURE_WRITE_PAUSE_WATERMARK}，已暂停接受新日志"
        logger.error("背压告警：%s", alert)
    elif write_depth >= BACKPRESSURE_WRITE_HIGH_WATERMARK:
        alert = f"写入队列深度 {write_depth} 超过告警阈值 {BACKPRESSURE_WRITE_HIGH_WATERMARK}"
        logger.warning("背压告警：%s", alert)

    if prefetch_depth < BACKPRESSURE_PREFETCH_LOW_WATERMARK and not write_paused:
        multiplier = min(3.0, multiplier * BACKPRESSURE_PREFETCH_BOOST_RATIO)
    elif prefetch_depth > base_prefetch_batch * 4:
        multiplier = max(0.5, multiplier * BACKPRESSURE_PREFETCH_REDUCE_RATIO)

    if mem >= BACKPRESSURE_MEMORY_WARN_PERCENT:
        multiplier = max(0.4, multiplier * BACKPRESSURE_PREFETCH_REDUCE_RATIO)
        mem_alert = f"内存占用 {mem:.1f}% 超过阈值 {BACKPRESSURE_MEMORY_WARN_PERCENT:.0f}%"
        alert = f"{alert}；{mem_alert}" if alert else mem_alert
        logger.warning("背压内存告警：%s", mem_alert)
    elif mem >= BACKPRESSURE_MEMORY_REDUCE_PERCENT:
        multiplier = max(0.6, multiplier * BACKPRESSURE_PREFETCH_REDUCE_RATIO)

    return BackpressureState(
        prefetch_queue_depth=int(prefetch_depth),
        write_queue_depth=int(write_depth),
        memory_percent=round(mem, 1),
        prefetch_batch_multiplier=round(multiplier, 3),
        write_paused=write_paused,
        alert_message=alert,
        last_check_at=now_value,
    )


def adjusted_prefetch_batch(base_batch: int, state: BackpressureState) -> int:
    base = max(1, int(base_batch))
    return max(1, int(base * max(0.4, min(3.0, float(state.prefetch_batch_multiplier)))))
