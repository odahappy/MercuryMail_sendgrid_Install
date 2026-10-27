"""垃圾回收调优：调整分代阈值，减少大对象扫描导致的间歇性暂停。"""

from __future__ import annotations

import gc
import logging

logger = logging.getLogger(__name__)

_gc_tuned = False
_original_thresholds: tuple[int, int, int] | None = None


def configure_gc_thresholds(
    generation0: int = 50_000,
    generation1: int = 20,
    generation2: int = 20,
) -> None:
    global _gc_tuned, _original_thresholds
    if _gc_tuned:
        return
    _original_thresholds = gc.get_threshold()
    gc.set_threshold(int(generation0), int(generation1), int(generation2))
    _gc_tuned = True
    logger.info(
        "GC 分代阈值已调整：%s -> (%s, %s, %s)",
        _original_thresholds,
        generation0,
        generation1,
        generation2,
    )


def restore_gc_thresholds() -> None:
    global _gc_tuned, _original_thresholds
    if not _gc_tuned or _original_thresholds is None:
        return
    gc.set_threshold(*_original_thresholds)
    _gc_tuned = False
    logger.info("GC 分代阈值已恢复：%s", _original_thresholds)
