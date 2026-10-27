"""全局性能监控聚合：并发水位、任务速率对比、连接池与 PID 状态。"""

from __future__ import annotations

import logging
from time import monotonic
from typing import Any

logger = logging.getLogger(__name__)

_perf_history: list[dict[str, Any]] = []
_PERF_HISTORY_MAX = 300


def record_global_perf_snapshot(payload: dict[str, Any]) -> None:
    entry = dict(payload)
    entry["ts"] = monotonic()
    _perf_history.append(entry)
    if len(_perf_history) > _PERF_HISTORY_MAX:
        del _perf_history[: len(_perf_history) - _PERF_HISTORY_MAX]


def recent_global_perf(limit: int = 60) -> list[dict[str, Any]]:
    return list(_perf_history)[-max(1, int(limit)) :]


def build_task_rate_comparison(running_task_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items = []
    for item in running_task_metrics:
        target = float(item.get("rate_limit_per_second") or item.get("target_rate") or 0)
        actual = float(item.get("current_rate") or item.get("success_rate") or 0)
        ratio = round((actual / target) * 100, 1) if target > 0 else 0.0
        items.append(
            {
                "task_id": int(item.get("task_id") or 0),
                "task_name": str(item.get("task_name") or ""),
                "target_rate": round(target, 2),
                "actual_rate": round(actual, 2),
                "rate_ratio_percent": ratio,
                "active_workers": int(item.get("active_workers") or 0),
                "alert": actual < target * 0.8 if target > 0 else False,
            }
        )
    return items
