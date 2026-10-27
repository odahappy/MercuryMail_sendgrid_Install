"""任务生命周期状态中文标签（API 展示用，不改变 status 枚举值）。"""

from __future__ import annotations

TASK_STATUS_LABELS: dict[str, str] = {
    "draft": "草稿",
    "scheduled": "等待中",
    "pending": "等待中",
    "running": "运行中",
    "paused": "已暂停",
    "finished": "已完成",
    "completed": "已完成",
    "stopped": "已终止",
    "aborted": "已终止",
    "failed": "已失败",
    "cancelled": "已取消",
}


def resolve_task_status_label(status: str | None) -> str:
    normalized = str(status or "").strip().lower()
    return TASK_STATUS_LABELS.get(normalized, "未知")
