"""面向用户的操作审计：导出类请求永久豁免，不写入任何操作日志。"""

from __future__ import annotations

import re
from typing import Any

from starlette.requests import Request

# 导出操作永不记录面向用户的操作日志（用户、时间、参数等均不留痕）
EXPORT_OPERATION_LOG_SKIP = True

_EXPORT_PATH_PREFIXES = (
    "/api/export/",
)

_EXPORT_PATH_PATTERNS = (
    re.compile(r"^/api/tasks/\d+/export/(?:success|failed|unset)$"),
)


def is_export_operation_path(path: str) -> bool:
    normalized = str(path or "").split("?", 1)[0].rstrip("/") or "/"
    for prefix in _EXPORT_PATH_PREFIXES:
        if normalized.startswith(prefix):
            return True
    for pattern in _EXPORT_PATH_PATTERNS:
        if pattern.match(normalized):
            return True
    return False


def should_skip_user_operation_log(request: Request | None = None, *, path: str | None = None) -> bool:
    if not EXPORT_OPERATION_LOG_SKIP:
        return False
    if request is not None:
        if bool(getattr(request.state, "skip_operation_audit", False)):
            return True
        if bool(getattr(request.state, "log_export", True) is False):
            return True
        path = request.url.path
    return is_export_operation_path(path or "")


def mark_export_operation_request(request: Request) -> None:
    request.state.skip_operation_audit = True
    request.state.log_export = False


async def record_user_operation_log(
    request: Request,
    *,
    action_category: str,
    action_label: str,
    username: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """写入面向用户的操作日志；导出请求直接跳过。"""
    if should_skip_user_operation_log(request):
        return
    _ = (action_category, action_label, username, extra)
    # 当前版本无持久化操作日志表；若后续接入审计存储，须继续遵守导出豁免。
