import logging

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field

from app.api.deps import require_admin
from app.utils.task_status import resolve_task_status_label
from app.services.dashboard_stats_service import (
    DashboardServiceBusyError,
    build_dashboard_stats,
    empty_dashboard_stats,
    merge_dashboard_stats_payload,
)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])
logger = logging.getLogger(__name__)

DASHBOARD_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
}


class RecentTaskOut(BaseModel):
    id: int
    name: str
    status: str
    status_label: str = "未知"
    sent_count: int = 0
    failed_count: int = 0
    total_count: int = 0


class OverviewResponse(BaseModel):
    total_tasks: int
    running_tasks: int
    total_recipients: int
    sent_count: int
    failed_count: int
    global_rate: float
    channel_health: str
    proxy_health: str
    recent_tasks: list[RecentTaskOut]


class ChannelHealthItem(BaseModel):
    id: int
    name: str
    online: bool
    latency_ms: int = 0


class ProxyHealthItem(BaseModel):
    id: int
    proxy: str
    online: bool
    latency_ms: int = 0


class EventPollerAccountItem(BaseModel):
    api_key_hash: str = ""
    channel_id: int
    channel_name: str
    sender_email: str = ""
    success_count: int = 0
    failure_count: int = 0
    last_success_at: str | None = None
    last_poll_at: str | None = None
    last_error: str = ""
    paused: bool = False
    paused_reason: str = ""
    last_event_count: int = 0
    last_updated_count: int = 0
    last_unmatched_count: int = 0
    last_raw_event_count: int = 0
    last_selected_event_count: int = 0
    last_updated_row_count: int = 0
    last_attempted_max_event_time: str | None = None
    last_max_event_time: str | None = None
    last_cursor_advanced: bool = False
    last_apply_deferred: bool = False
    worker_alive: bool = False


class RecentTaskStatsItem(BaseModel):
    id: int
    name: str
    status: str
    status_label: str = "未知"
    current_rate: float = 0
    pending_count: int = 0
    progress: str = "0/0"
    delivered_count: int = 0
    total_count: int = 0


class DashboardStatsResponse(BaseModel):
    total_tasks: int
    running_tasks: int
    valid_recipients: int
    today_delivered: int
    today_api_sent: int
    today_quota: int
    hard_bounced_total: int
    soft_bounced_or_retrying: int
    global_current_rate: float
    global_rate_limit_sum: float
    global_max_theoretical_rate: float = 0
    global_rate_achievement_percent: float = 0
    recent_429_count: int
    avg_proxy_latency_ms: int = 0
    proxy_latency_max_ms: int = 0
    channels_health: list[ChannelHealthItem] = Field(default_factory=list)
    proxies_health: list[ProxyHealthItem] = Field(default_factory=list)
    cpu_percent: float = 0
    memory_percent: float = 0
    recent_tasks: list[RecentTaskStatsItem] = Field(default_factory=list)
    alert_all_channels_offline: bool = False
    alert_all_proxies_offline: bool = False
    alert_global_concurrency_rejected: bool = False
    alert_recent_429_burst: bool = False
    recent_concurrency_rejections: int = 0
    global_concurrency_occupied: int = 0
    global_concurrency_limit: int = 0
    task_rate_comparison: list[dict] = Field(default_factory=list)
    sender_pool_stats: dict = Field(default_factory=dict)
    event_poller_accounts: list[EventPollerAccountItem] = Field(default_factory=list)


def _apply_dashboard_no_cache_headers(response: Response) -> None:
    for key, value in DASHBOARD_NO_CACHE_HEADERS.items():
        response.headers[key] = value


async def _resolve_dashboard_stats_payload() -> dict:
    """直接查询实时数据；繁忙时明确返回 503，禁止返回过期缓存。"""
    try:
        return merge_dashboard_stats_payload(await build_dashboard_stats())
    except DashboardServiceBusyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="服务繁忙，请稍后重试",
        ) from exc


@router.get("/stats", response_model=DashboardStatsResponse, dependencies=[Depends(require_admin)])
async def dashboard_stats(response: Response):
    _apply_dashboard_no_cache_headers(response)
    try:
        payload = await _resolve_dashboard_stats_payload()
        try:
            return DashboardStatsResponse(**payload)
        except Exception:
            logger.exception("仪表盘统计响应校验失败，返回空壳数据")
            return DashboardStatsResponse(**empty_dashboard_stats())
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("仪表盘统计接口异常 error=%s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="服务繁忙，请稍后重试",
        ) from exc


@router.get("/overview", response_model=OverviewResponse, dependencies=[Depends(require_admin)])
async def overview(response: Response):
    """兼容旧版仪表盘接口，映射到 final_status 统计。"""
    _apply_dashboard_no_cache_headers(response)
    try:
        stats = await _resolve_dashboard_stats_payload()
    except Exception as exc:
        logger.exception("仪表盘概览接口异常，返回空壳数据 error=%s", exc)
        stats = empty_dashboard_stats()
    channels_health = stats.get("channels_health") or []
    channel_health = "gray"
    if channels_health:
        online_count = sum(1 for item in channels_health if isinstance(item, dict) and item.get("online"))
        if online_count == len(channels_health):
            channel_health = "green"
        elif online_count > 0:
            channel_health = "yellow"
        else:
            channel_health = "red"
    proxies_health = stats.get("proxies_health") or []
    proxy_health = "gray"
    if proxies_health:
        online_count = sum(1 for item in proxies_health if isinstance(item, dict) and item.get("online"))
        if online_count == len(proxies_health):
            proxy_health = "green"
        elif online_count > 0:
            proxy_health = "yellow"
        else:
            proxy_health = "red"
    recent_tasks_raw = stats.get("recent_tasks") or []
    recent_tasks = []
    for item in recent_tasks_raw:
        if not isinstance(item, dict):
            continue
        recent_tasks.append(
            RecentTaskOut(
                id=int(item.get("id", 0) or 0),
                name=str(item.get("name") or ""),
                status=str(item.get("status") or ""),
                status_label=str(item.get("status_label") or resolve_task_status_label(item.get("status"))),
                sent_count=int(item.get("delivered_count", 0) or 0),
                failed_count=int(item.get("failed_count", 0) or 0),
                total_count=int(item.get("total_count", 0) or 0),
            )
        )
    return OverviewResponse(
        total_tasks=int(stats.get("total_tasks", 0) or 0),
        running_tasks=int(stats.get("running_tasks", 0) or 0),
        total_recipients=int(stats.get("valid_recipients", 0) or 0),
        sent_count=int(stats.get("today_delivered", 0) or 0),
        failed_count=int(stats.get("hard_bounced_total", 0) or 0) + int(stats.get("soft_bounced_or_retrying", 0) or 0),
        global_rate=float(stats.get("global_current_rate", 0) or 0),
        channel_health=channel_health,
        proxy_health=proxy_health,
        recent_tasks=recent_tasks,
    )
