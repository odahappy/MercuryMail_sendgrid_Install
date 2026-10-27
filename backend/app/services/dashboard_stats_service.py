"""仪表盘全局统计：汇总发送任务、通道、代理和系统资源状态。"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict

from sqlalchemy import func, select, text, and_
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import (
    DashboardSessionLocal,
    POSTGRES_MAX_CONNECTIONS_BUDGET,
    dashboard_engine,
)
from app.models.models import Channel, Proxy, Task, TaskStatsSnapshot
from app.services.channel_manager import get_sender_pool_stats, mask_proxy_for_log
from app.utils.task_status import resolve_task_status_label
from app.services.health import channel_health_disabled_key
from app.services.performance_monitor import build_task_rate_comparison, record_global_perf_snapshot
from app.services.task_runner import (
    _running_tasks,
    count_recent_429_events,
    get_active_http_inflight_total,
    get_admitted_workers_total,
    get_executing_tasks_total,
    get_mutable_task_runtime,
    get_task_runtime_metrics,
    is_task_runner_active,
)

logger = logging.getLogger(__name__)

DASHBOARD_STATS_BUILD_TIMEOUT_SECONDS = 15.0
DASHBOARD_TASK_STATS_CACHE_TTL_SECONDS = 10.0
DASHBOARD_TASK_STATS_ACTIVE_SEND_CACHE_TTL_SECONDS = 30.0
DASHBOARD_SYSTEM_STATS_CACHE_TTL_SECONDS = 30.0
DASHBOARD_CHANNEL_LATENCY_CACHE_MIN_SECONDS = 5.0
GLOBAL_429_WINDOW_SECONDS = 900.0

# 终态任务不进入「最近任务」列表（已删除任务由 deleted_at 过滤）。
TERMINAL_TASK_STATUSES = frozenset(
    {"finished", "completed", "stopped", "aborted", "failed", "cancelled"}
)


# 降级/陈旧缓存时也必须实时刷新的任务相关字段（系统级指标不在此列）。
TASK_RELATED_DASHBOARD_KEYS = frozenset(
    {
        "total_tasks",
        "running_tasks",
        "valid_recipients",
        "today_delivered",
        "today_api_sent",
        "hard_bounced_total",
        "soft_bounced_or_retrying",
        "global_current_rate",
        "global_rate_limit_sum",
        "global_max_theoretical_rate",
        "recent_429_count",
        "recent_tasks",
        "task_rate_comparison",
        "alert_recent_429_burst",
    }
)

_REMOVED_VALID_RECIPIENTS_SQL = text(
    """
    /* 性能建议：该聚合可能扫描 recipients/send_logs/send_log_archive 大表。
       推荐索引：
       CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_dashboard_valid_email ON recipients (list_id, LOWER(BTRIM(email))) WHERE is_valid = TRUE AND COALESCE(is_unsubscribed, FALSE) = FALSE AND email IS NOT NULL AND BTRIM(email) <> '';
       CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_dashboard_regular_final_email ON send_logs (task_id, LOWER(BTRIM(email)), final_status, bounce_type) WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> '';
       CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_log_archive_dashboard_regular_final_email ON send_log_archive (task_id, LOWER(BTRIM(email)), final_status, bounce_type) WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> '';
    */
    WITH active_recipient_emails AS (
        SELECT DISTINCT LOWER(BTRIM(r.email)) AS email
        FROM recipients AS r
        WHERE r.is_valid IS TRUE
          AND COALESCE(r.is_unsubscribed, FALSE) = FALSE
          AND r.email IS NOT NULL
          AND BTRIM(r.email) <> ''
          AND EXISTS (
              SELECT 1
              FROM tasks AS active_task
              WHERE active_task.recipient_list_id = r.list_id
                AND active_task.deleted_at IS NULL
          )
    ), hard_failed_emails AS (
        SELECT DISTINCT email
        FROM (
            SELECT
                LOWER(BTRIM(sl.email)) AS email,
                CASE
                    WHEN LOWER(COALESCE(BTRIM(sl.final_status), '')) IN ('bounce', 'bounced') THEN 'bounced'
                    WHEN LOWER(COALESCE(BTRIM(sl.final_status), '')) IN ('spamreport', 'spam_report', 'spamreported') THEN 'spamreported'
                    ELSE LOWER(COALESCE(BTRIM(sl.final_status), ''))
                END AS final_status,
                LOWER(COALESCE(BTRIM(sl.bounce_type), '')) AS bounce_type
            FROM send_logs AS sl
            WHERE COALESCE(sl.log_type, 'regular') = 'regular'
              AND sl.email IS NOT NULL
              AND BTRIM(sl.email) <> ''
              AND sl.task_id IS NOT NULL
              AND EXISTS (SELECT 1 FROM tasks AS active_task WHERE active_task.id = sl.task_id AND active_task.deleted_at IS NULL)
            UNION ALL
            SELECT
                LOWER(BTRIM(sla.email)) AS email,
                CASE
                    WHEN LOWER(COALESCE(BTRIM(sla.final_status), '')) IN ('bounce', 'bounced') THEN 'bounced'
                    WHEN LOWER(COALESCE(BTRIM(sla.final_status), '')) IN ('spamreport', 'spam_report', 'spamreported') THEN 'spamreported'
                    ELSE LOWER(COALESCE(BTRIM(sla.final_status), ''))
                END AS final_status,
                LOWER(COALESCE(BTRIM(sla.bounce_type), '')) AS bounce_type
            FROM send_log_archive AS sla
            WHERE COALESCE(sla.log_type, 'regular') = 'regular'
              AND sla.email IS NOT NULL
              AND BTRIM(sla.email) <> ''
              AND sla.task_id IS NOT NULL
              AND EXISTS (SELECT 1 FROM tasks AS active_task WHERE active_task.id = sla.task_id AND active_task.deleted_at IS NULL)
        ) AS normalized_logs
        WHERE final_status IN ('dropped', 'spamreported', 'unsubscribed')
           OR (final_status = 'bounced' AND bounce_type NOT IN ('soft', 'blocked', 'deferred', 'transient'))
    )
    SELECT COUNT(*)::int AS total
    FROM active_recipient_emails AS candidate
    WHERE NOT EXISTS (
        SELECT 1
        FROM unsubscribes AS u
        WHERE u.email IS NOT NULL
          AND BTRIM(u.email) <> ''
          AND LOWER(BTRIM(u.email)) = candidate.email
    )
      AND NOT EXISTS (
          SELECT 1
          FROM hard_failed_emails AS failed
          WHERE failed.email = candidate.email
      )
    """
)

_system_resources_cache = {"cpu_percent": 0.0, "memory_percent": 0.0, "updated_at": 0.0}
_dashboard_task_stats_cache: dict[str, object] = {"payload": None, "channel_latency_samples": {}, "updated_at": 0.0}
_dashboard_system_stats_cache: dict[str, object] = {"payload": None, "updated_at": 0.0}
_dashboard_task_stats_lock = asyncio.Lock()
_dashboard_system_stats_lock = asyncio.Lock()
_system_resources_sampler_task: asyncio.Task | None = None
_dashboard_degrade_log_samples: dict[str, tuple[int, float]] = {}


def _active_tasks_filter():
    """仅统计未软删除任务。"""
    return Task.deleted_at.is_(None)


def _recent_tasks_filter():
    """最近任务：未软删除且非终态。"""
    return and_(
        _active_tasks_filter(),
        func.lower(func.coalesce(Task.status, "draft")).notin_(tuple(TERMINAL_TASK_STATUSES)),
    )


def resolve_dashboard_today_quota() -> int:
    """今日配额：0 表示未配置，仪表板隐藏配额卡片且不限制发送。"""
    return max(0, int(settings.sendgrid_daily_quota or 0))


def invalidate_dashboard_stats_cache() -> None:
    """保留兼容调用；仪表盘统计已不再使用进程内全量缓存。"""
    return None


def log_dashboard_task_metrics(payload: dict[str, object], *, source: str = "live") -> None:
    """记录任务相关仪表盘指标，便于排查删除任务后数据是否同步。"""
    recent_tasks = payload.get("recent_tasks") or []
    recent_count = len(recent_tasks) if isinstance(recent_tasks, list) else 0
    logger.info(
        "仪表盘任务指标[%s] total_tasks=%s running_tasks=%s recent_tasks=%s "
        "valid_recipients=%s today_delivered=%s soft_bounced=%s",
        source,
        payload.get("total_tasks"),
        payload.get("running_tasks"),
        recent_count,
        payload.get("valid_recipients"),
        payload.get("today_delivered"),
        payload.get("soft_bounced_or_retrying"),
    )


def empty_task_related_dashboard_stats() -> dict[str, object]:
    """任务相关指标的安全归零默认值（刷新失败时使用）。"""
    return {
        "total_tasks": 0,
        "running_tasks": 0,
        "valid_recipients": 0,
        "today_delivered": 0,
        "today_api_sent": 0,
        "hard_bounced_total": 0,
        "soft_bounced_or_retrying": 0,
        "global_current_rate": 0.0,
        "global_rate_limit_sum": 0.0,
        "global_max_theoretical_rate": 0.0,
        "global_rate_achievement_percent": 0.0,
        "recent_429_count": 0,
        "recent_tasks": [],
        "task_rate_comparison": [],
        "alert_recent_429_burst": False,
    }


def _is_query_cancelled_error(exc: BaseException) -> bool:
    text_value = str(exc).lower()
    return (
        "canceling statement due to user request" in text_value
        or "querycanceled" in text_value
        or "query cancelled" in text_value
        or "cancellederror" in text_value
        or "statement timeout" in text_value
    )


def _log_sampled_dashboard_warning(sample_key: str, message: str, *args) -> None:
    now_value = time.monotonic()
    suppressed_count, last_logged_at = _dashboard_degrade_log_samples.get(sample_key, (0, 0.0))
    suppressed_count += 1
    if now_value - last_logged_at >= 60.0:
        if suppressed_count > 1:
            logger.warning(message + " | suppressed_similar=%s", *args, suppressed_count)
        else:
            logger.warning(message, *args)
        _dashboard_degrade_log_samples[sample_key] = (0, now_value)
    else:
        _dashboard_degrade_log_samples[sample_key] = (suppressed_count, last_logged_at)


def _sample_system_resources_blocking() -> tuple[float, float]:
    try:
        import psutil

        return (
            round(float(psutil.cpu_percent(interval=0)), 1),
            round(float(psutil.virtual_memory().percent), 1),
        )
    except Exception:
        return 0.0, 0.0


async def _system_resources_sampler_loop() -> None:
    while True:
        try:
            cpu_percent, memory_percent = await asyncio.to_thread(_sample_system_resources_blocking)
            _system_resources_cache["cpu_percent"] = cpu_percent
            _system_resources_cache["memory_percent"] = memory_percent
            _system_resources_cache["updated_at"] = time.time()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("系统资源采样失败")
        await asyncio.sleep(1.0)


def _ensure_system_resources_sampler() -> None:
    global _system_resources_sampler_task
    if _system_resources_sampler_task is not None and not _system_resources_sampler_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _system_resources_sampler_task = loop.create_task(_system_resources_sampler_loop())


def get_cached_system_resources() -> tuple[float, float]:
    return (
        float(_system_resources_cache["cpu_percent"]),
        float(_system_resources_cache["memory_percent"]),
    )


def read_system_resources() -> tuple[float, float]:
    """兼容旧调用：优先返回后台采样缓存。"""
    if _system_resources_cache["updated_at"] > 0:
        return get_cached_system_resources()
    return _sample_system_resources_blocking()


def empty_dashboard_stats() -> dict[str, object]:
    """仪表盘统计降级空壳，避免接口返回超时错误。"""
    return {
        "total_tasks": 0,
        "running_tasks": 0,
        "valid_recipients": 0,
        "today_delivered": 0,
        "today_api_sent": 0,
        "today_quota": resolve_dashboard_today_quota(),
        "hard_bounced_total": 0,
        "soft_bounced_or_retrying": 0,
        "global_current_rate": 0.0,
        "global_rate_limit_sum": 0.0,
        "global_max_theoretical_rate": 0.0,
        "recent_429_count": 0,
        "avg_proxy_latency_ms": 0,
        "proxy_latency_max_ms": 0,
        "channels_health": [],
        "proxies_health": [],
        "cpu_percent": 0.0,
        "memory_percent": 0.0,
        "recent_tasks": [],
        "alert_all_channels_offline": False,
        "alert_all_proxies_offline": False,
        "alert_global_concurrency_rejected": False,
        "alert_recent_429_burst": False,
        "recent_concurrency_rejections": 0,
        "admitted_workers": 0,
        "active_http_inflight": 0,
        "executing_tasks": 0,
        "admitted_workers_limit": 0,
        "task_rate_comparison": [],
        "sender_pool_stats": {},
    }


def merge_dashboard_stats_payload(payload: dict | None) -> dict[str, object]:
    """将 Redis/陈旧缓存与默认空壳合并，避免缺字段导致前端或 Pydantic 校验失败。"""
    merged = empty_dashboard_stats()
    if not isinstance(payload, dict):
        return merged
    for key, default_value in merged.items():
        if key not in payload or payload[key] is None:
            continue
        value = payload[key]
        if isinstance(default_value, list):
            merged[key] = value if isinstance(value, list) else []
        elif isinstance(default_value, dict):
            merged[key] = value if isinstance(value, dict) else {}
        elif isinstance(default_value, bool):
            merged[key] = bool(value)
        elif isinstance(default_value, float):
            try:
                merged[key] = float(value)
            except (TypeError, ValueError):
                pass
        elif isinstance(default_value, int):
            try:
                merged[key] = int(value)
            except (TypeError, ValueError):
                pass
        else:
            merged[key] = value
    return merged


async def _fetch_system_resources() -> tuple[float, float]:
    _ensure_system_resources_sampler()
    cpu_percent, memory_percent = get_cached_system_resources()
    if _system_resources_cache["updated_at"] <= 0:
        cpu_percent, memory_percent = await asyncio.to_thread(_sample_system_resources_blocking)
    return cpu_percent, memory_percent


class DashboardServiceBusyError(RuntimeError):
    """实时仪表盘查询超时或独立连接池耗尽。"""


async def _configure_dashboard_transaction(session: AsyncSession) -> None:
    """设置只读查询预算；数据库使用率超过 80% 时降低到 5 秒。"""
    try:
        used_connections = int((await session.execute(text(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()"
        ))).scalar_one() or 0)
        busy = used_connections >= max(1, int(POSTGRES_MAX_CONNECTIONS_BUDGET * 0.8))
        timeout_ms = 5_000 if busy else 10_000
        if busy:
            _log_sampled_dashboard_warning(
                "dashboard_database_busy",
                "数据库繁忙，仪表盘查询可能延迟",
            )
        await session.execute(text(f"SET LOCAL statement_timeout = '{timeout_ms}ms'"))
        await session.execute(text("SET LOCAL default_transaction_read_only = on"))
    except Exception as exc:
        if _is_query_cancelled_error(exc):
            raise DashboardServiceBusyError("服务繁忙，请稍后重试") from exc
        raise


async def _run_dashboard_query(operation):
    try:
        async with asyncio.timeout(10.5):
            async with DashboardSessionLocal() as session:
                async with session.begin():
                    await _configure_dashboard_transaction(session)
                    return await operation(session)
    except (TimeoutError, OperationalError, DBAPIError) as exc:
        raise DashboardServiceBusyError("服务繁忙，请稍后重试") from exc


async def _fetch_total_tasks() -> int:
    async with DashboardSessionLocal() as session:
        return int(
            (
                await session.execute(
                    select(func.count()).select_from(Task).where(_active_tasks_filter())
                )
            ).scalar_one()
            or 0
        )


async def _fetch_running_tasks() -> list[Task]:
    async with DashboardSessionLocal() as session:
        rows = (
            await session.execute(
                select(Task)
                .where(Task.status == "running", _active_tasks_filter())
                .order_by(Task.updated_at.desc(), Task.id.desc())
            )
        ).scalars().all()
        return list(rows)


async def _fetch_task_stats_snapshots(task_ids: list[int] | None = None) -> list[TaskStatsSnapshot]:
    """只读取预计算快照表；缓存未命中时返回空，不回退扫描发送日志。"""
    try:
        async with DashboardSessionLocal() as session:
            query = select(TaskStatsSnapshot)
            if task_ids is not None:
                if not task_ids:
                    return []
                query = query.where(TaskStatsSnapshot.task_id.in_(task_ids))
            return list((await session.execute(query)).scalars().all())
    except Exception as exc:
        _log_sampled_dashboard_warning(
            "task_stats_snapshot_read_failed",
            "读取任务统计快照失败，已返回上一次缓存或空数据 error_type=%s",
            type(exc).__name__,
        )
        return []


async def _fetch_channels() -> list[Channel]:
    async with DashboardSessionLocal() as session:
        return list(
            (
                await session.execute(
                    select(Channel)
                    .where(Channel.is_active.is_(True), Channel.name != "通道已删除")
                    .order_by(Channel.id.asc())
                )
            ).scalars().all()
        )


async def _fetch_proxies_for_channels(channels: list[Channel]) -> list[Proxy]:
    """仅返回当前通道仍绑定且未删除的代理，删除全部通道后代理健康区域为空。"""
    proxy_ids = sorted({int(channel.proxy_id) for channel in channels if channel.proxy_id})
    if not proxy_ids:
        return []
    async with DashboardSessionLocal() as session:
        return list(
            (
                await session.execute(
                    select(Proxy)
                    .where(Proxy.id.in_(proxy_ids), Proxy.is_active.is_(True))
                    .order_by(Proxy.id.asc())
                )
            ).scalars().all()
        )


async def _fetch_recent_tasks() -> list[Task]:
    async with DashboardSessionLocal() as session:
        return list(
            (
                await session.execute(
                    select(Task)
                    .where(_recent_tasks_filter())
                    .order_by(Task.updated_at.desc(), Task.id.desc())
                    .limit(10)
                )
            ).scalars().all()
        )


async def _fetch_task_final_status_counts(task_ids: list[int]) -> dict[int, dict[str, int]]:
    snapshots = await _fetch_task_stats_snapshots(task_ids)
    return {
        int(item.task_id): {
            "delivered": int(item.delivered_count or 0),
            "hard_bounced": int(item.hard_bounced_count or 0),
            "soft_bounced": int(item.soft_bounced_count or 0),
        }
        for item in snapshots
    }


async def _batch_channel_circuit_disabled(channel_ids: list[int]) -> dict[int, bool]:
    if not channel_ids:
        return {}
    from app.core.redis import redis_client

    try:
        pipe = redis_client.pipeline()
        for channel_id in channel_ids:
            pipe.get(channel_health_disabled_key(channel_id))
        values = await asyncio.wait_for(pipe.execute(), timeout=5.0)
        return {
            int(channel_id): bool(value)
            for channel_id, value in zip(channel_ids, values)
        }
    except Exception as exc:
        logger.warning("批量读取通道熔断状态失败，返回默认在线状态 error_type=%s error=%s", type(exc).__name__, exc)
        return {int(channel_id): False for channel_id in channel_ids}


def aggregate_running_task_metrics(running_tasks: list[Task]) -> dict[str, object]:
    global_current_rate = 0.0
    global_rate_limit_sum = 0.0
    recent_429_count = 0
    channel_latency_samples: dict[int, list[float]] = defaultdict(list)
    task_rate_map: dict[int, float] = {}

    running_ids = {int(task.id) for task in running_tasks}
    for task_id in list(_running_tasks.keys()):
        if int(task_id) not in running_ids or not is_task_runner_active(int(task_id)):
            continue
        runtime = get_mutable_task_runtime(int(task_id))
        if runtime is None:
            continue
        snapshot = get_task_runtime_metrics(int(task_id))
        current_rate = float(
            snapshot.get(
                "sdk_current_rate_1s",
                snapshot.get("success_rate", snapshot.get("current_rate", 0)),
            )
            or snapshot.get("success_rate", snapshot.get("current_rate", 0))
            or 0
        )
        global_current_rate += current_rate
        task_rate_map[int(task_id)] = round(current_rate, 2)
        global_rate_limit_sum += float(runtime.get("rate_limit_per_second", 0) or 0)
        recent_429_count += count_recent_429_events(runtime, GLOBAL_429_WINDOW_SECONDS)
        rtt_ms = float(runtime.get("proxy_rtt_seconds", 0) or 0) * 1000.0
        if rtt_ms > 0:
            for task in running_tasks:
                if int(task.id) == int(task_id) and task.channel_id:
                    channel_latency_samples[int(task.channel_id)].append(rtt_ms)

    task_rate_comparison: list[dict[str, object]] = []
    for task in running_tasks:
        runtime = get_mutable_task_runtime(int(task.id))
        if runtime is None:
            continue
        snapshot = get_task_runtime_metrics(int(task.id))
        task_rate_comparison.append(
            {
                "task_id": int(task.id),
                "task_name": str(task.name or f"任务{task.id}"),
                "target_rate": float(runtime.get("rate_limit_per_second", 0) or 0),
                "current_rate": float(
                    snapshot.get(
                        "sdk_current_rate_1s",
                        snapshot.get("success_rate", snapshot.get("current_rate", 0)),
                    )
                    or snapshot.get("success_rate", snapshot.get("current_rate", 0))
                    or 0
                ),
                "admitted_workers": int(runtime.get("admitted_workers", runtime.get("active_workers", 0)) or 0),
                "active_http_inflight": int(runtime.get("active_http_inflight", runtime.get("httpx_inflight", 0)) or 0),
            }
        )

    return {
        "global_current_rate": round(global_current_rate, 2),
        "global_rate_limit_sum": round(global_rate_limit_sum, 2),
        "recent_429_count": int(recent_429_count),
        "channel_latency_samples": channel_latency_samples,
        "task_rate_map": task_rate_map,
        "task_rate_comparison": build_task_rate_comparison(task_rate_comparison),
    }


def _build_channels_health(
    channels: list[Channel],
    channel_latency_samples: dict[int, list[float]],
    circuit_disabled_map: dict[int, bool],
) -> list[dict[str, object]]:
    channels_health: list[dict[str, object]] = []
    for channel in channels:
        samples = channel_latency_samples.get(int(channel.id), [])
        if samples:
            latency_ms = int(round(sum(samples) / len(samples)))
        else:
            latency_ms = int(round(float(settings.estimated_proxy_rtt_seconds or 1.5) * 1000))
        online = bool(channel.enabled and channel.is_active and not str(channel.last_error or "").strip())
        if online and circuit_disabled_map.get(int(channel.id), False):
            online = False
        channels_health.append(
            {
                "id": int(channel.id),
                "name": str(channel.name or f"通道{channel.id}"),
                "online": online,
                "latency_ms": latency_ms,
            }
        )
    return channels_health


def _build_proxies_health(
    channels: list[Channel],
    proxies: list[Proxy],
    channel_latency_samples: dict[int, list[float]],
) -> tuple[list[dict[str, object]], int, int]:
    proxy_latency_by_id: dict[int, list[float]] = defaultdict(list)
    channel_proxy_ids = {int(channel.proxy_id) for channel in channels if channel.proxy_id}
    for channel in channels:
        if not channel.proxy_id:
            continue
        samples = channel_latency_samples.get(int(channel.id), [])
        if samples:
            proxy_latency_by_id[int(channel.proxy_id)].extend(samples)

    proxies_health: list[dict[str, object]] = []
    proxy_latencies: list[float] = []
    for proxy in proxies:
        if int(proxy.id) not in channel_proxy_ids:
            continue
        samples = proxy_latency_by_id.get(int(proxy.id), [])
        if samples:
            latency_ms = int(round(sum(samples) / len(samples)))
        else:
            latency_ms = int(round(float(settings.estimated_proxy_rtt_seconds or 1.5) * 1000))
        proxy_latencies.append(float(latency_ms))
        online = bool(proxy.is_active and not str(proxy.last_error or "").strip())
        proxies_health.append(
            {
                "id": int(proxy.id),
                "proxy": mask_proxy_for_log(proxy),
                "online": online,
                "latency_ms": latency_ms,
            }
        )

    avg_proxy_latency_ms = int(round(sum(proxy_latencies) / len(proxy_latencies))) if proxy_latencies else 0
    proxy_latency_max_ms = int(max(proxy_latencies)) if proxy_latencies else 0
    return proxies_health, avg_proxy_latency_ms, proxy_latency_max_ms


async def _collect_task_related_dashboard_stats() -> tuple[dict[str, object], dict[int, list[float]]]:
    """汇总 runtime 快照和预计算表，不访问发送日志大表。"""
    total_tasks, running_tasks, recent_task_rows, persisted_snapshots = await asyncio.gather(
        _fetch_total_tasks(),
        _fetch_running_tasks(),
        _fetch_recent_tasks(),
        _fetch_task_stats_snapshots(),
    )
    today_delivered = sum(int(item.delivered_count or 0) for item in persisted_snapshots)
    today_api_sent = sum(int(item.success_count or 0) for item in persisted_snapshots)
    hard_bounced_total = sum(int(item.hard_bounced_count or 0) for item in persisted_snapshots)
    soft_bounced_or_retrying = sum(int(item.soft_bounced_count or 0) for item in persisted_snapshots)
    valid_recipients = sum(int(item.total_count or 0) for item in persisted_snapshots)
    runtime_metrics = aggregate_running_task_metrics(running_tasks)
    recent_ids = [int(task.id) for task in recent_task_rows]
    final_status_map = await _fetch_task_final_status_counts(recent_ids)
    recent_tasks = _build_recent_tasks(recent_task_rows, final_status_map, runtime_metrics["task_rate_map"])

    global_max_theoretical = 0.0
    for task in running_tasks:
        rtt = float(settings.estimated_proxy_rtt_seconds or 1.5)
        cycle = max(0.05, rtt)
        concurrency = max(1, int(task.concurrency or 1))
        global_max_theoretical += round(concurrency / cycle, 2)

    channel_latency_samples: dict[int, list[float]] = runtime_metrics["channel_latency_samples"]
    global_current_rate = float(runtime_metrics["global_current_rate"])
    global_rate_limit_sum = float(runtime_metrics["global_rate_limit_sum"])
    global_rate_achievement_percent = round(
        min(100.0, (global_current_rate / global_rate_limit_sum) * 100.0),
        1,
    ) if global_rate_limit_sum > 0 else (100.0 if global_current_rate > 0 else 0.0)
    stats = {
        "total_tasks": int(total_tasks),
        "running_tasks": len(running_tasks),
        "valid_recipients": int(valid_recipients),
        "today_delivered": int(today_delivered),
        "today_api_sent": int(today_api_sent),
        "hard_bounced_total": int(hard_bounced_total),
        "soft_bounced_or_retrying": int(soft_bounced_or_retrying),
        "global_current_rate": global_current_rate,
        "global_rate_limit_sum": global_rate_limit_sum,
        "global_max_theoretical_rate": round(global_max_theoretical, 2),
        "global_rate_achievement_percent": global_rate_achievement_percent,
        "recent_429_count": int(runtime_metrics["recent_429_count"]),
        "recent_tasks": recent_tasks,
        "task_rate_comparison": runtime_metrics.get("task_rate_comparison", []),
        "alert_recent_429_burst": int(runtime_metrics["recent_429_count"]) > 20,
    }
    return stats, channel_latency_samples


def _resolve_dashboard_task_cache_ttl() -> float:
    if _running_tasks:
        return DASHBOARD_TASK_STATS_ACTIVE_SEND_CACHE_TTL_SECONDS
    return DASHBOARD_TASK_STATS_CACHE_TTL_SECONDS


async def _get_cached_task_dashboard_bundle() -> tuple[dict[str, object], dict[int, list[float]]]:
    now_value = time.time()
    cache_ttl = _resolve_dashboard_task_cache_ttl()
    cached_payload = _dashboard_task_stats_cache.get("payload")
    if isinstance(cached_payload, dict) and now_value - float(_dashboard_task_stats_cache.get("updated_at", 0.0) or 0.0) < cache_ttl:
        cached_samples = _dashboard_task_stats_cache.get("channel_latency_samples")
        return dict(cached_payload), dict(cached_samples) if isinstance(cached_samples, dict) else {}
    async with _dashboard_task_stats_lock:
        now_value = time.time()
        cache_ttl = _resolve_dashboard_task_cache_ttl()
        cached_payload = _dashboard_task_stats_cache.get("payload")
        if isinstance(cached_payload, dict) and now_value - float(_dashboard_task_stats_cache.get("updated_at", 0.0) or 0.0) < cache_ttl:
            cached_samples = _dashboard_task_stats_cache.get("channel_latency_samples")
            return dict(cached_payload), dict(cached_samples) if isinstance(cached_samples, dict) else {}
        try:
            stats, channel_latency_samples = await _collect_task_related_dashboard_stats()
        except (asyncio.CancelledError, TimeoutError, OperationalError, DBAPIError) as exc:
            if _is_query_cancelled_error(exc):
                stale_payload = _dashboard_task_stats_cache.get("payload")
                stale_samples = _dashboard_task_stats_cache.get("channel_latency_samples")
                if isinstance(stale_payload, dict):
                    _log_sampled_dashboard_warning(
                        "dashboard_task_stats_query_cancelled_stale",
                        "仪表盘任务统计查询被取消，已返回陈旧缓存，避免前端超时 error_type=%s",
                        type(exc).__name__,
                    )
                    return dict(stale_payload), dict(stale_samples) if isinstance(stale_samples, dict) else {}
                _log_sampled_dashboard_warning(
                    "dashboard_task_stats_query_cancelled_empty",
                    "仪表盘任务统计查询被取消且暂无缓存，已返回降级空数据 error_type=%s",
                    type(exc).__name__,
                )
                return empty_task_related_dashboard_stats(), {}
            raise
        _dashboard_task_stats_cache["payload"] = dict(stats)
        _dashboard_task_stats_cache["channel_latency_samples"] = dict(channel_latency_samples)
        _dashboard_task_stats_cache["updated_at"] = time.time()
        return stats, channel_latency_samples


async def fetch_task_related_dashboard_stats() -> dict[str, object]:
    """实时读取独立仪表盘数据库池和当前 runtime，不经过 Redis 或内存缓存。"""
    try:
        stats, _channel_latency_samples = await asyncio.wait_for(
            _collect_task_related_dashboard_stats(),
            timeout=10.0,
        )
        return stats
    except (TimeoutError, OperationalError, DBAPIError) as exc:
        raise DashboardServiceBusyError("服务繁忙，请稍后重试") from exc


async def refresh_task_related_dashboard_stats(
    payload: dict[str, object],
    *,
    timeout_seconds: float = 3.0,
) -> dict[str, object]:
    """将任务相关指标覆盖为数据库实时值，保留系统级状态（通道/代理/SenderPool 等）。"""
    merged = merge_dashboard_stats_payload(payload)
    try:
        fresh_task_stats = await asyncio.wait_for(
            fetch_task_related_dashboard_stats(),
            timeout=timeout_seconds,
        )
    except Exception:
        logger.exception("刷新任务相关仪表盘统计失败，任务指标归零")
        fresh_task_stats = empty_task_related_dashboard_stats()
    for key in TASK_RELATED_DASHBOARD_KEYS:
        if key in fresh_task_stats:
            merged[key] = fresh_task_stats[key]
    log_dashboard_task_metrics(merged, source="task-refresh")
    return merged


def _build_recent_tasks(
    recent_task_rows: list[Task],
    final_status_map: dict[int, dict[str, int]],
    task_rate_map: dict[int, float],
) -> list[dict[str, object]]:
    recent_tasks: list[dict[str, object]] = []
    for task in recent_task_rows:
        if getattr(task, "deleted_at", None) is not None:
            continue
        normalized_status = str(task.status or "draft").strip().lower()
        if normalized_status in TERMINAL_TASK_STATUSES:
            continue
        final_counts = final_status_map.get(int(task.id), {})
        delivered_count = int(final_counts.get("delivered", 0) or 0)
        hard_bounced_count = int(final_counts.get("hard_bounced", 0) or 0)
        soft_bounced_count = int(final_counts.get("soft_bounced", 0) or 0)
        total_count = int(getattr(task, "total_count", 0) or 0)
        sent_count = int(task.sent_count or 0)
        failed_count = int(task.failed_count or 0)
        processed_count = min(total_count, sent_count + failed_count) if total_count > 0 else sent_count + failed_count
        final_processed_count = delivered_count + hard_bounced_count + soft_bounced_count
        processed_count = max(processed_count, min(total_count, final_processed_count) if total_count > 0 else final_processed_count)
        pending_count = max(0, total_count - processed_count) if total_count > 0 else 0
        recent_tasks.append(
            {
                "id": int(task.id),
                "name": str(task.name or f"任务{task.id}"),
                "status": str(task.status or "draft"),
                "status_label": resolve_task_status_label(task.status),
                "current_rate": float(task_rate_map.get(int(task.id), 0.0)),
                "pending_count": pending_count,
                "progress": f"{processed_count}/{total_count}" if total_count > 0 else f"{processed_count}/0",
                "hard_bounced_count": hard_bounced_count,
                "soft_bounced_count": soft_bounced_count,
                "delivered_count": delivered_count,
                "total_count": total_count,
            }
        )
    return recent_tasks


async def build_system_dashboard_stats(
    channel_latency_samples: dict[int, list[float]] | None = None,
) -> dict[str, object]:
    """构建系统级仪表盘指标（通道/代理/SenderPool/资源），与任务列表无关。"""
    latency_samples = channel_latency_samples if channel_latency_samples is not None else {}

    channels, system_resources = await asyncio.gather(
        _fetch_channels(),
        _fetch_system_resources(),
    )
    proxies = await _fetch_proxies_for_channels(channels)
    channel_ids = [int(channel.id) for channel in channels]
    circuit_disabled_map = await _batch_channel_circuit_disabled(channel_ids)
    channels_health = _build_channels_health(channels, latency_samples, circuit_disabled_map)
    proxies_health, avg_proxy_latency_ms, proxy_latency_max_ms = _build_proxies_health(
        channels,
        proxies,
        latency_samples,
    )
    cpu_percent, memory_percent = system_resources
    today_quota = resolve_dashboard_today_quota()

    enabled_channel_ids = {int(item.id) for item in channels if bool(item.enabled)}
    enabled_channels = [item for item in channels if bool(item.enabled)]
    active_proxies = [item for item in proxies if bool(item.is_active)]
    all_channels_offline = bool(enabled_channels) and not any(
        item["online"] for item in channels_health if int(item["id"]) in enabled_channel_ids
    )
    all_proxies_offline = bool(active_proxies) and not any(item["online"] for item in proxies_health)
    admitted_workers = int(get_admitted_workers_total())
    active_http_inflight = int(get_active_http_inflight_total())
    executing_tasks = int(get_executing_tasks_total())
    return {
        "today_quota": today_quota,
        "avg_proxy_latency_ms": avg_proxy_latency_ms,
        "proxy_latency_max_ms": proxy_latency_max_ms,
        "channels_health": channels_health,
        "proxies_health": proxies_health,
        "cpu_percent": cpu_percent,
        "memory_percent": memory_percent,
        "alert_all_channels_offline": all_channels_offline,
        "alert_all_proxies_offline": all_proxies_offline,
        "alert_global_concurrency_rejected": False,
        "recent_concurrency_rejections": 0,
        "admitted_workers": admitted_workers,
        "active_http_inflight": active_http_inflight,
        "executing_tasks": executing_tasks,
        "admitted_workers_limit": 0,
        "sender_pool_stats": await get_sender_pool_stats(),
    }


async def _get_cached_system_dashboard_stats(channel_latency_samples: dict[int, list[float]] | None = None) -> dict[str, object]:
    now_value = time.time()
    cached_payload = _dashboard_system_stats_cache.get("payload")
    cached_age = now_value - float(_dashboard_system_stats_cache.get("updated_at", 0.0) or 0.0)
    if isinstance(cached_payload, dict):
        if channel_latency_samples is None and cached_age < DASHBOARD_SYSTEM_STATS_CACHE_TTL_SECONDS:
            return dict(cached_payload)
        if channel_latency_samples is not None and cached_age < DASHBOARD_CHANNEL_LATENCY_CACHE_MIN_SECONDS:
            merged = dict(cached_payload)
            channels = await _fetch_channels()
            proxies = await _fetch_proxies_for_channels(channels)
            circuit_disabled_map = await _batch_channel_circuit_disabled([int(channel.id) for channel in channels])
            channels_health = _build_channels_health(channels, channel_latency_samples, circuit_disabled_map)
            proxies_health, avg_proxy_latency_ms, proxy_latency_max_ms = _build_proxies_health(channels, proxies, channel_latency_samples)
            merged.update({
                "channels_health": channels_health,
                "proxies_health": proxies_health,
                "avg_proxy_latency_ms": avg_proxy_latency_ms,
                "proxy_latency_max_ms": proxy_latency_max_ms,
            })
            return merged
    async with _dashboard_system_stats_lock:
        now_value = time.time()
        cached_payload = _dashboard_system_stats_cache.get("payload")
        cached_age = now_value - float(_dashboard_system_stats_cache.get("updated_at", 0.0) or 0.0)
        if isinstance(cached_payload, dict):
            if channel_latency_samples is None and cached_age < DASHBOARD_SYSTEM_STATS_CACHE_TTL_SECONDS:
                return dict(cached_payload)
            if channel_latency_samples is not None and cached_age < DASHBOARD_CHANNEL_LATENCY_CACHE_MIN_SECONDS:
                merged = dict(cached_payload)
                channels = await _fetch_channels()
                proxies = await _fetch_proxies_for_channels(channels)
                circuit_disabled_map = await _batch_channel_circuit_disabled([int(channel.id) for channel in channels])
                channels_health = _build_channels_health(channels, channel_latency_samples, circuit_disabled_map)
                proxies_health, avg_proxy_latency_ms, proxy_latency_max_ms = _build_proxies_health(channels, proxies, channel_latency_samples)
                merged.update({
                    "channels_health": channels_health,
                    "proxies_health": proxies_health,
                    "avg_proxy_latency_ms": avg_proxy_latency_ms,
                    "proxy_latency_max_ms": proxy_latency_max_ms,
                })
                return merged
        try:
            payload = await asyncio.wait_for(build_system_dashboard_stats(channel_latency_samples), timeout=5.0)
        except Exception as exc:
            stale_payload = _dashboard_system_stats_cache.get("payload")
            if isinstance(stale_payload, dict):
                _log_sampled_dashboard_warning(
                    "dashboard_system_stats_stale",
                    "仪表盘系统统计刷新失败，已返回缓存数据 error_type=%s",
                    type(exc).__name__,
                )
                return dict(stale_payload)
            _log_sampled_dashboard_warning(
                "dashboard_system_stats_empty",
                "仪表盘系统统计刷新失败且暂无缓存，已返回空数据 error_type=%s",
                type(exc).__name__,
            )
            return empty_dashboard_stats()
        _dashboard_system_stats_cache["payload"] = dict(payload)
        _dashboard_system_stats_cache["updated_at"] = time.time()
        return payload


def get_cached_dashboard_stats() -> dict[str, object]:
    """仅返回后台已生成的快照；API 调用此函数时绝不访问数据库、Redis 或代理。"""
    task_payload = _dashboard_task_stats_cache.get("payload")
    system_payload = _dashboard_system_stats_cache.get("payload")
    if not isinstance(task_payload, dict) and not isinstance(system_payload, dict):
        return empty_dashboard_stats()
    return merge_dashboard_stats_payload({
        **(task_payload if isinstance(task_payload, dict) else {}),
        **(system_payload if isinstance(system_payload, dict) else {}),
    })


async def dashboard_stats_refresh_loop() -> None:
    """每 30 秒在后台更新完整统计快照，刷新失败时保留最后一次成功结果。"""
    while True:
        try:
            task_stats, channel_latency_samples = await asyncio.wait_for(
                _collect_task_related_dashboard_stats(),
                timeout=DASHBOARD_STATS_BUILD_TIMEOUT_SECONDS,
            )
            system_stats = await asyncio.wait_for(
                build_system_dashboard_stats(channel_latency_samples),
                timeout=DASHBOARD_STATS_BUILD_TIMEOUT_SECONDS,
            )
            now_value = time.time()
            _dashboard_task_stats_cache["payload"] = dict(task_stats)
            _dashboard_task_stats_cache["channel_latency_samples"] = dict(channel_latency_samples)
            _dashboard_task_stats_cache["updated_at"] = now_value
            _dashboard_system_stats_cache["payload"] = dict(system_stats)
            _dashboard_system_stats_cache["updated_at"] = now_value
            logger.info("仪表盘后台统计刷新完成")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_sampled_dashboard_warning(
                "dashboard_background_refresh_failed",
                "仪表盘后台统计刷新失败，继续保留旧缓存 error_type=%s error=%s",
                type(exc).__name__,
                str(exc)[:500],
            )
        await asyncio.sleep(30)


async def build_dashboard_stats(db: AsyncSession | None = None) -> dict[str, object]:
    """实时构建仪表盘数据；传入会话会被忽略，强制使用 DashboardSessionLocal 独立池。"""
    del db
    try:
        task_stats, channel_latency_samples = await asyncio.wait_for(
            _collect_task_related_dashboard_stats(),
            timeout=10.0,
        )
        system_stats = await asyncio.wait_for(
            build_system_dashboard_stats(channel_latency_samples),
            timeout=10.0,
        )
        return merge_dashboard_stats_payload({**task_stats, **system_stats})
    except DashboardServiceBusyError:
        raise
    except (TimeoutError, OperationalError, DBAPIError) as exc:
        raise DashboardServiceBusyError("服务繁忙，请稍后重试") from exc
