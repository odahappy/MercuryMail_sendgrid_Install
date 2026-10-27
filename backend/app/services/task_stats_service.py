"""任务统计、性能快照与失败明细分页查询。"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import DashboardSessionLocal
from app.models.models import SendLog, Task
from app.services.task_runner import (
    build_task_perf_diagnostics,
    count_http_429_last_minute,
    get_mutable_task_runtime,
    get_task_admitted_workers,
    get_task_runtime_metrics,
    is_task_runner_active,
)

logger = logging.getLogger(__name__)

REGULAR_LOG_FILTER = or_(SendLog.log_type.is_(None), SendLog.log_type == "", SendLog.log_type == "regular")
TASK_STATS_CACHE_TTL_SECONDS = 10.0
_task_stats_cache: dict[int, tuple[float, dict]] = {}
_task_stats_degrade_log_samples: dict[str, tuple[int, float]] = {}

TASK_LOG_UNION_SQL = """
    SELECT id, task_id, email, status, final_status, bounce_type, log_type, created_at
    FROM send_logs
    WHERE task_id = :task_id
      AND COALESCE(log_type, 'regular') = 'regular'
    UNION ALL
    SELECT id, task_id, email, status, final_status, bounce_type, log_type, created_at
    FROM send_log_archive
    WHERE task_id = :task_id
      AND COALESCE(log_type, 'regular') = 'regular'
"""

SDK_TERMINAL_STATUSES_SQL = "'sent', 'success', 'failed', 'bounced', 'blocked', 'dropped'"


def _is_query_cancelled_error(exc: BaseException) -> bool:
    text_value = str(exc).lower()
    return (
        "canceling statement due to user request" in text_value
        or "querycanceled" in text_value
        or "query cancelled" in text_value
        or "cancellederror" in text_value
        or "statement timeout" in text_value
    )


def _log_sampled_task_stats_warning(sample_key: str, message: str, *args) -> None:
    now_value = time.monotonic()
    suppressed_count, last_logged_at = _task_stats_degrade_log_samples.get(sample_key, (0, 0.0))
    suppressed_count += 1
    if now_value - last_logged_at >= 60.0:
        if suppressed_count > 1:
            logger.warning(message + " | suppressed_similar=%s", *args, suppressed_count)
        else:
            logger.warning(message, *args)
        _task_stats_degrade_log_samples[sample_key] = (0, now_value)
    else:
        _task_stats_degrade_log_samples[sample_key] = (suppressed_count, last_logged_at)


def _get_cached_task_stats_payload(task_id: int) -> dict | None:
    cached = _task_stats_cache.get(int(task_id))
    if cached is None:
        return None
    updated_at, payload = cached
    if time.time() - float(updated_at or 0) > TASK_STATS_CACHE_TTL_SECONDS:
        return None
    return dict(payload)


def _set_cached_task_stats_payload(task_id: int, payload: dict) -> None:
    _task_stats_cache[int(task_id)] = (time.time(), dict(payload))

SDK_STATS_SQL = text(
    f"""
    WITH combined_logs AS ({TASK_LOG_UNION_SQL}),
    latest_log AS (
        SELECT DISTINCT ON (LOWER(TRIM(email)))
            LOWER(TRIM(email)) AS email,
            LOWER(COALESCE(BTRIM(status), '')) AS status
        FROM combined_logs
        WHERE email IS NOT NULL
          AND TRIM(email) <> ''
          AND LOWER(COALESCE(BTRIM(status), '')) IN ({SDK_TERMINAL_STATUSES_SQL})
        ORDER BY LOWER(TRIM(email)),
                 CASE LOWER(COALESCE(BTRIM(status), ''))
                    WHEN 'sent' THEN 100
                    WHEN 'success' THEN 100
                    WHEN 'failed' THEN 90
                    WHEN 'bounced' THEN 90
                    WHEN 'blocked' THEN 90
                    WHEN 'dropped' THEN 90
                    ELSE 0
                 END DESC,
                 created_at DESC NULLS LAST,
                 id DESC
    )
    SELECT
        COUNT(*) FILTER (WHERE status IN ('sent', 'success'))::int AS success_sdk,
        COUNT(*) FILTER (WHERE status IN ('failed', 'bounced', 'blocked', 'dropped'))::int AS failed_sdk
    FROM latest_log
    """
)

UNSENT_RECIPIENTS_SQL = text(
    f"""
    WITH combined_logs AS ({TASK_LOG_UNION_SQL})
    SELECT COUNT(DISTINCT LOWER(TRIM(r.email)))::int AS unsent_count
    FROM recipients r
    WHERE r.list_id = :list_id
      AND r.email IS NOT NULL
      AND TRIM(r.email) <> ''
      AND COALESCE(r.is_valid, TRUE) IS TRUE
      AND COALESCE(r.is_unsubscribed, FALSE) IS FALSE
      AND NOT EXISTS (
        SELECT 1
        FROM unsubscribes u
        WHERE COALESCE(NULLIF(BTRIM(u.normalized_email), ''), LOWER(BTRIM(u.email))) = LOWER(TRIM(r.email))
      )
      AND NOT EXISTS (
        SELECT 1
        FROM combined_logs sl
        WHERE LOWER(TRIM(sl.email)) = LOWER(TRIM(r.email))
          AND LOWER(COALESCE(BTRIM(sl.status), '')) IN ({SDK_TERMINAL_STATUSES_SQL})
      )
    """
)

FINAL_DELIVERY_STATS_SQL = text(
    f"""
    WITH combined_logs AS ({TASK_LOG_UNION_SQL}),
    normalized_logs AS (
        SELECT
            id,
            LOWER(TRIM(email)) AS email,
            LOWER(COALESCE(BTRIM(status), '')) AS status,
            CASE
                WHEN LOWER(COALESCE(BTRIM(final_status), '')) IN ('bounce', 'bounced') THEN 'bounced'
                WHEN LOWER(COALESCE(BTRIM(final_status), '')) IN ('spamreport', 'spam_report', 'spamreported') THEN 'spamreported'
                ELSE LOWER(COALESCE(BTRIM(final_status), ''))
            END AS final_status,
            LOWER(COALESCE(BTRIM(bounce_type), '')) AS bounce_type,
            created_at
        FROM combined_logs
        WHERE email IS NOT NULL
          AND TRIM(email) <> ''
    ), latest_log AS (
        SELECT DISTINCT ON (email)
            email,
            status,
            final_status,
            bounce_type
        FROM normalized_logs
        ORDER BY email,
                 CASE final_status
                    WHEN 'unsubscribed' THEN 120
                    WHEN 'bounced' THEN 110
                    WHEN 'blocked' THEN 100
                    WHEN 'dropped' THEN 100
                    WHEN 'spamreported' THEN 100
                    WHEN 'delivered' THEN 90
                    WHEN 'deferred' THEN 20
                    WHEN 'processed' THEN 10
                    ELSE 0
                 END DESC,
                 created_at DESC NULLS LAST,
                 id DESC
    )
    SELECT
        COUNT(*) FILTER (WHERE final_status = 'delivered')::int AS delivered_final,
        COUNT(*) FILTER (WHERE final_status IN ('blocked', 'dropped', 'spamreported') OR (final_status = 'bounced' AND bounce_type <> 'soft'))::int AS hard_bounce_final,
        COUNT(*) FILTER (WHERE final_status = 'deferred' OR (final_status = 'bounced' AND bounce_type = 'soft'))::int AS deferred_final,
        COUNT(*) FILTER (WHERE final_status = 'spamreported')::int AS spam_reported_final,
        COUNT(*) FILTER (WHERE status IN ('sent', 'success') AND COALESCE(final_status, '') = '')::int AS waiting_final
    FROM latest_log
    """
)

RECIPIENT_DISTRIBUTION_SQL = text(
    f"""
    /* 性能建议：任务详情统计在终态任务上会按 task_id 聚合 SendLog 与收件人池。
       推荐索引：
       CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_email_created_status ON send_logs (task_id, LOWER(BTRIM(email)), created_at DESC, id DESC) INCLUDE (status, final_status, bounce_type) WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> '';
       CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_log_archive_task_regular_email_created_status ON send_log_archive (task_id, LOWER(BTRIM(email)), created_at DESC, id DESC) INCLUDE (status, final_status, bounce_type) WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> '';
       CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_task_stats_list_email_valid ON recipients (list_id, LOWER(BTRIM(email))) INCLUDE (is_unsubscribed) WHERE COALESCE(is_valid, TRUE) = TRUE AND email IS NOT NULL AND BTRIM(email) <> '';
       PostgreSQL 建议：shared_buffers≈物理内存25%，effective_cache_size≈物理内存60%-70%，work_mem=16MB-64MB。
    */
    WITH pool AS (
        SELECT LOWER(TRIM(email)) AS email, BOOL_OR(COALESCE(is_unsubscribed, FALSE)) AS is_unsubscribed
        FROM recipients
        WHERE list_id = :list_id
          AND email IS NOT NULL
          AND TRIM(email) <> ''
          AND COALESCE(is_valid, TRUE) IS TRUE
        GROUP BY LOWER(TRIM(email))
    ),
    unsubs AS (
        SELECT DISTINCT COALESCE(NULLIF(BTRIM(normalized_email), ''), LOWER(BTRIM(email))) AS email
        FROM unsubscribes
        WHERE COALESCE(NULLIF(BTRIM(normalized_email), ''), LOWER(BTRIM(email))) <> ''
    ),
    combined_logs AS ({TASK_LOG_UNION_SQL}),
    latest_log AS (
        SELECT DISTINCT ON (LOWER(TRIM(email)))
            LOWER(TRIM(email)) AS email,
            status,
            CASE
                WHEN COALESCE(BTRIM(final_status), '') IN ('processed', 'success') THEN ''
                ELSE COALESCE(BTRIM(final_status), '')
            END AS final_status,
            bounce_type
        FROM combined_logs
        WHERE email IS NOT NULL
          AND TRIM(email) <> ''
        ORDER BY LOWER(TRIM(email)),
                 CASE COALESCE(BTRIM(final_status), '')
                    WHEN 'bounced' THEN 110
                    WHEN 'blocked' THEN 100
                    WHEN 'dropped' THEN 100
                    WHEN 'spamreported' THEN 100
                    WHEN 'delivered' THEN 90
                    WHEN 'deferred' THEN 20
                    WHEN 'processed' THEN 10
                    ELSE 0
                 END DESC,
                 created_at DESC NULLS LAST,
                 id DESC
    ),
    classified AS (
        SELECT
            p.email,
            CASE
                WHEN p.is_unsubscribed OR u.email IS NOT NULL THEN 'unsubscribed'
                WHEN l.final_status IN ('blocked', 'dropped', 'spamreported')
                  OR (l.final_status = 'bounced' AND COALESCE(l.bounce_type, '') <> 'soft') THEN 'hard'
                WHEN l.final_status = 'deferred'
                  OR (l.final_status = 'bounced' AND COALESCE(l.bounce_type, '') = 'soft') THEN 'soft'
                WHEN l.final_status = 'unsubscribed' THEN 'unsubscribed'
                WHEN l.final_status = 'delivered' THEN 'delivered'
                ELSE 'pending'
            END AS category
        FROM pool p
        LEFT JOIN unsubs u ON u.email = p.email
        LEFT JOIN latest_log l ON l.email = p.email
    )
    SELECT category, COUNT(*)::int AS total
    FROM classified
    GROUP BY category
    """
)


def _sdk_failure_condition(task_id: int):
    """失败明细：仅 SDK 即时失败，不含 Event Poller 投递状态。"""
    return and_(
        SendLog.task_id == int(task_id),
        REGULAR_LOG_FILTER,
        func.lower(func.btrim(func.coalesce(SendLog.status, ""))).in_(["failed", "bounced", "blocked", "dropped"]),
    )


def _resolve_runtime_snapshot(task: Task) -> dict:
    runtime = get_task_runtime_metrics(int(task.id))
    runner_active = is_task_runner_active(int(task.id))
    status_running = str(task.status or "") == "running"
    has_runtime = bool(runtime)
    running = runner_active or status_running
    return {
        "running": running,
        "admitted_workers": get_task_admitted_workers(int(task.id)) if running else 0,
        "avg_http_latency_ms": round(float(runtime.get("avg_http_send_ms", 0) or 0), 1) if has_runtime else 0.0,
        "queue_prefetch_length": int(runtime.get("recipient_queue_size", 0) or 0) if has_runtime else 0,
        "queue_write_length": int(runtime.get("result_queue_size", 0) or 0) if has_runtime else 0,
        "last_429_count_per_minute": count_http_429_last_minute(runtime) if has_runtime else 0,
        "current_rate": round(float(runtime.get("sdk_current_rate_1s", runtime.get("success_rate", runtime.get("current_rate", 0))) or 0), 1) if has_runtime else 0.0,
    }


async def _build_recipient_distribution(db: AsyncSession, task: Task) -> dict[str, int]:
    rows = (
        await db.execute(
            RECIPIENT_DISTRIBUTION_SQL,
            {"list_id": int(task.recipient_list_id), "task_id": int(task.id)},
        )
    ).all()
    counts = {str(row.category): int(row.total or 0) for row in rows}
    total_recipients = sum(counts.values())
    if total_recipients <= 0:
        total_recipients = int(task.total_count or 0)
    delivered = int(counts.get("delivered", 0))
    soft_bounced_or_retrying = int(counts.get("soft", 0))
    hard_bounced = int(counts.get("hard", 0)) + int(counts.get("spamreported", 0))
    unsubscribed = int(counts.get("unsubscribed", 0))
    pending = int(counts.get("pending", 0))
    if total_recipients <= 0:
        total_recipients = delivered + soft_bounced_or_retrying + hard_bounced + unsubscribed + pending
    return {
        "total_recipients": total_recipients,
        "delivered": delivered,
        "soft_bounced_or_retrying": soft_bounced_or_retrying,
        "hard_bounced": hard_bounced,
        "unsubscribed": unsubscribed,
        "pending": pending,
    }


async def _build_sdk_stats(db: AsyncSession, task: Task) -> dict[str, int]:
    row = (
        await db.execute(SDK_STATS_SQL, {"task_id": int(task.id)})
    ).mappings().one()
    unsent_row = (
        await db.execute(
            UNSENT_RECIPIENTS_SQL,
            {"list_id": int(task.recipient_list_id), "task_id": int(task.id)},
        )
    ).mappings().one()
    return {
        "success_sdk": int(row.get("success_sdk") or 0),
        "failed_sdk": int(row.get("failed_sdk") or 0),
        "unsent_count": int(unsent_row.get("unsent_count") or 0),
    }


async def build_task_stats(db: AsyncSession, task_id: int) -> dict:
    task = (
        await db.execute(select(Task).where(Task.id == int(task_id)))
    ).scalar_one_or_none()
    if task is None:
        raise LookupError("任务不存在")

    runtime_snapshot = _resolve_runtime_snapshot(task)
    cached_payload = _get_cached_task_stats_payload(int(task.id))
    if cached_payload is not None and not runtime_snapshot["running"]:
        return cached_payload
    if runtime_snapshot["running"]:
        runtime = get_task_runtime_metrics(int(task.id))
        success_sdk = int(runtime.get("live_success_count", task.sent_count or 0)) if runtime else int(task.sent_count or 0)
        failed_sdk = int(runtime.get("live_fail_count", task.failed_count or 0)) if runtime else int(task.failed_count or 0)
        total = int(task.total_count or 0)
        processed = max(0, success_sdk + failed_sdk)
        payload = {
            "total_recipients": total,
            "delivered": 0,
            "soft_bounced_or_retrying": 0,
            "hard_bounced": 0,
            "unsubscribed": 0,
            "pending": max(0, total - processed),
            "success_sdk": success_sdk,
            "failed_sdk": failed_sdk,
            "unsent_count": max(0, total - processed),
            "success": success_sdk,
            "failed": failed_sdk,
            "soft_bounced": 0,
            "retrying": 0,
            "active_workers": runtime_snapshot["admitted_workers"],
            "avg_http_latency_ms": runtime_snapshot["avg_http_latency_ms"],
            "queue_prefetch_length": runtime_snapshot["queue_prefetch_length"],
            "queue_write_length": runtime_snapshot["queue_write_length"],
            "last_429_count_per_minute": runtime_snapshot["last_429_count_per_minute"],
        }
        _set_cached_task_stats_payload(int(task.id), payload)
        return payload

    try:
        async with DashboardSessionLocal() as read_db:
            distribution = await _build_recipient_distribution(read_db, task)
            sdk_stats = await _build_sdk_stats(read_db, task)
    except (asyncio.CancelledError, TimeoutError, OperationalError, DBAPIError) as exc:
        if _is_query_cancelled_error(exc):
            cached_payload = _get_cached_task_stats_payload(int(task.id))
            if cached_payload is not None:
                _log_sampled_task_stats_warning(
                    f"task_stats_query_cancelled_stale:{int(task.id)}",
                    "任务 %s 统计查询被取消，已返回陈旧缓存，避免前端超时 error_type=%s",
                    int(task.id),
                    type(exc).__name__,
                )
                return cached_payload
            _log_sampled_task_stats_warning(
                f"task_stats_query_cancelled_empty:{int(task.id)}",
                "任务 %s 统计查询被取消且暂无缓存，已返回任务字段降级统计 error_type=%s",
                int(task.id),
                type(exc).__name__,
            )
            total = int(task.total_count or 0)
            success_sdk = int(task.sent_count or 0)
            failed_sdk = int(task.failed_count or 0)
            processed = max(0, success_sdk + failed_sdk)
            return {
                "total_recipients": total,
                "delivered": 0,
                "soft_bounced_or_retrying": 0,
                "hard_bounced": 0,
                "unsubscribed": 0,
                "pending": max(0, total - processed),
                "success_sdk": success_sdk,
                "failed_sdk": failed_sdk,
                "unsent_count": max(0, total - processed),
                "success": success_sdk,
                "failed": failed_sdk,
                "soft_bounced": 0,
                "retrying": 0,
                "active_workers": runtime_snapshot["active_workers"],
                "avg_http_latency_ms": runtime_snapshot["avg_http_latency_ms"],
                "queue_prefetch_length": runtime_snapshot["queue_prefetch_length"],
                "queue_write_length": runtime_snapshot["queue_write_length"],
                "last_429_count_per_minute": runtime_snapshot["last_429_count_per_minute"],
            }
        raise

    payload = {
        **distribution,
        **sdk_stats,
        "success": sdk_stats["success_sdk"],
        "failed": sdk_stats["failed_sdk"],
        "soft_bounced": 0,
        "retrying": 0,
        "active_workers": runtime_snapshot["active_workers"],
        "avg_http_latency_ms": runtime_snapshot["avg_http_latency_ms"],
        "queue_prefetch_length": runtime_snapshot["queue_prefetch_length"],
        "queue_write_length": runtime_snapshot["queue_write_length"],
        "last_429_count_per_minute": runtime_snapshot["last_429_count_per_minute"],
    }
    _set_cached_task_stats_payload(int(task.id), payload)
    return payload


async def build_task_perf(db: AsyncSession, task_id: int) -> dict:
    task = (
        await db.execute(select(Task).where(Task.id == int(task_id)))
    ).scalar_one_or_none()
    if task is None:
        raise LookupError("任务不存在")

    runtime_snapshot = _resolve_runtime_snapshot(task)
    # API 进程可能与发送 Runner 分离；必须使用可序列化的本地/Redis 共享快照，不能只读取 Runner 进程内可变字典。
    runtime_source = get_task_runtime_metrics(int(task_id))
    configured_concurrency = max(1, int(task.concurrency or 1))
    batch_size = max(1, int(task.send_http_batch_size or 50))
    diagnostics = build_task_perf_diagnostics(runtime_source, task) if runtime_snapshot["running"] and runtime_source else {
        "wait_token_ms": 0.0,
        "http_request_ms": 0.0,
        "db_write_ms": 0.0,
        "preprocess_ms": 0.0,
        "prefetch_queue_len": 0,
        "prefetch_fetch_count": 0,
        "prefetch_total_rows": 0,
        "write_queue_len": 0,
        "write_dropped": 0,
        "recent_429_list": [],
        "rate_limit_setting": max(0.0, float(task.rate_limit or 0)),
        "current_rate": 0.0,
        "sdk_current_rate_1s": 0.0,
        "sdk_success_rate_1s": 0.0,
        "sdk_current_rate_10s": 0.0,
        "max_theoretical_http_per_sec": 0.0,
        "suggested_action": "",
        "http_rps_recent": [],
        "prefetch_queue_empty_seconds": 0.0,
        "runtime_uptime_seconds": 0.0,
        "prefetch_starvation": False,
        "recent_429_count_2min": 0,
        "auto_concurrency": configured_concurrency,
        "concurrency_reason": "",
    }

    current_rate = float(diagnostics.get("current_rate", runtime_snapshot["current_rate"]) or 0)
    sdk_current_rate_1s = float(diagnostics.get("sdk_current_rate_1s", current_rate) or current_rate or 0)
    runtime_http_rps = float(diagnostics.get("http_requests_per_second", 0) or 0)
    http_rps = runtime_http_rps if runtime_http_rps > 0 else (round(current_rate / batch_size, 2) if current_rate > 0 else 0.0)
    effective_concurrency = max(0, int(diagnostics.get("effective_concurrency", runtime_source.get("effective_concurrency", runtime_snapshot["active_workers"])) or runtime_snapshot["active_workers"] or configured_concurrency))
    diagnostics.setdefault("rate_limit_setting", max(0.0, float(task.rate_limit or 0)))
    diagnostics.setdefault("send_http_batch_size", batch_size)
    diagnostics.setdefault("configured_user_concurrency", configured_concurrency)
    diagnostics.setdefault("effective_concurrency", effective_concurrency)
    diagnostics.setdefault("active_workers", runtime_source.get("active_workers", runtime_source.get("admitted_workers", runtime_snapshot["active_workers"])))
    diagnostics.setdefault("http_requests_per_second", runtime_source.get("http_requests_per_second", 0.0))
    diagnostics.setdefault("http_rps_recent", runtime_source.get("http_rps_recent", []))

    return {
        "current_rate": current_rate,
        "success_rate": float(diagnostics.get("success_rate", current_rate) or 0),
        "rate_window_seconds": int(diagnostics.get("rate_window_seconds", 10) or 10),
        "rate_window_success_count": int(diagnostics.get("rate_window_success_count", 0) or 0),
        "rate_window_processed_count": int(diagnostics.get("rate_window_processed_count", 0) or 0),
        "realtime_rate": sdk_current_rate_1s,
        "sdk_realtime_rate": sdk_current_rate_1s,
        "avg_http_latency_ms": runtime_snapshot["avg_http_latency_ms"],
        "active_workers": runtime_snapshot["active_workers"],
        "configured_concurrency": effective_concurrency,
        "effective_concurrency": effective_concurrency,
        "configured_user_concurrency": configured_concurrency,
        "auto_concurrency_enabled": bool(getattr(task, "auto_concurrency", False)),
        "queue_prefetch_length": runtime_snapshot["queue_prefetch_length"],
        "queue_write_length": runtime_snapshot["queue_write_length"],
        "last_429_count": runtime_snapshot["last_429_count_per_minute"],
        "http_requests_per_second": http_rps,
        "send_http_batch_size": batch_size,
        "running": runtime_snapshot["running"],
        **diagnostics,
    }


async def list_task_failures(
    db: AsyncSession,
    task_id: int,
    *,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    task = (
        await db.execute(select(Task.id).where(Task.id == int(task_id)))
    ).scalar_one_or_none()
    if task is None:
        raise LookupError("任务不存在")

    query_filter = _sdk_failure_condition(task_id)

    total = int((await db.execute(select(func.count()).select_from(SendLog).where(query_filter))).scalar_one() or 0)
    offset = (max(1, int(page)) - 1) * max(1, min(int(page_size), 100))
    limit = max(1, min(int(page_size), 100))

    rows = (
        await db.execute(
            select(
                SendLog.email,
                SendLog.sender_email,
                SendLog.error,
                SendLog.error_message,
                SendLog.attempt_count,
                SendLog.created_at,
            )
            .where(query_filter)
            .order_by(SendLog.created_at.desc(), SendLog.id.desc())
            .offset(offset)
            .limit(limit)
        )
    ).all()

    items = []
    for row in rows:
        error_text = str(row.error or row.error_message or "").strip() or "发送失败"
        items.append(
            {
                "email": str(row.email or "").strip(),
                "sender_email": str(getattr(row, "sender_email", "") or "").strip() or None,
                "error": error_text,
                "created_at": row.created_at or datetime.utcnow(),
                "retry_count": int(row.attempt_count or 0),
            }
        )

    return {
        "items": items,
        "total": total,
        "page": max(1, int(page)),
        "page_size": limit,
    }
