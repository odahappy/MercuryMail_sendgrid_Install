import asyncio
import copy
import gc
import hashlib
import json
import logging
import math
import os
import random
import re
import socket
import threading
import traceback
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date as date_type, datetime, time as datetime_time, timedelta, timezone
from time import monotonic, time as wall_time
from typing import Any

from sendgrid.helpers.mail import Content, CustomArg, Email, Header, Mail, Personalization, Substitution, To

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import and_, delete, exists, func, select, text, update
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError, TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.orm import aliased, selectinload

from app.core.database import (
    AsyncSessionLocal,
    DashboardSessionLocal,
    RecipientClaimSessionLocal,
    _is_transient_db_error,
    commit_session_with_retry,
    get_all_database_pool_status,
    is_database_pool_pressure_error,
    verify_writer_prerequisites_ready,
)
from app.models.models import Channel, CompactOutcome, Recipient, SendLog, Task, Template, Unsubscribe
from app.core.config import SEND_EMAIL_HTTP_BUDGET_SECONDS, SEND_EMAIL_TIMEOUT_SECONDS, settings
from app.services.channel_health import ensure_channel_ready_for_task_start
from app.services.channel_manager import (
    SENDGRID_HTTPX_POOL_MAX_SIZE,
    SENDGRID_HTTPX_POOL_MIN_SIZE,
    batch_pick_next_sender,
    build_concurrency_warning,
    ensure_send_threadpool_capacity,
    get_estimated_proxy_rtt_seconds,
    maybe_warn_send_threadpool_pressure,
    parse_sender_emails,
    pick_next_sender,
    reload_httpx_client,
    force_close_httpx_client,
    send_batch_mail_async,
)
from app.services.realtime_rate_tracker import RealtimeRateTracker
from app.services.pid_controller import (
    PID_INTERVAL_SECONDS,
    QueueAwareConcurrencyState,
    create_task_pid_controller,
    refine_pid_concurrency_target,
)
from app.services.send_log_allocator import SendLogIdAllocator, allocate_send_log_ids
from app.services.sender_pool import (
    DeliveryUnknownError,
    SENDER_POOL_MAX_WORKERS,
    get_sender_pool,
    shutdown_task_sender_pool,
)
from app.services.send_log_state import is_hard_bounce_error
from app.services.async_writer import AsyncBatchWriter, DetailLog, SendLogFlushStats
from app.services.backpressure import (
    BACKPRESSURE_CHECK_INTERVAL_SECONDS,
    BackpressureState,
    adjusted_prefetch_batch,
    evaluate_backpressure,
)
from app.services.connection_manager import (
    channel_proxy_fingerprint,
    mask_channel_proxy_info,
    measure_channel_proxy_latency,
    resolve_sendgrid_sdk_pool_limits,
    test_sendgrid_scopes,
    validate_sendgrid_api_key_proxy_consistency,
)
from app.services.sendgrid_sdk import (
    SyncSendGridClientManager,
    build_batch_mail,
    classify_sendgrid_auth_error,
    classify_sendgrid_response_origin,
)
from app.services.health import handle_channel_auth_failure, record_send_outcome, record_send_outcomes
from app.services.locker import (
    CHANNEL_TASK_BUSY_MESSAGE,
    RedisLockUnavailableError,
    acquire_task_lock,
    is_task_lock_held,
    is_warmup_lock_held,
    release_task_lock,
    try_acquire_task_lock,
)
from app.services.proxy_sharing_audit import warn_if_channel_shares_proxy_endpoint
from app.services import template_service as _template_service_module
from app.services.recipient_service import normalize_recipient_row_numbers
from app.services.template_service import (
    StaticMailContent,
    build_recipient_substitutions,
    build_static_mail_content,
    get_template_cache_key,
    render,
    render_text,
    validate_task_url_pool_binding,
    warm_up_template,
)
from app.services.url_service import (
    TaskUrlPoolCache,
    URL_POOL_EMPTY_ERROR,
    close_task_url_pool_cache,
    get_next_urls_batch,
    start_task_url_pool_cache,
)
from app.utils.errors import sanitize_user_error_message
from app.utils.sendgrid_errors import is_sendgrid_messaging_limit_error, normalize_send_log_error
from app.utils.concurrency import calculate_recommended_concurrency
from app.utils.security import (
    SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY,
    UnsubscribeUrlCache,
    build_substitution_unsubscribe,
    build_unsubscribe_url,
)
from app.core.redis import REDIS_OPERATION_TIMEOUT_SECONDS, REDIS_RETRY_ATTEMPTS, REDIS_TRANSIENT_ERRORS, redis_background_client
from app.utils.redis_helpers import safe_redis_op

logger = logging.getLogger(__name__)
DEFAULT_TASK_TIMEZONE = "Asia/Shanghai"
SCHEDULED_CHANNEL_BUSY_REASON = "通道被占用"


_SENDGRID_ACCOUNT_AUTH_ERROR_TYPES = {"api_key_invalid", "api_key_unauthorized", "auth_401_unknown"}
_SENDGRID_SEND_CONFIG_ERROR_TYPES = {"missing_mail_send_scope", "sender_identity_forbidden", "send_forbidden_unknown"}
SEND_AUTH_FAILURE_PAUSE_THRESHOLD = 1


def _is_sendgrid_auth_failure(status_code: int, error_message: str = "", headers: dict[str, str] | None = None) -> bool:
    if is_sendgrid_messaging_limit_error(error_message):
        return False
    error_type = classify_sendgrid_auth_error(status_code, error_message, headers)
    if error_type:
        return error_type in _SENDGRID_ACCOUNT_AUTH_ERROR_TYPES
    message = str(error_message or "").lower()
    return (
        "invalid api key" in message
        or "api key is invalid" in message
        or "authenticated user is not authorized to send mail" in message
    )


def _is_canonical_sendgrid_mail_auth_error(status_code: int, error_message: str) -> bool:
    text_value = str(error_message or "").lower()
    return int(status_code or 0) in (401, 403) and (
        "authenticated user is not authorized to send mail" in text_value
        or "invalid api key" in text_value
        or "api key is invalid" in text_value
        or "api key has been revoked" in text_value
    )


def _sendgrid_scopes_include_mail_send(scopes_body: str) -> bool | None:
    """从 /v3/scopes 响应判断 Key 是否具有 Mail Send 权限；无法解析时返回 None。"""
    try:
        payload = json.loads(str(scopes_body or ""))
    except (TypeError, ValueError):
        return None
    scopes = payload.get("scopes") if isinstance(payload, dict) else None
    if not isinstance(scopes, list):
        return None
    normalized_scopes = {str(scope or "").strip().lower() for scope in scopes}
    return "mail.send" in normalized_scopes or "*" in normalized_scopes


async def _confirm_sendgrid_auth_failure(task: Task, status_code: int, error_message: str) -> bool:
    """401/403 属于发送链路的硬鉴权失败，立即进入暂停流程，禁止同批次重试。"""
    normalized_status = int(status_code or 0)
    # 401/403 仅代表 Mail Send 请求被拒绝，不能单凭 HTTP 状态确认 API Key 已失效。
    # 必须由 canonical SendGrid 错误体或 /v3/scopes 复核确认，避免代理/WAF 伪响应误暂停通道。
    if normalized_status in (401, 403):
        text_value = str(error_message or "").lower()
        return (
            "authenticated user is not authorized to send mail" in text_value
            or "invalid api key" in text_value
            or "api key is invalid" in text_value
            or "api key has been revoked" in text_value
        )
    return _is_sendgrid_auth_failure(status_code, error_message)


def _auth_unconfirmed_send_error_message(status_code: int, error_message: str) -> str:
    normalized_status = int(status_code or 0)
    raw = str(error_message or "").strip()
    lowered = raw.lower()
    if "does not match a verified sender identity" in lowered or "verified sender" in lowered:
        base = "SendGrid 拒绝发送：发件人邮箱未验证或与当前账号的已验证 Sender Identity 不匹配"
    elif "mail send" in lowered or "mail.send" in lowered or "access forbidden" in lowered:
        base = "SendGrid 拒绝发送：当前 API Key 账号有效，但缺少 Mail Send 发送权限或该发信身份无权发送"
    elif normalized_status == 403:
        base = "SendGrid 拒绝发送：账号鉴权有效，但发送接口返回 403，请检查 API Key 的 Mail Send 权限、发件人验证状态、账号套餐或代理出口策略"
    elif normalized_status == 401:
        base = "SendGrid 发送接口返回 401，但通道鉴权复核未确认账号失效，请检查 API Key 是否具备发送权限及当前代理出口是否被 SendGrid 拒绝"
    else:
        base = "SendGrid 发送接口返回权限相关错误，但通道鉴权复核未确认账号失效"
    detail = normalize_send_log_error(raw) if raw else ""
    if detail and detail != base:
        return f"{base}（原始: {detail[:400]}）"
    return base


async def _mark_channel_unauthorized_for_task(task: Task, *, reason: str) -> None:
    channel = getattr(task, "channel", None)
    if channel is None:
        return
    logger.warning(
        "任务 %s 发送遇 401/403，仅暂停当前发送任务 channel_id=%s reason=%s",
        int(task.id),
        int(getattr(task, "channel_id", 0) or 0),
        reason,
    )


async def _handle_task_send_auth_failure(task: Task, status_code: int, error_message: str) -> bool:
    """仅连续确认账号级 401/403 后暂停；单次或瞬时代理/WAF 异常只降速重试。"""
    task_id = int(task.id or 0)
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        confirmed_count = int(runtime.get("confirmed_send_auth_failure_count", 0) or 0) + 1
        runtime["confirmed_send_auth_failure_count"] = confirmed_count
        runtime["last_confirmed_send_auth_status"] = int(status_code or 0)
        runtime["last_confirmed_send_auth_error"] = sanitize_user_error_message(str(error_message or ""))[:500]
    else:
        confirmed_count = SEND_AUTH_FAILURE_PAUSE_THRESHOLD
    if confirmed_count < SEND_AUTH_FAILURE_PAUSE_THRESHOLD:
        logger.warning(
            "任务 %s 发送接口出现已确认鉴权失败第 %s/%s 次，暂不暂停任务，先降速重试 channel_id=%s status_code=%s detail=%s",
            task_id,
            confirmed_count,
            SEND_AUTH_FAILURE_PAUSE_THRESHOLD,
            int(task.channel_id or 0),
            int(status_code or 0),
            sanitize_user_error_message(str(error_message or ""))[:500],
        )
        return False
    safe_reason = "通道认证失败，已自动暂停"
    logger.error(
        "%s task_id=%s channel_id=%s status_code=%s confirmed_count=%s detail=%s",
        safe_reason,
        task_id,
        int(task.channel_id or 0),
        int(status_code or 0),
        confirmed_count,
        sanitize_user_error_message(str(error_message or ""))[:500],
    )
    await _mark_channel_unauthorized_for_task(task, reason=safe_reason)
    error_text = str(error_message or "").lower()
    account_key_invalid = any(
        marker in error_text
        for marker in ("invalid api key", "api key is invalid", "api key has been revoked", "api key revoked")
    )
    if account_key_invalid:
        await handle_channel_auth_failure(int(task.channel_id or 0))
    else:
        logger.warning(
            "任务 %s 已因 SendGrid 发送权限/身份拒绝暂停，但不将通道标记为 API Key 认证失败 channel_id=%s",
            task_id,
            int(task.channel_id or 0),
        )
    return True


def get_scheduled_start_retry_interval_seconds() -> float:
    return max(10.0, float(settings.scheduled_start_retry_interval_seconds))


def get_scheduled_start_retry_max_attempts() -> int:
    return max(1, int(settings.scheduled_start_retry_max_attempts))
MAX_SEND_HTTP_BATCH_SIZE = 1000


def get_default_send_http_batch_size() -> int:
    return max(1, min(int(settings.default_http_batch_size), MAX_SEND_HTTP_BATCH_SIZE))


def get_warmup_default_http_batch_size() -> int:
    return max(1, min(int(settings.warmup_default_http_batch_size), MAX_SEND_HTTP_BATCH_SIZE))


DEFAULT_SEND_HTTP_BATCH_SIZE = get_default_send_http_batch_size()
WARMUP_DEFAULT_HTTP_BATCH_SIZE = get_warmup_default_http_batch_size()
# 非批量模式遗留常量，仅用于 API 超时回退展示；生产发送恒为 SDK 批量。
SENDGRID_RECIPIENTS_PER_HTTP_REQUEST = 1
SENDGRID_URL_SUBSTITUTION_KEY = "-url-"
SENDGRID_URL_POOL_URL_SUBSTITUTION_KEY = "-url_pool_url-"


def _index_recipient_substitutions(recipients_data: list[dict]) -> dict[str, dict[str, str]]:
    indexed: dict[str, dict[str, str]] = {}
    for item in recipients_data:
        email_key = str(item.get("email") or "").strip().lower()
        substitutions = item.get("substitutions")
        if email_key and isinstance(substitutions, dict):
            indexed[email_key] = substitutions
    return indexed


class DynamicConcurrencyLimiter:
    """任务本地动态并发门；缩容不会取消在途请求，扩容立即唤醒等待者。"""

    def __init__(self, initial_limit: int, max_limit: int) -> None:
        self._max_limit = max(1, int(max_limit or 1))
        self._limit = max(1, min(int(initial_limit or 1), self._max_limit))
        self._inflight = 0
        self._condition = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def max_limit(self) -> int:
        return self._max_limit

    @property
    def inflight(self) -> int:
        return self._inflight

    async def set_limit(self, value: int) -> int:
        normalized = max(1, min(int(value or 1), self._max_limit))
        async with self._condition:
            self._limit = normalized
            self._condition.notify_all()
        return normalized

    @asynccontextmanager
    async def slot(self):
        async with self._condition:
            await self._condition.wait_for(lambda: self._inflight < self._limit)
            self._inflight += 1
        try:
            yield
        finally:
            async with self._condition:
                self._inflight = max(0, self._inflight - 1)
                self._condition.notify_all()


@dataclass
class RecipientVariableBundle:
    """收件人变量包：Jinja2 渲染变量与 SendGrid Substitution 退订变量分离。"""

    render_variables: dict[str, str]
    unsubscribe_url: str
    substitution_unsubscribe: dict[str, str]


@dataclass(frozen=True)
class TaskCapacityProof:
    task_id: int
    configured_rate: float
    batch_size: int
    effective_rtt: float
    required_http_rps: float
    required_http_inflight: int
    global_concurrency_limit: int
    available_global_concurrency: int
    estimated_rate_limit: float
    constrained_resources: tuple[str, ...]


@dataclass
class QueuedSendResult:
    task_id: int
    recipient_id: int
    email: str
    success: bool
    response_text: str
    attempt_count: int
    egress_ip: str | None = None
    sender_email: str | None = None
    send_log_id: int | None = None
    status_override: str | None = None


@dataclass
class PendingEnqueueResult:
    recipient: Recipient
    success: bool
    response_text: str
    attempt_count: int
    sender_email: str | None = None
    send_log_id: int | None = None
    status_override: str | None = None


_running_tasks: dict[int, asyncio.Task] = {}
_task_runtime_metrics: dict[int, dict[str, float | int]] = {}
_task_runtime_shared_snapshots: dict[int, dict[str, float | int]] = {}
_task_lease_tasks: dict[int, asyncio.Task] = {}
# 旧 Claim 锁已废弃：发送链路只保留启动游标校准 + 内存队列，不再按批反查 send_logs。
ASYNC_LOCK_ACQUIRE_TIMEOUT_SECONDS = 30.0


@asynccontextmanager
async def _async_lock_with_timeout(lock: asyncio.Lock, *, label: str, task_id: int = 0):
    """带超时的锁上下文；锁的获取与释放由同一个 async with 严格配对。"""
    try:
        async with asyncio.timeout(ASYNC_LOCK_ACQUIRE_TIMEOUT_SECONDS):
            async with lock:
                yield
    except TimeoutError as exc:
        logger.error("任务 %s 获取 asyncio 锁超时，锁名称=%s", task_id, label)
        raise RuntimeError(f"任务内部锁等待超时（{label}）") from exc


_task_inflight_emails: dict[int, set[str]] = {}
_task_shutdown_requested: set[int] = set()
_task_shutdown_hooks: dict[int, dict[str, object]] = {}
_task_active_workers: dict[int, int] = {}
_task_performance_alerts: dict[int, dict[str, object]] = {}
_task_channel_ids: dict[int, int] = {}
_task_pid_controllers: dict[int, object] = {}
_task_queue_aware_states: dict[int, QueueAwareConcurrencyState] = {}
_delivery_unknown_alert_tasks: dict[int, asyncio.Task] = {}
_claim_cursor_writer_tasks: dict[int, asyncio.Task] = {}
_claim_cursor_writer_events: dict[int, asyncio.Event] = {}
_warmup_concurrency_reserved: set[int] = set()
_concurrency_reserve_lock = asyncio.Lock()


@dataclass
class ScheduledStartRetryState:
    task_id: int
    attempts: int = 0
    next_retry_at: float = 0.0
    reason: str = ""
    scheduled_at: datetime | None = None
    timezone: str | None = None


_scheduled_start_retries: dict[int, ScheduledStartRetryState] = {}
_scheduled_dispatcher_tasks: set[int] = set()
_scheduler_claimed_tasks: set[int] = set()
_scheduler_claimed_tasks_lock = asyncio.Lock()


def _resolve_send_proxy_log_fields(channel) -> tuple[str, str | None]:
    if channel is None:
        return "直连", None
    if bool(getattr(channel, "use_proxy", False)):
        proxy_info = mask_channel_proxy_info(channel)
        outbound_ip = str(getattr(channel, "proxy_outbound_ip", "") or "").strip() or None
        return proxy_info, outbound_ip
    return "直连", None


def _sync_runtime_send_proxy_fields(task_id: int, channel) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return
    proxy_info, outbound_ip = _resolve_send_proxy_log_fields(channel)
    runtime["send_proxy_info"] = proxy_info
    runtime["send_outbound_ip"] = outbound_ip or ""


def _runtime_send_proxy_snapshot(task_id: int) -> tuple[str, str | None]:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return "直连", None
    proxy_info = str(runtime.get("send_proxy_info") or "直连")
    outbound_raw = str(runtime.get("send_outbound_ip") or "").strip()
    return proxy_info, outbound_raw or None


_FINISH_REASON_CHANNEL_DAILY_LIMIT = "通道日限额已达上限"
_FINISH_REASON_CHANNEL_HOURLY_LIMIT = "通道小时限额已达上限"
CHANNEL_LOCK_LOST_MESSAGE = "通道任务锁已丢失，任务已自动停止以避免并发冲突"
CHANNEL_LIMIT_PAUSE_MESSAGE = "通道发送限额已达上限，任务已自动暂停，等待限额恢复后可继续"
INSTANCE_ID = str(os.getenv("HOSTNAME") or getattr(socket, "gethostname", lambda: "local")() or "local").strip().replace(":", "_")[:80]
TASK_RUNNER_LEASE_SECONDS = 120
TASK_RUNNER_LEASE_RENEW_INTERVAL_SECONDS = 15
TASK_STARTABLE_STATUSES = ("pending", "scheduled", "paused", "draft", "failed", "stopped")
REDIS_ACTIVE_WORKERS_KEY_PREFIX = "mercury:active_workers"
REDIS_ACTIVE_WORKERS_KEY = f"{REDIS_ACTIVE_WORKERS_KEY_PREFIX}:{INSTANCE_ID}"
REDIS_ACTIVE_WORKERS_INSTANCES_KEY = "mercury:active_worker_instances"
REDIS_ACTIVE_WORKERS_SYNC_TIMEOUT_SECONDS = REDIS_OPERATION_TIMEOUT_SECONDS
REDIS_ACTIVE_WORKERS_TTL_SECONDS = 7 * 24 * 60 * 60
REDIS_ACTIVE_WORKERS_FAILURE_TTL_SECONDS = 120
TASK_STATUS_CHANGED_CHANNEL = "task_status_changed"
CHANNEL_RTT_CACHE_TTL_SECONDS = 300
ANALYZE_RECIPIENTS_MIN_INTERVAL_SECONDS = 600.0
_last_recipients_analyze_at = 0.0
_task_status_versions: dict[int, int] = {}
_task_status_pubsub_task: asyncio.Task | None = None

# Redis 仅用于缓存、状态通知和监控抢占，不参与发送实时并发或槽位决策。
DEFAULT_TASK_PROXY_RTT_SECONDS = 1.5
DEFAULT_DIRECT_RTT_SECONDS = 0.05
TASK_PROXY_LATENCY_PROBE_TIMEOUT_SECONDS = 15.0
PROXY_RTT_DYNAMIC_PROBE_INTERVAL_SECONDS = 300.0
PROXY_RTT_EMA_ALPHA = 0.1
PROXY_RTT_MAX_STEP_RATIO = 0.15
AUTO_TUNE_INTERVAL_SECONDS = max(1.0, min(5.0, PID_INTERVAL_SECONDS))
AUTO_TUNE_WINDOW_SECONDS = 60.0
CONCURRENCY_ADJUST_COOLDOWN_SECONDS = 5.0
WORKER_SCALE_MAX_STEP = max(4, min(32, int(getattr(settings, "worker_scale_max_step", 12) or 12)))
WORKER_SCALE_DOWN_FACTOR = 1.5
RATE_MONITOR_CHECK_INTERVAL_SECONDS = 60.0
RATE_ALERT_THRESHOLD_RATIO = 0.8
MIN_RATE_MONITOR_ELAPSED_SECONDS = 60.0
CLAIM_PREFETCH_MIN_LIMIT = 1000
RECIPIENT_QUEUE_MAXSIZE = 100000
RECIPIENT_HUGE_LIST_THRESHOLD = 1_000_000
RECIPIENT_MEMORY_LOAD_MIN_BATCH = 1000
RECIPIENT_MEMORY_LOAD_MAX_BATCH = max(2000, min(5000, int(getattr(settings, "recipient_memory_load_max_batch", 5000) or 5000)))
PREFETCH_QUEUE_TARGET_SECONDS = 60.0
PREFETCH_QUEUE_MIN_HIGH_WATERMARK = 5000
PREFETCH_QUEUE_MAX_HIGH_WATERMARK = RECIPIENT_QUEUE_MAXSIZE
PREFETCH_PAUSE_SLEEP_SECONDS = 0.005
PREFETCH_MAX_CONSECUTIVE_FAILURES = 5
PREFETCH_RETRY_DELAY_SECONDS = 0.5
COMPLETION_AUDIT_BATCH_SIZE = max(1, int(getattr(settings, "completion_audit_batch_size", 1000) or 1000))
PREFETCH_EMPTY_BACKOFF_STREAK = 3
PREFETCH_STALLED_EMPTY_EOF_THRESHOLD = 5
# 连续 N 次预取 loaded=0，且游标已达列表末尾行号，并经 unfinished COUNT 确认后，才允许 EOF。
PREFETCH_EOF_EMPTY_CONFIRM_STREAK = 3
# claim 窗口内候选被 SendLog/退订过滤殆尽时，最多连续跳过多少个窗口以免卡死游标。
CLAIM_FILTERED_WINDOW_SKIP_MAX = 32
COMPLETION_GUARD_REWIND_LIMIT = 3
PREFETCH_BACKOFF_MAX_SECONDS = 5.0
# 多任务并行时限制全局 claim 并发；上限随配置放宽，避免高 RTT 发送下预取抖动造成 Worker 等待收件人。
# 每个 claim 都包含收件人、退订及发送日志真相校验；如 DB 压力升高，背压模块会降低预取批次。
RECIPIENT_CLAIM_GLOBAL_MAX_INFLIGHT = max(4, min(64, int(getattr(settings, "recipient_claim_global_max_inflight", 24) or 24)))
_recipient_claim_semaphore: asyncio.Semaphore | None = None
_recipient_claim_semaphore_lock = threading.Lock()


def get_result_flush_batch_size_default() -> int:
    return max(1, int(getattr(settings, "writer_batch_size", 100) or 100))


def get_result_flush_batch_size_turbo() -> int:
    return max(500, int(getattr(settings, "writer_turbo_batch_size", 500) or 500))


RESULT_FLUSH_BATCH_SIZE_DEFAULT = get_result_flush_batch_size_default()
RESULT_FLUSH_BATCH_SIZE_TURBO = get_result_flush_batch_size_turbo()
RESULT_FLUSH_INTERVAL_SECONDS_DEFAULT = 1.0
RESULT_FLUSH_INTERVAL_SECONDS_TURBO = 0.1
RESULT_QUEUE_BACKLOG_TURBO_THRESHOLD = 50
RESULT_QUEUE_BACKLOG_NORMAL_THRESHOLD = 30
RESULT_FLUSH_BATCH_SIZE = RESULT_FLUSH_BATCH_SIZE_DEFAULT
RESULT_FLUSH_INTERVAL_SECONDS = RESULT_FLUSH_INTERVAL_SECONDS_DEFAULT
RESULT_QUEUE_MAX_SIZE = 5000
CLAIM_WAIT_SECONDS = 0.05
MAIN_LOOP_CLAIM_WAIT_TIMEOUT_SECONDS = 5.0
CLAIM_QUERY_TIMEOUT_SECONDS = 20.0
CLAIM_STATEMENT_TIMEOUT_SECONDS = 15.0
CLAIM_SLOW_LOG_MS = 250.0
CLAIM_WARN_LOG_MS = 10000.0
CLAIM_QUERY_THROTTLE_THRESHOLD_MS = 500.0
CLAIM_QUERY_THROTTLE_MAX_SECONDS = 1.0
PID_PREFETCH_SCALE_BLOCK_P95_MS = 3000.0
PID_PREFETCH_SCALE_REQUIRED_QUEUE_MULTIPLIER = 3
CLAIM_QUERY_LATENCY_SAMPLE_SIZE = 240
CLAIM_DEGRADED_SUSTAINED_STREAK = 3
CLAIM_PERSIST_TIMEOUT_SECONDS = 10.0
SEND_HTTP_SLOW_LOG_MS = 3000.0
SEND_HTTP_WARN_LOG_MS = 10000.0
SEND_HTTP_CRITICAL_WARN_MS = 10000.0
INFRASTRUCTURE_SOFT_RETRY_MAX_ATTEMPTS = 3
INFRASTRUCTURE_SOFT_RETRY_TOTAL_SECONDS = 15.0
MONITOR_SEND_TIMEOUT_SECONDS = max(30.0, float(SEND_EMAIL_TIMEOUT_SECONDS) + 5.0)
CLAIM_SUMMARY_INTERVAL_SECONDS = 10.0
GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 30.0
CANCEL_RUNNER_GRACEFUL_WAIT_SECONDS = 15.0
RUNNER_EXIT_WAIT_SECONDS = 60.0
WRITER_DRAIN_TIMEOUT_SECONDS = 15.0
TASK_CONFIG_REFRESH_INTERVAL_SECONDS = 30.0
TASK_STATUS_DB_POLL_INTERVAL_SECONDS = 5.0
MAIN_LOOP_IDLE_SLEEP_SECONDS = 0.02
WRITER_FLUSH_RETRY_ATTEMPTS = 3
WRITER_FLUSH_RETRY_BASE_DELAY_SECONDS = 0.2
# 领取与游标校准共用：任意 regular SendLog 均视为已处理，不得重复 claim
CLAIM_BLOCKING_SEND_LOG_LOG_TYPE = "regular"
_HOT_PATH_ERROR_SAMPLE_INTERVAL_SECONDS = 60.0
MAIN_LOOP_STALL_TIMEOUT_SECONDS = 60.0
PREFETCH_IDLE_RECOVERY_SECONDS = 60.0
SEND_PROGRESS_STALL_RECOVERY_SECONDS = 120.0
SEND_PROGRESS_BLOCKED_SECONDS = 300.0
TASK_STATE_SNAPSHOT_INTERVAL_SECONDS = 1.0
TASK_STATE_LOG_INTERVAL_SECONDS = 30.0
# 主循环停滞保护只用于发现队列/写入死锁；HTTP 在飞批次可能因代理或 SendGrid 慢响应超过 60s，不能误判为任务失败。
HTTP_INFLIGHT_STALL_TIMEOUT_SECONDS = max(300.0, float(SEND_EMAIL_TIMEOUT_SECONDS) * 4.0)
MAIN_LOOP_DIAGNOSTIC_INTERVAL_SECONDS = 120.0
MONITOR_TRIGGER_POLL_INTERVAL_SECONDS = 5.0
MONITOR_DISTRIBUTED_CLAIM_TTL_SECONDS = max(300, int(getattr(settings, "monitor_distributed_claim_ttl_seconds", 3600) or 3600))
MONITOR_SENDER_POOL_TASK_ID_OFFSET = 1_000_000_000
MONITOR_SDK_MIN_ATTEMPTS = 3
MONITOR_DRAIN_TIMEOUT_SECONDS = max(120.0, MONITOR_SEND_TIMEOUT_SECONDS * MONITOR_SDK_MIN_ATTEMPTS + 30.0)
MONITOR_INFRASTRUCTURE_RETRY_ATTEMPTS = 3
DELIVERY_UNKNOWN_ALERT_AFTER_SECONDS = max(30.0, float(getattr(settings, "delivery_unknown_pause_after_seconds", 300.0) or 300.0))
DELIVERY_UNKNOWN_ALERT_THRESHOLD = max(1, int(getattr(settings, "delivery_unknown_pause_threshold", 1) or 1))
INFRASTRUCTURE_RETRY_BASE_SECONDS = 2.0
INFRASTRUCTURE_RETRY_MAX_SECONDS = 30.0
TASK_RUNNER_LEASE_MAX_TRANSIENT_FAILURES = 6
CLAIM_MAX_CONSECUTIVE_FAILURES = 5
CLAIM_RETRY_DELAY_SECONDS = 0.25
CLAIM_CURSOR_PERSIST_EVERY_N_BATCHES = 20
CLAIM_CURSOR_COALESCE_INTERVAL_SECONDS = 1.0
RESULT_ENQUEUE_TIMEOUT_SECONDS = 30.0
RESULT_ENQUEUE_CHUNK_SIZE = 500
RESULT_ENQUEUE_CHUNK_YIELD_SECONDS = 0.0
CLAIM_SINGLE_FETCH_MAX = RECIPIENT_MEMORY_LOAD_MAX_BATCH
RECIPIENT_FAST_CLAIM_PAGE_SIZE = RECIPIENT_MEMORY_LOAD_MAX_BATCH
CLAIM_WRITER_BACKLOG_PAUSE_THRESHOLD = 50_000
CLAIM_WRITER_BACKLOG_RESUME_THRESHOLD = 20_000
CLAIM_WRITER_BACKLOG_SLEEP_SECONDS = 0.05
HTTP_SEND_EMA_ALPHA = 0.12
# HTTP 批量在任务启动时固定；运行期只调整并发，不动态改批量。
HTTP_429_BATCH_MAX_RETRIES = 3
HTTP_429_WARN_LOG_INTERVAL_SECONDS = 60.0
WRITER_FLUSH_P99_SAMPLE_SIZE = 10
SEND_QUEUE_PUT_TIMEOUT_SECONDS = 30.0
SEND_WORKER_BATCH_FILL_MAX_WAIT_SECONDS = max(0.0, float(getattr(settings, "send_worker_batch_fill_max_wait_seconds", 0.02) or 0.02))
WRITER_MAX_RESTART_COUNT = 3
WARMUP_CONCURRENCY_PER_PLAN = 1
HTTPX_CLIENT_RELOAD_QUIESCE_TIMEOUT_SECONDS = 30.0
HTTPX_CLIENT_RELOAD_QUIESCE_POLL_SECONDS = 0.05
SEND_OUTCOME_QUEUE_MAX_SIZE = max(20_000, int(getattr(settings, "send_outcome_queue_max_size", 50_000) or 50_000))
SEND_OUTCOME_WORKER_COUNT = max(8, min(64, int(getattr(settings, "send_outcome_worker_count", 16) or 16)))
SEND_OUTCOME_WORKER_DRAIN_TIMEOUT_SECONDS = 5.0
SEND_OUTCOME_FLUSH_MAX_BATCH_ITEMS = max(1000, int(getattr(settings, "send_outcome_flush_max_batch_items", 2000) or 2000))
SEND_OUTCOME_FLUSH_MAX_WAIT_SECONDS = 0.1
WORKER_LOCAL_BATCH_INFLIGHT_KEY = "worker_local_batch_inflight"
SUCCESS_RATE_WINDOW_SECONDS = 10.0
REALTIME_RATE_WINDOW_SECONDS = 5.0
REALTIME_RATE_EMA_HALF_LIFE_SECONDS = 2.0
RATE_SAMPLE_RETENTION_SECONDS = 180.0
RATE_ALERT_EMA_WINDOW_SECONDS = 60.0
HTTP_INSTANT_RATE_WINDOW_SECONDS = 1.0
PHASE_TIMING_WINDOW_SECONDS = 30.0
PREFETCH_STATS_WINDOW_SECONDS = 10.0
HTTP_RPS_HISTORY_WINDOW_SECONDS = 60.0
LOW_RATE_SUGGESTION_WINDOW_SECONDS = 60.0
PREFETCH_EMPTY_WARN_SECONDS = 10.0
PERF_METRICS_LOG_INTERVAL_SECONDS = 5.0
TASK_STATUS_LOG_INTERVAL_SECONDS = max(10.0, float(os.getenv("TASK_STATUS_LOG_INTERVAL", "30") or 30))
LOW_RATE_DIAGNOSTIC_RATIO = 0.8
RECENT_429_LIST_WINDOW_SECONDS = 300.0
RECENT_429_WARN_WINDOW_SECONDS = 120.0
RECENT_429_WARN_COUNT = 3
_gc_suppress_refcount = 0
_gc_suppress_lock = threading.Lock()
_hot_path_error_samples: dict[str, tuple[int, float]] = {}
_send_outcome_queue_full_samples: dict[str, tuple[int, float]] = {}
_task_status_cache: dict[int, str] = {}
_task_status_wake_events: dict[int, asyncio.Event] = {}
_redis_worker_count_degraded = False
_redis_worker_count_degraded_last_log_at = 0.0
_redis_coordination_available = True
_redis_coordination_state_lock = asyncio.Lock()
_send_outcome_queue: asyncio.Queue[tuple[int, int, int] | None] | None = None
_send_outcome_worker_tasks: list[asyncio.Task] = []
_send_outcome_worker_refs = 0
_send_outcome_worker_lock = asyncio.Lock()
_global_render_threadpool: ThreadPoolExecutor | None = None
_unsubscribe_token_cache = UnsubscribeUrlCache(max_size=1000)


def configure_global_render_threadpool() -> int:
    global _global_render_threadpool
    workers = max(32, min(128, (os.cpu_count() or 4) * 4))
    if _global_render_threadpool is None:
        _global_render_threadpool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mercury-render")
        logger.debug("MercuryMail 全局渲染线程池已创建 workers=%s", workers)
    return workers


def shutdown_global_render_threadpool() -> None:
    global _global_render_threadpool
    if _global_render_threadpool is not None:
        _global_render_threadpool.shutdown(wait=False, cancel_futures=True)
        _global_render_threadpool = None
        logger.debug("MercuryMail 全局渲染线程池已关闭")


def _get_global_render_threadpool() -> ThreadPoolExecutor | None:
    return _global_render_threadpool


async def _render_template_async(template, variables) -> object:
    pool = _get_global_render_threadpool()
    if pool is None:
        return await asyncio.to_thread(render, template, variables)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(pool, render, template, variables)


async def _render_text_async(source: str, template, variables) -> str:
    pool = _get_global_render_threadpool()
    if pool is None:
        return await asyncio.to_thread(render_text, source, template, variables)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(pool, render_text, source, template, variables)


async def _increment_active_sends(runtime: dict[str, float | int], task_id: int = 0, count: int = 1) -> None:
    normalized_count = max(0, int(count or 0))
    if normalized_count <= 0:
        return
    runtime["active_sends"] = int(runtime.get("active_sends", 0) or 0) + normalized_count
    runtime["active_sends_updated_at"] = round(monotonic(), 6)


async def _decrement_active_sends(runtime: dict[str, float | int], task_id: int, count: int = 1) -> None:
    normalized_count = max(0, int(count or 0))
    if normalized_count <= 0:
        return
    runtime["active_sends"] = max(0, int(runtime.get("active_sends", 0) or 0) - normalized_count)
    runtime["active_sends_updated_at"] = round(monotonic(), 6)
    _signal_claim_slots_available(task_id)


def _build_task_channel_proxy_fingerprint(channel: Channel | None) -> tuple:
    if channel is None:
        return ("missing", 0)
    use_proxy = bool(getattr(channel, "use_proxy", False))
    proxy_id = int(getattr(channel, "proxy_id", 0) or 0)
    if not use_proxy:
        return ("direct", proxy_id)
    return ("proxy", proxy_id, channel_proxy_fingerprint(channel))


def estimate_http_requests_per_second(rate_limit: float, send_http_batch_size: int = 1) -> float:
    """估算稳态下每秒新建的 SendGrid HTTP 请求数：rate_limit / send_http_batch_size。"""
    normalized_rate = max(0.0, float(rate_limit or 0))
    normalized_batch = max(1, int(send_http_batch_size or 1))
    if normalized_rate > 0:
        return normalized_rate / float(normalized_batch)
    return 0.0


def validate_send_http_batch_config(rate_limit: float, send_http_batch_size: int) -> str | None:
    """校验 HTTP 批量大小与速率限制的基础范围。

    速率限制单位是“封/秒”，HTTP 批量大小单位是“每请求收件人数”。批量大小可以大于速率，
    发送循环会按实际收件人数逐枚消耗令牌，从而自动拉长批次间隔；强制要求 batch <= rate
    会让低速批量任务无法创建或启动，而养号路径没有这个限制。
    """
    try:
        normalized_rate = float(rate_limit or 0)
    except (TypeError, ValueError):
        return "发送速率必须是数字"
    try:
        normalized_batch = int(send_http_batch_size or DEFAULT_SEND_HTTP_BATCH_SIZE)
    except (TypeError, ValueError):
        return "批量大小必须是数字"
    if normalized_rate < 0 or normalized_rate > 100000:
        return "发送速率必须在 0 到 100000 之间"
    if normalized_batch < 1 or normalized_batch > MAX_SEND_HTTP_BATCH_SIZE:
        return f"批量大小必须在 1 到 {MAX_SEND_HTTP_BATCH_SIZE} 之间"
    return None


class GlobalConcurrencyLimitError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class ChannelNotReadyError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class TaskServiceUnavailableError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


def _log_event_loop_diagnostics(task_id: int, runtime: dict[str, float | int]) -> None:
    loop = asyncio.get_running_loop()
    asyncio_task_count = len(asyncio.all_tasks(loop))
    runtime["event_loop_tasks_count"] = asyncio_task_count
    logger.debug(
        "task %s 事件循环诊断 asyncio_tasks=%s active_sends=%s main_loop_work_ms=%s scheduling_delay_ms=%s result_queue_size=%s",
        task_id,
        asyncio_task_count,
        int(runtime.get("active_sends", 0)),
        float(runtime.get("main_loop_work_ms", 0.0)),
        float(runtime.get("event_loop_latency_ms", 0.0)),
        int(runtime.get("result_queue_size", 0)),
    )


def get_global_max_concurrency() -> int:
    """已废弃的兼容入口；返回单任务物理执行上限，不参与全局准入或任务间分配。"""
    return get_per_task_max_effective_concurrency()


def configure_global_max_concurrency(async_send_workers: int | None = None) -> int:
    """已废弃的兼容入口；不配置任何全局并发限制。"""
    _ = async_send_workers
    return get_global_max_concurrency()


def normalize_task_concurrency(concurrency: int | None) -> int:
    return max(1, min(int(concurrency or 1), 1000))


@dataclass(frozen=True)
class SendPipelinePlan:
    target_rate: float
    send_http_batch_size: int
    rtt_seconds: float
    http_rps: float
    effective_concurrency: int
    min_concurrency: int
    prefetch_limit: int
    prefetch_high_watermark: int
    result_enqueue_chunk_size: int
    local_pid_concurrency: int


def build_send_pipeline_plan(
    *,
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float,
    configured_concurrency: int | None,
    auto_concurrency_enabled: bool,
) -> SendPipelinePlan:
    batch_size = max(1, int(send_http_batch_size or 1))
    target_rate = max(0.0, float(rate_limit or 0))
    rtt = max(0.01, float(rtt_seconds or DEFAULT_DIRECT_RTT_SECONDS))
    http_rps = target_rate / float(batch_size) if target_rate > 0 else 0.0
    min_concurrency = calculate_min_concurrency(target_rate, batch_size, rtt)
    # This is the single, explicit scheduler target after task/channel policy resolution.
    effective_target_rate_rps = target_rate
    if auto_concurrency_enabled:
        effective_concurrency, _initial = resolve_effective_concurrency(
            configured_concurrency,
            target_rate,
            batch_size,
            rtt,
        )
    else:
        effective_concurrency, _initial = resolve_fixed_task_concurrency(
            configured_concurrency,
            target_rate,
            batch_size,
            rtt,
        )
    effective_concurrency = max(effective_concurrency, min_concurrency)
    prefetch_limit = calculate_prefetch_limit(target_rate)
    prefetch_high_watermark = calculate_prefetch_queue_high_watermark(target_rate)
    result_chunk = max(RESULT_ENQUEUE_CHUNK_SIZE, batch_size * max(1, effective_concurrency))
    local_pid_concurrency = effective_concurrency
    return SendPipelinePlan(
        target_rate=effective_target_rate_rps,
        send_http_batch_size=batch_size,
        rtt_seconds=rtt,
        http_rps=round(http_rps, 4),
        effective_concurrency=effective_concurrency,
        min_concurrency=min_concurrency,
        prefetch_limit=prefetch_limit,
        prefetch_high_watermark=prefetch_high_watermark,
        result_enqueue_chunk_size=result_chunk,
        local_pid_concurrency=local_pid_concurrency,
    )


def calculate_initial_concurrency(
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float,
) -> int:
    """仅在任务启动时依据固定批次计算初始并发；运行期 PID 不读取批次大小。"""
    rate = max(0.0, float(rate_limit or 0))
    batch = max(1, int(send_http_batch_size or 1))
    rtt = max(0.01, min(float(rtt_seconds or DEFAULT_TASK_PROXY_RTT_SECONDS), 30.0))
    if rate <= 0:
        return 1
    required = math.ceil((rate / batch) * (rtt + 0.10) * (1.35 if rtt < 1 else 1.25))
    startup_floor = 2 if rtt < 1 else min(16, max(4, math.ceil(rtt / 2.0)))
    return max(startup_floor, min(required + 1, get_per_task_max_effective_concurrency()))


def calculate_min_concurrency(
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float,
) -> int:
    """仅用于启动容量规划的最小并发，不参与 PID 运行期输入。"""
    return calculate_initial_concurrency(rate_limit, send_http_batch_size, rtt_seconds)


def resolve_effective_concurrency(
    user_concurrency: int | None,
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float,
) -> tuple[int, int]:
    """返回 (effective_concurrency, initial_concurrency)。

    自动并发模式下，表单里的 concurrency 只能作为启动下限，不能作为上限；否则默认值 1 会把
    前馈并发和 PID 扩容全部锁死在单 Worker，代理 RTT 较高时速率必然低于目标。
    """
    initial_concurrency = calculate_initial_concurrency(rate_limit, send_http_batch_size, rtt_seconds)
    user_value = max(0, int(user_concurrency or 0))
    effective = cap_task_effective_concurrency(max(user_value, initial_concurrency, 1))
    return effective, min(initial_concurrency, effective)


def resolve_fixed_task_concurrency(
    user_concurrency: int | None,
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float,
) -> tuple[int, int]:
    """固定并发模式：尊重用户配置；前馈值仅作为启动诊断建议，不强制覆盖。"""
    initial_concurrency = calculate_initial_concurrency(rate_limit, send_http_batch_size, rtt_seconds)
    user_value = max(1, int(user_concurrency or 1))
    effective = cap_task_effective_concurrency(user_value)
    if user_value < initial_concurrency:
        logger.warning(
            "固定并发 %s 低于推荐前馈并发 %s，固定模式将尊重用户设置",
            user_value,
            initial_concurrency,
        )
    return effective, min(initial_concurrency, effective)


def resolve_task_startup_concurrency(
    task: Task,
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float,
) -> tuple[int, int, bool]:
    """返回 (effective_concurrency, initial_concurrency, auto_concurrency_enabled)。"""
    auto_enabled = bool(getattr(task, "auto_concurrency", False))
    configured = int(task.concurrency or 0)
    if auto_enabled:
        effective, initial = resolve_effective_concurrency(
            configured,
            rate_limit,
            send_http_batch_size,
            rtt_seconds,
        )
    else:
        effective, initial = resolve_fixed_task_concurrency(
            configured,
            rate_limit,
            send_http_batch_size,
            rtt_seconds,
        )
    return effective, initial, auto_enabled


def calculate_prefetch_limit(rate_limit: float) -> int:
    rate = max(0.0, float(rate_limit or 0))
    if rate <= 0:
        return CLAIM_PREFETCH_MIN_LIMIT
    prefetch_target = int(math.ceil(rate * 120.0))
    return max(
        CLAIM_PREFETCH_MIN_LIMIT,
        min(RECIPIENT_MEMORY_LOAD_MAX_BATCH, prefetch_target),
    )


def calculate_prefetch_queue_high_watermark(rate_limit: float) -> int:
    rate = max(0.0, float(rate_limit or 0))
    if rate <= 0:
        return PREFETCH_QUEUE_MIN_HIGH_WATERMARK
    queue_target = int(math.ceil(rate * PREFETCH_QUEUE_TARGET_SECONDS))
    return max(
        PREFETCH_QUEUE_MIN_HIGH_WATERMARK,
        min(PREFETCH_QUEUE_MAX_HIGH_WATERMARK, queue_target),
    )


def channel_uses_proxy(channel: Channel | None) -> bool:
    if channel is None:
        return False
    return bool(getattr(channel, "use_proxy", False)) or bool(getattr(channel, "proxy_id", None))


async def resolve_live_channel_rtt_seconds(
    channel: Channel | None,
    *,
    fallback_seconds: float | None = None,
) -> float:
    """探测通道实时 RTT：未启用代理视为 50ms，否则 GET /v3/scopes 测时。"""
    if channel is None or not channel_uses_proxy(channel):
        return DEFAULT_DIRECT_RTT_SECONDS
    fallback = float(fallback_seconds if fallback_seconds is not None else DEFAULT_TASK_PROXY_RTT_SECONDS)
    try:
        success, latency_ms, error_message, _proxy_used = await asyncio.wait_for(
            measure_channel_proxy_latency(channel),
            timeout=TASK_PROXY_LATENCY_PROBE_TIMEOUT_SECONDS,
        )
        if success and latency_ms is not None and int(latency_ms) > 0:
            return max(0.05, int(latency_ms) / 1000.0)
        if error_message and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "通道 %s 动态 RTT 探测未成功，沿用 fallback=%ss：%s",
                getattr(channel, "id", ""),
                fallback,
                error_message,
            )
    except asyncio.TimeoutError:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "通道 %s 动态 RTT 探测超时（%s 秒），沿用 fallback=%ss",
                getattr(channel, "id", ""),
                TASK_PROXY_LATENCY_PROBE_TIMEOUT_SECONDS,
                fallback,
            )
    except Exception as exc:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "通道 %s 动态 RTT 探测异常，沿用 fallback=%ss：%s",
                getattr(channel, "id", ""),
                fallback,
                exc,
            )
    return max(0.05, fallback)


def smooth_runtime_rtt_seconds(previous_seconds: float, measured_seconds: float) -> float:
    previous = max(0.05, min(float(previous_seconds or DEFAULT_TASK_PROXY_RTT_SECONDS), 30.0))
    measured = max(0.05, min(float(measured_seconds or previous), 30.0))
    max_up = previous * (1.0 + PROXY_RTT_MAX_STEP_RATIO)
    max_down = previous * (1.0 - PROXY_RTT_MAX_STEP_RATIO)
    bounded = min(max(measured, max_down), max_up)
    return round(previous * (1.0 - PROXY_RTT_EMA_ALPHA) + bounded * PROXY_RTT_EMA_ALPHA, 4)


async def resolve_task_proxy_rtt_seconds(channel: Channel | None) -> float:
    """启动容量规划只读历史观测；禁止为启动发送额外健康探测请求。"""
    if channel is None:
        return DEFAULT_TASK_PROXY_RTT_SECONDS
    if not channel_uses_proxy(channel):
        return DEFAULT_DIRECT_RTT_SECONDS
    channel_id = int(getattr(channel, "id", 0) or 0)
    cache_key = f"channel_rtt:{channel_id}" if channel_id > 0 else ""
    if cache_key:
        try:
            from app.core.redis import redis_background_client

            cached = await redis_background_client.get(cache_key)
            if cached not in (None, ""):
                return max(0.05, float(cached))
        except Exception:
            logger.debug("通道 %s RTT 缓存读取失败，使用保守默认值", channel_id, exc_info=True)
    return DEFAULT_TASK_PROXY_RTT_SECONDS


def count_live_worker_tasks(worker_tasks: list[asyncio.Task] | None) -> int:
    if not worker_tasks:
        return 0
    return sum(1 for worker in worker_tasks if not worker.done())


def calculate_dynamic_worker_target(
    current_workers: int,
    min_concurrency: int,
    user_cap: int,
) -> int | None:
    """根据 min_con 与当前 Worker 数计算目标并发；无需调整时返回 None。"""
    current = max(1, int(current_workers))
    min_con = max(1, int(min_concurrency))
    cap = max(min_con, int(user_cap))
    shrink_threshold = max(min_con + 1, int(math.ceil(min_con * WORKER_SCALE_DOWN_FACTOR)))
    if current < min_con:
        new_target = min(current + WORKER_SCALE_MAX_STEP, min_con, cap)
        return new_target if new_target != current else None
    if current > shrink_threshold:
        new_target = max(min_con, current - WORKER_SCALE_MAX_STEP)
        return new_target if new_target != current else None
    return None


async def _try_claim_worker_retire(task_id: int) -> bool:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return False
    lock = runtime.get("worker_retire_lock")
    if not isinstance(lock, asyncio.Lock):
        return False
    async with lock:
        pending = int(runtime.get("worker_retire_count", 0))
        if pending <= 0:
            return False
        runtime["worker_retire_count"] = pending - 1
        return True


def record_task_performance_alert(
    task_id: int,
    *,
    reason: str,
    actual_rate: float,
    configured_rate: float,
) -> None:
    threshold_rate = round(float(configured_rate) * RATE_ALERT_THRESHOLD_RATIO, 2)
    alert_payload = {
        "alert_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "reason": str(reason or "").strip(),
        "actual_rate": round(float(actual_rate), 2),
        "configured_rate": round(float(configured_rate), 2),
        "threshold_rate": threshold_rate,
    }
    _task_performance_alerts[int(task_id)] = alert_payload
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is not None:
        runtime["last_performance_alert_at"] = monotonic()
        runtime["last_performance_alert_reason"] = alert_payload["reason"]


def get_task_performance_alert(task_id: int) -> dict[str, object]:
    task_key = int(task_id)
    alert = _task_performance_alerts.get(task_key)
    if not alert:
        return {}
    runtime = _task_runtime_metrics.get(task_key)
    if runtime is not None:
        configured_rate = max(0.0, float(runtime.get("rate_limit_per_second", alert.get("configured_rate", 0)) or 0))
        if configured_rate > 0:
            _success_rate, current_rate = _compute_rolling_rates(runtime)
            if current_rate <= 0:
                current_rate = float(runtime.get("current_rate", 0) or 0)
            if current_rate >= configured_rate * RATE_ALERT_THRESHOLD_RATIO:
                clear_task_performance_alert(task_key)
                return {}
    return dict(alert)


def clear_task_performance_alert(task_id: int) -> None:
    _task_performance_alerts.pop(int(task_id), None)
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is not None:
        runtime.pop("last_performance_alert_at", None)
        runtime.pop("last_performance_alert_reason", None)


async def notify_task_performance_alert(
    task_id: int,
    *,
    reason: str,
    actual_rate: float,
    configured_rate: float,
    threshold_rate: float,
) -> None:
    """记录性能告警；告警路径不执行网络、数据库或发送操作。"""
    logger.warning(
        "任务 %s 性能告警已记录 actual_rate=%s configured_rate=%s threshold_rate=%s reason=%s",
        task_id,
        actual_rate,
        configured_rate,
        threshold_rate,
        reason,
    )


async def check_send_rate_performance_alert(
    task_id: int,
    task: Task,
    runtime: dict[str, float | int],
) -> None:
    configured_rate = max(0.0, float(get_effective_second_rate_limit(task)))
    if configured_rate <= 0 or is_confirmed_prefetch_eof(runtime) or bool(runtime.get("task_finished", False)):
        clear_task_performance_alert(task_id)
        return
    started_at = float(runtime.get("started_at", 0.0) or 0.0)
    elapsed_seconds = max(0.0, monotonic() - started_at)
    if elapsed_seconds < MIN_RATE_MONITOR_ELAPSED_SECONDS:
        return
    success_count = int(runtime.get("success", 0))
    if success_count <= 0:
        return
    rolling_success_rate, rolling_current_rate = _compute_rolling_rates(runtime, RATE_ALERT_EMA_WINDOW_SECONDS)
    sample_rate = float(rolling_current_rate)
    if sample_rate <= 0:
        processed_count = int(runtime.get("processed", success_count) or success_count)
        sample_rate = round(processed_count / max(1.0, elapsed_seconds), 2)
    previous_ema = float(runtime.get("send_rate_ema_60s", sample_rate) or sample_rate)
    last_ema_at = float(runtime.get("send_rate_ema_last_at", monotonic()) or monotonic())
    now_value = monotonic()
    elapsed_ema = max(0.001, now_value - last_ema_at)
    alpha = 1.0 - math.exp(-elapsed_ema / RATE_ALERT_EMA_WINDOW_SECONDS)
    actual_rate = round(previous_ema + alpha * (sample_rate - previous_ema), 2)
    runtime["send_rate_ema_60s"] = actual_rate
    runtime["send_rate_ema_last_at"] = now_value
    runtime["send_rate_alert_sample_rps"] = round(sample_rate, 2)
    threshold_rate = configured_rate * RATE_ALERT_THRESHOLD_RATIO
    # 当前 SDK 成功速率已恢复到目标 90% 时，EMA 的启动历史滞后不应继续制造低速告警。
    if actual_rate >= threshold_rate or rolling_success_rate >= configured_rate * 0.9:
        clear_task_performance_alert(task_id)
        return
    last_alert_at = float(runtime.get("last_performance_alert_at", 0.0) or 0.0)
    if last_alert_at > 0 and monotonic() - last_alert_at < RATE_MONITOR_CHECK_INTERVAL_SECONDS:
        return
    success_rate_note = f"，SDK 成功速率 {rolling_success_rate:.2f} 封/秒" if rolling_success_rate > 0 else ""
    reason = (
        f"60 秒 EMA 处理速率 {actual_rate:.2f} 封/秒低于有效目标速率 {configured_rate:.2f} 封/秒的 "
        f"{int(RATE_ALERT_THRESHOLD_RATIO * 100)}%（阈值 {threshold_rate:.2f} 封/秒）{success_rate_note}，"
        "请检查代理延迟、并发数或 SendGrid 限流"
    )
    logger.warning("任务 %s 发送速率告警：%s", task_id, reason)
    record_task_performance_alert(
        task_id,
        reason=reason,
        actual_rate=actual_rate,
        configured_rate=configured_rate,
    )
    await notify_task_performance_alert(
        task_id,
        reason=reason,
        actual_rate=actual_rate,
        configured_rate=configured_rate,
        threshold_rate=threshold_rate,
    )


def get_per_task_max_effective_concurrency() -> int:
    """单任务不做公平比例裁剪；仅遵守 SenderPool 的物理执行上限。"""
    return max(1, min(1000, int(SENDER_POOL_MAX_WORKERS)))


def calculate_required_concurrency_for_rate(
    *,
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float | None,
) -> int:
    return calculate_initial_concurrency(
        max(0.0, float(rate_limit or 0)),
        max(1, int(send_http_batch_size or 1)),
        float(rtt_seconds or DEFAULT_TASK_PROXY_RTT_SECONDS),
    )


def calculate_task_capacity_requirement(
    *,
    rate_limit: float,
    send_http_batch_size: int,
    rtt_seconds: float | None,
) -> int:
    """为一个任务的配置速率计算必须预留的发送 Worker 数。"""
    return max(
        1,
        calculate_required_concurrency_for_rate(
            rate_limit=rate_limit,
            send_http_batch_size=send_http_batch_size,
            rtt_seconds=rtt_seconds,
        ),
    )


def cap_task_effective_concurrency(concurrency: int | None) -> int:
    per_task_max = get_per_task_max_effective_concurrency()
    return max(1, min(normalize_task_concurrency(concurrency), per_task_max))


def validate_task_concurrency_config(concurrency: int | None) -> str | None:
    try:
        normalized = int(concurrency or 1)
    except (TypeError, ValueError):
        return "任务并发数必须是整数"
    physical_limit = get_per_task_max_effective_concurrency()
    if normalized < 1 or normalized > physical_limit:
        return f"任务并发数必须在 1 到 {physical_limit} 之间（SenderPool 物理执行上限）"
    return None


def get_active_workers_total(*, exclude_task_id: int | None = None) -> int:
    if exclude_task_id is None:
        return sum(int(value) for value in _task_active_workers.values())
    excluded = int(exclude_task_id)
    return sum(int(value) for task_id, value in _task_active_workers.items() if int(task_id) != excluded)


def get_reserved_concurrency_total(*, exclude_task_id: int | None = None) -> int:
    """兼容旧指标名：返回实际活跃发送 Worker 总数。"""
    return get_active_workers_total(exclude_task_id=exclude_task_id)


async def _set_redis_coordination_state(available: bool, *, operation_name: str, task_id: int | None = None) -> None:
    global _redis_coordination_available
    normalized = bool(available)
    async with _redis_coordination_state_lock:
        changed = _redis_coordination_available != normalized
        _redis_coordination_available = normalized
    if not changed:
        return
    if normalized:
        logger.warning("Redis 服务已恢复 operation=%s task_id=%s", operation_name, task_id)
    else:
        logger.warning("Redis 暂不可用，本地并发正常，暂停同步 operation=%s task_id=%s", operation_name, task_id)


def is_redis_coordination_available() -> bool:
    return bool(_redis_coordination_available)


async def _redis_wait_with_retry(operation, *, operation_name: str, task_id: int | None = None):
    last_error: BaseException | None = None
    operation_timeout = max(0.5, float(REDIS_OPERATION_TIMEOUT_SECONDS))
    for attempt in range(1, REDIS_RETRY_ATTEMPTS + 1):
        operation_started_at = monotonic()
        result = await safe_redis_op(
            operation(),
            fallback_value=None,
            op_name=f"{operation_name}:task={task_id}:attempt={attempt}",
            timeout_seconds=operation_timeout,
        )
        operation_latency_ms = max(0.0, (monotonic() - operation_started_at) * 1000.0)
        runtime = _task_runtime_metrics.get(int(task_id or 0))
        if runtime is not None:
            samples = runtime.setdefault("redis_operation_latency_samples_ms", deque(maxlen=240))
            if hasattr(samples, "append"):
                samples.append(operation_latency_ms)
            runtime["redis_operation_latency_ms_p95"] = round(_percentile_value(list(samples), 0.95), 2)
        if result is not None:
            await _set_redis_coordination_state(True, operation_name=operation_name, task_id=task_id)
            return result
        last_error = RuntimeError(f"Redis {operation_name} 返回降级值")
        if attempt < REDIS_RETRY_ATTEMPTS:
            await asyncio.sleep((0.5, 1.0, 2.0)[min(attempt - 1, 2)])
    await _set_redis_coordination_state(False, operation_name=operation_name, task_id=task_id)
    logger.warning(
        "Redis %s 连续不可用；现有并发保持不变，Redis 恢复前拒绝新增扩容 task_id=%s attempts=%s",
        operation_name,
        task_id,
        REDIS_RETRY_ATTEMPTS,
    )
    raise last_error or RuntimeError(f"Redis {operation_name} 失败")


async def _fetch_redis_task_worker_count(task_id: int) -> int:
    global _redis_worker_count_degraded, _redis_worker_count_degraded_last_log_at
    task_key = int(task_id)
    local_count = max(0, int(_task_active_workers.get(task_key, 0) or 0))
    try:
        value = await asyncio.wait_for(
            _redis_wait_with_retry(
                lambda: redis_background_client.hget(REDIS_ACTIVE_WORKERS_KEY, str(task_key)),
                operation_name="读取任务 Worker 数",
                task_id=task_key,
            ),
            timeout=max(REDIS_OPERATION_TIMEOUT_SECONDS * REDIS_RETRY_ATTEMPTS + sum((0.5, 1.0)), 5.0),
        )
        if _redis_worker_count_degraded:
            logger.warning("Redis 已恢复；任务发送始终使用本地独立并发 task_id=%s local_count=%s", task_key, local_count)
            _redis_worker_count_degraded = False
        runtime = _task_runtime_metrics.get(task_key)
        if runtime is not None:
            runtime["redis_worker_count_degraded"] = 0
            runtime["redis_worker_count_source"] = "redis"
            runtime["redis_coordination_degraded"] = 0
            runtime["redis_coordination_mode"] = "normal"
        return max(local_count, int(value or 0))
    except Exception as exc:
        now_value = monotonic()
        if (not _redis_worker_count_degraded) or now_value - float(_redis_worker_count_degraded_last_log_at or 0.0) >= 60.0:
            logger.warning(
                "Redis 读取任务 Worker 数不可用；改用本地槽位镜像，任务可继续扩容，后台暂停同步 task_id=%s error_type=%s error=%s",
                task_key,
                type(exc).__name__,
                exc,
            )
            _redis_worker_count_degraded_last_log_at = now_value
        _redis_worker_count_degraded = True
        runtime = _task_runtime_metrics.get(task_key)
        if runtime is not None:
            runtime["redis_worker_count_degraded"] = 1
            runtime["redis_worker_count_source"] = "local_mirror"
            runtime["redis_coordination_degraded"] = 1
            runtime["redis_coordination_mode"] = "local_first_sync_paused"
            runtime["redis_worker_count_last_error"] = f"{type(exc).__name__}: {exc}"[:300]
        return local_count


async def get_global_active_workers_occupied(*, exclude_task_id: int | None = None) -> int:
    """返回当前进程活跃 Worker；Redis 不参与发送实时决策。"""
    return get_active_workers_total(exclude_task_id=exclude_task_id)


def get_active_sdk_send_coroutines_total(*, exclude_task_id: int | None = None) -> int:
    total = 0
    for task_id, runtime in list(_task_runtime_metrics.items()):
        if exclude_task_id is not None and int(task_id) == int(exclude_task_id):
            continue
        total += max(0, int((runtime or {}).get("httpx_inflight", 0) or 0))
    return total


def get_send_path_pressure_snapshot(*, exclude_task_id: int | None = None) -> dict[str, float | int]:
    total_writer_backlog = 0
    max_writer_flush_p99_ms = 0.0
    total_outstanding_writes = 0
    active_tasks = 0
    for task_id, runtime in list(_task_runtime_metrics.items()):
        if exclude_task_id is not None and int(task_id) == int(exclude_task_id):
            continue
        active_tasks += 1
        writer_backlog = max(0, int((runtime or {}).get("result_queue_size", 0) or 0))
        total_writer_backlog += writer_backlog
        total_outstanding_writes += max(0, int((runtime or {}).get("outstanding_writes", 0) or 0))
        max_writer_flush_p99_ms = max(max_writer_flush_p99_ms, float((runtime or {}).get("writer_flush_p99_ms", 0) or 0))
        writer = _get_send_log_writer(int(task_id))
        if writer is not None:
            total_writer_backlog = max(total_writer_backlog, int(writer.queue_size))
    return {
        "active_tasks": active_tasks,
        "active_sends": get_active_sdk_send_coroutines_total(exclude_task_id=exclude_task_id),
        "writer_backlog": total_writer_backlog,
        "outstanding_writes": total_outstanding_writes,
        "writer_flush_p99_ms": round(max_writer_flush_p99_ms, 2),
    }


def get_admitted_workers_total(*, exclude_task_id: int | None = None) -> int:
    """Redis 已准入的 Worker 槽位总数；不代表 HTTP 请求正在执行。"""
    return get_active_workers_total(exclude_task_id=exclude_task_id)


def get_active_http_inflight_total(*, exclude_task_id: int | None = None) -> int:
    """当前正在执行 SendGrid HTTP 请求的数量。"""
    return get_active_sdk_send_coroutines_total(exclude_task_id=exclude_task_id)


def get_executing_tasks_total(*, exclude_task_id: int | None = None) -> int:
    """当前仍有运行中 runner 的发送任务数量。"""
    return sum(
        1
        for task_id, runner in list(_running_tasks.items())
        if (exclude_task_id is None or int(task_id) != int(exclude_task_id)) and not runner.done()
    )


def get_global_concurrency_occupied(*, exclude_task_id: int | None = None) -> int:
    """兼容指标：返回进程实际发送 Worker 数，不执行全局封顶。"""
    return get_admitted_workers_total(exclude_task_id=exclude_task_id)


def update_sender_pool_runtime_metrics(
    task_id: int,
    stats: dict[str, int | float] | None,
    *,
    queue_wait_ms: float = 0.0,
    timed_out: bool = False,
) -> None:
    runtime = _task_runtime_metrics.get(int(task_id or 0))
    if runtime is None or not stats:
        return
    runtime["sender_pool_workers"] = int(stats.get("workers", 0) or 0)
    runtime["sender_pool_alive_workers"] = int(stats.get("alive_workers", 0) or 0)
    runtime["sender_pool_queue_depth"] = int(stats.get("queue_depth", 0) or 0)
    runtime["sender_pool_inflight"] = int(stats.get("inflight", 0) or 0)
    runtime["sender_pool_dropped"] = int(stats.get("dropped", 0) or 0)
    queue_wait_samples = runtime.setdefault("sender_pool_queue_wait_samples_ms", deque(maxlen=240))
    if hasattr(queue_wait_samples, "append"):
        queue_wait_samples.append(max(0.0, float(queue_wait_ms or 0.0)))
    runtime["sender_pool_queue_wait_ms"] = float(queue_wait_ms or 0.0)
    runtime["sender_pool_queue_wait_ms_p95"] = round(_percentile_value(list(queue_wait_samples), 0.95), 2)
    if timed_out:
        runtime["sender_pool_timeout_count"] = int(runtime.get("sender_pool_timeout_count", 0) or 0) + 1


async def allocate_task_worker_slots(
    task_id: int,
    worker_count: int,
    *,
    is_increment: bool = False,
    utilization: float | None = None,
    task_kind: str = "send",
) -> tuple[int, str | None]:
    """登记任务实际 Worker 数；不做系统全局槽位准入或任务间公平裁剪。"""
    _ = is_increment, utilization, task_kind
    normalized = normalize_task_concurrency(worker_count)
    if normalized <= 0:
        return 0, None
    task_key = int(task_id)
    async with _concurrency_reserve_lock:
        current = int(_task_active_workers.get(task_key, 0))
        _task_active_workers[task_key] = current + normalized
        ensure_send_threadpool_capacity(get_active_workers_total())
    return normalized, None


async def _release_reserved_worker_slots_and_sync(task_id: int, worker_count: int) -> int:
    """先释放本地槽位，再由稳定协调器异步同步 Redis。"""
    task_key = int(task_id)
    requested = max(0, int(worker_count or 0))
    async with _concurrency_reserve_lock:
        current = int(_task_active_workers.get(task_key, 0))
        released = min(current, requested)
        if current <= released:
            _task_active_workers.pop(task_key, None)
        else:
            _task_active_workers[task_key] = current - released
    return released


async def _release_task_worker_slot(task_id: int) -> None:
    released = await _release_reserved_worker_slots_and_sync(task_id, 1)
    if released <= 0:
        logger.debug("任务 %s Worker 槽位已由并发收尾统一释放，无需重复释放", int(task_id))


async def ensure_task_concurrency_reservation(task_id: int, effective_concurrency: int) -> tuple[int, str | None]:
    """在自动提升并发后追加分配 Worker 槽位。"""
    normalized = cap_task_effective_concurrency(effective_concurrency)
    current_active = int(_task_active_workers.get(int(task_id), 0))
    if normalized <= current_active:
        return current_active, None
    delta = normalized - current_active
    allocated, error_message = await allocate_task_worker_slots(task_id, delta, is_increment=True)
    if error_message is not None:
        return current_active, error_message
    return int(_task_active_workers.get(int(task_id), current_active + allocated)), None


def get_warmup_concurrency_reserved_count() -> int:
    return len(_warmup_concurrency_reserved) * WARMUP_CONCURRENCY_PER_PLAN


def check_global_concurrency_limit_sync(concurrency: int | None, *, exclude_task_id: int | None = None) -> str | None:
    """兼容入口：系统全局并发准入已移除。"""
    _ = concurrency, exclude_task_id
    return None


def _global_concurrency_limit_message(occupied: int, required: int, limit: int) -> str:
    return f"系统发送并发容量不足：当前占用 {occupied}，本次需要 {required}，上限 {limit}"


async def get_running_tasks_concurrency_total(*, exclude_task_id: int | None = None) -> int:
    from app.core.database import is_database_pool_pressure_error, maybe_warn_database_pool_pressure

    try:
        async with AsyncSessionLocal() as db:
            stmt = select(func.coalesce(func.sum(Task.concurrency), 0)).where(Task.status == "running")
            if exclude_task_id is not None:
                stmt = stmt.where(Task.id != exclude_task_id)
            return max(0, int((await db.execute(stmt)).scalar_one() or 0))
    except (OperationalError, DBAPIError, asyncio.TimeoutError) as exc:
        maybe_warn_database_pool_pressure(force=True)
        logger.warning(
            "运行中任务并发查询失败 exclude_task_id=%s error_type=%s error=%s",
            exclude_task_id,
            type(exc).__name__,
            exc,
        )
        if is_database_pool_pressure_error(exc) or _is_transient_db_error(exc):
            raise TaskServiceUnavailableError("数据库繁忙，请稍后重试") from exc
        raise


async def check_global_concurrency_limit(concurrency: int | None, *, exclude_task_id: int | None = None) -> str | None:
    """兼容入口：系统全局并发准入已移除。"""
    _ = concurrency, exclude_task_id
    return None


async def try_reserve_warmup_concurrency(plan_id: int) -> tuple[bool, str]:
    async with _concurrency_reserve_lock:
        _warmup_concurrency_reserved.add(int(plan_id))
        logger.debug(
            "warmup plan %s registered concurrency observation=%s process_send_workers=%s global_admission=false",
            plan_id,
            WARMUP_CONCURRENCY_PER_PLAN,
            get_global_concurrency_occupied(),
        )
        return True, ""


def release_warmup_concurrency(plan_id: int) -> None:
    if int(plan_id) in _warmup_concurrency_reserved:
        _warmup_concurrency_reserved.discard(int(plan_id))
        logger.debug(
            "warmup plan %s released global concurrency slot=%s remaining_total=%s",
            plan_id,
            WARMUP_CONCURRENCY_PER_PLAN,
            get_global_concurrency_occupied(),
        )


async def try_reserve_task_concurrency(task_id: int, concurrency: int | None) -> tuple[bool, str]:
    normalized = cap_task_effective_concurrency(concurrency)
    allocated, error_message = await allocate_task_worker_slots(task_id, normalized)
    if error_message is not None:
        return False, error_message
    return True, ""


async def release_task_concurrency(task_id: int) -> None:
    task_key = int(task_id)
    async with _concurrency_reserve_lock:
        released = int(_task_active_workers.pop(task_key, 0) or 0)
        _task_channel_ids.pop(task_key, None)
        expected_total = get_active_workers_total()
        if released > 0:
            logger.debug(
                "任务 %s 已释放本地并发槽位=%s，剩余槽位=%s",
                task_key,
                released,
                expected_total,
            )


async def cleanup_task_runtime_state(task_id: int) -> None:
    """清理任务删除后的进程内与 Redis 临时状态；历史 SendLog 不在此处删除。"""
    task_key = int(task_id)
    await release_task_concurrency(task_key)
    _task_runtime_metrics.pop(task_key, None)
    _task_runtime_shared_snapshots.pop(task_key, None)
    _task_inflight_emails.pop(task_key, None)
    _task_shutdown_hooks.pop(task_key, None)
    _task_shutdown_requested.discard(task_key)
    _task_performance_alerts.pop(task_key, None)
    _task_channel_ids.pop(task_key, None)
    _task_pid_controllers.pop(task_key, None)
    _task_queue_aware_states.pop(task_key, None)
    unknown_alert_task = _delivery_unknown_alert_tasks.pop(task_key, None)
    if unknown_alert_task is not None and not unknown_alert_task.done():
        unknown_alert_task.cancel()
        await asyncio.gather(unknown_alert_task, return_exceptions=True)
    cursor_writer = _claim_cursor_writer_tasks.pop(task_key, None)
    _claim_cursor_writer_events.pop(task_key, None)
    if cursor_writer is not None and not cursor_writer.done():
        cursor_writer.cancel()
        await asyncio.gather(cursor_writer, return_exceptions=True)
    clear_task_status_watch(task_key)
    _clear_unsubscribe_cache_for_task(task_key)
    try:
        from app.core.redis import redis_background_client

        redis_keys = [
            f"task:{task_key}:metrics",
            f"task:{task_key}:state",
            f"task:{task_key}:lock",
            f"task:{task_key}:status_watch",
            f"task:{task_key}:claim_cursor",
            f"task:{task_key}:url_pool_cache",
        ]
        scan_patterns = [
            f"task:{task_key}:*",
            f"url_pool:task:{task_key}:*",
        ]
        for pattern in scan_patterns:
            redis_keys.extend([str(key) async for key in redis_background_client.scan_iter(match=pattern, count=500)])
        unique_keys = sorted({key for key in redis_keys if key})
        if unique_keys:
            for offset in range(0, len(unique_keys), 500):
                await redis_background_client.delete(*unique_keys[offset : offset + 500])
    except Exception:
        logger.warning("清理任务 Redis 临时状态失败 task_id=%s，请等待 TTL 自动过期", task_key, exc_info=True)


def register_task_channel(task_id: int, channel_id: int) -> None:
    _task_channel_ids[int(task_id)] = int(channel_id or 0)


def get_channel_aggregated_worker_concurrency(
    channel_id: int,
    worker_concurrency: int | None = None,
) -> int:
    """汇总同通道所有已占用发送并发（含多任务场景），用于 httpx 连接池 sizing。"""
    channel_id = int(channel_id or 0)
    aggregated = 0
    for task_id, active_count in _task_active_workers.items():
        if _task_channel_ids.get(int(task_id)) == channel_id:
            aggregated += int(active_count)
    single_task = normalize_task_concurrency(worker_concurrency) if worker_concurrency is not None else 0
    return max(aggregated, single_task, 1)


def resolve_channel_httpx_pool_concurrency(
    channel_id: int,
    worker_concurrency: int | None = None,
) -> int:
    return min(
        SENDGRID_HTTPX_POOL_MAX_SIZE,
        max(
            SENDGRID_HTTPX_POOL_MIN_SIZE,
            get_channel_aggregated_worker_concurrency(channel_id, worker_concurrency),
        ),
    )


def _signal_claim_slots_available(task_id: int) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return
    wake_event = runtime.get("claim_slots_wake_event")
    if isinstance(wake_event, asyncio.Event) and not wake_event.is_set():
        wake_event.set()


def _record_main_loop_scheduling_delay(runtime: dict[str, float | int], now_value: float) -> None:
    prev_at = runtime.get("main_loop_prev_iter_at")
    expected_interval = float(runtime.get("main_loop_expected_interval", 0.0))
    if prev_at is not None and expected_interval > 0:
        actual_interval = now_value - float(prev_at)
        delay_ms = max(0.0, (actual_interval - expected_interval) * 1000.0)
        previous_latency = float(runtime.get("event_loop_latency_ms", 0.0))
        smoothed_latency = previous_latency * 0.7 + delay_ms * 0.3 if previous_latency > 0 else delay_ms
        runtime["event_loop_latency_ms"] = round(smoothed_latency, 2)
    runtime["main_loop_prev_iter_at"] = now_value


def _record_main_loop_work(runtime: dict[str, float | int], iter_started_at: float) -> None:
    work_ms = max(0.0, (monotonic() - float(iter_started_at)) * 1000.0)
    previous_work = float(runtime.get("main_loop_work_ms", 0.0))
    smoothed_work = previous_work * 0.7 + work_ms * 0.3 if previous_work > 0 else work_ms
    runtime["main_loop_work_ms"] = round(smoothed_work, 2)


def _finalize_main_loop_iteration(task_id: int, iter_started_at: float) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        _record_main_loop_work(runtime, iter_started_at)


async def _wait_for_claim_slots(runtime: dict[str, float | int]) -> float:
    wake_event = runtime.get("claim_slots_wake_event")
    if not isinstance(wake_event, asyncio.Event):
        await asyncio.sleep(CLAIM_WAIT_SECONDS)
        return CLAIM_WAIT_SECONDS
    if wake_event.is_set():
        wake_event.clear()
        return 0.0
    wait_started = monotonic()
    try:
        await asyncio.wait_for(wake_event.wait(), timeout=MAIN_LOOP_CLAIM_WAIT_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        pass
    wake_event.clear()
    return max(0.0, monotonic() - wait_started)


def _log_sampled_task_warning(sample_key: str, message: str, *args) -> None:
    now_value = monotonic()
    suppressed_count, last_logged_at = _hot_path_error_samples.get(sample_key, (0, 0.0))
    suppressed_count += 1
    if now_value - last_logged_at >= _HOT_PATH_ERROR_SAMPLE_INTERVAL_SECONDS:
        if suppressed_count > 1:
            logger.warning(message + " | suppressed_similar=%s", *args, suppressed_count)
        else:
            logger.warning(message, *args)
        _hot_path_error_samples[sample_key] = (0, now_value)
    else:
        _hot_path_error_samples[sample_key] = (suppressed_count, last_logged_at)


def _log_sampled_send_outcome_queue_full(channel_id: int, success: bool) -> None:
    sample_key = "send_outcome_queue_full"
    now_value = monotonic()
    suppressed_count, last_logged_at = _send_outcome_queue_full_samples.get(sample_key, (0, 0.0))
    suppressed_count += 1
    if now_value - last_logged_at >= _HOT_PATH_ERROR_SAMPLE_INTERVAL_SECONDS:
        if suppressed_count > 1:
            logger.warning(
                "发送结果队列已满 capacity=%s，丢弃 channel=%s success=%s | suppressed_similar=%s",
                SEND_OUTCOME_QUEUE_MAX_SIZE,
                channel_id,
                success,
                suppressed_count,
            )
        else:
            logger.warning(
                "发送结果队列已满 capacity=%s，丢弃 channel=%s success=%s",
                SEND_OUTCOME_QUEUE_MAX_SIZE,
                channel_id,
                success,
            )
        _send_outcome_queue_full_samples[sample_key] = (0, now_value)
    else:
        _send_outcome_queue_full_samples[sample_key] = (suppressed_count, last_logged_at)


def _decrement_outstanding_writes(task_id: int, count: int) -> None:
    if count <= 0:
        return
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        runtime["outstanding_writes"] = max(0, int(runtime.get("outstanding_writes", 0)) - count)


def _prune_recent_timestamps(timestamps: deque[float], now_value: float, window_seconds: float) -> None:
    cutoff = now_value - window_seconds
    while timestamps and timestamps[0] < cutoff:
        timestamps.popleft()


def _ensure_timestamp_deque(runtime: dict[str, float | int], key: str) -> deque[float]:
    existing = runtime.get(key)
    if isinstance(existing, deque):
        return existing
    if isinstance(existing, list):
        converted = deque(existing)
    else:
        converted = deque()
    runtime[key] = converted
    return converted


def _get_realtime_rate_tracker(runtime: dict[str, Any]) -> RealtimeRateTracker:
    tracker = runtime.get("realtime_rate_tracker")
    if isinstance(tracker, RealtimeRateTracker):
        return tracker
    tracker = RealtimeRateTracker(
        window_seconds=REALTIME_RATE_WINDOW_SECONDS,
        half_life_seconds=REALTIME_RATE_EMA_HALF_LIFE_SECONDS,
        retention_seconds=RATE_SAMPLE_RETENTION_SECONDS,
    )
    runtime["realtime_rate_tracker"] = tracker
    return tracker


def _record_realtime_rate_events(
    runtime: dict[str, Any],
    *,
    success_count: int,
    processed_count: int,
    at: float | None = None,
) -> None:
    tracker = _get_realtime_rate_tracker(runtime)
    tracker.record(success_count=success_count, processed_count=processed_count, at=at)


def _sample_realtime_rate(runtime: dict[str, Any], *, at: float | None = None) -> dict[str, float | int]:
    tracker = _get_realtime_rate_tracker(runtime)
    sample = tracker.sample(at=at)
    runtime["sdk_realtime_success_rate"] = float(sample["success_rate"])
    runtime["sdk_realtime_processed_rate"] = float(sample["processed_rate"])
    runtime["sdk_success_rate_1s"] = float(sample["success_rate"])
    runtime["sdk_current_rate_1s"] = float(sample["processed_rate"])
    runtime["sdk_success_last_5s"] = int(sample["success_count"])
    runtime["sdk_processed_last_5s"] = int(sample["processed_count"])
    runtime["sdk_realtime_window_seconds"] = float(sample["window_seconds"])
    runtime["sdk_realtime_sampled_at"] = float(sample["sampled_at"])
    runtime["sdk_raw_success_rate_5s"] = float(sample["raw_success_rate"])
    runtime["sdk_raw_processed_rate_5s"] = float(sample["raw_processed_rate"])
    return sample


def _record_throughput_sample(runtime: dict[str, float | int], *, success: bool) -> None:
    now_value = monotonic()
    processed_times = _ensure_timestamp_deque(runtime, "processed_recent_times")
    processed_times.append(now_value)
    _prune_recent_timestamps(processed_times, now_value, RATE_SAMPLE_RETENTION_SECONDS)
    if success:
        success_times = _ensure_timestamp_deque(runtime, "success_recent_times")
        success_times.append(now_value)
        _prune_recent_timestamps(success_times, now_value, RATE_SAMPLE_RETENTION_SECONDS)
    _record_realtime_rate_events(runtime, success_count=1 if success else 0, processed_count=1, at=now_value)


def _record_http_completion(runtime: dict[str, float | int]) -> None:
    now_value = monotonic()
    http_times = _ensure_timestamp_deque(runtime, "http_completion_times")
    http_times.append(now_value)
    _prune_recent_timestamps(http_times, now_value, HTTP_INSTANT_RATE_WINDOW_SECONDS)
    runtime["http_completions_last_1s"] = len(http_times)


def _record_http_send_health_metrics(runtime: dict[str, float | int], *, http_send_ms: float, result_body: str) -> None:
    if http_send_ms >= SEND_HTTP_CRITICAL_WARN_MS:
        runtime["slow_http_count"] = int(runtime.get("slow_http_count", 0)) + 1
    normalized_body = str(result_body or "")
    if "超时" in normalized_body or http_send_ms >= (SEND_EMAIL_TIMEOUT_SECONDS * 1000) - 1:
        runtime["http_timeout_count"] = int(runtime.get("http_timeout_count", 0)) + 1


HTTP_SEND_P99_SAMPLE_SIZE = 240
HTTP_SEND_LATENCY_PERCENTILE = 0.95
_channel_http_send_ms_samples: dict[int, deque[float]] = {}


def _percentile_value(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(max(0.0, float(item)) for item in values)
    index = max(0, min(len(sorted_values) - 1, math.ceil(len(sorted_values) * float(percentile)) - 1))
    return sorted_values[index]


def _robust_http_latency_ms(samples: deque[float]) -> float:
    values = [max(0.0, float(item)) for item in samples if float(item or 0) > 0]
    if not values:
        return 0.0
    baseline = sum(values) / len(values)
    filtered = [item for item in values if baseline <= 0 or item <= baseline * 3.0]
    source = filtered or values
    return round(_percentile_value(source, HTTP_SEND_LATENCY_PERCENTILE), 2)


def _record_http_send_latency_metrics(
    runtime: dict[str, float | int],
    http_send_ms: float,
    *,
    recipient_count: int = 1,
    channel_id: int | None = None,
    success: bool = True,
) -> None:
    send_ms_value = round(max(0.0, float(http_send_ms)), 2)
    normalized_recipients = max(1, int(recipient_count or 1))
    normalized_channel_id = int(channel_id or runtime.get("channel_id", 0) or 0)
    runtime["last_http_send_ms"] = send_ms_value
    runtime["batch_efficiency"] = round(send_ms_value / normalized_recipients, 2)
    samples = runtime.get("http_send_ms_samples")
    if not isinstance(samples, deque):
        samples = deque(maxlen=HTTP_SEND_P99_SAMPLE_SIZE)
        runtime["http_send_ms_samples"] = samples
    samples.append(send_ms_value)
    channel_samples = _channel_http_send_ms_samples.setdefault(normalized_channel_id, deque(maxlen=HTTP_SEND_P99_SAMPLE_SIZE))
    channel_samples.append(send_ms_value)
    robust_task_ms = _robust_http_latency_ms(samples)
    robust_channel_ms = _robust_http_latency_ms(channel_samples)
    runtime["avg_http_send_ms"] = robust_channel_ms or robust_task_ms or send_ms_value
    runtime["http_send_p95_ms"] = robust_task_ms
    runtime["channel_http_send_p95_ms"] = robust_channel_ms
    # Compatibility aliases for older API clients.
    runtime["http_send_p90_ms"] = robust_task_ms
    runtime["channel_http_send_p90_ms"] = robust_channel_ms
    sorted_samples = sorted(samples)
    p99_index = max(0, min(len(sorted_samples) - 1, math.ceil(len(sorted_samples) * 0.99) - 1))
    runtime["http_send_p99_ms"] = round(sorted_samples[p99_index], 2)
    if bool(getattr(settings, "perf_debug", False)):
        logger.info(
            "通道 %s 延迟采样 batch=%s http_ms=%s success=%s",
            normalized_channel_id,
            normalized_recipients,
            send_ms_value,
            bool(success),
        )


def _update_claim_cursor_from_recipients(task_id: int, recipients: list[Recipient]) -> None:
    if not recipients:
        return
    last_recipient = max(
        recipients,
        key=lambda recipient: (int(getattr(recipient, "row_number", 0) or 0), int(getattr(recipient, "id", 0) or 0)),
    )
    cursor_row = int(getattr(last_recipient, "row_number", 0) or 0)
    cursor_id = int(getattr(last_recipient, "id", 0) or 0)
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        current_cursor = (
            int(runtime.get("claim_cursor_row", 0) or 0),
            int(runtime.get("claim_cursor_id", 0) or 0),
        )
        if (cursor_row, cursor_id) > current_cursor:
            runtime["claim_cursor_row"] = cursor_row
            runtime["claim_cursor_id"] = cursor_id
            runtime["pending_claim_cursor_row"] = cursor_row
            runtime["pending_claim_cursor_id"] = cursor_id
    hooks = _task_shutdown_hooks.get(int(task_id))
    if hooks is not None:
        cached_task = hooks.get("cached_task")
        if cached_task is not None:
            cached_cursor = (
                int(getattr(cached_task, "last_claimed_row_number", 0) or 0),
                int(getattr(cached_task, "last_claimed_recipient_id", 0) or 0),
            )
            if (cursor_row, cursor_id) > cached_cursor:
                cached_task.last_claimed_row_number = cursor_row
                cached_task.last_claimed_recipient_id = cursor_id


def _get_send_log_writer(task_id: int) -> AsyncBatchWriter | None:
    hooks = _task_shutdown_hooks.get(int(task_id))
    if hooks is None:
        return None
    writer = hooks.get("send_log_writer")
    return writer if isinstance(writer, AsyncBatchWriter) else None


def _get_recipient_queue(task_id: int) -> asyncio.Queue | None:
    hooks = _task_shutdown_hooks.get(int(task_id))
    if hooks is None:
        return None
    queue = hooks.get("recipient_queue")
    return queue if isinstance(queue, asyncio.Queue) else None


def _record_result_queue_peak_after_batch(
    runtime: dict[str, float | int] | None,
    send_log_writer: AsyncBatchWriter | None,
) -> None:
    if runtime is None or send_log_writer is None:
        return
    runtime["result_queue_peak_after_batch"] = int(send_log_writer.queue_size)


def _compute_instant_http_rate(runtime: dict[str, float | int]) -> float:
    http_times = runtime.get("http_completion_times")
    if not isinstance(http_times, deque):
        http_times = deque(http_times if isinstance(http_times, list) else [])
    now_value = monotonic()
    _prune_recent_timestamps(http_times, now_value, HTTP_INSTANT_RATE_WINDOW_SECONDS)
    return round(len(http_times) / HTTP_INSTANT_RATE_WINDOW_SECONDS, 2)


def _compute_rolling_rates(
    runtime: dict[str, float | int],
    window_seconds: float | None = None,
) -> tuple[float, float]:
    processed_times = runtime.get("processed_recent_times")
    success_times = runtime.get("success_recent_times")
    if not isinstance(processed_times, deque):
        processed_times = deque(processed_times if isinstance(processed_times, list) else [])
    if not isinstance(success_times, deque):
        success_times = deque(success_times if isinstance(success_times, list) else [])
    window = float(window_seconds if window_seconds is not None else SUCCESS_RATE_WINDOW_SECONDS)
    if window <= 0:
        return 0.0, 0.0
    now_value = monotonic()
    cutoff = now_value - window
    processed_count = sum(1 for timestamp in processed_times if float(timestamp) >= cutoff)
    success_count = sum(1 for timestamp in success_times if float(timestamp) >= cutoff)
    if processed_count <= 0:
        return 0.0, 0.0
    success_rate = round(success_count / window, 2)
    current_rate = round(processed_count / window, 2)
    runtime["rate_window_seconds"] = round(window, 2)
    runtime["rate_window_success_count"] = int(success_count)
    runtime["rate_window_processed_count"] = int(processed_count)
    if bool(getattr(settings, "perf_debug", False)) and window == SUCCESS_RATE_WINDOW_SECONDS:
        task_id = int(runtime.get("task_id", 0) or 0)
        logger.info(
            "任务 %s 速率计算: 成功=%s 窗口=%ss 速率=%s封/秒",
            task_id,
            success_count,
            int(window) if float(window).is_integer() else round(window, 2),
            success_rate,
        )
    return success_rate, current_rate


def _compute_instant_sdk_rates(runtime: dict[str, Any]) -> tuple[float, float]:
    sample = _sample_realtime_rate(runtime)
    return float(sample["success_rate"]), float(sample["processed_rate"])


def _refresh_runtime_rate_metrics(runtime: dict[str, float | int], window_seconds: float | None = None) -> tuple[float, float]:
    success_rate, current_rate = _compute_rolling_rates(runtime, window_seconds)
    if success_rate > 0 or current_rate > 0:
        runtime["success_rate"] = success_rate
        runtime["sdk_success_rate_10s"] = success_rate
        runtime["current_rate"] = current_rate
        runtime["current_rate_10s"] = current_rate
        runtime["sdk_current_rate_10s"] = current_rate
        return success_rate, current_rate
    started_at = float(runtime.get("started_at", 0) or 0)
    elapsed_seconds = max(0.0, monotonic() - started_at) if started_at > 0 else 0.0
    if elapsed_seconds <= 0:
        runtime["success_rate"] = 0.0
        runtime["sdk_success_rate_10s"] = 0.0
        runtime["current_rate"] = 0.0
        runtime["current_rate_10s"] = 0.0
        runtime["sdk_current_rate_10s"] = 0.0
        return 0.0, 0.0
    success_total = int(runtime.get("success", 0) or 0)
    processed_total = int(runtime.get("processed", 0) or 0)
    average_success_rate = round(success_total / elapsed_seconds, 2)
    average_current_rate = round(processed_total / elapsed_seconds, 2)
    runtime["success_rate"] = average_success_rate
    runtime["sdk_success_rate_10s"] = average_success_rate
    runtime["current_rate"] = average_current_rate
    runtime["current_rate_10s"] = average_current_rate
    runtime["sdk_current_rate_10s"] = average_current_rate
    return average_success_rate, average_current_rate


def _ensure_phase_timing_samples(runtime: dict[str, float | int]) -> deque:
    samples = runtime.get("phase_timing_samples")
    if not isinstance(samples, deque):
        samples = deque(maxlen=1000)
        runtime["phase_timing_samples"] = samples
    return samples


def _record_phase_timing_sample(runtime: dict[str, float | int], **fields: float) -> None:
    sample = {
        "at": monotonic(),
        "wait_token_ms": 0.0,
        "http_request_ms": 0.0,
        "db_write_ms": 0.0,
        "preprocess_ms": 0.0,
    }
    for key, value in fields.items():
        if key in sample:
            sample[key] = round(float(value or 0), 2)
    _ensure_phase_timing_samples(runtime).append(sample)


def average_phase_timings(runtime: dict[str, float | int], window_seconds: float = PHASE_TIMING_WINDOW_SECONDS) -> dict[str, float]:
    samples = runtime.get("phase_timing_samples")
    if not isinstance(samples, deque):
        return {
            "wait_token_ms": 0.0,
            "http_request_ms": 0.0,
            "db_write_ms": 0.0,
            "preprocess_ms": 0.0,
        }
    now_value = monotonic()
    cutoff = now_value - float(window_seconds)
    totals = {
        "wait_token_ms": 0.0,
        "http_request_ms": 0.0,
        "db_write_ms": 0.0,
        "preprocess_ms": 0.0,
    }
    count = 0
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        if float(sample.get("at", 0) or 0) < cutoff:
            continue
        count += 1
        for key in totals:
            totals[key] += float(sample.get(key, 0) or 0)
    if count <= 0:
        return {key: 0.0 for key in totals}
    return {key: round(value / count, 2) for key, value in totals.items()}


def maybe_log_send_pipeline_perf(
    task_id: int,
    runtime: dict[str, float | int],
    *,
    recipient_queue: asyncio.Queue | None = None,
    send_log_writer: AsyncBatchWriter | None = None,
    interval_seconds: float = 30.0,
) -> None:
    now_value = monotonic()
    last_logged_at = float(runtime.get("last_pipeline_perf_logged_at", 0) or 0)
    if now_value - last_logged_at < float(interval_seconds):
        return
    runtime["last_pipeline_perf_logged_at"] = now_value
    phase_avg = average_phase_timings(runtime, interval_seconds)
    token_samples = list(runtime.get("token_wait_samples_ms", []) or [])
    token_samples.sort()
    token_p50 = token_samples[max(0, math.ceil(len(token_samples) * 0.50) - 1)] if token_samples else 0.0
    token_p95 = token_samples[max(0, math.ceil(len(token_samples) * 0.95) - 1)] if token_samples else 0.0
    runtime["token_wait_p50_ms"] = round(float(token_p50), 2)
    runtime["token_wait_p95_ms"] = round(float(token_p95), 2)
    success_rate, actual_rate = _compute_rolling_rates(runtime, interval_seconds)
    queue_size = int(recipient_queue.qsize()) if recipient_queue is not None else int(runtime.get("recipient_queue_size", 0) or 0)
    writer_queue_size = int(send_log_writer.queue_size) if send_log_writer is not None else int(runtime.get("result_queue_size", 0) or 0)
    batch_size = max(1, int(runtime.get("http_recipients_per_request", runtime.get("send_http_batch_size", 1)) or 1))
    http_p95_ms = max(0.0, float(runtime.get("http_send_p95_ms", runtime.get("channel_http_send_p95_ms", runtime.get("avg_http_send_ms", 0))) or 0))
    rtt_seconds = max(0.001, http_p95_ms / 1000.0 if http_p95_ms > 0 else float(runtime.get("proxy_rtt_seconds", 1.0) or 1.0))
    local_concurrency = max(0, int(runtime.get("effective_concurrency", runtime.get("active_workers", 0)) or 0))
    sender_alive = max(0, int(runtime.get("sender_pool_alive_workers", 0) or 0))
    limiter_capacity = max(0.0, float(runtime.get("effective_target_rate_rps", runtime.get("rate_limit_per_second", 0)) or 0))
    local_capacity = local_concurrency * batch_size / rtt_seconds
    sender_capacity = sender_alive * batch_size / rtt_seconds
    redis_mode = str(runtime.get("redis_coordination_mode", "normal" if is_redis_coordination_available() else "local_first_sync_paused"))
    logger.info(
        "任务 %s 容量诊断30s：目标=%s封/秒 实际=%s封/秒 成功=%s封/秒 实际批量P50=%s HTTP速率=%s请求/秒 HTTP_P95=%sms Worker=%s HTTP在飞=%s 本地PID并发=%s 本地并发等待P95=%sms SenderPool存活=%s 排队深度=%s 排队等待P95=%sms 令牌等待P95=%sms writer队列=%s 刷盘P99=%sms 最近429=%s Redis协调=%s 理论容量[本地并发=%.2f封/秒 SenderPool=%.2f封/秒 限速器=%.2f封/秒]；阶段均值[令牌=%sms HTTP=%sms DB=%sms 预处理=%sms]",
        task_id,
        limiter_capacity,
        actual_rate,
        success_rate,
        batch_size,
        float(runtime.get("http_requests_per_second", _compute_instant_http_rate(runtime)) or 0),
        http_p95_ms,
        int(runtime.get("active_workers", runtime.get("effective_concurrency", 0)) or 0),
        int(runtime.get("httpx_inflight", 0) or 0),
        local_concurrency,
        float(runtime.get("local_concurrency_wait_p95_ms", 0) or 0),
        sender_alive,
        int(runtime.get("sender_pool_queue_depth", 0) or 0),
        float(runtime.get("sender_pool_queue_wait_ms_p95", 0) or 0),
        float(runtime.get("token_wait_p95_ms", 0) or 0),
        writer_queue_size,
        float(runtime.get("writer_flush_p99_ms", 0) or 0),
        count_recent_429_events(runtime, 60.0),
        redis_mode,
        local_capacity,
        sender_capacity,
        limiter_capacity,
        phase_avg.get("wait_token_ms", 0.0),
        phase_avg.get("http_request_ms", 0.0),
        phase_avg.get("db_write_ms", 0.0),
        phase_avg.get("preprocess_ms", 0.0),
    )


def _record_prefetch_fetch(runtime: dict[str, float | int], row_count: int) -> None:
    events = runtime.get("prefetch_fetch_events")
    if not isinstance(events, deque):
        events = deque(maxlen=500)
        runtime["prefetch_fetch_events"] = events
    events.append((monotonic(), max(0, int(row_count or 0))))


def summarize_prefetch_stats(
    runtime: dict[str, float | int],
    window_seconds: float = PREFETCH_STATS_WINDOW_SECONDS,
) -> tuple[int, int]:
    events = runtime.get("prefetch_fetch_events")
    if not isinstance(events, deque):
        return 0, 0
    now_value = monotonic()
    cutoff = now_value - float(window_seconds)
    fetch_count = 0
    total_rows = 0
    for timestamp, row_count in events:
        if float(timestamp) >= cutoff:
            fetch_count += 1
            total_rows += int(row_count or 0)
    return fetch_count, total_rows


def _record_http_rps_snapshot(runtime: dict[str, float | int]) -> None:
    history = runtime.get("http_rps_history")
    if not isinstance(history, deque):
        history = deque(maxlen=60)
        runtime["http_rps_history"] = history
    now_value = monotonic()
    history.append((now_value, _compute_instant_http_rate(runtime)))
    cutoff = now_value - HTTP_RPS_HISTORY_WINDOW_SECONDS
    while history and float(history[0][0]) < cutoff:
        history.popleft()


def recent_http_rps_points(runtime: dict[str, float | int], count: int = 5) -> list[float]:
    history = runtime.get("http_rps_history")
    if not isinstance(history, deque) or not history:
        return []
    values = [round(float(item[1]), 1) for item in history]
    if len(values) <= count:
        return values
    return values[-count:]


def _update_prefetch_queue_empty_watch(runtime: dict[str, float | int], queue_size: int, now_value: float | None = None) -> None:
    current_now = now_value if now_value is not None else monotonic()
    if int(queue_size or 0) <= 0:
        if runtime.get("prefetch_queue_empty_since") in (None, 0, 0.0):
            runtime["prefetch_queue_empty_since"] = current_now
    else:
        runtime["prefetch_queue_empty_since"] = 0.0


def prefetch_queue_empty_seconds(runtime: dict[str, float | int]) -> float:
    empty_since = float(runtime.get("prefetch_queue_empty_since", 0) or 0)
    if empty_since <= 0:
        return 0.0
    return round(max(0.0, monotonic() - empty_since), 1)


def build_recent_429_list(runtime: dict[str, float | int], limit: int = 5) -> list[dict[str, object]]:
    events = runtime.get("http_429_recent_events")
    if not isinstance(events, deque):
        return []
    now_value = monotonic()
    cutoff = now_value - RECENT_429_LIST_WINDOW_SECONDS
    filtered = [item for item in events if isinstance(item, dict) and float(item.get("at", 0) or 0) >= cutoff]
    recent = filtered[-limit:]
    return [
        {
            "time": str(item.get("time") or ""),
            "retry_after": int(item.get("retry_after") or 0),
        }
        for item in reversed(recent)
    ]


def count_recent_429_events(runtime: dict[str, float | int], window_seconds: float) -> int:
    events = runtime.get("http_429_recent_events")
    if not isinstance(events, deque):
        return 0
    now_value = monotonic()
    cutoff = now_value - float(window_seconds)
    return sum(1 for item in events if isinstance(item, dict) and float(item.get("at", 0) or 0) >= cutoff)


def _compute_wait_token_ratio(phase_avg: dict[str, float]) -> float:
    wait_ms = float(phase_avg.get("wait_token_ms", 0) or 0)
    http_ms = float(phase_avg.get("http_request_ms", 0) or 0)
    db_ms = float(phase_avg.get("db_write_ms", 0) or 0)
    prep_ms = float(phase_avg.get("preprocess_ms", 0) or 0)
    total_ms = wait_ms + http_ms + db_ms + prep_ms
    if total_ms <= 0:
        return 0.0
    return wait_ms / total_ms


def evaluate_auto_tune_concurrency(
    runtime: dict[str, float | int],
    *,
    live_workers: int,
    rate_limit: float,
    per_task_max: int,
    last_adjust_at: float,
    adjust_cooldown: float = CONCURRENCY_ADJUST_COOLDOWN_SECONDS,
    tune_window_seconds: float = AUTO_TUNE_WINDOW_SECONDS,
) -> tuple[int, str] | None:
    """评估是否需调整并发；返回 (new_target, reason) 或 None。"""
    now_value = monotonic()
    if now_value - float(last_adjust_at or 0) < float(adjust_cooldown):
        return None
    if rate_limit <= 0:
        return None
    _, avg_rate = _compute_rolling_rates(runtime, tune_window_seconds)
    phase_avg = average_phase_timings(runtime, tune_window_seconds)
    wait_ratio = _compute_wait_token_ratio(phase_avg)
    recent_429 = count_recent_429_events(runtime, tune_window_seconds)
    current = max(1, int(live_workers))
    new_target = current
    reason = ""
    window_label = int(tune_window_seconds)
    if recent_429 >= 3 and avg_rate > rate_limit * 1.05:
        new_target = max(1, current - 2)
        reason = (
            f"SendGrid 限流频繁（近 {window_label} 秒 429 次数 {recent_429}），"
            f"且实际速率 {avg_rate:.1f} 封/秒超过目标 {rate_limit:.1f} 的 105%，降低并发"
        )
    elif avg_rate < rate_limit * 0.9:
        if wait_ratio < 0.2:
            new_target = min(per_task_max, current + 2)
            reason = (
                f"实际速率 {avg_rate:.1f} 封/秒低于目标 {rate_limit:.1f} 的 90%，"
                f"且令牌等待占比 {wait_ratio:.0%} 较低，扩容并发"
            )
    elif avg_rate >= rate_limit * 0.95 and wait_ratio > 0.4:
        new_target = max(1, current - 1)
        reason = (
            f"实际速率 {avg_rate:.1f} 封/秒已接近目标 {rate_limit:.1f}，"
            f"且令牌等待占比 {wait_ratio:.0%} 过高，缩容并发"
        )
    if new_target == current or not reason:
        return None
    return new_target, reason


def resolve_runtime_effective_http_rtt_seconds(runtime: dict[str, float | int] | None, fallback_seconds: float | None = None) -> float:
    runtime_ref = runtime or {}
    measured_http_ms = max(
        float(runtime_ref.get("channel_http_send_p95_ms", runtime_ref.get("channel_http_send_p90_ms", 0)) or 0),
        float(runtime_ref.get("http_send_p95_ms", runtime_ref.get("http_send_p90_ms", 0)) or 0),
        float(runtime_ref.get("avg_http_send_ms", 0) or 0),
        float(runtime_ref.get("last_http_send_ms", 0) or 0),
    )
    measured = measured_http_ms / 1000.0 if measured_http_ms > 0 else 0.0
    configured = float(runtime_ref.get("proxy_rtt_seconds", 0) or 0)
    fallback = float(fallback_seconds if fallback_seconds is not None else DEFAULT_TASK_PROXY_RTT_SECONDS)
    return max(0.05, min(max(measured, configured, fallback), 30.0))


def compute_max_theoretical_http_per_sec(runtime: dict[str, float | int]) -> float:
    rtt_seconds = resolve_runtime_effective_http_rtt_seconds(runtime)
    phase_avg = average_phase_timings(runtime)
    preprocess_seconds = max(0.0, float(phase_avg.get("preprocess_ms", 0) or 0) / 1000.0)
    cycle_seconds = max(0.05, rtt_seconds + preprocess_seconds)
    workers = max(1, int(runtime.get("effective_concurrency", runtime.get("active_workers", 1)) or 1))
    return round(workers / cycle_seconds, 2)


def _runtime_uptime_seconds(runtime: dict[str, float | int]) -> float:
    started_at = float(runtime.get("started_at", 0) or 0)
    if started_at <= 0:
        return 0.0
    return round(max(0.0, monotonic() - started_at), 1)


def _is_prefetch_starvation(runtime: dict[str, float | int], current_rate: float, rate_limit_setting: float) -> bool:
    if is_confirmed_prefetch_eof(runtime) or bool(runtime.get("prefetch_failed", False)):
        return False
    if _runtime_uptime_seconds(runtime) < max(30.0, PREFETCH_EMPTY_WARN_SECONDS * 2):
        return False
    if prefetch_queue_empty_seconds(runtime) < PREFETCH_EMPTY_WARN_SECONDS:
        return False
    if int(runtime.get("active_sends", 0) or 0) <= 0:
        return False
    if int(runtime.get("outstanding_writes", 0) or 0) > 0:
        return False
    if int(runtime.get("claim_paused_for_writer_backlog", 0) or 0) > 0:
        return False
    if bool(runtime.get("claim_query_timed_out", False)):
        return False
    if int(runtime.get("last_claim_candidate_count", 0) or 0) <= 0 and int(runtime.get("last_claim_size", 0) or 0) <= 0:
        return False
    if rate_limit_setting > 0 and current_rate >= rate_limit_setting * 0.8:
        return False
    return True


def build_suggested_action(runtime: dict[str, float | int], task: Task | None) -> str:
    if task is None:
        return ""
    rate_limit_setting = max(0.0, float(get_effective_second_rate_limit(task)))
    _, rolling_current_rate = _compute_rolling_rates(runtime)
    current_rate = float(rolling_current_rate or runtime.get("current_rate", 0) or 0)
    now_value = monotonic()
    prefetch_starvation = _is_prefetch_starvation(runtime, current_rate, rate_limit_setting)

    if rate_limit_setting > 0 and current_rate < rate_limit_setting * 0.8:
        streak_started = float(runtime.get("low_rate_streak_started_at", 0) or 0)
        if streak_started <= 0:
            runtime["low_rate_streak_started_at"] = now_value
        elif (now_value - streak_started) >= LOW_RATE_SUGGESTION_WINDOW_SECONDS:
            batch_size = max(1, int(runtime.get("send_http_batch_size", 1) or 1))
            latency_ms = max(1, int(round(float(runtime.get("proxy_rtt_seconds", DEFAULT_TASK_PROXY_RTT_SECONDS) or DEFAULT_TASK_PROXY_RTT_SECONDS) * 1000)))
            recommended = calculate_recommended_concurrency(rate_limit_setting, latency_ms, batch_size)
            if prefetch_starvation:
                return "预取队列持续为空且发送速率低于目标，请检查收件人领取是否被数据库或写入背压拖慢"
            return f"网络延迟较高，建议提高并发数至 {recommended}"
    else:
        runtime["low_rate_streak_started_at"] = 0.0

    if prefetch_starvation:
        return "预取队列持续为空且发送速率低于目标，请检查收件人领取是否被数据库或写入背压拖慢"

    write_dropped = int(runtime.get("write_dropped_total", 0) or 0)
    if write_dropped > 0:
        return "日志写入过载，已丢失部分日志，建议降低发送速率或优化磁盘"

    if count_recent_429_events(runtime, RECENT_429_WARN_WINDOW_SECONDS) >= RECENT_429_WARN_COUNT:
        return "频繁限流，请降低并发或增大批次间隔"

    return ""


def build_task_perf_diagnostics(runtime: dict[str, float | int], task: Task | None) -> dict[str, object]:
    phase_avg = average_phase_timings(runtime)
    prefetch_fetch_count, prefetch_total_rows = summarize_prefetch_stats(runtime)
    rate_limit_setting = max(0.0, float(get_effective_second_rate_limit(task))) if task is not None else float(runtime.get("rate_limit_per_second", 0) or 0)
    instant_sdk_success_rate, instant_sdk_current_rate = _compute_instant_sdk_rates(runtime)
    rolling_success_rate, rolling_current_rate = _compute_rolling_rates(runtime)
    current_rate = float(rolling_current_rate or runtime.get("current_rate", 0) or 0)
    suggested_action = build_suggested_action(runtime, task)
    channel_id = int(getattr(task, "channel_id", 0) or runtime.get("channel_id", 0) or 0)
    local_wait = float(runtime.get("local_concurrency_wait_p95_ms", 0) or 0)
    diagnostics: dict[str, object] = {
        "min_concurrency":  int(runtime.get("min_concurrency", runtime.get("initial_concurrency", 1)) or 1),
        "initial_concurrency": int(runtime.get("initial_concurrency", 1) or 1),
        "effective_concurrency": int(runtime.get("effective_concurrency", runtime.get("active_workers", 1)) or 1),
        "active_workers": int(runtime.get("active_workers", runtime.get("effective_concurrency", 1)) or 1),
        "wait_token_ms": phase_avg["wait_token_ms"],
        "token_wait_p50_ms": float(runtime.get("token_wait_p50_ms", 0) or 0),
        "token_wait_p95_ms": float(runtime.get("token_wait_p95_ms", 0) or 0),
        "rate_limiter_mode": str(runtime.get("rate_limiter_mode", settings.task_rate_limiter_mode)),
        "current_http_batch_size": int(runtime.get("send_http_batch_size", 0) or 0),
        "http_request_ms": phase_avg["http_request_ms"],
        "db_write_ms": phase_avg["db_write_ms"],
        "preprocess_ms": phase_avg["preprocess_ms"],
        "prefetch_queue_len": int(runtime.get("recipient_queue_size", 0) or 0),
        "prefetch_fetch_count": prefetch_fetch_count,
        "prefetch_total_rows": prefetch_total_rows,
        "prefetch_eof": bool(runtime.get("prefetch_eof", False)),
        "prefetch_failed": bool(runtime.get("prefetch_failed", False)),
        "active_sends": int(runtime.get("active_sends", 0) or 0),
        "outstanding_writes": int(runtime.get("outstanding_writes", 0) or 0),
        "write_queue_len": int(runtime.get("result_queue_size", 0) or 0),
        "write_dropped": int(runtime.get("write_dropped_total", 0) or 0),
        "recent_429_list": build_recent_429_list(runtime),
        "rate_limit_setting": round(rate_limit_setting, 2),
        "current_rate": round(current_rate, 2),
        "success_rate": round(rolling_success_rate, 2),
        "rate_window_seconds": int(SUCCESS_RATE_WINDOW_SECONDS),
        "rate_window_success_count": int(runtime.get("rate_window_success_count", 0) or 0),
        "rate_window_processed_count": int(runtime.get("rate_window_processed_count", 0) or 0),
        "sdk_current_rate_1s": round(instant_sdk_current_rate, 2),
        "sdk_success_rate_1s": round(instant_sdk_success_rate, 2),
        "sdk_realtime_rate": round(instant_sdk_success_rate, 2),
        "sdk_realtime_processed_rate": round(instant_sdk_current_rate, 2),
        "sdk_realtime_window_seconds": float(runtime.get("sdk_realtime_window_seconds", REALTIME_RATE_WINDOW_SECONDS) or REALTIME_RATE_WINDOW_SECONDS),
        "sdk_success_last_1s": int(runtime.get("sdk_success_last_5s", 0) or 0),
        "sdk_processed_last_1s": int(runtime.get("sdk_processed_last_5s", 0) or 0),
        "sdk_current_rate_10s": round(rolling_current_rate, 2),
        "http_requests_per_second": _compute_instant_http_rate(runtime),
        "max_theoretical_http_per_sec": compute_max_theoretical_http_per_sec(runtime),
        "suggested_action": suggested_action,
        "http_rps_recent": recent_http_rps_points(runtime),
        "prefetch_queue_empty_seconds": prefetch_queue_empty_seconds(runtime),
        "runtime_uptime_seconds": _runtime_uptime_seconds(runtime),
        "prefetch_starvation": _is_prefetch_starvation(runtime, current_rate, rate_limit_setting),
        "recent_429_count_2min": count_recent_429_events(runtime, RECENT_429_WARN_WINDOW_SECONDS),
        "auto_concurrency": int(runtime.get("auto_concurrency", runtime.get("effective_concurrency", 0)) or 0),
        "concurrency_reason": str(runtime.get("concurrency_reason") or ""),
        "pid_history": [],
        "prefetch_queue_history": list(runtime.get("prefetch_queue_history", []) or [])[-60:],
        "write_latency_percentiles": dict(runtime.get("write_latency_percentiles", {}) or {}),
        "backpressure_alert": str(runtime.get("backpressure_alert") or ""),
        "dynamic_rtt_scale_down_skipped": int(runtime.get("dynamic_rtt_scale_down_skipped", 0) or 0),
        "prefetch_limit_boosted": int(runtime.get("prefetch_limit_boosted", 0) or 0),
        "effective_prefetch_limit": int(runtime.get("effective_prefetch_limit", 0) or 0),
        "inflight_email_count": int(runtime.get("inflight_email_count", 0) or 0),
        "last_claim_candidate_count": int(runtime.get("last_claim_candidate_count", 0) or 0),
        "last_claim_skipped_reserved": int(runtime.get("last_claim_skipped_reserved", 0) or 0),
        "last_claim_skipped_duplicate_candidate": int(runtime.get("last_claim_skipped_duplicate_candidate", 0) or 0),
        "last_claim_error": str(runtime.get("last_claim_error") or ""),
        "claim_query_p50_ms": float(runtime.get("claim_query_p50_ms", 0) or 0),
        "claim_query_p95_ms": float(runtime.get("claim_query_p95_ms", 0) or 0),
        "last_claim_semaphore_wait_ms": float(runtime.get("last_claim_semaphore_wait_ms", 0) or 0),
        "last_claim_connection_setup_ms": float(runtime.get("last_claim_connection_setup_ms", 0) or 0),
        "last_claim_sql_ms": float(runtime.get("last_claim_sql_ms", 0) or 0),
        "rate_limiter_reserved_ahead_seconds": float(runtime.get("rate_limiter_reserved_ahead_seconds", 0) or 0),
        "local_concurrency_wait_p50_ms": local_wait,
        "sender_pool_queue_wait_ms_p95": float(runtime.get("sender_pool_queue_wait_ms_p95", 0) or 0),
        "local_concurrency_wait_ms_p95": local_wait,
        "local_concurrency_wait_p95_ms": local_wait,
        "redis_operation_latency_ms_p95": float(runtime.get("redis_operation_latency_ms_p95", 0) or 0),
        "monitor_schedule_drift_ms": float(runtime.get("monitor_schedule_drift_ms", 0) or 0),
        "effective_target_rate_rps": float(runtime.get("effective_target_rate_rps", runtime.get("rate_limit_per_second", 0)) or 0),
        "channel_capacity_rps": float(runtime.get("channel_capacity_rps", 0) or 0),
        "channel_active_task_count": int(runtime.get("channel_active_task_count", 0) or 0),
        "channel_fair_share_rps": float(runtime.get("channel_fair_share_rps", 0) or 0),
        "pid_latest_reason": "",
    }
    task_id = int(getattr(task, "id", 0) or runtime.get("task_id", 0) or 0)
    pid_controller = _task_pid_controllers.get(task_id)
    if pid_controller is not None and hasattr(pid_controller, "history_points"):
        diagnostics["pid_history"] = pid_controller.history_points(60)
        diagnostics["pid_latest_reason"] = pid_controller.latest_reason()
    queue_state = _task_queue_aware_states.get(task_id)
    if queue_state is not None:
        diagnostics["prefetch_limit_boosted"] = bool(queue_state.prefetch_limit_boosted)
        diagnostics["write_backlog_active"] = bool(queue_state.write_backlog_active)
        diagnostics["write_backlog_critical"] = bool(queue_state.write_backlog_critical)
    return diagnostics


def _log_task_runtime_status(task_id: int, channel_id: int, runtime: dict[str, float | int]) -> None:
    process_workers = get_active_workers_total()
    logger.info(
        "任务运行状态：任务ID=%s，实际并发=%s，PID建议并发=%s，任务最大并发=%s，本地并发等待P95=%.2f毫秒，"
        "SenderPool活跃线程=%s/%s，排队深度=%s，预取队列深度=%s，预取耗时P95=%.2f毫秒，"
        "最近429=%s，最近401/403=%s，有效目标速率=%.2f封/秒，进程发送Worker总数=%s，Redis协调=%s",
        task_id,
        int(runtime.get("effective_concurrency", _task_active_workers.get(task_id, 0)) or 0),
        int(runtime.get("pid_proposed_concurrency", runtime.get("effective_concurrency", 0)) or 0),
        int(runtime.get("capacity_pid_max_concurrency", get_per_task_max_effective_concurrency()) or get_per_task_max_effective_concurrency()),
        float(runtime.get("local_concurrency_wait_p95_ms", 0) or 0),
        int(runtime.get("sender_pool_alive_workers", 0) or 0),
        int(runtime.get("sender_pool_workers", 0) or 0),
        int(runtime.get("sender_pool_queue_depth", 0) or 0),
        int(runtime.get("recipient_queue_size", 0) or 0),
        float(runtime.get("claim_query_p95_ms", 0) or 0),
        count_recent_429_events(runtime, AUTO_TUNE_WINDOW_SECONDS),
        int(runtime.get("confirmed_send_auth_failure_count", 0) or 0),
        float(runtime.get("effective_target_rate_rps", runtime.get("rate_limit_per_second", 0)) or 0),
        process_workers,
        "正常" if is_redis_coordination_available() else "本地发送不受影响，Redis同步暂停",
    )


def _log_per_second_performance_metrics(task_id: int, runtime: dict[str, float | int]) -> None:
    phase_avg = average_phase_timings(runtime)
    prefetch_fetch_count, prefetch_total_rows = summarize_prefetch_stats(runtime)
    rate_limit_setting = float(runtime.get("rate_limit_per_second", 0) or 0)
    _, current_rate = _compute_rolling_rates(runtime)
    channel_id = int(runtime.get("channel_id", 0) or 0)
    logger.warning(
        "任务 %s 性能诊断（5秒采样） current_rate=%s/%s http_rps=%s workers=%s/%s effective=%s "
        "rtt=%s token_ms=%s http_ms=%s prep_ms=%s db_ms=%s prefetch_q=%s prefetch_10s=%s/%s "
        "write_q=%s dropped=%s http_inflight=%s local_concurrency=%s prefetch_high=%s "
        "sender_pool_queue_wait_ms_p95=%s local_concurrency_wait_ms_p95=%s "
        "redis_operation_latency_ms_p95=%s sender_pool=%s/%s queue=%s inflight=%s db_pools=%s monitor_schedule_drift_ms=%s",
        task_id,
        current_rate,
        rate_limit_setting,
        _compute_instant_http_rate(runtime),
        int(runtime.get("active_workers", 0) or 0),
        int(runtime.get("configured_concurrency", 0) or 0),
        int(runtime.get("effective_concurrency", 0) or 0),
        float(runtime.get("proxy_rtt_seconds", 0) or 0),
        phase_avg["wait_token_ms"],
        phase_avg["http_request_ms"],
        phase_avg["preprocess_ms"],
        phase_avg["db_write_ms"],
        int(runtime.get("recipient_queue_size", 0) or 0),
        prefetch_fetch_count,
        prefetch_total_rows,
        int(runtime.get("result_queue_size", 0) or 0),
        int(runtime.get("write_dropped_total", 0) or 0),
        int(runtime.get("httpx_inflight", 0) or 0),
        int(runtime.get("effective_concurrency", runtime.get("active_workers", 0)) or 0),
        int(runtime.get("prefetch_queue_high_watermark", 0) or 0),
        float(runtime.get("sender_pool_queue_wait_ms_p95", 0) or 0),
        float(runtime.get("local_concurrency_wait_p95_ms", 0) or 0),
        float(runtime.get("redis_operation_latency_ms_p95", 0) or 0),
        int(runtime.get("sender_pool_alive_workers", 0) or 0),
        int(runtime.get("sender_pool_workers", 0) or 0),
        int(runtime.get("sender_pool_queue_depth", 0) or 0),
        int(runtime.get("sender_pool_inflight", 0) or 0),
        get_all_database_pool_status(),
        float(runtime.get("monitor_schedule_drift_ms", 0) or 0),
    )
    if rate_limit_setting > 0 and current_rate < rate_limit_setting * LOW_RATE_DIAGNOSTIC_RATIO:
        logger.error(
            "task %s 速率低于目标80%%：current_rate=%s target=%s concurrency=%s min=%s active_workers=%s "
            "rtt=%s prefetch_q=%s write_q=%s token_ms=%s http_ms=%s last_claim_ms=%s claim_size=%s "
            "http429=%s sender_pool_workers=%s",
            task_id,
            current_rate,
            rate_limit_setting,
            int(runtime.get("effective_concurrency", 0) or 0),
            int(runtime.get("min_concurrency", 0) or 0),
            int(runtime.get("active_workers", 0) or 0),
            float(runtime.get("proxy_rtt_seconds", 0) or 0),
            int(runtime.get("recipient_queue_size", 0) or 0),
            int(runtime.get("result_queue_size", 0) or 0),
            phase_avg["wait_token_ms"],
            phase_avg["http_request_ms"],
            float(runtime.get("last_claim_ms", 0) or 0),
            int(runtime.get("last_claim_size", 0) or 0),
            int(runtime.get("http_429_count", 0) or 0),
            ensure_send_threadpool_capacity(int(runtime.get("effective_concurrency", 0) or 0)),
        )


def _touch_task_progress(task_id: int, now_value: float | None = None) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        runtime["last_progress_at"] = now_value if now_value is not None else monotonic()


def _touch_task_send_progress(task_id: int, now_value: float | None = None) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        runtime["last_send_progress_at"] = now_value if now_value is not None else monotonic()


def _runtime_processed_count(runtime: dict[str, float | int]) -> int:
    return (
        int(runtime.get("initial_sent_count", 0) or 0)
        + int(runtime.get("initial_failed_count", 0) or 0)
        + int(runtime.get("success", 0) or 0)
        + int(runtime.get("failed", 0) or 0)
        + int(runtime.get("delivery_unknown", 0) or 0)
    )


def _update_send_progress_watch(task_id: int, runtime: dict[str, float | int], now_value: float) -> None:
    current_processed = _runtime_processed_count(runtime)
    previous_processed = int(runtime.get("send_progress_watch_processed", -1) or -1)
    if previous_processed < 0 or current_processed > previous_processed:
        _touch_task_send_progress(task_id, now_value)
    runtime["send_progress_watch_processed"] = current_processed


def _update_stall_progress_watch(
    task_id: int,
    runtime: dict[str, float | int],
    recipient_queue: asyncio.Queue | None,
    send_log_writer: AsyncBatchWriter | None,
    now_value: float,
) -> None:
    active_sends = int(runtime.get("active_sends", 0))
    outstanding_writes = int(runtime.get("outstanding_writes", 0))
    recipient_queue_size = int(recipient_queue.qsize()) if recipient_queue is not None else 0
    previous = (
        int(runtime.get("stall_watch_active_sends", -1)),
        int(runtime.get("stall_watch_outstanding_writes", -1)),
        int(runtime.get("stall_watch_recipient_queue_size", -1)),
    )
    current = (active_sends, outstanding_writes, recipient_queue_size)
    if previous != (-1, -1, -1) and current != previous:
        _touch_task_progress(task_id, now_value)
    runtime["stall_watch_active_sends"] = active_sends
    runtime["stall_watch_outstanding_writes"] = outstanding_writes
    runtime["stall_watch_recipient_queue_size"] = recipient_queue_size


def _increment_worker_local_batch_inflight(runtime: dict[str, float | int] | None, count: int) -> None:
    if runtime is None:
        return
    normalized_count = max(0, int(count or 0))
    if normalized_count <= 0:
        return
    runtime[WORKER_LOCAL_BATCH_INFLIGHT_KEY] = int(runtime.get(WORKER_LOCAL_BATCH_INFLIGHT_KEY, 0) or 0) + normalized_count


def _decrement_worker_local_batch_inflight(runtime: dict[str, float | int] | None, count: int) -> None:
    if runtime is None:
        return
    normalized_count = max(0, int(count or 0))
    if normalized_count <= 0:
        return
    runtime[WORKER_LOCAL_BATCH_INFLIGHT_KEY] = max(
        0,
        int(runtime.get(WORKER_LOCAL_BATCH_INFLIGHT_KEY, 0) or 0) - normalized_count,
    )


def is_confirmed_prefetch_eof(runtime: dict[str, float | int] | None) -> bool:
    """EOF 只能由预取侧明确确认；禁止以协程结束或单独标志推断。"""
    if runtime is None:
        return False
    return bool(runtime.get("prefetch_eof", False)) and bool(runtime.get("prefetch_eof_confirmed", False))


def _has_pending_send_work(
    runtime: dict[str, float | int],
    recipient_queue: asyncio.Queue | None,
    send_log_writer: AsyncBatchWriter | None,
) -> bool:
    active_sends = int(runtime.get("active_sends", 0))
    worker_local_batch_inflight = int(runtime.get(WORKER_LOCAL_BATCH_INFLIGHT_KEY, 0) or 0)
    outstanding_writes = int(runtime.get("outstanding_writes", 0))
    pending_log_queue = int(send_log_writer.queue_size) if send_log_writer is not None else 0
    httpx_inflight = int(runtime.get("httpx_inflight", 0) or 0)
    if active_sends > 0 and httpx_inflight <= 0 and outstanding_writes <= 0 and pending_log_queue <= 0:
        pending_recipient_queue = int(recipient_queue.qsize()) if recipient_queue is not None else 0
        if pending_recipient_queue <= 0:
            runtime["stale_active_sends_reset_total"] = int(runtime.get("stale_active_sends_reset_total", 0) or 0) + active_sends
            runtime["stale_active_sends_last_value"] = active_sends
            runtime["active_sends"] = 0
            active_sends = 0
            logger.warning(
                "任务 %s 检测到 active_sends 残留但无 HTTP/写入/队列在途，已清零以允许正常完成 stale_active_sends=%s",
                int(runtime.get("task_id", 0) or 0),
                runtime["stale_active_sends_last_value"],
            )
    if active_sends > 0 or worker_local_batch_inflight > 0 or outstanding_writes > 0 or pending_log_queue > 0:
        return True
    if is_confirmed_prefetch_eof(runtime):
        # 这里只判断进度，绝不能消费 EOF 哨兵；否则尚未调度到的 Worker 会永久阻塞。
        pending_recipient_queue = int(recipient_queue.qsize()) if recipient_queue is not None else 0
        return pending_recipient_queue > 0
    pending_recipient_queue = int(recipient_queue.qsize()) if recipient_queue is not None else 0
    return pending_recipient_queue > 0


def _check_main_loop_stall(
    task_id: int,
    runtime: dict[str, float | int],
    recipient_queue: asyncio.Queue | None,
    send_log_writer: AsyncBatchWriter | None,
    now_value: float,
) -> str | None:
    if not _has_pending_send_work(runtime, recipient_queue, send_log_writer):
        _touch_task_progress(task_id, now_value)
        return None
    last_progress_at = float(runtime.get("last_progress_at", now_value))
    if now_value - last_progress_at < MAIN_LOOP_STALL_TIMEOUT_SECONDS:
        return None
    pending_recipient_queue = int(recipient_queue.qsize()) if recipient_queue is not None else 0
    pending_log_queue = int(send_log_writer.queue_size) if send_log_writer is not None else 0
    active_sends = int(runtime.get("active_sends", 0))
    outstanding_writes = int(runtime.get("outstanding_writes", 0))
    if active_sends > 0 and outstanding_writes <= 0 and pending_recipient_queue <= 0 and pending_log_queue <= 0:
        inflight_started_at = float(runtime.get("send_batch_http_started_at", 0) or 0)
        inflight_seconds = max(0.0, now_value - inflight_started_at) if inflight_started_at > 0 else 0.0
        if inflight_seconds < HTTP_INFLIGHT_STALL_TIMEOUT_SECONDS:
            runtime["main_loop_stall_suppressed_for_http_inflight"] = int(
                runtime.get("main_loop_stall_suppressed_for_http_inflight", 0) or 0
            ) + 1
            runtime["last_http_inflight_seconds"] = round(inflight_seconds, 1)
            _touch_task_progress(task_id, now_value)
            _log_sampled_task_warning(
                f"main_loop_stall_http_inflight:{int(task_id or 0)}",
                "任务 %s 主循环 60 秒无新进展但仍有 HTTP 在飞 active_sends=%s inflight_seconds=%s，按慢发送处理，不标记失败",
                int(task_id or 0),
                active_sends,
                round(inflight_seconds, 1),
            )
            return None
        return (
            f"任务 HTTP 在飞发送超过 {int(HTTP_INFLIGHT_STALL_TIMEOUT_SECONDS)} 秒无完成，需要启动自愈："
            f"活跃发送数={active_sends} "
            f"待写入数={outstanding_writes} "
            f"收件人队列={pending_recipient_queue} "
            f"结果队列={pending_log_queue}"
        )
    return (
        f"任务主循环超过 {int(MAIN_LOOP_STALL_TIMEOUT_SECONDS)} 秒无进展，需要启动自愈："
        f"活跃发送数={active_sends} "
        f"待写入数={outstanding_writes} "
        f"收件人队列={pending_recipient_queue} "
        f"结果队列={pending_log_queue}"
    )


class TaskRateLimiter:
    """任务级共享 GCRA 限速器；锁内仅预约虚拟发送时间，等待发生在锁外。"""

    def __init__(self, rate: float, mode: str = "smooth"):
        self.rate_limit = max(0.0, min(float(rate or 0), 100000.0))
        self.max_rate = self.rate_limit
        self.mode = "smooth"
        self._lock = asyncio.Lock()
        self._tat = monotonic()
        if str(mode or "smooth").strip().lower() != "smooth":
            logger.warning("任务限速器仅支持平滑 GCRA，已忽略 mode=%s", mode)

    @property
    def reserved_ahead_seconds(self) -> float:
        """尚未到期的发送时间预约；值过大表示 Worker 已足够，继续扩容只会增加等待协程。"""
        return max(0.0, self._tat - monotonic())

    async def acquire(self, amount: float = 1.0) -> float:
        requested = max(0.0, float(amount or 0))
        if requested <= 0 or self.rate_limit <= 0:
            return 0.0
        started = monotonic()
        max_reservation_window = 5.0
        while True:
            wait_for_window = 0.0
            async with self._lock:
                now_value = monotonic()
                reserved_ahead = max(0.0, self._tat - now_value)
                if reserved_ahead >= max_reservation_window:
                    wait_for_window = reserved_ahead - max_reservation_window + min(0.05, requested / self.rate_limit)
                else:
                    interval = requested / self.rate_limit
                    scheduled_at = max(now_value, self._tat)
                    wait_seconds = max(0.0, scheduled_at - now_value)
                    self._tat = scheduled_at + interval
                    break
            await asyncio.sleep(max(0.001, wait_for_window))
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        return max(0.0, (monotonic() - started) * 1000.0)


def build_rate_limiter(rate_limit: float | int | None) -> TaskRateLimiter | None:
    normalized_rate = max(0.0, min(float(rate_limit or 0), 100000.0))
    if normalized_rate <= 0:
        return None
    return TaskRateLimiter(normalized_rate, settings.task_rate_limiter_mode)


def _suppress_gc_for_task() -> None:
    global _gc_suppress_refcount
    with _gc_suppress_lock:
        _gc_suppress_refcount += 1
        should_disable = _gc_suppress_refcount == 1
    if should_disable and gc.isenabled():
        gc.disable()


def _restore_gc_for_task() -> None:
    global _gc_suppress_refcount
    with _gc_suppress_lock:
        _gc_suppress_refcount = max(0, _gc_suppress_refcount - 1)
        should_enable = _gc_suppress_refcount == 0
    if should_enable and not gc.isenabled():
        gc.enable()
    try:
        gc.collect(0)
    except Exception:
        logger.debug("任务级 gen0 GC 回收失败", exc_info=True)


@dataclass
class RateLimiterHolder:
    """任务级共享令牌桶；所有 Worker 从同一个单调时钟限速器取令牌。"""

    per_second_rate: float
    limiter: TaskRateLimiter | None
    worker_count: int = 1
    task_id: int = 0

    def __post_init__(self) -> None:
        self.worker_count = max(1, int(self.worker_count or 1))

    def refresh(self, new_rate: float) -> bool:
        normalized_rate = max(0.0, min(float(new_rate), 100000.0))
        if abs(normalized_rate - float(self.per_second_rate or 0.0)) < 0.001:
            return False
        previous_rate = float(self.per_second_rate or 0.0)
        self.per_second_rate = normalized_rate
        if self.limiter is not None and normalized_rate > 0:
            # 保留已有虚拟时间预约。替换限速器会让新桶从“现在”重新起步，而旧桶中已等待
            # 的 Worker 随后仍会放行，热更新瞬间会叠加两组流量并破坏平滑速率。
            self.limiter.rate_limit = normalized_rate
            self.limiter.max_rate = normalized_rate
        else:
            self.limiter = build_rate_limiter(normalized_rate)
        logger.info(
            "任务 %s 独立限速器已热更新 previous_rate=%.2f new_rate=%.2f，保留既有发送时间预约",
            self.task_id,
            previous_rate,
            normalized_rate,
        )
        return True


def validate_rate_limiter_holder(holder: RateLimiterHolder, label: str) -> str | None:
    if holder.limiter is None:
        return None
    if float(holder.limiter.rate_limit) <= 0:
        return f"{label}速率限制配置无效（每秒 {holder.per_second_rate} 封），请调整任务或通道速率限制"
    return None


def resolve_send_http_batch_size_value(value: int | None) -> int:
    configured = int(value or 0)
    if configured <= 0:
        configured = get_default_send_http_batch_size()
    return max(1, min(configured, MAX_SEND_HTTP_BATCH_SIZE))


def get_send_http_batch_size(task: Task | None) -> int:
    if task is None:
        return DEFAULT_SEND_HTTP_BATCH_SIZE
    return resolve_send_http_batch_size_value(getattr(task, "send_http_batch_size", None))


async def acquire_rate_token(limiter: TaskRateLimiter | None) -> None:
    if limiter is None:
        return
    await limiter.acquire(1)


async def acquire_rate_tokens(limiter: TaskRateLimiter | None, count: int) -> float:
    """按实际收件人数从全局令牌桶取令牌，只保护总速率，不安排批次先后顺序。"""
    normalized_count = max(0, int(count or 0))
    if limiter is None or normalized_count <= 0:
        return 0.0
    max_chunk = max(1, int(float(limiter.max_rate)))
    remaining = normalized_count
    waited_ms = 0.0
    while remaining > 0:
        chunk = min(remaining, max_chunk)
        started = monotonic()
        measured = await limiter.acquire(chunk)
        waited_ms += float(measured if isinstance(measured, (int, float)) else (monotonic() - started) * 1000.0)
        remaining -= chunk
    return waited_ms


async def acquire_worker_rate_tokens(
    holder: RateLimiterHolder | None,
    worker_id: int | None,
    count: int,
) -> None:
    """兼容调用入口：所有 Worker 直接共享任务级令牌桶，不再二次本地限速。"""
    _ = worker_id
    normalized_count = max(0, int(count or 0))
    if holder is None or normalized_count <= 0:
        return
    waited_ms = await acquire_rate_tokens(holder.limiter, normalized_count)
    runtime = _task_runtime_metrics.get(int(getattr(holder, "task_id", 0) or 0))
    if runtime is not None:
        samples = runtime.get("token_wait_samples_ms")
        if not isinstance(samples, deque):
            samples = deque(maxlen=500)
            runtime["token_wait_samples_ms"] = samples
        samples.append(float(waited_ms or 0.0))
        runtime["rate_limiter_reserved_ahead_seconds"] = round(
            float(holder.limiter.reserved_ahead_seconds if holder.limiter is not None else 0.0),
            3,
        )


def chunk_recipients_for_sdk_batch(recipients: list[Recipient], batch_size: int | None) -> list[list[Recipient]]:
    """按 SDK HTTP 批量大小切分收件人；禁止将批量失败重试退化为逐封发送。"""
    normalized_batch_size = max(1, int(batch_size or 1))
    if not recipients:
        return []
    return [recipients[index : index + normalized_batch_size] for index in range(0, len(recipients), normalized_batch_size)]


def _normalize_positive_limit(value: int | None) -> int | None:
    if value is None:
        return None
    normalized = int(value or 0)
    return normalized if normalized > 0 else None


def _safe_loaded_relationship(obj: object, relationship_name: str) -> object | None:
    state = getattr(obj, "__dict__", {})
    if isinstance(state, dict) and relationship_name in state:
        return state.get(relationship_name)
    return None


def get_effective_second_rate_limit(task: Task) -> float:
    task_limit = max(0.0, float(task.rate_limit or 0))
    if task_limit > 0:
        return task_limit
    channel = _safe_loaded_relationship(task, "channel")
    channel_second_limit = _normalize_positive_limit(getattr(channel, "second_limit", None)) if channel is not None else None
    if channel_second_limit is not None:
        return float(channel_second_limit)
    return 0.0


def get_monitor_rate_limit(per_second_rate: float) -> float:
    normalized_rate = max(0.0, float(per_second_rate or 0))
    if normalized_rate <= 0:
        return 0.0
    return max(1.0, min(normalized_rate * 0.1, 5.0))


def has_reached_channel_limit(sent_count: int, limit: int | None) -> bool:
    normalized_limit = _normalize_positive_limit(limit)
    return normalized_limit is not None and sent_count >= normalized_limit


async def _send_outcome_worker_loop(worker_index: int) -> None:
    while True:
        queue = _send_outcome_queue
        if queue is None:
            return
        item = await queue.get()
        drained_items = [item]
        try:
            if item is None:
                return
            deadline = monotonic() + SEND_OUTCOME_FLUSH_MAX_WAIT_SECONDS
            while len(drained_items) < SEND_OUTCOME_FLUSH_MAX_BATCH_ITEMS:
                timeout_seconds = max(0.0, deadline - monotonic())
                if timeout_seconds <= 0:
                    break
                try:
                    next_item = await asyncio.wait_for(queue.get(), timeout=timeout_seconds)
                except asyncio.TimeoutError:
                    break
                drained_items.append(next_item)
                if next_item is None:
                    break
            aggregated: dict[int, list[int]] = {}
            stop_after_flush = False
            for queued_item in drained_items:
                if queued_item is None:
                    stop_after_flush = True
                    continue
                channel_id, success_count, failure_count = queued_item
                bucket = aggregated.setdefault(int(channel_id), [0, 0])
                bucket[0] += max(0, int(success_count or 0))
                bucket[1] += max(0, int(failure_count or 0))
            for channel_id, (success_count, failure_count) in aggregated.items():
                try:
                    await record_send_outcomes(channel_id, success_count=success_count, failure_count=failure_count)
                except Exception:
                    logger.warning(
                        "发送结果 Worker %s channel %s record_send_outcomes 失败 success=%s failure=%s",
                        worker_index,
                        channel_id,
                        success_count,
                        failure_count,
                        exc_info=True,
                    )
            if stop_after_flush:
                return
        finally:
            for _ in drained_items:
                queue.task_done()


async def _acquire_send_outcome_worker(*, task_id: int | None = None, channel_id: int | None = None) -> None:
    global _send_outcome_queue, _send_outcome_worker_tasks, _send_outcome_worker_refs
    async with _send_outcome_worker_lock:
        if _send_outcome_queue is None:
            _send_outcome_queue = asyncio.Queue(maxsize=SEND_OUTCOME_QUEUE_MAX_SIZE)
            logger.info(
                "发送结果队列已初始化 task_id=%s channel_id=%s capacity=%s",
                task_id or "",
                channel_id or "",
                SEND_OUTCOME_QUEUE_MAX_SIZE,
            )
        alive_workers = [task for task in _send_outcome_worker_tasks if not task.done()]
        if len(alive_workers) < SEND_OUTCOME_WORKER_COUNT:
            for worker_index in range(len(alive_workers), SEND_OUTCOME_WORKER_COUNT):
                _send_outcome_worker_tasks.append(
                    asyncio.create_task(
                        _send_outcome_worker_loop(worker_index),
                        name=f"send-outcome-record-worker-{worker_index}",
                    )
                )
        _send_outcome_worker_refs += 1


async def _release_send_outcome_worker(*, drain_timeout: float = SEND_OUTCOME_WORKER_DRAIN_TIMEOUT_SECONDS) -> None:
    global _send_outcome_queue, _send_outcome_worker_tasks, _send_outcome_worker_refs
    async with _send_outcome_worker_lock:
        _send_outcome_worker_refs = max(0, _send_outcome_worker_refs - 1)
        if _send_outcome_worker_refs > 0:
            return
        queue = _send_outcome_queue
        worker_tasks = [task for task in _send_outcome_worker_tasks if not task.done()]
    if queue is not None:
        try:
            await asyncio.wait_for(queue.join(), timeout=drain_timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "发送结果队列排空超时 %ss pending=%s",
                drain_timeout,
                queue.qsize(),
            )
    async with _send_outcome_worker_lock:
        if _send_outcome_worker_refs > 0:
            return
        queue_ref = _send_outcome_queue
        worker_tasks = [task for task in _send_outcome_worker_tasks if not task.done()]
        if queue_ref is not None and worker_tasks:
            for _ in worker_tasks:
                try:
                    queue_ref.put_nowait(None)
                except asyncio.QueueFull:
                    logger.warning("停止 Worker 时发送结果队列已满，正在取消 Worker 任务")
                    break
            for worker_task in worker_tasks:
                try:
                    await asyncio.wait_for(worker_task, timeout=drain_timeout)
                except asyncio.TimeoutError:
                    worker_task.cancel()
                    try:
                        await worker_task
                    except asyncio.CancelledError:
                        pass
                except Exception:
                    logger.exception("发送结果 Worker 关闭失败")
        _send_outcome_worker_tasks = []
        _send_outcome_queue = None


def _schedule_record_send_outcome(channel_id: int, success: bool) -> None:
    _schedule_record_send_outcomes(channel_id, success_count=1 if success else 0, failure_count=0 if success else 1)


def _ensure_send_outcome_queue_sync(channel_id: int) -> asyncio.Queue[tuple[int, int, int] | None]:
    global _send_outcome_queue
    queue = _send_outcome_queue
    if queue is None:
        logger.warning("发送结果队列异常丢失，正在自动重建；当前批次发送不受影响 channel=%s", channel_id)
        queue = asyncio.Queue(maxsize=SEND_OUTCOME_QUEUE_MAX_SIZE)
        _send_outcome_queue = queue
        for worker_index in range(SEND_OUTCOME_WORKER_COUNT):
            _send_outcome_worker_tasks.append(
                asyncio.create_task(
                    _send_outcome_worker_loop(worker_index),
                    name=f"send-outcome-record-worker-recovered-{worker_index}",
                )
            )
        if _send_outcome_worker_refs <= 0:
            globals()["_send_outcome_worker_refs"] = 1
    return queue


def _schedule_record_send_outcomes(channel_id: int, *, success_count: int = 0, failure_count: int = 0) -> None:
    normalized_success = max(0, int(success_count or 0))
    normalized_failure = max(0, int(failure_count or 0))
    if normalized_success + normalized_failure <= 0:
        return
    queue = _ensure_send_outcome_queue_sync(int(channel_id or 0))
    try:
        queue.put_nowait((int(channel_id), normalized_success, normalized_failure))
    except asyncio.QueueFull:
        # Health telemetry is best-effort. Never create unbounded Redis tasks from the
        # send result path when its isolated queue is saturated.
        _log_sampled_send_outcome_queue_full(int(channel_id), normalized_failure <= 0)


async def _reconcile_task_claim_cursor_from_send_logs(task_id: int) -> tuple[int, int]:
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            return 0, 0
        current_cursor = (
            max(0, int(task.last_claimed_row_number or 0)),
            max(0, int(task.last_claimed_recipient_id or 0)),
        )
        if not task.recipient_list_id:
            return current_cursor
        result = await db.execute(
            text(
                """
                SELECT r.row_number AS cursor_row, r.id AS cursor_id
                FROM recipients AS r
                WHERE r.list_id = :list_id
                  AND r.row_number > 0
                  AND (
                      EXISTS (
                          SELECT 1
                          FROM compact_outcomes AS outcome
                          WHERE outcome.task_id = :task_id
                            AND outcome.recipient_id = r.id
                            AND COALESCE(outcome.log_type, 'regular') = 'regular'
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM send_logs AS log
                          WHERE log.task_id = :task_id
                            AND log.recipient_id = r.id
                            AND COALESCE(log.log_type, 'regular') = 'regular'
                            AND LOWER(COALESCE(log.status, '')) IN (
                                'sent', 'success', 'delivered', 'failed', 'bounced', 'blocked', 'dropped',
                                'unknown', 'delivery_unknown'
                            )
                      )
                  )
                ORDER BY r.row_number DESC, r.id DESC
                LIMIT 1
                """
            ),
            {"task_id": int(task_id), "list_id": int(task.recipient_list_id)},
        )
        completed_row = result.first()
        completed_cursor = (
            max(0, int(completed_row.cursor_row or 0)),
            max(0, int(completed_row.cursor_id or 0)),
        ) if completed_row is not None else (0, 0)
        # 持久游标可能位于已预取但尚未完成的记录之后。恢复时必须以结果账本为上限，
        # 取 min(current, completed) 才能重新扫描游标之后的未完成缺口；唯一约束负责跳过已处理记录。
        reconciled_cursor = min(current_cursor, completed_cursor) if current_cursor > (0, 0) else completed_cursor
        if reconciled_cursor != current_cursor:
            await db.execute(
                update(Task)
                .where(Task.id == int(task_id))
                .values(
                    last_claimed_row_number=reconciled_cursor[0],
                    last_claimed_recipient_id=reconciled_cursor[1],
                    updated_at=datetime.utcnow(),
                )
            )
            await commit_session_with_retry(db)
        logger.info(
            "task %s 启动游标校准完成 current_cursor=%s completed_cursor=%s reconciled_cursor=%s",
            task_id,
            current_cursor,
            completed_cursor,
            reconciled_cursor,
        )
        return reconciled_cursor


async def _get_persisted_regular_result_counts(task_id: int) -> tuple[int, int]:
    success_statuses = {"success"}
    failed_statuses = {"failed", "bounced"}

    def _count_expr(model, statuses: set[str]):
        return (
            select(func.count())
            .select_from(model)
            .where(
                model.task_id == int(task_id),
                func.coalesce(model.log_type, "regular") == "regular",
                func.lower(func.coalesce(model.status, "")).in_(statuses),
            )
        )

    try:
        async with AsyncSessionLocal() as db:
            hot_success = int((await db.execute(_count_expr(CompactOutcome, success_statuses))).scalar_one() or 0)
            hot_failed = int((await db.execute(_count_expr(CompactOutcome, failed_statuses))).scalar_one() or 0)
            return hot_success, hot_failed
    except Exception:
        logger.exception("任务 %s 从发送日志恢复初始成功/失败计数失败，将回退使用 tasks 计数字段", task_id)
        return 0, 0


async def reconcile_task_resume_state(task_id: int) -> tuple[int, int, tuple[int, int]]:
    """恢复任务前用已落库发送日志校准计数和领取游标，避免断点续发读到收尾期旧状态。"""
    task_key = int(task_id)
    cursor_row, cursor_id = await _reconcile_task_claim_cursor_from_send_logs(task_key)
    persisted_success_count, persisted_failed_count = await _get_persisted_regular_result_counts(task_key)
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == task_key))).scalar_one_or_none()
        if task is None:
            return persisted_success_count, persisted_failed_count, (cursor_row, cursor_id)
        current_success = int(task.sent_count or 0)
        current_failed = int(task.failed_count or 0)
        next_success = max(current_success, persisted_success_count)
        next_failed = max(current_failed, persisted_failed_count)
        if next_success != current_success or next_failed != current_failed:
            task.sent_count = next_success
            task.failed_count = next_failed
            task.updated_at = datetime.utcnow()
            await db.commit()
            logger.info(
                "任务 %s 恢复前计数校准 sent=%s->%s failed=%s->%s cursor=(%s,%s)",
                task_key,
                current_success,
                next_success,
                current_failed,
                next_failed,
                cursor_row,
                cursor_id,
            )
        else:
            await db.rollback()
    return persisted_success_count, persisted_failed_count, (cursor_row, cursor_id)


def _record_claim_success(task_id: int, claimed_count: int) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return
    runtime["last_claim_size"] = int(claimed_count)
    runtime["claimed_recipients_total"] = int(runtime.get("claimed_recipients_total", 0)) + int(claimed_count)
    runtime["claim_success_cycles"] = int(runtime.get("claim_success_cycles", 0)) + 1
    now_value = monotonic()
    last_summary_at = float(runtime.get("last_claim_summary_at", now_value))
    elapsed_summary = now_value - last_summary_at
    if elapsed_summary < CLAIM_SUMMARY_INTERVAL_SECONDS:
        return
    logger.debug(
        "task %s claim summary interval=%ss claimed=%s cycles=%s last_claim_size=%s last_claim_ms=%s",
        task_id,
        round(elapsed_summary, 1),
        int(runtime.get("claimed_recipients_total", 0)),
        int(runtime.get("claim_success_cycles", 0)),
        int(runtime.get("last_claim_size", 0)),
        runtime.get("last_claim_ms", 0),
    )
    runtime["last_claim_summary_at"] = now_value
    runtime["claimed_recipients_total"] = 0
    runtime["claim_success_cycles"] = 0


def _cleanup_runner(task_id: int, task: asyncio.Task) -> None:
    current = _running_tasks.get(task_id)
    if current is task:
        _running_tasks.pop(task_id, None)
    lease_task = _task_lease_tasks.pop(int(task_id), None)
    if lease_task is not None and not lease_task.done():
        lease_task.cancel()
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        logger.debug("任务 %s 后台 asyncio 任务清理：已取消", task_id)
        return
    if exc is not None:
        logger.exception("任务 %s 后台 asyncio 任务清理异常崩溃", task_id, exc_info=exc)
    else:
        logger.debug("任务 %s 后台 asyncio 任务清理：已完成", task_id)


async def _drain_result_writer_for_task(task_id: int, timeout: float = WRITER_DRAIN_TIMEOUT_SECONDS) -> bool:
    hooks = _task_shutdown_hooks.get(int(task_id))
    if not hooks:
        return True
    writer = hooks.get("send_log_writer")
    if not isinstance(writer, AsyncBatchWriter):
        return True
    try:
        await writer.flush_and_stop(timeout=timeout)
        return True
    except Exception as exc:
        logger.error("任务 %s SendLog 写入器刷盘停止失败：%s", task_id, exc)
        return False


async def wait_for_task_runner_exit(task_id: int, timeout: float = RUNNER_EXIT_WAIT_SECONDS) -> bool:
    deadline = monotonic() + max(0.1, float(timeout))
    while monotonic() < deadline:
        current = _running_tasks.get(int(task_id))
        if current is None or current.done():
            _running_tasks.pop(int(task_id), None)
            return True
        await asyncio.sleep(0.1)
    return not is_task_runner_active(task_id)


def _normalize_inflight_email(email: str | None) -> str:
    return str(email or "").strip().lower()


def _inflight_recipient_key(recipient: Recipient) -> str:
    recipient_id = int(getattr(recipient, "id", 0) or 0)
    if recipient_id > 0:
        return f"id:{recipient_id}"
    return f"email:{_normalize_inflight_email(str(getattr(recipient, 'email', '') or ''))}"


def _recipient_handled_keys(recipient: Recipient) -> list[str]:
    """同一收件人可能同时用 id: 与 email: 键标记，查询时需全部比对。"""
    keys: list[str] = []
    recipient_id = int(getattr(recipient, "id", 0) or 0)
    if recipient_id > 0:
        keys.append(f"id:{recipient_id}")
    email = _normalize_inflight_email(str(getattr(recipient, "email", "") or ""))
    if email:
        keys.append(f"email:{email}")
    return keys


def _is_recipient_in_key_set(key_set: set[str] | set[int] | None, recipient: Recipient) -> bool:
    if not key_set:
        return False
    recipient_id = int(getattr(recipient, "id", 0) or 0)
    if recipient_id > 0 and recipient_id in key_set:
        return True
    return any(key in key_set for key in _recipient_handled_keys(recipient))


def _mark_completed_recipient(completed_recipient_ids: set[int] | None, recipient: Recipient) -> None:
    """发送 API 结果已产生后立即记录 recipient_id，不等待 SendLog 落盘。"""
    if completed_recipient_ids is None:
        return
    recipient_id = int(getattr(recipient, "id", 0) or 0)
    if recipient_id > 0:
        completed_recipient_ids.add(recipient_id)


def _mark_completed_recipients(completed_recipient_ids: set[int] | None, recipients: list[Recipient]) -> None:
    if completed_recipient_ids is None:
        return
    for recipient in recipients:
        _mark_completed_recipient(completed_recipient_ids, recipient)


def _release_inflight_recipients(inflight_emails: set[str], recipients: list[Recipient]) -> None:
    for recipient in recipients:
        for key in _recipient_handled_keys(recipient):
            inflight_emails.discard(key)


def _resolve_recipients_rollback_cursor(recipients: list[Recipient]) -> tuple[int, int] | None:
    positions = sorted(
        (
            max(0, int(getattr(recipient, "row_number", 0) or 0)),
            max(0, int(getattr(recipient, "id", 0) or 0)),
        )
        for recipient in recipients
        if int(getattr(recipient, "row_number", 0) or 0) > 0
    )
    if not positions:
        return None
    first_row, first_id = positions[0]
    # Claim 使用 (row_number, id) 的严格大于比较。回滚必须同时处理两列；
    # 只回退 row 而保留旧 id 会跳过同 row_number 中 id 较小的未发送记录。
    return first_row, max(0, first_id - 1)


def _claim_cursor_rollback_floor(runtime: dict[str, float | int] | None) -> tuple[int, int]:
    if runtime is None:
        return 0, 0
    return (
        max(0, int(runtime.get("startup_cursor_row", 0) or 0)),
        max(0, int(runtime.get("startup_cursor_id", 0) or 0)),
    )


def _clamp_rollback_cursor(
    runtime: dict[str, float | int] | None,
    rollback_cursor: tuple[int, int],
) -> tuple[int, int]:
    normalized = max(0, int(rollback_cursor[0] or 0)), max(0, int(rollback_cursor[1] or 0))
    return max(_claim_cursor_rollback_floor(runtime), normalized)


def _rollback_prefetch_cursor_for_unqueued(
    runtime: dict[str, float | int] | None,
    task_id: int,
    unqueued: list[Recipient],
) -> None:
    if runtime is None or not unqueued:
        return
    pending_unqueued = list(unqueued)
    if not pending_unqueued:
        return
    rollback_cursor = _resolve_recipients_rollback_cursor(pending_unqueued)
    if rollback_cursor is None:
        return
    rollback_cursor = _clamp_rollback_cursor(runtime, rollback_cursor)
    current_cursor = (
        int(runtime.get("claim_cursor_row", 0) or 0),
        int(runtime.get("claim_cursor_id", 0) or 0),
    )
    if rollback_cursor >= current_cursor:
        return
    runtime["claim_cursor_row"], runtime["claim_cursor_id"] = rollback_cursor
    runtime["pending_claim_cursor_row"], runtime["pending_claim_cursor_id"] = rollback_cursor
    logger.info(
        "任务 %s 预取游标回退：%s -> %s（%s 个收件人未入队）",
        task_id,
        current_cursor,
        rollback_cursor,
        len(pending_unqueued),
    )


def _try_reserve_inflight_recipient(
    inflight_emails: set[str] | None,
    recipient: Recipient,
) -> bool:
    if inflight_emails is None:
        return True
    key = _inflight_recipient_key(recipient)
    if not key:
        return True
    if key in inflight_emails or _is_recipient_in_key_set(inflight_emails, recipient):
        return False
    inflight_emails.add(key)
    # 同时登记 email 键，避免 id/email 键不一致导致漏拦
    email = _normalize_inflight_email(str(getattr(recipient, "email", "") or ""))
    if email:
        inflight_emails.add(f"email:{email}")
    return True


def _reserve_inflight_recipients(
    inflight_emails: set[str] | None,
    recipients: list[Recipient],
) -> None:
    if inflight_emails is None:
        return
    for recipient in recipients:
        _try_reserve_inflight_recipient(inflight_emails, recipient)


def _rollback_unsent_recipients_for_resume(
    runtime: dict[str, float | int] | None,
    task_id: int,
    recipients: list[Recipient],
    inflight_emails: set[str] | None,
    *,
    reason: str,
) -> None:
    pending_recipients = list(recipients)
    if runtime is None or not pending_recipients:
        if inflight_emails is not None and recipients:
            _release_inflight_recipients(inflight_emails, recipients)
        return
    rollback_cursor = _resolve_recipients_rollback_cursor(pending_recipients)
    if rollback_cursor is None:
        if inflight_emails is not None:
            _release_inflight_recipients(inflight_emails, recipients)
        return
    rollback_cursor = _clamp_rollback_cursor(runtime, rollback_cursor)
    current_cursor = (
        int(runtime.get("claim_cursor_row", 0) or runtime.get("pending_claim_cursor_row", 0) or 0),
        int(runtime.get("claim_cursor_id", 0) or runtime.get("pending_claim_cursor_id", 0) or 0),
    )
    if rollback_cursor < current_cursor:
        runtime["claim_cursor_row"], runtime["claim_cursor_id"] = rollback_cursor
        runtime["pending_claim_cursor_row"], runtime["pending_claim_cursor_id"] = rollback_cursor
        runtime["claim_persist_pending_batches"] = 0
        runtime["http_batch_persist_pending_count"] = 0
    runtime["claim_cursor_sealed_for_shutdown"] = True
    if inflight_emails is not None:
        _release_inflight_recipients(inflight_emails, recipients)
    runtime["paused_unsent_recipients_requeued"] = int(runtime.get("paused_unsent_recipients_requeued", 0) or 0) + len(pending_recipients)
    logger.warning(
        "任务 %s %s，%s 个已预取但未发送收件人将回滚到 cursor=%s，恢复后继续发送",
        task_id,
        reason,
        len(pending_recipients),
        rollback_cursor,
    )


async def _drain_unsent_recipient_queue_for_resume(
    task_id: int,
    recipient_queue: asyncio.Queue[Recipient | None] | None,
    inflight_emails: set[str] | None,
    runtime: dict[str, float | int] | None,
    *,
    reason: str,
) -> int:
    if recipient_queue is None:
        return 0
    drained: list[Recipient] = []
    discarded_sentinels = 0
    while True:
        try:
            item = recipient_queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            if item is None:
                discarded_sentinels += 1
                continue
            drained.append(item)
        finally:
            recipient_queue.task_done()
    if drained:
        _rollback_unsent_recipients_for_resume(runtime, task_id, drained, inflight_emails, reason=reason)
    if runtime is not None:
        runtime["recipient_queue_size"] = int(recipient_queue.qsize())
        runtime["discarded_shutdown_sentinels"] = int(runtime.get("discarded_shutdown_sentinels", 0) or 0) + discarded_sentinels
    return len(drained)


async def _cancel_claim_cursor_writer(task_id: int) -> None:
    """停止可能仍持有旧前进游标的合并写入器，避免暂停回滚后被迟到写入覆盖。"""
    writer = _claim_cursor_writer_tasks.pop(int(task_id), None)
    _claim_cursor_writer_events.pop(int(task_id), None)
    if writer is not None and not writer.done():
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)


async def _persist_runtime_claim_cursor_now(task_id: int, runtime: dict[str, float | int] | None) -> None:
    if runtime is None:
        return
    cursor = int(runtime.get("pending_claim_cursor_row", 0) or runtime.get("claim_cursor_row", 0) or 0)
    cursor_id = int(runtime.get("pending_claim_cursor_id", 0) or runtime.get("claim_cursor_id", 0) or 0)
    if cursor < 0:
        cursor = 0
    if cursor_id < 0:
        cursor_id = 0
    await _persist_claim_cursor_to_db(task_id, cursor, cursor_id, force=True, allow_decrease=True)


def _get_task_inflight_emails(task_id: int) -> set[str] | None:
    return _task_inflight_emails.get(int(task_id))


async def _count_regular_send_logs(task_id: int) -> int:
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM send_logs
                    WHERE task_id = :task_id
                      AND COALESCE(log_type, 'regular') = :regular_log_type
                    """
                ),
                {
                    "task_id": int(task_id),
                    "regular_log_type": CLAIM_BLOCKING_SEND_LOG_LOG_TYPE,
                },
            )
            return max(0, int(result.scalar_one() or 0))
    except Exception as exc:
        logger.warning(
            "任务 %s 统计 regular SendLog 数量失败 error_type=%s error=%s",
            int(task_id),
            type(exc).__name__,
            exc,
        )
        return -1


async def _claim_cursor_coalescing_writer(task_id: int, wake_event: asyncio.Event) -> None:
    """每任务单一 writer：合并短时间内的游标推进，只写入最新值。"""
    try:
        while True:
            await wake_event.wait()
            wake_event.clear()
            await asyncio.sleep(CLAIM_CURSOR_COALESCE_INTERVAL_SECONDS)
            runtime = _task_runtime_metrics.get(task_id)
            if runtime is None:
                return
            cursor_row = int(runtime.get("pending_claim_cursor_row", 0) or runtime.get("claim_cursor_row", 0) or 0)
            cursor_id = int(runtime.get("pending_claim_cursor_id", 0) or runtime.get("claim_cursor_id", 0) or 0)
            if cursor_row <= 0 and cursor_id <= 0:
                continue
            await _persist_claim_cursor_to_db(task_id, cursor_row, cursor_id, force=True)
            runtime["http_batch_persist_pending_count"] = 0
    finally:
        _claim_cursor_writer_tasks.pop(task_id, None)
        _claim_cursor_writer_events.pop(task_id, None)


def _schedule_claim_cursor_persist(task_id: int) -> None:
    wake_event = _claim_cursor_writer_events.get(task_id)
    if wake_event is None:
        wake_event = asyncio.Event()
        _claim_cursor_writer_events[task_id] = wake_event
    writer = _claim_cursor_writer_tasks.get(task_id)
    if writer is None or writer.done():
        writer = asyncio.create_task(
            _claim_cursor_coalescing_writer(task_id, wake_event),
            name=f"claim-cursor-writer-{task_id}",
        )
        _claim_cursor_writer_tasks[task_id] = writer
    wake_event.set()


async def _persist_claim_cursor_after_http_batch(task_id: int) -> None:
    """批量完成只更新 pending 状态，由每任务单一 writer 合并刷盘。"""
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None or not bool(runtime.get("send_http_batch_enabled", False)):
        return
    pending_cursor = int(runtime.get("pending_claim_cursor_row", 0) or runtime.get("claim_cursor_row", 0) or 0)
    if pending_cursor <= 0:
        return
    runtime["http_batch_persist_pending_count"] = int(runtime.get("http_batch_persist_pending_count", 0)) + 1
    _schedule_claim_cursor_persist(task_id)


async def cancel_task_runner(task_id: int, *, graceful_timeout: float = CANCEL_RUNNER_GRACEFUL_WAIT_SECONDS) -> bool:
    current = _running_tasks.get(task_id)
    if current is None or current.done():
        _running_tasks.pop(task_id, None)
        return False
    logger.warning("任务 %s 请求协作式停机后再取消", task_id)
    _task_shutdown_requested.add(int(task_id))
    wait_budget = GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS + graceful_timeout + WRITER_DRAIN_TIMEOUT_SECONDS
    try:
        await asyncio.wait_for(asyncio.shield(current), timeout=wait_budget)
        _task_shutdown_requested.discard(int(task_id))
        return True
    except asyncio.TimeoutError:
        logger.error("任务 %s 协作式停机在 %ss 后超时", task_id, wait_budget)
    except Exception:
        logger.exception("任务 %s 协作式停机失败", task_id)
    if not current.done():
        await _drain_result_writer_for_task(task_id, timeout=graceful_timeout)
        if not current.done():
            logger.warning("任务 %s 在协作式停机超时后强制取消 asyncio.Task", task_id)
            current.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(current), timeout=10.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
    _task_shutdown_requested.discard(int(task_id))
    if _running_tasks.get(task_id) is current and current.done():
        _running_tasks.pop(task_id, None)
    return True


def _task_zoneinfo(timezone_name: str | None) -> ZoneInfo:
    name = (timezone_name or DEFAULT_TASK_TIMEZONE).strip() or DEFAULT_TASK_TIMEZONE
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        logger.warning("不支持的任务时区=%s；已回退到=%s", timezone_name, DEFAULT_TASK_TIMEZONE)
        return ZoneInfo(DEFAULT_TASK_TIMEZONE)


def _normalize_task_utc_datetime(value: datetime | None) -> datetime:
    current_utc = value or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        return current_utc.replace(tzinfo=timezone.utc)
    return current_utc.astimezone(timezone.utc)


def _scheduled_time_from_value(scheduled_at: datetime) -> datetime_time:
    return scheduled_at.timetz().replace(tzinfo=None) if scheduled_at.tzinfo is not None else scheduled_at.time()


def scheduled_local_to_utc(scheduled_at: datetime, timezone_name: str | None) -> datetime:
    local_time = scheduled_at.replace(tzinfo=None)
    local_zone = _task_zoneinfo(timezone_name)
    return local_time.replace(tzinfo=local_zone).astimezone(timezone.utc)


def next_task_run_utc(scheduled_at: datetime | None, timezone_name: str | None, now_utc: datetime | None = None) -> datetime | None:
    if scheduled_at is None:
        return None
    current_utc = _normalize_task_utc_datetime(now_utc)
    local_zone = _task_zoneinfo(timezone_name)
    local_now = current_utc.astimezone(local_zone)
    scheduled_time = _scheduled_time_from_value(scheduled_at)
    today_due_local = datetime.combine(local_now.date(), scheduled_time, tzinfo=local_zone)
    if local_now <= today_due_local:
        next_due_local = today_due_local
    else:
        next_due_local = datetime.combine(local_now.date() + timedelta(days=1), scheduled_time, tzinfo=local_zone)
    return next_due_local.astimezone(timezone.utc)


def is_task_due_now(scheduled_at: datetime | None, timezone_name: str | None, now_utc: datetime | None = None) -> bool:
    if scheduled_at is None:
        return False
    current_utc = _normalize_task_utc_datetime(now_utc)
    due_utc = scheduled_local_to_utc(scheduled_at, timezone_name)
    next_run_utc = next_task_run_utc(scheduled_at, timezone_name, current_utc)
    if current_utc < due_utc:
        logger.debug(
            "task scheduled start not due yet scheduled_at_local=%s timezone=%s due_utc=%s next_run_utc=%s",
            scheduled_at,
            timezone_name or DEFAULT_TASK_TIMEZONE,
            due_utc.isoformat(),
            next_run_utc.isoformat() if next_run_utc else "",
        )
        return False
    return True


def _loads_json_email_list_text(text: str) -> list[str]:
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        # 兼容早期部署中以逗号/分号/换行保存的监控邮箱文本，避免配置存在却被静默解析为空。
        parsed = re.split(r"[,;\r\n]+", text)
    if isinstance(parsed, str):
        parsed = re.split(r"[,;\r\n]+", parsed)
    if not isinstance(parsed, list):
        return []
    emails: list[str] = []
    seen: set[str] = set()
    for item in parsed:
        email = str(item or "").strip()
        if email and email not in seen:
            emails.append(email)
            seen.add(email)
    return emails


async def _parse_json_email_list(raw) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    return await asyncio.to_thread(_loads_json_email_list_text, text)


async def parse_monitor_emails_from_task(task: Task) -> list[str]:
    """读取 monitor_emails 为主列；仅当其为空时回退 monitor_emails_json（迁移兼容）。"""
    primary = await _parse_json_email_list(getattr(task, "monitor_emails", None))
    if primary:
        legacy_json = await _parse_json_email_list(getattr(task, "monitor_emails_json", None))
        if legacy_json and legacy_json != primary:
            logger.warning(
                "任务 %s monitor_emails 两列内容不一致，primary=%s json=%s；将使用 monitor_emails",
                getattr(task, "id", ""),
                primary,
                legacy_json,
            )
        return primary
    fallback = await _parse_json_email_list(getattr(task, "monitor_emails_json", None))
    if fallback:
        logger.warning(
            "任务 %s monitor_emails 主列为空，已从 monitor_emails_json 兼容列加载 %s 个监控邮箱",
            getattr(task, "id", ""),
            len(fallback),
        )
    return fallback


async def _parse_monitor_emails(task: Task) -> list[str]:
    return await parse_monitor_emails_from_task(task)


def _get_monitor_success_count(task_id: int, runtime: dict[str, float | int]) -> int:
    """只读取指定任务的普通邮件成功数，禁止跨任务共享计数。"""
    normalized_task_id = int(task_id or 0)
    runtime_task_id = int(runtime.get("task_id", 0) or 0)
    if normalized_task_id <= 0 or runtime_task_id != normalized_task_id:
        logger.error(
            "监控成功计数任务隔离校验失败 task_id=%s runtime_task_id=%s，已拒绝触发",
            normalized_task_id,
            runtime_task_id,
        )
        return 0
    # sent_count 可能来自较早快照或异步计数写入；监控里程碑必须取所有任务内成功来源的最大值，
    # 否则真实成功数已跨过里程碑但陈旧 sent_count 会让监控触发长期滞后。
    runtime_success_count = max(
        0,
        int(runtime.get("initial_sent_count", 0) or 0) + int(runtime.get("success", 0) or 0),
    )
    task_success_count = max(
        runtime_success_count,
        int(runtime.get("sent_count", 0) or 0),
        int(runtime.get("live_success_count", 0) or 0),
    )
    runtime["sent_count"] = task_success_count
    return task_success_count


def _is_monitor_sampling_enabled(monitor_interval: int, monitor_emails: list[str]) -> bool:
    if int(monitor_interval or 0) <= 0:
        return False
    return bool(monitor_emails)


def _monitor_milestone_floor(success_count: int, monitor_interval: int) -> int:
    interval = max(1, int(monitor_interval or 1))
    count = max(0, int(success_count or 0))
    if count < interval:
        return 0
    return (count // interval) * interval


def _next_monitor_milestone(last_milestone: int, monitor_interval: int) -> int:
    interval = max(1, int(monitor_interval or 1))
    return int(last_milestone or 0) + interval


def _monitor_trigger_marker(trigger_count: int) -> str:
    return f"monitor_trigger_count={max(0, int(trigger_count or 0))}"


async def _monitor_email_already_resolved(task_id: int, email: str, trigger_count: int) -> bool:
    normalized_email = str(email or "").strip().lower()
    if not normalized_email:
        return False
    normalized_trigger_count = max(0, int(trigger_count or 0))
    normalized_status = func.lower(func.coalesce(SendLog.status, ""))
    monitor_filters = (
        SendLog.task_id == int(task_id),
        SendLog.log_type == "monitor",
        func.lower(func.btrim(SendLog.email)) == normalized_email,
        SendLog.monitor_trigger_count == normalized_trigger_count,
        (
            (
                normalized_status.in_(("monitor_sent", "sent", "success", "delivered"))
                & (func.length(func.coalesce(SendLog.provider_message_id, "")) > 0)
            )
            | normalized_status.in_(("delivery_unknown", "unknown"))
        ),
    )
    last_error: Exception | None = None
    for attempt in range(1, MONITOR_INFRASTRUCTURE_RETRY_ATTEMPTS + 1):
        try:
            async with AsyncSessionLocal() as db:
                existing_id = (
                    await db.execute(select(SendLog.id).where(*monitor_filters).limit(1))
                ).scalar_one_or_none()
                return existing_id is not None
        except Exception as exc:
            last_error = exc
            logger.warning(
                "任务 %s 监控去重查询失败，第 %s/%s 次重试，邮箱=%s，触发计数=%s，错误=%s",
                task_id,
                attempt,
                MONITOR_INFRASTRUCTURE_RETRY_ATTEMPTS,
                normalized_email,
                normalized_trigger_count,
                sanitize_user_error_message(str(exc)),
            )
            if attempt < MONITOR_INFRASTRUCTURE_RETRY_ATTEMPTS:
                await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
    raise RuntimeError(
        f"监控去重查询连续失败，邮件已保留待重试：{sanitize_user_error_message(str(last_error or '未知错误'))}"
    )


async def _get_latest_monitor_sent_milestone(
    task_id: int,
    monitor_emails: list[str],
    monitor_interval: int,
    success_count: int,
) -> int:
    """返回所有当前监控邮箱均已成功发送的最后一个连续里程碑。

    不能简单取 MAX(trigger_count)：一个里程碑有多个监控邮箱时，可能只有其中一部分成功。
    恢复任务必须从首个未完整里程碑继续，数据库逐邮箱去重会跳过已成功邮箱，只补发缺失邮箱。
    """
    expected_emails = {str(email or "").strip().lower() for email in monitor_emails if str(email or "").strip()}
    interval = max(1, int(monitor_interval or 1))
    regular_floor = _monitor_milestone_floor(success_count, interval)
    if not expected_emails or regular_floor <= 0:
        return 0
    try:
        async with AsyncSessionLocal() as db:
            rows = (
                await db.execute(
                    select(SendLog.monitor_trigger_count, func.lower(func.btrim(SendLog.email))).where(
                        SendLog.task_id == int(task_id),
                        SendLog.log_type == "monitor",
                        (
                            (
                                func.lower(func.coalesce(SendLog.status, "")).in_(("monitor_sent", "sent", "success", "delivered"))
                                & (func.length(func.coalesce(SendLog.provider_message_id, "")) > 0)
                            )
                            | func.lower(func.coalesce(SendLog.status, "")).in_(("delivery_unknown", "unknown"))
                        ),
                        SendLog.monitor_trigger_count.is_not(None),
                        SendLog.monitor_trigger_count > 0,
                        SendLog.monitor_trigger_count <= regular_floor,
                    )
                )
            ).all()
    except Exception as exc:
        # 无法读取去重真相时必须拒绝触发；返回 0 会在恢复后从头重发历史监控邮件。
        raise RuntimeError(
            f"读取任务 {task_id} 监控里程碑失败，已阻止监控恢复发送以避免重复：{sanitize_user_error_message(str(exc))}"
        ) from exc
    sent_by_milestone: dict[int, set[str]] = {}
    for raw_trigger, raw_email in rows:
        try:
            trigger_count = int(raw_trigger or 0)
        except (TypeError, ValueError):
            continue
        normalized_email = str(raw_email or "").strip().lower()
        if trigger_count > 0 and normalized_email:
            sent_by_milestone.setdefault(trigger_count, set()).add(normalized_email)
    latest_complete = 0
    for trigger_count in range(interval, regular_floor + 1, interval):
        if not expected_emails.issubset(sent_by_milestone.get(trigger_count, set())):
            logger.info(
                "任务 %s 监控恢复定位到未完整里程碑 trigger_count=%s completed=%s expected=%s",
                task_id,
                trigger_count,
                len(expected_emails.intersection(sent_by_milestone.get(trigger_count, set()))),
                len(expected_emails),
            )
            break
        latest_complete = trigger_count
    return latest_complete


def _get_monitor_success_bucket(task_id: int, runtime: dict[str, float | int], monitor_interval: int) -> int:
    interval = max(1, int(monitor_interval or 1))
    return _get_monitor_success_count(task_id, runtime) // interval


async def _enqueue_monitor_emails_to_queue(
    task_id: int,
    task_snapshot: Task,
    trigger_count: int,
    monitor_queue: asyncio.Queue,
    batch_sender: dict[str, str] | None = None,
) -> bool:
    active_monitor_emails = await _parse_monitor_emails(task_snapshot)
    if not active_monitor_emails:
        return True
    deduped_monitor_emails: list[str] = []
    seen_emails: set[str] = set()
    for raw_email in active_monitor_emails:
        normalized_email = str(raw_email or "").strip()
        if not normalized_email:
            continue
        dedupe_key = normalized_email.lower()
        if dedupe_key in seen_emails:
            logger.warning(
                "task %s skipped duplicate monitor email in config trigger_count=%s email=%s",
                task_id,
                trigger_count,
                normalized_email,
            )
            continue
        seen_emails.add(dedupe_key)
        deduped_monitor_emails.append(normalized_email)
    if not deduped_monitor_emails:
        return True
    if task_snapshot.channel is None:
        return False
    pending_monitor_emails: list[str] = []
    monitor_claim_keys: dict[str, str] = {}
    runtime = _task_runtime_metrics.get(task_id)
    local_monitor_inflight = runtime.get("monitor_local_inflight") if runtime is not None else None
    if not isinstance(local_monitor_inflight, set):
        local_monitor_inflight = set()
        if runtime is not None:
            runtime["monitor_local_inflight"] = local_monitor_inflight

    async def release_monitor_claims(emails: list[str]) -> None:
        for email in emails:
            local_monitor_inflight.discard((max(0, int(trigger_count or 0)), str(email or "").strip().lower()))
        keys = [monitor_claim_keys[email] for email in emails if email in monitor_claim_keys]
        if not keys:
            return
        try:
            await asyncio.wait_for(redis_background_client.delete(*keys), timeout=REDIS_OPERATION_TIMEOUT_SECONDS)
        except Exception:
            logger.warning(
                "任务 %s 释放未入队监控邮件抢占标记失败，将等待 TTL 自动回收 trigger_count=%s count=%s",
                task_id,
                trigger_count,
                len(keys),
                exc_info=True,
            )

    for monitor_email in deduped_monitor_emails:
        local_inflight_key = (max(0, int(trigger_count or 0)), monitor_email.strip().lower())
        if local_inflight_key in local_monitor_inflight:
            logger.debug(
                "任务 %s 监控邮件已在本地队列或发送中，跳过重复入队 trigger_count=%s email=%s",
                task_id,
                trigger_count,
                monitor_email,
            )
            continue
        if await _monitor_email_already_resolved(task_id, monitor_email, trigger_count):
            logger.info(
                "task %s monitor email already resolved, skipping enqueue trigger_count=%s email=%s",
                task_id,
                trigger_count,
                monitor_email,
            )
            continue
        claim_key = f"monitor_claim:{int(task_id)}:{int(trigger_count)}:{monitor_email.strip().lower()}"
        try:
            claim_token = f"{INSTANCE_ID}:{uuid.uuid4().hex}"
            claimed = await asyncio.wait_for(
                redis_background_client.set(
                    claim_key,
                    claim_token,
                    nx=True,
                    ex=MONITOR_DISTRIBUTED_CLAIM_TTL_SECONDS,
                ),
                timeout=REDIS_OPERATION_TIMEOUT_SECONDS,
            )
        except Exception:
            # 任务本身已有分布式独占锁。Redis 抢占只用于额外去重，不能成为监控邮件发送的硬依赖；
            # 否则 Redis 短暂不可用会让全部监控邮件被静默跳过。
            claimed = True
            claim_key = ""
            logger.warning(
                "任务 %s 监控邮件分布式抢占不可用，已降级为任务锁内发送 trigger_count=%s email=%s",
                task_id,
                trigger_count,
                monitor_email,
                exc_info=True,
            )
        if not claimed:
            # Runner 已持有任务级分布式独占锁，且本进程还有 monitor_trigger_lock。遗留的
            # monitor_claim 可能来自崩溃实例，不能让它在整个 TTL 内吞掉真实里程碑。
            # 再查一次数据库真相后降级发送；成功日志唯一索引仍是最终防重边界。
            if await _monitor_email_already_resolved(task_id, monitor_email, trigger_count):
                logger.info(
                    "任务 %s 监控邮件已由其他实例完成 trigger_count=%s email=%s",
                    task_id,
                    trigger_count,
                    monitor_email,
                )
                continue
            logger.warning(
                "任务 %s 监控抢占标记已存在但数据库无完成记录，按任务独占锁降级发送 trigger_count=%s email=%s",
                task_id,
                trigger_count,
                monitor_email,
            )
            claim_key = ""
        pending_monitor_emails.append(monitor_email)
        local_monitor_inflight.add(local_inflight_key)
        if claim_key:
            monitor_claim_keys[monitor_email] = claim_key
    if not pending_monitor_emails:
        logger.info(
            "task %s monitor milestone already satisfied in database trigger_count=%s",
            task_id,
            trigger_count,
        )
        return True
    resolved_batch_sender = batch_sender
    if resolved_batch_sender is None or not str(resolved_batch_sender.get("email") or "").strip():
        try:
            resolved_batch_sender = await batch_pick_next_sender(
                task_snapshot.channel,
                rotation_enabled=_task_sender_rotation_enabled(task_snapshot),
            )
            _set_task_current_sender(task_id, resolved_batch_sender)
        except Exception as exc:
            logger.warning(
                "任务 %s 监控邮件发件人选择失败 trigger_count=%s error_type=%s error=%s",
                task_id,
                trigger_count,
                type(exc).__name__,
                exc,
            )
            await release_monitor_claims(pending_monitor_emails)
            return False
    live_monitor_workers = 0
    hooks = _task_shutdown_hooks.get(task_id) or {}
    monitor_workers = hooks.get("monitor_workers")
    if isinstance(monitor_workers, list):
        live_monitor_workers = count_live_worker_tasks(monitor_workers)
    if live_monitor_workers <= 0:
        logger.error(
            "任务 %s 监控里程碑已生成但没有存活的监控 Worker，拒绝入队以便主循环重启 Worker trigger_count=%s",
            task_id,
            trigger_count,
        )
        runtime = _task_runtime_metrics.get(task_id)
        if runtime is not None:
            runtime["monitor_worker_missing"] = True
            runtime["monitor_trigger_pending"] = True
        await release_monitor_claims(pending_monitor_emails)
        return False
    logger.info(
        "任务 %s 触发监控抽样，共 %s 个监控邮箱，触发计数=%s，发件人=%s",
        task_id,
        len(pending_monitor_emails),
        trigger_count,
        resolved_batch_sender.get("email", ""),
    )
    for monitor_index, monitor_email in enumerate(pending_monitor_emails):
        monitor_recipient = Recipient(
            id=0,
            list_id=task_snapshot.recipient_list_id,
            email=monitor_email,
            normalized_email=_normalize_inflight_email(monitor_email),
            row_number=0,
            is_valid=True,
            is_unsubscribed=False,
        )
        try:
            await asyncio.wait_for(
                monitor_queue.put(
                    (
                        task_snapshot,
                        monitor_recipient,
                        trigger_count,
                        resolved_batch_sender,
                        monitor_claim_keys.get(monitor_email, ""),
                    )
                ),
                timeout=SEND_QUEUE_PUT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.error(
                "任务 %s 监控邮件入队超时，等待 %s 秒后仍未成功 queue_size=%s email=%s trigger_count=%s",
                task_id,
                SEND_QUEUE_PUT_TIMEOUT_SECONDS,
                monitor_queue.qsize(),
                monitor_email,
                trigger_count,
            )
            # 已成功入队的邮箱必须保留抢占标记，直到发送结果落库；只释放当前及后续未入队邮箱。
            await release_monitor_claims(pending_monitor_emails[monitor_index:])
            return False
    return True


def _build_unsubscribe_url_cached(email: str, recipient_id: int | None, task_id: int | None) -> str:
    return _unsubscribe_token_cache.get_url(settings.public_base_url, email, recipient_id, task_id)


def _clear_unsubscribe_cache_for_task(task_id: int) -> None:
    _unsubscribe_token_cache.clear_task(task_id)


def _schedule_monitor_trigger(task_id: int, reason: str = "") -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None or not bool(runtime.get("has_monitor_emails", False)):
        return
    if bool(runtime.get("monitor_trigger_inflight", False)):
        runtime["monitor_trigger_pending"] = True
        return
    try:
        asyncio.get_running_loop().create_task(
            _maybe_trigger_monitor_emails(task_id),
            name=f"task-monitor-trigger-{task_id}",
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("任务 %s 已调度监控触发协程 reason=%s", task_id, reason)
    except RuntimeError:
        runtime["monitor_trigger_pending"] = True


async def _maybe_trigger_monitor_emails(task_id: int) -> None:
    await _run_monitor_trigger_loop(task_id, force_completion_sample=False)


async def _run_monitor_completion_trigger(task_id: int) -> None:
    await _run_monitor_trigger_loop(task_id, force_completion_sample=True)
    runtime = _task_runtime_metrics.get(task_id)
    idle_event = (runtime or {}).get("monitor_trigger_idle_event")
    if isinstance(idle_event, asyncio.Event) and not idle_event.is_set():
        try:
            await asyncio.wait_for(idle_event.wait(), timeout=MONITOR_SEND_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.error("任务 %s 等待监控触发协程完成超时，完成抽样将由恢复逻辑补发", task_id)


async def _run_monitor_trigger_loop(task_id: int, *, force_completion_sample: bool) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return
    if bool(runtime.get("monitor_trigger_inflight", False)):
        runtime["monitor_trigger_pending"] = True
        if force_completion_sample:
            runtime["monitor_completion_pending"] = True
        idle_event = runtime.get("monitor_trigger_idle_event")
        if force_completion_sample and isinstance(idle_event, asyncio.Event):
            try:
                await asyncio.wait_for(idle_event.wait(), timeout=MONITOR_SEND_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                logger.error("任务 %s 等待已有监控触发协程接管完成抽样超时", task_id)
        return
    runtime["monitor_trigger_inflight"] = True
    idle_event = runtime.get("monitor_trigger_idle_event")
    if isinstance(idle_event, asyncio.Event):
        idle_event.clear()
    try:
        run_completion_sample = force_completion_sample or bool(runtime.get("monitor_completion_pending", False))
        while True:
            runtime["monitor_trigger_pending"] = False
            runtime["monitor_completion_pending"] = False
            await _run_monitor_trigger_once(task_id, force_completion_sample=run_completion_sample)
            pending_regular = bool(runtime.get("monitor_trigger_pending", False))
            pending_completion = bool(runtime.get("monitor_completion_pending", False))
            if not pending_regular and not pending_completion:
                break
            run_completion_sample = pending_completion
    except Exception as exc:
        runtime["monitor_trigger_failure_count"] = int(runtime.get("monitor_trigger_failure_count", 0) or 0) + 1
        runtime["last_monitor_trigger_error"] = f"{type(exc).__name__}: {exc}"[:500]
        logger.exception(
            "任务 %s 监控触发失败 error_type=%s error=%s",
            task_id,
            type(exc).__name__,
            exc,
        )
    finally:
        active_runtime = _task_runtime_metrics.get(task_id)
        if active_runtime is not None:
            active_runtime["monitor_trigger_inflight"] = False
            active_idle_event = active_runtime.get("monitor_trigger_idle_event")
            if isinstance(active_idle_event, asyncio.Event):
                active_idle_event.set()


async def _run_monitor_trigger_once(task_id: int, *, force_completion_sample: bool) -> None:
    current_status = str(_task_status_cache.get(task_id) or "").strip().lower()
    if current_status in {"paused", "stopped", "failed", "aborted", "blocked"}:
        logger.info("任务 %s 状态=%s，停止创建新的监控发送里程碑", task_id, current_status)
        return
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None or not bool(runtime.get("has_monitor_emails", False)):
        return
    hooks = _task_shutdown_hooks.get(task_id)
    if hooks is None:
        return
    monitor_queue = hooks.get("monitor_queue")
    if not isinstance(monitor_queue, asyncio.Queue):
        return
    monitor_trigger_lock = hooks.get("monitor_trigger_lock")
    if not isinstance(monitor_trigger_lock, asyncio.Lock):
        return

    async with monitor_trigger_lock:
        runtime = _task_runtime_metrics.get(task_id)
        if runtime is None or not bool(runtime.get("has_monitor_emails", False)):
            return
        task_snapshot = hooks.get("cached_task")
        if not isinstance(task_snapshot, Task):
            task_snapshot = await _load_task(task_id)
            if task_snapshot is None:
                return
            hooks["cached_task"] = task_snapshot
        active_monitor_emails = await _parse_monitor_emails(task_snapshot)
        if not active_monitor_emails:
            runtime["has_monitor_emails"] = False
            logger.info("任务 %s 监控抽样未触发：监控邮箱为空", task_id)
            return
        runtime["has_monitor_emails"] = True
        raw_monitor_interval = int(
            task_snapshot.monitor_interval
            if task_snapshot.monitor_interval is not None
            else runtime.get("monitor_interval", 100)
        )
        if raw_monitor_interval <= 0:
            runtime["has_monitor_emails"] = False
            logger.info("任务 %s 监控抽样未触发：监控间隔=%s，只有 interval > 0 才会启用", task_id, raw_monitor_interval)
            return
        monitor_interval = max(1, raw_monitor_interval)
        previous_interval = max(1, int(runtime.get("monitor_interval", monitor_interval) or monitor_interval))
        if monitor_interval != previous_interval:
            logger.warning(
                "task %s monitor_interval changed during run previous=%s current=%s last_milestone=%s success=%s",
                task_id,
                previous_interval,
                monitor_interval,
                int(runtime.get("last_monitor_trigger_count", 0)),
                _get_monitor_success_count(task_id, runtime),
            )
        runtime["monitor_interval"] = monitor_interval
        success_count = _get_monitor_success_count(task_id, runtime)
        # 内存游标只表示曾经入队，不能作为已发送真相。每轮都从数据库恢复“所有监控邮箱均
        # 已确认成功”的连续里程碑；失败、Worker 崩溃或日志落库失败的单封会在下一轮自动补发。
        last_milestone = await _get_latest_monitor_sent_milestone(
            task_id,
            active_monitor_emails,
            monitor_interval,
            success_count,
        )
        runtime["last_monitor_confirmed_count"] = last_milestone
        logger.info(
            "任务 %s 监控抽样检查：当前成功计数=%s，监控间隔=%s，监控邮箱数=%s，上次触发=%s",
            task_id,
            success_count,
            monitor_interval,
            len(active_monitor_emails),
            last_milestone,
        )
        while True:
            next_milestone = _next_monitor_milestone(last_milestone, monitor_interval)
            if success_count < next_milestone:
                break
            logger.info(
                "任务 %s 监控触发计数=%s，实际成功数=%s",
                task_id,
                next_milestone,
                success_count,
            )
            logger.info(
                "任务 %s 触发监控抽样：当前成功计数=%s，触发里程碑=%s，监控间隔=%s，共 %s 个监控邮箱",
                task_id,
                success_count,
                next_milestone,
                monitor_interval,
                len(active_monitor_emails),
            )
            enqueued = await _enqueue_monitor_emails_to_queue(
                task_id,
                task_snapshot,
                next_milestone,
                monitor_queue,
            )
            if not enqueued:
                logger.warning(
                    "task %s monitor milestone enqueue failed trigger_count=%s last=%s success=%s",
                    task_id,
                    next_milestone,
                    last_milestone,
                    success_count,
                )
                break
            last_milestone = next_milestone
            runtime["last_monitor_trigger_count"] = last_milestone
            runtime["last_monitor_success_bucket"] = last_milestone // monitor_interval
            # 仅以 monitor_sent 且有 provider_message_id 的记录作为已完成里程碑；
            # 此处只更新内存游标，重启时仍由 SendLog 重新恢复，避免失败触发被永久跳过。
            success_count = _get_monitor_success_count(task_id, runtime)
        if force_completion_sample and success_count > last_milestone:
            # 完成里程碑不能依赖恰好达到 interval。短任务或最后一个不足整桶的尾批次也必须
            # 至少产生一次可追踪的监控邮件，否则默认 interval=100 时小任务永远不会触发。
            completion_trigger_count = success_count
            logger.info(
                "任务 %s 触发完成监控抽样：普通邮件成功数=%s，上次里程碑=%s，监控间隔=%s，邮箱数=%s",
                task_id,
                success_count,
                last_milestone,
                monitor_interval,
                len(active_monitor_emails),
            )
            enqueued = await _enqueue_monitor_emails_to_queue(
                task_id,
                task_snapshot,
                completion_trigger_count,
                monitor_queue,
            )
            if enqueued:
                runtime["last_monitor_completion_trigger_count"] = completion_trigger_count
            else:
                logger.error(
                    "任务 %s 完成监控抽样入队失败 trigger_count=%s，将保留未完成里程碑供恢复时重试",
                    task_id,
                    completion_trigger_count,
                )


def _set_task_current_sender(task_id: int, sender: dict[str, str]) -> None:
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is None:
        return
    runtime["current_sender_name"] = str(sender.get("name") or "")
    runtime["current_sender_email"] = str(sender.get("email") or "")


def _task_sender_rotation_enabled(task: Task | None) -> bool:
    if task is None:
        return False
    return bool(getattr(task, "sender_rotation_enabled", False))


def _task_fixed_sender(task: Task | None) -> dict[str, str] | None:
    if task is None or task.channel is None:
        return None
    configured_email = str(getattr(task, "sender_email", "") or "").strip().lower()
    if not configured_email:
        return None
    for sender in parse_sender_emails(task.channel, active_only=True):
        if str(sender.get("email") or "").strip().lower() == configured_email:
            return {"name": str(sender.get("name") or ""), "email": configured_email}
    return None


async def _pick_task_sender(task: Task) -> dict[str, str]:
    fixed_sender = _task_fixed_sender(task)
    if fixed_sender is not None:
        return fixed_sender
    return await pick_next_sender(task.channel, rotation_enabled=_task_sender_rotation_enabled(task))


def _task_unsubscribe_enabled(task: Task | None) -> bool:
    if task is None:
        return False
    template = getattr(task, "template", None)
    return bool(getattr(task, "unsubscribe_enabled", False)) or bool(getattr(template, "unsubscribe_enabled", False))


def _resolve_log_sender_email(task_id: int, fallback_sender: str | None = None) -> str | None:
    fallback = str(fallback_sender or "").strip()
    if fallback:
        return fallback[:255]
    runtime = _task_runtime_metrics.get(task_id)
    email = str((runtime or {}).get("current_sender_email") or "").strip()
    return email[:255] if email else None


def _loads_recipient_custom_fields(raw: str) -> dict:
    try:
        custom_fields = json.loads(raw or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return custom_fields if isinstance(custom_fields, dict) else {}


def _recipient_variables(
    recipient: Recipient,
    task: Task | None = None,
    unsubscribe_enabled: bool = True,
    sender: dict[str, str] | None = None,
) -> RecipientVariableBundle:
    custom_fields = _loads_recipient_custom_fields(recipient.custom_fields_json or "{}")
    if not isinstance(custom_fields, dict):
        custom_fields = {}
    task_id = task.id if task is not None else None
    channel = task.channel if task is not None else None
    template = task.template if task is not None else None
    email = recipient.email or ""
    substitution_unsubscribe: dict[str, str] = {}
    unsubscribe_url = ""
    if unsubscribe_enabled and email:
        unsubscribe_url = _build_unsubscribe_url_cached(email, recipient.id, task_id)
        substitution_unsubscribe = {SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY: unsubscribe_url}
    from_name = (sender or {}).get("name") or getattr(channel, "sender_name", "") or ""
    from_email = (sender or {}).get("email") or getattr(channel, "sender_email", "") or ""
    render_variables = {
        "email": email,
        "name": recipient.name or "",
        "company": recipient.company or "",
        "phone": recipient.phone or "",
        "from_name": from_name,
        "from_email": from_email,
        "campaign_id": getattr(template, "campaign_id", "") or "",
        # date/random 由 template_service.prepare_variables 在渲染时注入。
        "date": "",
    }
    render_variables.update(custom_fields)
    if unsubscribe_url:
        render_variables["unsubscribe_url"] = unsubscribe_url
    else:
        render_variables.pop("unsubscribe_url", None)
    return RecipientVariableBundle(
        render_variables=render_variables,
        unsubscribe_url=unsubscribe_url,
        substitution_unsubscribe=substitution_unsubscribe,
    )


def _monitor_variables(
    email: str,
    task: Task | None = None,
    unsubscribe_enabled: bool = True,
    sender: dict[str, str] | None = None,
    *,
    monitor_recipient: Recipient | None = None,
) -> RecipientVariableBundle:
    task_id = task.id if task is not None else None
    channel = task.channel if task is not None else None
    template = task.template if task is not None else None
    from_name = (sender or {}).get("name") or getattr(channel, "sender_name", "") or ""
    from_email = (sender or {}).get("email") or getattr(channel, "sender_email", "") or ""
    normalized_email = str(email or "").strip()
    recipient_proxy = monitor_recipient
    if recipient_proxy is None:
        recipient_proxy = Recipient(email=normalized_email)
    substitution_unsubscribe: dict[str, str] = {}
    unsubscribe_url = ""
    if unsubscribe_enabled and normalized_email:
        recipient_id = getattr(recipient_proxy, "id", None)
        unsubscribe_url = _build_unsubscribe_url_cached(normalized_email, recipient_id, task_id)
        substitution_unsubscribe = {SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY: unsubscribe_url}
    render_variables = {
        "email": normalized_email,
        "name": normalized_email.split("@", 1)[0] if "@" in normalized_email else normalized_email,
        "company": "",
        "phone": "",
        "from_name": from_name,
        "from_email": from_email,
        "campaign_id": getattr(template, "campaign_id", "") or "",
        "date": "",
    }
    if unsubscribe_url:
        render_variables["unsubscribe_url"] = unsubscribe_url
    return RecipientVariableBundle(
        render_variables=render_variables,
        unsubscribe_url=unsubscribe_url,
        substitution_unsubscribe=substitution_unsubscribe,
    )


def build_personalization_with_unsubscribe(
    recipient: Recipient,
    substitutions: dict[str, str] | None = None,
    *,
    task_id: int | None = None,
    unsubscribe_enabled: bool = True,
    substitution_unsubscribe: dict[str, str] | None = None,
) -> "Personalization":
    """为单个收件人构建 SendGrid Personalization，在 personalization 级别注入退订 Substitution 与 RFC 8058 头。"""
    from sendgrid.helpers.mail import Header, Personalization, Substitution, To

    normalized_email = str(getattr(recipient, "email", "") or "").strip()
    personalization = Personalization()
    if normalized_email:
        personalization.add_to(To(normalized_email))

    merged_substitutions: dict[str, str] = {}
    for key, value in (substitutions or {}).items():
        normalized_key = str(key or "").strip()
        if normalized_key:
            merged_substitutions[normalized_key] = "" if value is None else str(value)

    if unsubscribe_enabled and normalized_email:
        if substitution_unsubscribe:
            merged_substitutions.update(substitution_unsubscribe)
        else:
            merged_substitutions.update(build_substitution_unsubscribe(recipient, task_id))
        unsubscribe_url = str(merged_substitutions.get(SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY, "") or "").strip()
        if unsubscribe_url:
            personalization.add_header(Header("List-Unsubscribe", f"<{unsubscribe_url}>"))
            personalization.add_header(Header("List-Unsubscribe-Post", "List-Unsubscribe=One-Click"))

    for key, value in merged_substitutions.items():
        personalization.add_substitution(Substitution(key, value))

    return personalization


_build_personalization_with_unsubscribe = build_personalization_with_unsubscribe


def _build_url_substitution_map(url: str) -> dict[str, str]:
    normalized_url = str(url or "").strip()
    return {
        SENDGRID_URL_SUBSTITUTION_KEY: normalized_url,
        SENDGRID_URL_POOL_URL_SUBSTITUTION_KEY: normalized_url,
    }


async def _assign_urls_to_recipients(
    recipients: list[Recipient],
    cache: TaskUrlPoolCache | None,
    pool_id: int,
    task_id: int | None,
) -> list[dict[str, str]]:
    """为一批收件人批量分配 URL Substitution 变量；池非空时循环轮询，保证每人一条链接。"""
    if not recipients:
        return []
    url_results = await get_next_urls_batch(cache, int(pool_id), task_id, len(recipients))
    if not url_results:
        raise ValueError(URL_POOL_EMPTY_ERROR)
    if len(url_results) < len(recipients):
        logger.error(
            "任务 %s URL 池批量分配数量不足 pool_id=%s 需要=%s 实际=%s，缺失项将单独失败",
            task_id,
            pool_id,
            len(recipients),
            len(url_results),
        )
    return [_build_url_substitution_map(item.url) for item in url_results]


async def _preallocate_queued_recipient_metadata(
    task: Task,
    recipients: list[Recipient],
    url_pool_cache: TaskUrlPoolCache | None,
    send_log_id_allocator: SendLogIdAllocator | None,
    inflight_emails: set[str],
) -> list[Recipient]:
    """批量准备 URL；缺失结果按收件人失败，基础设施错误才回滚整批。"""
    if not recipients:
        return []
    task_id = int(task.id or 0)
    successful: list[Recipient] = []
    individual_failures: list[tuple[Recipient, str]] = []
    url_assignments: dict[int, dict[str, str]] = {}
    if task.url_pool_id:
        try:
            assignments = await _assign_urls_to_recipients(
                recipients,
                url_pool_cache,
                int(task.url_pool_id),
                task_id,
            )
        except Exception:
            logger.error("任务 %s URL 池批量预分配基础设施不可用，回滚批次", task_id, exc_info=True)
            raise
        for index, recipient in enumerate(recipients):
            assignment = assignments[index] if index < len(assignments) else None
            if assignment and str(assignment.get(SENDGRID_URL_SUBSTITUTION_KEY, "") or "").strip():
                url_assignments[id(recipient)] = assignment
                successful.append(recipient)
                continue
            reason = URL_POOL_EMPTY_ERROR
            logger.warning(
                "任务 %s URL 池批量预分配缺少单条结果 email=%s index=%s assigned=%s requested=%s",
                task_id,
                str(recipient.email or "").strip(),
                index,
                len(assignments),
                len(recipients),
            )
            individual_failures.append((recipient, reason))
    else:
        successful = list(recipients)
        url_assignments = {id(recipient): _build_url_substitution_map("") for recipient in recipients}

    id_recipients = successful + [recipient for recipient, _reason in individual_failures]
    if send_log_id_allocator is not None:
        send_log_ids = await send_log_id_allocator.allocate_for_recipients(id_recipients)
    else:
        send_log_ids = await allocate_send_log_ids(task_id, id_recipients)
    missing_ids = [
        recipient for recipient in id_recipients
        if not send_log_ids.get(str(int(recipient.id or 0)))
    ]
    if missing_ids:
        logger.error(
            "任务 %s SendLog ID 预分配不足，继续处理已分配收件人并逐条标记其余失败 requested=%s allocated=%s missing=%s",
            task_id,
            len(id_recipients),
            len(send_log_ids),
            len(missing_ids),
        )

    sendable_successful: list[Recipient] = []
    for recipient in successful:
        send_log_id = send_log_ids.get(str(int(recipient.id or 0)))
        if not send_log_id:
            await _enqueue_result(
                task_id,
                recipient,
                False,
                "SendLog ID 预分配失败，本收件人未进入发送队列",
                0,
                inflight_emails=inflight_emails,
                send_log_id=None,
            )
            continue
        recipient._mercury_url_substitutions = url_assignments[id(recipient)]
        recipient._mercury_send_log_id = send_log_id
        sendable_successful.append(recipient)
    for recipient, reason in individual_failures:
        send_log_id = send_log_ids.get(str(int(recipient.id or 0)))
        await _enqueue_result(
            task_id,
            recipient,
            False,
            f"预分配 URL 失败：{reason}" if send_log_id else f"预分配 URL 失败：{reason}；SendLog ID 同时分配失败",
            0,
            inflight_emails=inflight_emails,
            send_log_id=send_log_id,
        )
    return sendable_successful


async def _get_task_status(task_id: int) -> str:
    try:
        async with asyncio.timeout(15):
            async with AsyncSessionLocal() as db:
                row = (
                    await db.execute(
                        select(Task.status, Task.deleted_at).where(Task.id == task_id)
                    )
                ).one_or_none()
                if row is None or row.deleted_at is not None:
                    return "stopped"
                return str(row.status or "")
    except TimeoutError:
        logger.exception("任务 %s 查询状态超时", task_id)
        raise


async def publish_task_status_changed(task_id: int, status: str) -> None:
    task_key = int(task_id)
    normalized = str(status or "").strip()
    version = int(_task_status_versions.get(task_key, 0) or 0) + 1
    _task_status_versions[task_key] = version
    try:
        from app.core.redis import redis_background_client

        await redis_background_client.publish(
            TASK_STATUS_CHANGED_CHANNEL,
            json.dumps({"task_id": task_key, "status": normalized, "version": version}, ensure_ascii=False),
        )
    except Exception:
        logger.debug("任务 %s 状态变更 Redis 发布失败，保留本地事件唤醒", task_key, exc_info=True)


def notify_task_status_changed(task_id: int, status: str) -> None:
    normalized = str(status or "").strip()
    task_key = int(task_id)
    _task_status_cache[task_key] = normalized
    _task_status_versions[task_key] = int(_task_status_versions.get(task_key, 0) or 0) + 1
    wake_event = _task_status_wake_events.get(task_key)
    if wake_event is not None:
        wake_event.set()


async def notify_task_status_changed_async(task_id: int, status: str) -> None:
    notify_task_status_changed(task_id, status)
    await publish_task_status_changed(task_id, status)


async def start_task_status_pubsub_listener() -> None:
    global _task_status_pubsub_task
    if _task_status_pubsub_task is not None and not _task_status_pubsub_task.done():
        return

    async def _listen() -> None:
        from app.core.redis import redis_background_client

        while True:
            pubsub = redis_background_client.pubsub()
            try:
                await pubsub.subscribe(TASK_STATUS_CHANGED_CHANNEL)
                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    try:
                        payload = json.loads(str(message.get("data") or "{}"))
                        task_key = int(payload.get("task_id") or 0)
                        if task_key <= 0:
                            continue
                        version = int(payload.get("version") or 0)
                        if version < int(_task_status_versions.get(task_key, 0) or 0):
                            continue
                        _task_status_versions[task_key] = version
                        _task_status_cache[task_key] = str(payload.get("status") or "")
                        wake_event = _task_status_wake_events.get(task_key)
                        if wake_event is not None:
                            wake_event.set()
                    except Exception:
                        logger.debug("任务状态 pub/sub 消息处理失败", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("任务状态 pub/sub 监听异常，3 秒后重连", exc_info=True)
                await asyncio.sleep(3.0)
            finally:
                try:
                    await pubsub.close()
                except Exception:
                    pass

    _task_status_pubsub_task = asyncio.create_task(_listen(), name="task-status-pubsub-listener")


def ensure_task_status_watch(task_id: int, initial_status: str = "running") -> asyncio.Event:
    task_id = int(task_id)
    _task_status_cache[task_id] = str(initial_status or "running")
    wake_event = _task_status_wake_events.get(task_id)
    if wake_event is None:
        wake_event = asyncio.Event()
        _task_status_wake_events[task_id] = wake_event
    else:
        wake_event.clear()
    return wake_event


def clear_task_status_watch(task_id: int) -> None:
    task_id = int(task_id)
    _task_status_cache.pop(task_id, None)
    _task_status_wake_events.pop(task_id, None)


async def _resolve_loop_task_status(
    task_id: int,
    status_wake_event: asyncio.Event,
    last_db_poll_at: float,
    runtime: dict[str, float | int] | None = None,
) -> tuple[str, float]:
    task_id = int(task_id)
    now_value = monotonic()
    if status_wake_event.is_set():
        status_wake_event.clear()
        cached = _task_status_cache.get(task_id)
        if cached:
            if runtime is not None:
                runtime["task_status_source"] = "event"
            return cached, last_db_poll_at
    cached = _task_status_cache.get(task_id)
    if cached is not None and now_value - last_db_poll_at < TASK_STATUS_DB_POLL_INTERVAL_SECONDS:
        if runtime is not None:
            runtime["task_status_source"] = "cache"
        return cached, last_db_poll_at
    status = await _get_task_status(task_id)
    normalized = str(status or "")
    _task_status_cache[task_id] = normalized
    if runtime is not None:
        runtime["task_status_source"] = "db"
    return normalized, now_value


def _runtime_run_generation(task_id: int) -> int | None:
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is None:
        return None
    generation = int(runtime.get("run_generation", 0) or 0)
    return generation if generation > 0 else None


async def _write_runner_task_status(task_id: int, *, status: str, run_message: str | None = None) -> bool:
    run_generation = _runtime_run_generation(task_id)
    if run_generation is None:
        logger.warning("任务状态写入已跳过：Runner generation 已过期 task_id=%s status=%s", task_id, status)
        return False
    values: dict[str, object] = {"status": status, "updated_at": datetime.utcnow()}
    if run_message is not None:
        values["run_message"] = run_message
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            update(Task)
            .where(Task.id == int(task_id), Task.run_generation == run_generation)
            .values(**values)
        )
        if int(result.rowcount or 0) <= 0:
            await db.rollback()
            logger.warning("任务状态写入已跳过：Runner generation 已过期 task_id=%s generation=%s status=%s", task_id, run_generation, status)
            return False
        await db.commit()
    return True


async def _mark_task_failed(task_id: int, message: str) -> None:
    safe_message = sanitize_user_error_message(message)
    stored_error = normalize_send_log_error(safe_message)
    logger.error("任务 %s 已标记失败：%s", task_id, stored_error)
    if await _write_runner_task_status(task_id, status="failed", run_message=stored_error):
        async with AsyncSessionLocal() as db:
            db.add(SendLog(task_id=task_id, email="", status="failed", error=stored_error, error_message=stored_error, response_text=safe_message[:4000], attempt_count=0))
            await db.commit()
        await notify_task_status_changed_async(task_id, "failed")


async def _mark_task_paused(task_id: int, message: str) -> None:
    logger.warning("任务 %s 已暂停：%s", task_id, message)
    if await _write_runner_task_status(task_id, status="paused", run_message=message):
        await notify_task_status_changed_async(task_id, "paused")


async def _pause_task_for_unexpected_exception(task_id: int, exc: BaseException, *, source: str) -> None:
    """意外编程异常统一转为可恢复暂停；状态写入失败不得掩盖原始异常。"""
    safe_detail = sanitize_user_error_message(str(exc) or type(exc).__name__)
    message = f"{source}发生意外异常，任务已自动暂停并保留发送进度：{safe_detail}"
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is not None:
        runtime["status"] = "paused"
        runtime["unexpected_exception_type"] = type(exc).__name__
        runtime["unexpected_exception_source"] = source
        runtime["unexpected_exception_detail"] = safe_detail[:500]
    logger.error(
        "任务 %s %s发生意外异常，已进入安全暂停 error_type=%s error=%s",
        task_id,
        source,
        type(exc).__name__,
        exc,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    try:
        await _mark_task_paused(task_id, message)
    except Exception as status_exc:
        logger.exception(
            "任务 %s 意外异常后的暂停状态写入失败，原始异常类型=%s，状态异常类型=%s",
            task_id,
            type(exc).__name__,
            type(status_exc).__name__,
        )
    _task_shutdown_requested.add(int(task_id))


async def _mark_task_blocked(task_id: int, message: str) -> None:
    safe_message = sanitize_user_error_message(message)
    logger.error("任务 %s 已阻塞（blocked）：%s", task_id, safe_message)
    if await _write_runner_task_status(task_id, status="blocked", run_message=safe_message):
        await notify_task_status_changed_async(task_id, "blocked")


async def _mark_task_aborted(task_id: int, message: str) -> None:
    safe_message = sanitize_user_error_message(message)
    logger.error("任务 %s 已终止（aborted）：%s", task_id, safe_message)
    if await _write_runner_task_status(task_id, status="aborted", run_message=safe_message):
        await notify_task_status_changed_async(task_id, "aborted")


async def _mark_scheduled_task_failed(task_id: int, message: str) -> None:
    safe_message = sanitize_user_error_message(message)
    logger.error("定时调度拒绝启动任务 %s：%s", task_id, safe_message)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            update(Task)
            .where(Task.id == int(task_id), Task.status.in_(("scheduled", "pending")))
            .values(
                status="failed",
                run_message=safe_message,
                scheduled_at=None,
                updated_at=datetime.utcnow(),
            )
        )
        if int(result.rowcount or 0) <= 0:
            await db.rollback()
            await _mark_task_failed(task_id, safe_message)
            return
        await db.commit()
    await notify_task_status_changed_async(task_id, "failed")


async def _mark_task_status(task_id: int, status: str) -> None:
    if await _write_runner_task_status(task_id, status=status):
        await notify_task_status_changed_async(task_id, status)


def _configured_task_template_ids(task: Task) -> list[int]:
    raw_ids = getattr(task, "template_ids", None)
    normalized = list(dict.fromkeys(int(item) for item in (raw_ids or []) if int(item) > 0))
    fallback_id = int(getattr(task, "template_id", 0) or 0)
    if not normalized and fallback_id > 0:
        normalized = [fallback_id]
    return normalized


async def _load_task_rotation_templates(task: Task) -> None:
    template_ids = _configured_task_template_ids(task)
    if not template_ids:
        task.rotation_templates = []
        return
    async with AsyncSessionLocal() as db:
        rows = list((await db.execute(select(Template).where(Template.id.in_(template_ids)))).scalars().all())
    templates_by_id = {int(template.id): template for template in rows}
    missing_ids = [template_id for template_id in template_ids if template_id not in templates_by_id]
    for template_id in missing_ids:
        logger.warning("模板 %s 已被删除，跳过 task_id=%s", template_id, task.id)
    task.rotation_templates = [templates_by_id[template_id] for template_id in template_ids if template_id in templates_by_id]
    if task.rotation_templates:
        task.template = task.rotation_templates[0]
        task.template_id = int(task.rotation_templates[0].id)


def _select_task_template(task: Task, runtime: dict[str, Any] | None) -> Template | None:
    templates = list(getattr(task, "rotation_templates", None) or ([task.template] if task.template is not None else []))
    if not templates:
        return None
    mode = str(getattr(task, "template_rotation_mode", "sequential") or "sequential").strip().lower()
    if mode == "random" and len(templates) > 1:
        selected = random.choice(templates)
    else:
        cursor = int((runtime or {}).get("template_rotation_cursor", 0) or 0)
        selected = templates[cursor % len(templates)]
        if runtime is not None:
            runtime["template_rotation_cursor"] = cursor + 1
    if runtime is not None:
        runtime["current_template_id"] = int(selected.id)
        runtime["current_template_name"] = str(selected.name or f"模板 {selected.id}")
    return selected


def _task_with_selected_template(task: Task, runtime: dict[str, Any] | None) -> Task:
    selected = _select_task_template(task, runtime)
    if selected is None:
        return task
    selected_task = copy.copy(task)
    selected_task.template = selected
    selected_task.template_id = int(selected.id)
    return selected_task


async def _load_task(task_id: int) -> Task | None:
    try:
        async with asyncio.timeout(20):
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Task)
                    .options(
                        selectinload(Task.template),
                        selectinload(Task.url_pool),
                        selectinload(Task.channel).selectinload(Channel.proxy),
                        selectinload(Task.recipient_list),
                    )
                    .where(Task.id == task_id)
                )
                task = result.scalar_one_or_none()
                if task is not None:
                    await db.refresh(task)
        if task is not None:
            await _load_task_rotation_templates(task)
        return task
    except TimeoutError:
        logger.exception("任务 %s 加载任务详情超时", task_id)
        raise


async def _load_task_for_loop_refresh(task_id: int) -> Task | None:
    """热路径配置刷新：仅加载 template 与 channel.proxy，不加载 url_pool / recipient_list 大字段。"""
    try:
        async with asyncio.timeout(10):
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Task)
                    .options(
                        selectinload(Task.template),
                        selectinload(Task.channel).selectinload(Channel.proxy),
                    )
                    .where(Task.id == task_id, Task.deleted_at.is_(None))
                )
                task = result.scalar_one_or_none()
                if task is None:
                    logger.warning("任务 %s 已软删除或不存在，发送引擎将退出", task_id)
        if task is not None:
            await _load_task_rotation_templates(task)
        return task
    except TimeoutError:
        logger.exception("任务 %s 热路径配置刷新超时", task_id)
        raise


async def _count_task_sendable_recipients(db: Any, task: Task) -> int:
    fixed_row_numbers = await normalize_recipient_row_numbers(db, int(task.recipient_list_id or 0))
    if fixed_row_numbers > 0 and hasattr(db, "flush"):
        await db.flush()
    normalized_recipient_email = func.lower(func.btrim(Recipient.email))
    unsubscribed_exists = exists(
        select(1).where(
            func.coalesce(
                func.nullif(func.btrim(Unsubscribe.normalized_email), ""),
                func.lower(func.btrim(Unsubscribe.email)),
            ) == normalized_recipient_email
        )
    )
    return int(
        (
            await db.execute(
                select(func.count())
                .select_from(Recipient)
                .where(
                    Recipient.list_id == task.recipient_list_id,
                    Recipient.email != "",
                    Recipient.is_valid.is_(True),
                    Recipient.is_unsubscribed.is_(False),
                    Recipient.row_number > 0,
                    ~unsubscribed_exists,
                )
            )
        ).scalar_one()
        or 0
    )


async def _validate_task_recipient_list_not_occupied(task: Task) -> str | None:
    normalized_list_id = int(getattr(task, "recipient_list_id", 0) or 0)
    task_id = int(getattr(task, "id", 0) or 0)
    if normalized_list_id <= 0 or task_id <= 0:
        return None
    try:
        async with AsyncSessionLocal() as db:
            occupied_task_id = (
                await db.execute(
                    select(Task.id).where(
                        Task.recipient_list_id == normalized_list_id,
                        Task.id != task_id,
                        Task.deleted_at.is_(None),
                        Task.status.in_(("running", "paused")),
                    ).limit(1)
                )
            ).scalar_one_or_none()
    except Exception as exc:
        logger.exception("任务 %s 启动前检查收件人池占用失败 list_id=%s", task_id, normalized_list_id)
        return sanitize_user_error_message(f"收件人池占用检查失败：{exc}")
    if occupied_task_id is not None:
        return f"该收件人池已被任务 {occupied_task_id} 使用，请先停止或完成该任务后再启动"
    return None


async def _validate_task_ready(task: Task) -> str | None:
    list_occupied_error = await _validate_task_recipient_list_not_occupied(task)
    if list_occupied_error:
        return list_occupied_error
    templates = list(getattr(task, "rotation_templates", None) or [])
    template = templates[0] if templates else _safe_loaded_relationship(task, "template")
    channel = _safe_loaded_relationship(task, "channel")
    recipient_list = _safe_loaded_relationship(task, "recipient_list")
    if template is None:
        return "任务模板缺失或全部已被删除"
    if channel is None:
        return "任务通道缺失"
    if recipient_list is None:
        return "任务收件人列表缺失"
    async with AsyncSessionLocal() as db:
        sendable_count = await _count_task_sendable_recipients(db, task)
        if sendable_count <= 0:
            return "收件人池为空或没有可发送邮箱"
        task.total_count = sendable_count
    if not bool(channel.enabled) or not bool(channel.is_active):
        return f"通道 {task.channel_id} 已禁用"
    if channel.proxy_id and not bool(channel.use_proxy):
        channel.use_proxy = True
    proxy = _safe_loaded_relationship(channel, "proxy")
    if bool(channel.use_proxy) and channel.proxy_id is None:
        return "通道代理不可用，发送中止"
    if bool(channel.use_proxy) and proxy is None:
        return "通道代理不可用，发送中止"
    if bool(channel.use_proxy) and proxy is not None and not bool(proxy.is_active):
        return "通道代理不可用，发送中止"
    if bool(channel.use_proxy) and channel.proxy_id and proxy is not None and proxy.id != channel.proxy_id:
        return "通道代理配置不一致，发送中止"
    consistency_error = await validate_sendgrid_api_key_proxy_consistency(channel)
    if consistency_error:
        return consistency_error
    batch_config_error = validate_send_http_batch_config(
        float(task.rate_limit or 0),
        get_send_http_batch_size(task),
    )
    if batch_config_error:
        return batch_config_error
    for configured_template in templates or [template]:
        if not (configured_template.html_body or "").strip() and not (configured_template.text_body or "").strip():
            return f"模板 {configured_template.id} 的正文为空"
    health_error = await ensure_channel_ready_for_task_start(int(task.channel_id))
    if health_error:
        return health_error
    url_pool_error = validate_task_url_pool_binding(task)
    if url_pool_error:
        return url_pool_error
    return None


async def _persist_claim_cursor_to_db(
    task_id: int,
    cursor_row: int,
    cursor_id: int = 0,
    *,
    force: bool = False,
    allow_decrease: bool = False,
) -> None:
    normalized_cursor = max(0, int(cursor_row or 0))
    normalized_cursor_id = max(0, int(cursor_id or 0))
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None and bool(runtime.get("claim_cursor_sealed_for_shutdown", False)) and not allow_decrease:
        sealed_cursor = (
            max(0, int(runtime.get("claim_cursor_row", 0) or runtime.get("pending_claim_cursor_row", 0) or 0)),
            max(0, int(runtime.get("claim_cursor_id", 0) or runtime.get("pending_claim_cursor_id", 0) or 0)),
        )
        if (normalized_cursor, normalized_cursor_id) > sealed_cursor:
            logger.info(
                "task %s skip stale claim cursor persist during shutdown cursor_row=%s sealed_cursor=%s",
                task_id,
                normalized_cursor,
                sealed_cursor,
            )
            return
    if runtime is not None and not force:
        pending_batches = int(runtime.get("claim_persist_pending_batches", 0)) + 1
        runtime["claim_persist_pending_batches"] = pending_batches
        runtime["pending_claim_cursor_row"] = normalized_cursor
        runtime["pending_claim_cursor_id"] = normalized_cursor_id
        if pending_batches < CLAIM_CURSOR_PERSIST_EVERY_N_BATCHES:
            return
        runtime["claim_persist_pending_batches"] = 0
    try:
        async with asyncio.timeout(CLAIM_PERSIST_TIMEOUT_SECONDS):
            async with AsyncSessionLocal() as db:
                stmt = update(Task).where(Task.id == task_id)
                if not allow_decrease:
                    stmt = stmt.where(
                        (Task.last_claimed_row_number.is_(None))
                        | (Task.last_claimed_row_number < normalized_cursor)
                        | (
                            (Task.last_claimed_row_number == normalized_cursor)
                            & (
                                Task.last_claimed_recipient_id.is_(None)
                                | (Task.last_claimed_recipient_id < normalized_cursor_id)
                            )
                        )
                    )
                await db.execute(
                    stmt.values(
                        last_claimed_row_number=normalized_cursor,
                        last_claimed_recipient_id=normalized_cursor_id,
                        updated_at=datetime.utcnow(),
                    )
                )
                await commit_session_with_retry(db)
    except TimeoutError:
        logger.warning(
            "task %s claim cursor persist timed out after %ss cursor_row=%s; will retry on next claim",
            task_id,
            CLAIM_PERSIST_TIMEOUT_SECONDS,
            normalized_cursor,
        )
        if runtime is not None:
            runtime["claim_persist_timeout_count"] = int(runtime.get("claim_persist_timeout_count", 0)) + 1
        return
    if runtime is not None:
        runtime["pending_claim_cursor_row"] = normalized_cursor
        runtime["pending_claim_cursor_id"] = normalized_cursor_id
        runtime["persisted_cursor_row"] = normalized_cursor
        runtime["persisted_cursor_id"] = normalized_cursor_id
        if force:
            runtime["claim_persist_pending_batches"] = 0


async def flush_pending_claim_cursor(task_id: int) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return
    writer = _claim_cursor_writer_tasks.pop(task_id, None)
    _claim_cursor_writer_events.pop(task_id, None)
    if writer is not None and not writer.done():
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
    pending_cursor = int(runtime.get("pending_claim_cursor_row", 0) or runtime.get("claim_cursor_row", 0) or 0)
    pending_cursor_id = int(runtime.get("pending_claim_cursor_id", 0) or runtime.get("claim_cursor_id", 0) or 0)
    if pending_cursor <= 0 and pending_cursor_id <= 0:
        return
    await _persist_claim_cursor_to_db(task_id, pending_cursor, pending_cursor_id, force=True)


def resolve_task_concurrency_guidance(
    concurrency: int | None,
    rate_limit: float,
    send_http_batch_size: int | None = None,
) -> tuple[int | None, str | None]:
    per_second_rate = max(0.0, float(rate_limit or 0))
    if per_second_rate <= 0:
        return None, None
    normalized = normalize_task_concurrency(concurrency)
    batch_size = max(1, int(send_http_batch_size or settings.default_http_batch_size))
    recommended = calculate_recommended_concurrency(
        per_second_rate,
        int(round(get_estimated_proxy_rtt_seconds() * 1000)),
        batch_size,
    )
    warning = build_concurrency_warning(normalized, per_second_rate, send_http_batch_size=batch_size)
    return recommended, warning


def get_task_concurrency_guidance_from_runtime(task_id: int, task: Task | None = None) -> tuple[int | None, str | None]:
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is not None:
        recommended = runtime.get("recommended_concurrency")
        warning = runtime.get("concurrency_warning")
        normalized_recommended = int(recommended) if recommended else None
        normalized_warning = str(warning).strip() if warning else None
        if normalized_recommended or normalized_warning:
            return normalized_recommended if normalized_recommended else None, normalized_warning
    if task is None:
        return None, None
    return resolve_task_concurrency_guidance(
        task.concurrency,
        float(task.rate_limit or 0),
        get_send_http_batch_size(task),
    )


async def refresh_recipient_statistics_for_task_start(task_id: int) -> None:
    """启动热路径不再执行 ANALYZE，统计信息由部署迁移和定时维护负责。"""
    logger.debug("任务 %s 启动跳过 recipients ANALYZE，避免与在线删除、日志读取和发送写入争用 I/O", task_id)


def _get_recipient_claim_semaphore() -> asyncio.Semaphore:
    global _recipient_claim_semaphore
    with _recipient_claim_semaphore_lock:
        if _recipient_claim_semaphore is None:
            _recipient_claim_semaphore = asyncio.Semaphore(max(1, int(RECIPIENT_CLAIM_GLOBAL_MAX_INFLIGHT)))
        return _recipient_claim_semaphore


def _is_recipient_claim_transient_error(exc: Exception) -> bool:
    """连接池耗尽、语句超时、瞬时网络错误应软重试，不得升级为任务致命失败。"""
    if isinstance(exc, (SQLAlchemyTimeoutError, OperationalError, DBAPIError, TimeoutError, asyncio.TimeoutError)):
        return True
    if is_database_pool_pressure_error(exc) or _is_transient_db_error(exc):
        return True
    message = str(exc).lower()
    transient_markers = (
        "queuepool limit",
        "connection timed out",
        "timeout waiting for connection",
        "too many connections",
        "remaining connection slots",
        "statement timeout",
        "canceling statement due to statement timeout",
        "connection does not exist",
        "server closed the connection",
        "connection was closed",
    )
    return any(marker in message for marker in transient_markers)


def _mark_claim_soft_retry(task_id: int, exc: Exception | None = None, *, reason: str = "") -> None:
    runtime = _task_runtime_metrics.get(int(task_id or 0))
    if runtime is None:
        return
    runtime["claim_query_timed_out"] = True
    runtime["claim_fast_timeout_count"] = int(runtime.get("claim_fast_timeout_count", 0) or 0) + 1
    if exc is not None:
        runtime["last_claim_error"] = type(exc).__name__
    elif reason:
        runtime["last_claim_error"] = reason


async def _fetch_claim_candidate_recipients(
    task: Task,
    cursor_row: int,
    cursor_id: int,
    fetch_limit: int,
    *,
    exclude_logged: bool = True,
) -> list[Recipient]:
    claim_total_started_at = monotonic()
    normalized_cursor = max(0, int(cursor_row or 0))
    normalized_cursor_id = max(0, int(cursor_id or 0))
    normalized_limit = max(1, min(int(fetch_limit or 1), RECIPIENT_FAST_CLAIM_PAGE_SIZE))
    # 热路径不能逐页关联 send_logs：大任务会使数据库读取竞争发送链路。
    # 断点恢复时先完整加载 completed 集合，再由内存 completed/inflight 过滤；数据库只在启动校准与收尾守卫读取 SendLog。
    _ = exclude_logged
    claim_sql = text(
        f"""
        SELECT id, list_id, email, normalized_email,
               name, company, phone, tags, custom_fields_json,
               row_number, is_valid, is_unsubscribed, created_at
        FROM recipients
        WHERE list_id = :list_id
          AND (row_number, id) > (:cursor_row, :cursor_id)
          AND row_number > 0
          AND normalized_email <> ''
          AND is_valid IS TRUE
          AND COALESCE(is_unsubscribed, FALSE) IS FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM unsubscribes u
              WHERE u.normalized_email = recipients.normalized_email
          )
        ORDER BY row_number ASC, id ASC
        LIMIT :fetch_limit
        """
    )
    params = {
        "list_id": int(task.recipient_list_id or 0),
        "cursor_row": normalized_cursor,
        "cursor_id": normalized_cursor_id,
        "fetch_limit": normalized_limit,
    }
    claim_semaphore = _get_recipient_claim_semaphore()
    semaphore_wait_started_at = monotonic()
    async with claim_semaphore:
        semaphore_wait_ms = round((monotonic() - semaphore_wait_started_at) * 1000, 2)
        async with RecipientClaimSessionLocal() as db:
            # 连接已在 Engine 级配置 statement_timeout/work_mem；热路径避免每页显式 begin + 两次 SET LOCAL。
            sql_started_at = monotonic()
            result = await db.execute(claim_sql, params)
            rows = result.mappings().all()
            sql_ms = round((monotonic() - sql_started_at) * 1000, 2)
            await db.rollback()
    query_ms = round((monotonic() - claim_total_started_at) * 1000, 2)
    recipients: list[Recipient] = []
    runtime = _task_runtime_metrics.get(int(task.id or 0))
    if runtime is not None:
        samples = runtime.get("claim_query_latency_samples_ms")
        if not isinstance(samples, deque):
            samples = deque(maxlen=CLAIM_QUERY_LATENCY_SAMPLE_SIZE)
            runtime["claim_query_latency_samples_ms"] = samples
        samples.append(query_ms)
        sample_values = list(samples)
        runtime["last_claim_query_ms"] = query_ms
        runtime["last_claim_semaphore_wait_ms"] = semaphore_wait_ms
        runtime["last_claim_connection_setup_ms"] = round(max(0.0, query_ms - semaphore_wait_ms - sql_ms), 2)
        runtime["last_claim_sql_ms"] = sql_ms
        runtime["claim_query_p50_ms"] = round(_percentile_value(sample_values, 0.50), 2)
        runtime["claim_query_p95_ms"] = round(_percentile_value(sample_values, 0.95), 2)
        runtime["claim_query_p99_ms"] = round(_percentile_value(sample_values, 0.99), 2)
        if query_ms > CLAIM_QUERY_THROTTLE_THRESHOLD_MS:
            throttle_seconds = min(
                CLAIM_QUERY_THROTTLE_MAX_SECONDS,
                max(0.05, query_ms / 1000.0 * 0.25),
            )
            runtime["claim_query_throttle_until"] = monotonic() + throttle_seconds
            runtime["claim_query_throttle_seconds"] = round(throttle_seconds, 3)
            runtime["claim_query_throttle_count"] = int(runtime.get("claim_query_throttle_count", 0) or 0) + 1
            logger.warning(
                "任务 %s 单次收件人预取耗时 %sms 超过 500ms（信号量等待=%sms，连接及事务准备=%sms，SQL及取数=%sms），已触发 %.3fs 限频保护，预取将继续",
                int(task.id or 0),
                query_ms,
                semaphore_wait_ms,
                round(max(0.0, query_ms - semaphore_wait_ms - sql_ms), 2),
                sql_ms,
                throttle_seconds,
            )
    if runtime is not None and (query_ms >= CLAIM_QUERY_THROTTLE_THRESHOLD_MS or len(samples) == 1 or int(runtime.get("claim_count", 0) or 0) % 20 == 0):
        logger.info(
            "任务 %s 收件人预取延迟：当前=%sms P50=%sms P95=%sms P99=%sms 样本数=%s",
            int(task.id or 0),
            query_ms,
            runtime["claim_query_p50_ms"],
            runtime["claim_query_p95_ms"],
            runtime["claim_query_p99_ms"],
            len(samples),
        )
    maybe_explain = bool(getattr(settings, "perf_debug", False))
    runtime = _task_runtime_metrics.get(int(task.id or 0))
    if runtime is not None:
        explain_logged = bool(runtime.get("claim_explain_logged", False))
    else:
        explain_logged = False
    if runtime is not None:
        runtime["last_claim_query_rows"] = len(rows)
        runtime["last_claim_list_id"] = int(task.recipient_list_id or 0)
        runtime["last_claim_query_cursor"] = normalized_cursor
        runtime["last_claim_query_cursor_id"] = normalized_cursor_id
        runtime["last_claim_query_limit"] = normalized_limit
    if maybe_explain and not explain_logged:
        await _log_recipient_claim_explain_plan(task, normalized_cursor, normalized_cursor_id, normalized_limit)
        if runtime is not None:
            runtime["claim_explain_logged"] = True
    if not rows and normalized_cursor <= 0 and runtime is not None:
        runtime["empty_claim_diagnostics_requested"] = True
    for row in rows:
        recipient = Recipient()
        recipient.id = int(row["id"] or 0)
        recipient.list_id = int(row["list_id"] or 0)
        recipient.email = str(row["email"] or "")
        recipient.normalized_email = str(row.get("normalized_email") or "")
        recipient.name = str(row.get("name") or "")
        recipient.company = str(row.get("company") or "")
        recipient.phone = str(row.get("phone") or "")
        recipient.tags = str(row.get("tags") or "")
        recipient.custom_fields_json = str(row.get("custom_fields_json") or "{}")
        recipient.row_number = int(row["row_number"] or 0)
        recipient.is_valid = bool(row["is_valid"])
        recipient.is_unsubscribed = bool(row["is_unsubscribed"])
        recipient.created_at = row.get("created_at")
        recipients.append(recipient)
    return recipients


async def _log_empty_claim_diagnostics(task: Task, cursor_row: int) -> None:
    """低频记录首个游标页为空时的过滤计数，不在 claim 热路径中执行。"""
    task_id = int(task.id or 0)
    list_id = int(task.recipient_list_id or 0)
    try:
        async with RecipientClaimSessionLocal() as db:
            result = await db.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE COALESCE(row_number, 0) > 0) AS positive_rows,
                        COUNT(*) FILTER (WHERE NULLIF(BTRIM(email), '') IS NOT NULL) AS nonempty_email_rows,
                        COUNT(*) FILTER (WHERE COALESCE(is_valid, FALSE) IS TRUE) AS valid_rows,
                        COUNT(*) FILTER (WHERE COALESCE(is_unsubscribed, FALSE) IS FALSE) AS unsubscribed_ok_rows
                    FROM recipients
                    WHERE list_id = :list_id
                    """
                ),
                {"list_id": list_id},
            )
            counts = result.mappings().one()
        runtime = _task_runtime_metrics.get(task_id) or {}
        logger.warning(
            "任务 %s 首次 claim 为空诊断 list_id=%s cursor=%s total=%s positive_row=%s nonempty_email=%s valid=%s unsubscribed_ok=%s cached_max_row=%s",
            task_id,
            list_id,
            int(cursor_row or 0),
            int(counts.get("total_rows") or 0),
            int(counts.get("positive_rows") or 0),
            int(counts.get("nonempty_email_rows") or 0),
            int(counts.get("valid_rows") or 0),
            int(counts.get("unsubscribed_ok_rows") or 0),
            int(runtime.get("recipient_list_max_row_number", 0) or 0),
        )
    except Exception as exc:
        logger.exception(
            "任务 %s 首次 claim 为空诊断失败 task_id=%s list_id=%s cursor=%s error_type=%s",
            task_id,
            task_id,
            list_id,
            int(cursor_row or 0),
            type(exc).__name__,
        )


async def _log_recipient_claim_explain_plan(task: Task, cursor_row: int, cursor_id: int, fetch_limit: int) -> None:
    """仅在 PERF_DEBUG 开启时输出收件人 claim 查询计划，用于确认覆盖索引是否命中。"""
    normalized_limit = max(1, min(int(fetch_limit or 1), RECIPIENT_FAST_CLAIM_PAGE_SIZE))
    try:
        async with RecipientClaimSessionLocal() as db:
            await db.execute(text("SET statement_timeout = '15s'"))
            await db.execute(text("SET random_page_cost = 1.1"))
            await db.execute(text("SET enable_seqscan = off"))
            result = await db.execute(
                text(
                    """
                    EXPLAIN (ANALYZE, BUFFERS)
                    SELECT id, list_id, email,
                           name, company, phone, tags, custom_fields_json,
                           row_number, is_valid, is_unsubscribed, created_at
                    FROM recipients
                    WHERE list_id = :list_id
                      AND (row_number, id) > (:cursor_row, :cursor_id)
                      AND row_number > 0
                      AND email IS NOT NULL
                      AND BTRIM(email) <> ''
                      AND is_valid IS TRUE
                      AND COALESCE(is_unsubscribed, FALSE) IS FALSE
                      AND NOT EXISTS (
                          SELECT 1
                          FROM unsubscribes u
                          WHERE u.normalized_email = recipients.normalized_email
                      )
                    ORDER BY row_number ASC, id ASC
                    LIMIT :fetch_limit
                    """
                ),
                {
                    "list_id": int(task.recipient_list_id or 0),
                    "cursor_row": max(0, int(cursor_row or 0)),
                    "cursor_id": max(0, int(cursor_id or 0)),
                    "fetch_limit": normalized_limit,
                },
            )
            plan_lines = [str(row[0]) for row in result.all()]
        plan_text = "\n".join(plan_lines)
        expected_indexes = ("ix_recipients_claim_row_id_v17", "ix_recipients_claim_covering_v18")
        matched_index = next((name for name in expected_indexes if name in plan_text), "")
        if not matched_index:
            logger.warning(
                "任务 %s 收件人领取查询未命中预期索引 %s，请检查迁移与 ANALYZE。执行计划：\n%s",
                int(task.id or 0),
                " 或 ".join(expected_indexes),
                plan_text,
            )
        else:
            logger.info(
                "任务 %s 收件人领取查询执行计划确认命中索引 %s：\n%s",
                int(task.id or 0),
                matched_index,
                plan_text,
            )
    except Exception as exc:
        logger.warning("任务 %s 收件人 claim 查询计划验证失败：%s", int(task.id or 0), exc)


async def _has_unclaimed_recipients_ahead(
    task: Task,
    cursor_row: int,
    inflight_emails: set[str],
    completed_emails: set[str] | None = None,
    cursor_id: int = 0,
) -> bool:
    """低频守卫从已知游标向前分页，禁止回到游标 0 或执行额外窗口/最大行扫描。"""
    probe_row = max(0, int(cursor_row or 0))
    probe_id = max(0, int(cursor_id or 0))
    completed = completed_emails if completed_emails is not None else set()
    for _ in range(COMPLETION_GUARD_REWIND_LIMIT + 2):
        candidates = await _fetch_claim_candidate_recipients(task, probe_row, probe_id, 50, exclude_logged=True)
        if not candidates:
            return False
        last_candidate = max(candidates, key=lambda item: (int(item.row_number or 0), int(item.id or 0)))
        probe_row = int(last_candidate.row_number or probe_row)
        probe_id = int(last_candidate.id or probe_id)
        for recipient in candidates:
            if _is_recipient_in_key_set(inflight_emails, recipient):
                continue
            if _is_recipient_in_key_set(completed, recipient):
                continue
            if _normalize_inflight_email(str(recipient.email or "")):
                return True
    return True


async def _has_unfinished_regular_recipients(
    task: Task,
    inflight_emails: set[str],
    completed_emails: set[str] | None = None,
    *,
    force: bool = False,
) -> bool:
    """完成守卫：从已成功发送最大 row_number 附近补漏扫描，并从该行 id=0 开始避免跳过同 row_number 的收件人。"""
    completed = completed_emails if completed_emails is not None else set()
    if inflight_emails:
        return True
    runtime = _task_runtime_metrics.get(int(task.id or 0))
    max_success_row = int((runtime or {}).get("max_successful_regular_row_number", 0) or 0)
    cursor_row = max(0, max_success_row - int(RECIPIENT_MEMORY_LOAD_MAX_BATCH))
    if cursor_row <= 0:
        cursor_row = max(0, int((runtime or {}).get("startup_cursor_row", 0) or 0))
    cursor_id = 0
    try:
        has_ahead = await asyncio.wait_for(
            _has_unclaimed_recipients_ahead(
                task,
                cursor_row,
                inflight_emails,
                completed,
                cursor_id,
            ),
            timeout=15.0,
        )
        if runtime is not None:
            runtime["unfinished_guard_last_at"] = monotonic()
            runtime["unfinished_guard_last_result"] = has_ahead
            runtime["unfinished_guard_last_count"] = 1 if has_ahead else 0
            runtime["completion_unfinished_count"] = 1 if has_ahead else 0
        return has_ahead
    except Exception as exc:
        if runtime is not None:
            runtime["unfinished_guard_timeout_count"] = int(runtime.get("unfinished_guard_timeout_count", 0) or 0) + 1
            runtime["unfinished_guard_last_result"] = True
        logger.warning(
            "任务 %s 内存完成守卫查询失败，为避免提前结束，本轮继续预取 error_type=%s error=%s",
            int(task.id or 0),
            type(exc).__name__,
            exc,
        )
        return True


async def _load_recipients_page_for_memory_queue(
    task: Task,
    cursor_row: int,
    limit: int,
    inflight_emails: set[str] | None = None,
    completed_emails: set[str] | None = None,
) -> tuple[list[Recipient], int, str | None, int]:
    load_started_at = monotonic()
    normalized_limit = max(1, min(int(limit or 1), CLAIM_SINGLE_FETCH_MAX))
    original_cursor = max(0, int(cursor_row or 0))
    reserved = inflight_emails if inflight_emails is not None else set()
    completed = completed_emails if completed_emails is not None else set()
    runtime = _task_runtime_metrics.get(int(task.id or 0))
    if runtime is not None:
        runtime["last_claim_error"] = ""
        runtime["last_claim_cursor_before"] = original_cursor
        runtime["last_claim_requested_limit"] = normalized_limit
    try:
        # 兼容旧调用方：该函数仅保留为一次 keyset 读取，不执行 EOF、窗口或补漏探测。
        candidates = await _fetch_claim_candidate_recipients(task, original_cursor, 0, normalized_limit, exclude_logged=True)
    except TimeoutError as exc:
        # 内置 TimeoutError / asyncio.TimeoutError
        _release_inflight_recipients(reserved, list(locals().get("candidates", []) or []))
        _mark_claim_soft_retry(int(task.id or 0), exc, reason="timeout")
        logger.warning(
            "任务 %s 收件人极速加载超过 %sms，已放弃本轮并立即重试 cursor_row=%s limit=%s",
            int(task.id or 0),
            int(CLAIM_QUERY_TIMEOUT_SECONDS * 1000),
            original_cursor,
            normalized_limit,
        )
        return [], original_cursor, "recipient_claim_timeout", 0
    except SQLAlchemyError as exc:
        # 数据库错误时保持原游标。调用方只能重试，不能将空结果误判为物理 EOF。
        _release_inflight_recipients(reserved, list(locals().get("candidates", []) or []))
        _mark_claim_soft_retry(int(task.id or 0), exc, reason="database_error")
        logger.exception(
            "任务 %s 收件人预取数据库异常，游标不推进并等待重试 cursor_row=%s limit=%s：\n%s",
            int(task.id or 0),
            original_cursor,
            normalized_limit,
            "数据库异常完整 traceback 已由本日志附带",
        )
        return [], original_cursor, f"recipient_claim_database_error:{type(exc).__name__}", 0
    except Exception as exc:
        # 注意：sqlalchemy.exc.TimeoutError 不继承内置 TimeoutError；多任务抢 claim 池时最常见。
        _release_inflight_recipients(reserved, list(locals().get("candidates", []) or []))
        if _is_recipient_claim_transient_error(exc):
            _mark_claim_soft_retry(int(task.id or 0), exc)
            logger.warning(
                "任务 %s 收件人极速加载瞬时失败（将软重试） cursor_row=%s error_type=%s error=%s",
                int(task.id or 0),
                original_cursor,
                type(exc).__name__,
                exc,
            )
            return [], original_cursor, f"recipient_claim_transient:{type(exc).__name__}", 0
        runtime = _task_runtime_metrics.get(int(task.id or 0))
        if runtime is not None:
            runtime["last_claim_error"] = type(exc).__name__
        logger.exception("任务 %s 全内存收件人加载异常 cursor_row=%s", int(task.id or 0), original_cursor)
        return [], original_cursor, f"recipient_memory_load_unexpected_error:{type(exc).__name__}", 0

    loaded: list[Recipient] = []
    skipped_reserved = 0
    skipped_completed = 0
    skipped_duplicate_candidate = 0
    skipped_empty_email = 0
    new_cursor = original_cursor
    max_candidate_row = new_cursor
    seen_candidate_keys: set[str] = set()
    for recipient in candidates:
        row_number = int(recipient.row_number or 0)
        if row_number > 0:
            max_candidate_row = max(max_candidate_row, row_number)
        email = _normalize_inflight_email(str(recipient.email or ""))
        if not email:
            skipped_empty_email += 1
            continue
        inflight_key = _inflight_recipient_key(recipient)
        if inflight_key and inflight_key in seen_candidate_keys:
            skipped_duplicate_candidate += 1
            continue
        if inflight_key:
            seen_candidate_keys.add(inflight_key)
        if _is_recipient_in_key_set(completed, recipient):
            skipped_completed += 1
            continue
        if _is_recipient_in_key_set(reserved, recipient):
            skipped_reserved += 1
            continue
        # 加载成功即占用 inflight，关闭「游标已推进但尚未入队」窗口，避免二次预取。
        if not _try_reserve_inflight_recipient(reserved, recipient, completed):
            skipped_reserved += 1
            continue
        loaded.append(recipient)
    if loaded:
        last_loaded = max(
            loaded,
            key=lambda recipient: (int(recipient.row_number or 0), int(recipient.id or 0)),
        )
        new_cursor = max(original_cursor, int(last_loaded.row_number or 0))
        if runtime is not None:
            runtime["last_claim_loaded_cursor_row"] = new_cursor
            runtime["last_claim_loaded_cursor_id"] = int(last_loaded.id or 0)
    elif candidates and skipped_reserved > 0 and (
        skipped_reserved + skipped_completed + skipped_duplicate_candidate + skipped_empty_email >= len(candidates)
    ):
        # 仍有在途收件人时不能推进游标，否则可能越过尚未落库的发送。
        new_cursor = original_cursor
    elif candidates and skipped_completed > 0 and (
        skipped_completed + skipped_duplicate_candidate + skipped_empty_email >= len(candidates)
    ):
        # 本页候选均已完成：推进游标越过本页，避免 completed 过滤后空转死循环。
        new_cursor = max(original_cursor, max_candidate_row)
    elif candidates and skipped_empty_email >= len(candidates):
        new_cursor = max(original_cursor, max_candidate_row)
    elif candidates:
        new_cursor = max(original_cursor, max_candidate_row)
    load_ms = round((monotonic() - load_started_at) * 1000, 2)
    runtime = _task_runtime_metrics.get(int(task.id or 0))
    if runtime is not None:
        runtime["last_claim_ms"] = load_ms
        runtime["last_claim_size"] = len(loaded)
        runtime["last_claim_candidate_count"] = len(candidates)
        runtime["last_claim_skipped_reserved"] = skipped_reserved
        runtime["last_claim_skipped_completed"] = skipped_completed
        runtime["last_claim_skipped_duplicate_candidate"] = skipped_duplicate_candidate
        runtime["last_claim_skipped_empty_email"] = skipped_empty_email
        runtime["inflight_email_count"] = len(reserved)
        runtime["completed_email_count"] = len(completed)
        runtime["claim_cursor_row"] = new_cursor
        runtime["pending_claim_cursor_row"] = new_cursor
        claim_samples = list(runtime.get("claim_query_samples_ms", []) or [])[-119:]
        claim_samples.append(load_ms)
        claim_samples.sort()
        runtime["claim_query_samples_ms"] = claim_samples
        runtime["claim_query_p50_ms"] = claim_samples[len(claim_samples) // 2]
        runtime["claim_query_p95_ms"] = claim_samples[min(len(claim_samples) - 1, int(len(claim_samples) * 0.95))]
        runtime["claim_count"] = int(runtime.get("claim_count", 0)) + 1
    if runtime is not None and loaded:
        runtime["claim_query_timed_out"] = False
        runtime["claim_fast_timeout_count"] = 0
    slow_claim_threshold_ms = CLAIM_SLOW_LOG_MS
    if load_ms >= slow_claim_threshold_ms:
        logger.warning(
            "任务 %s 收件人预取超过 %sms cursor_row=%s new_cursor=%s loaded=%s candidates=%s skipped_reserved=%s skipped_completed=%s skipped_duplicate=%s inflight=%s completed=%s load_ms=%s",
            int(task.id or 0),
            int(slow_claim_threshold_ms),
            original_cursor,
            new_cursor,
            len(loaded),
            len(candidates),
            skipped_reserved,
            skipped_completed,
            skipped_duplicate_candidate,
            len(reserved),
            len(completed),
            load_ms,
        )
    else:
        logger.info(
            "任务 %s 全内存收件人加载完成 cursor_row=%s new_cursor=%s loaded=%s candidates=%s skipped_reserved=%s skipped_completed=%s skipped_duplicate=%s inflight=%s completed=%s load_ms=%s",
            int(task.id or 0),
            original_cursor,
            new_cursor,
            len(loaded),
            len(candidates),
            skipped_reserved,
            skipped_completed,
            skipped_duplicate_candidate,
            len(reserved),
            len(completed),
            load_ms,
        )
    _record_claim_success(int(task.id or 0), len(loaded))
    _touch_task_progress(int(task.id or 0))
    return loaded, new_cursor, None, len(candidates)


async def _load_recipients_keyset_page(
    task: Task,
    cursor_row: int,
    cursor_id: int,
    limit: int,
    inflight_emails: set[str] | None = None,
) -> tuple[list[Recipient], int, int, bool, str | None, int]:
    """热路径只执行一次 keyset claim，并依据启动时缓存的最大行号返回 has_more。"""
    started_at = monotonic()
    task_id = int(task.id or 0)
    original_cursor = max(0, int(cursor_row or 0))
    original_cursor_id = max(0, int(cursor_id or 0))
    normalized_limit = max(1, min(int(limit or 1), CLAIM_SINGLE_FETCH_MAX))
    reserved = inflight_emails if inflight_emails is not None else set()
    runtime = _task_runtime_metrics.get(task_id)
    cached_max_row = max(0, int((runtime or {}).get("recipient_list_max_row_number", 0) or 0))
    try:
        candidates = await _fetch_claim_candidate_recipients(
            task,
            original_cursor,
            original_cursor_id,
            normalized_limit,
            exclude_logged=True,
        )
    except Exception as exc:
        if _is_recipient_claim_transient_error(exc) or isinstance(exc, SQLAlchemyError):
            _mark_claim_soft_retry(task_id, exc)
            logger.warning(
                "任务 %s keyset 收件人预取瞬时失败 cursor=%s error_type=%s error=%s",
                task_id,
                original_cursor,
                type(exc).__name__,
                exc,
            )
            return [], original_cursor, original_cursor_id, True, f"recipient_claim_transient:{type(exc).__name__}", 0
        logger.exception("任务 %s keyset 收件人预取异常 cursor=(%s,%s)", task_id, original_cursor, original_cursor_id)
        return [], original_cursor, original_cursor_id, True, f"recipient_claim_error:{type(exc).__name__}", 0

    next_cursor = original_cursor
    next_cursor_id = original_cursor_id
    if candidates:
        last_candidate = max(
            candidates,
            key=lambda item: (int(item.row_number or 0), int(item.id or 0)),
        )
        next_cursor = int(last_candidate.row_number or 0)
        next_cursor_id = int(last_candidate.id or 0)
    loaded: list[Recipient] = []
    seen_keys: set[str] = set()
    for recipient in candidates:
        key = _inflight_recipient_key(recipient)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        if _is_recipient_in_key_set(reserved, recipient):
            continue
        if _try_reserve_inflight_recipient(reserved, recipient):
            loaded.append(recipient)

    has_more = bool(candidates) and (len(candidates) >= normalized_limit or next_cursor < cached_max_row)
    if not candidates:
        has_more = False
    elif len(candidates) < normalized_limit and (cached_max_row <= 0 or next_cursor >= cached_max_row):
        # 末页不足一个预取批次时，当前页就是物理末尾；不能再依赖下一次空查询确认 EOF。
        has_more = False
    if runtime is not None:
        elapsed_ms = round((monotonic() - started_at) * 1000, 2)
        runtime["last_claim_ms"] = elapsed_ms
        runtime["last_claim_size"] = len(loaded)
        runtime["last_claim_candidate_count"] = len(candidates)
        runtime["claim_cursor_row"] = next_cursor
        runtime["claim_cursor_id"] = next_cursor_id
        runtime["pending_claim_cursor_row"] = next_cursor
        runtime["pending_claim_cursor_id"] = next_cursor_id
        runtime["claim_has_more"] = has_more
        runtime["inflight_email_count"] = len(reserved)
        runtime["claim_count"] = int(runtime.get("claim_count", 0) or 0) + 1
    _record_claim_success(task_id, len(loaded))
    if loaded:
        _touch_task_progress(task_id)
    return loaded, next_cursor, next_cursor_id, has_more, None, len(candidates)


async def _get_recipient_list_max_row_number(task: Task, *, timeout_seconds: float = 15.0) -> int:
    list_id = int(getattr(task, "recipient_list_id", 0) or 0)
    if list_id <= 0:
        return 0

    async def _query_max_row() -> int:
        async with RecipientClaimSessionLocal() as db:
            result = await db.execute(
                select(func.max(Recipient.row_number)).where(
                    Recipient.list_id == list_id,
                    Recipient.row_number > 0,
                )
            )
            return max(0, int(result.scalar_one_or_none() or 0))

    return await asyncio.wait_for(_query_max_row(), timeout=max(1.0, float(timeout_seconds or 15.0)))


async def _completion_auditor_loop(
    task_id: int,
    context: dict[str, object],
) -> None:
    """独立完成审计器：低频查询结果账本、补入缺口并确认任务完成。"""
    recipient_queue = context["recipient_queue"]
    inflight_emails = context["inflight_emails"]
    stop_event = context["stop_event"]
    if not isinstance(recipient_queue, asyncio.Queue) or not isinstance(inflight_emails, set) or not isinstance(stop_event, asyncio.Event):
        logger.error("任务 %s CompletionAuditor 上下文无效，无法启动", task_id)
        return
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return
    wake_event = runtime.get("completion_auditor_wake_event")
    finished_event = runtime.get("completion_auditor_finished_event")
    if not isinstance(wake_event, asyncio.Event) or not isinstance(finished_event, asyncio.Event):
        logger.error("任务 %s CompletionAuditor 缺少事件对象，无法启动", task_id)
        return
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(wake_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass
        wake_event.clear()
        runtime = _task_runtime_metrics.get(task_id)
        if runtime is None or stop_event.is_set():
            break
        if not bool(runtime.get("prefetch_exhausted", False)):
            continue
        writer = _get_send_log_writer(task_id)
        busy = (
            recipient_queue.qsize() > 0
            or bool(inflight_emails)
            or int(runtime.get("active_sends", 0) or 0) > 0
            or int(runtime.get(WORKER_LOCAL_BATCH_INFLIGHT_KEY, 0) or 0) > 0
            or int(runtime.get("httpx_inflight", 0) or 0) > 0
            or int(runtime.get("outstanding_writes", 0) or 0) > 0
            or (writer is not None and writer.pending_count > 0)
        )
        if busy:
            runtime["completion_auditor_reason"] = "发送链路仍有在途数据"
            # EOF 后不会再有预取事件；排空过程中主动复查，避免首次审计命中在途数据后长时间等待。
            await asyncio.sleep(0.2)
            wake_event.set()
            continue
        task = await _load_task_for_loop_refresh(task_id)
        if task is None:
            runtime["completion_auditor_error"] = "任务已不存在"
            break
        try:
            async with RecipientClaimSessionLocal() as db:
                gap_result = await db.execute(
                    select(Recipient)
                    .where(
                        Recipient.list_id == int(task.recipient_list_id or 0),
                        Recipient.row_number > 0,
                        Recipient.row_number <= int(runtime.get("claim_cursor_row", 0) or 0),
                        Recipient.email.is_not(None),
                        func.btrim(Recipient.email) != "",
                        Recipient.is_valid.is_(True),
                        func.coalesce(Recipient.is_unsubscribed, False).is_(False),
                        ~select(CompactOutcome.id).where(
                            CompactOutcome.task_id == task_id,
                            CompactOutcome.recipient_id == Recipient.id,
                            CompactOutcome.log_type == "regular",
                            CompactOutcome.status.in_(("success", "failed", "bounced", "unknown")),
                        ).exists(),
                    )
                    .order_by(Recipient.row_number.asc(), Recipient.id.asc())
                    .limit(COMPLETION_AUDIT_BATCH_SIZE)
                )
                gaps = list(gap_result.scalars().all())
                max_success_result = await db.execute(
                    select(func.max(Recipient.row_number))
                    .join(CompactOutcome, CompactOutcome.recipient_id == Recipient.id)
                    .where(
                        CompactOutcome.task_id == task_id,
                        CompactOutcome.log_type == "regular",
                        CompactOutcome.status == "success",
                    )
                )
                max_success_row = int(max_success_result.scalar_one_or_none() or 0)
        except Exception as exc:
            runtime["completion_auditor_error"] = f"{type(exc).__name__}: {exc}"[:300]
            _log_sampled_task_warning(
                f"completion_auditor:{task_id}",
                "任务 %s CompletionAuditor 查询失败 error_type=%s error=%s",
                task_id,
                type(exc).__name__,
                exc,
            )
            continue
        runtime["completion_auditor_max_success_row"] = max_success_row
        runtime["completion_auditor_last_at"] = monotonic()
        runtime["completion_auditor_error"] = ""
        if gaps:
            queued = 0
            prepared: list[Recipient] = []
            hooks = _task_shutdown_hooks.get(task_id) or {}
            url_pool_cache = hooks.get("url_pool_cache")
            send_log_id_allocator = hooks.get("send_log_id_allocator")
            reserved_gaps = [
                recipient
                for recipient in gaps
                if _try_reserve_inflight_recipient(inflight_emails, recipient)
            ]
            if reserved_gaps:
                try:
                    prepared = await _preallocate_queued_recipient_metadata(
                        task,
                        reserved_gaps,
                        url_pool_cache if isinstance(url_pool_cache, TaskUrlPoolCache) else None,
                        send_log_id_allocator if isinstance(send_log_id_allocator, SendLogIdAllocator) else None,
                        inflight_emails,
                    )
                except Exception as exc:
                    _release_inflight_recipients(inflight_emails, reserved_gaps)
                    runtime["completion_auditor_error"] = f"补发元数据分配失败：{type(exc).__name__}: {exc}"[:300]
                    _log_sampled_task_warning(
                        f"completion_auditor_metadata:{task_id}",
                        "任务 %s CompletionAuditor 补发元数据分配失败 count=%s error_type=%s error=%s",
                        task_id,
                        len(reserved_gaps),
                        type(exc).__name__,
                        exc,
                    )
                    continue
            for recipient in prepared:
                await recipient_queue.put(recipient)
                queued += 1
            runtime["completion_auditor_gap_count"] = len(gaps)
            runtime["completion_auditor_reason"] = f"发现 {len(gaps)} 个结果账本缺口，完成元数据分配后补入 {queued} 个"
            runtime["prefetch_exhausted"] = False
            logger.warning("任务 %s CompletionAuditor 发现缺口并完成元数据分配后补发 count=%s queued=%s", task_id, len(gaps), queued)
            continue
        runtime["completion_auditor_gap_count"] = 0
        runtime["completion_auditor_reason"] = "结果账本无缺口，任务完成"
        runtime["task_finished"] = True
        finished_event.set()
        logger.info(
            "任务 %s CompletionAuditor 确认完成 cursor=%s max_success_row=%s",
            task_id,
            int(runtime.get("claim_cursor_row", 0) or 0),
            max_success_row,
        )
        break


async def _prefetch_recipients_loop(
    task_id: int,
    recipient_queue: asyncio.Queue[Recipient | None],
    inflight_emails: set[str],
    prefetch_batch_size: int,
    stop_event: asyncio.Event,
    url_pool_cache: TaskUrlPoolCache | None = None,
    send_log_id_allocator: SendLogIdAllocator | None = None,
) -> None:
    """后台预取收件人：DB 游标按 row_number 顺序批量领取，受背压调节批次大小。"""
    normalized_batch_size = max(1, int(prefetch_batch_size or 1))
    consecutive_failures = 0
    consecutive_empty_or_reserved = 0
    stalled_empty_cursor_count = 0
    last_progress_cursor = None
    stalled_cursor_started_at = 0.0
    forced_cursor_advances = 0
    last_empty_cursor_row: int | None = None
    eof_sent = False
    fatal_error_message = ""
    exit_reason = "未知"
    backpressure_state: BackpressureState | None = None
    current_claim_batch: list[Recipient] = []
    current_queued_recipients: list[Recipient] = []
    cached_task: Task | None = None
    last_task_refresh_at = 0.0

    async def _ensure_eof_sentinel(*, mark_eof: bool = True) -> None:
        """仅在 EOF 已被明确确认或预取已失败时停止发送 Worker。"""
        nonlocal eof_sent
        nonlocal exit_reason
        if eof_sent:
            return
        runtime_ref = _task_runtime_metrics.get(task_id)
        # Worker 可能被动态扩缩容；用实际存活数投递哨兵，不能依赖不存在的 worker_count 指标。
        hooks_ref = _task_shutdown_hooks.get(task_id) or {}
        send_workers = hooks_ref.get("send_workers")
        live_worker_count = count_live_worker_tasks(send_workers if isinstance(send_workers, list) else None)
        sentinel_count = max(1, live_worker_count)
        for _ in range(sentinel_count):
            await recipient_queue.put(None)
        eof_sent = True
        exit_reason = "EOF确认" if mark_eof else (fatal_error_message or "预取失败")
        if runtime_ref is not None and mark_eof:
            runtime_ref["prefetch_eof"] = True
            runtime_ref["prefetch_eof_confirmed"] = True
            runtime_ref["prefetch_eof_sentinel_count"] = sentinel_count

    def _mark_prefetch_failed(message: str) -> None:
        nonlocal fatal_error_message
        fatal_error_message = str(message or "收件人预取失败").strip() or "收件人预取失败"
        runtime_ref = _task_runtime_metrics.get(task_id)
        if runtime_ref is not None:
            runtime_ref["prefetch_failed"] = True
            runtime_ref["prefetch_error"] = fatal_error_message
            runtime_ref["prefetch_eof"] = False

    try:
        while not stop_event.is_set():
            try:
                runtime = _task_runtime_metrics.get(task_id)
                claim_throttle_until = float((runtime or {}).get("claim_query_throttle_until", 0.0) or 0.0)
                if claim_throttle_until > monotonic():
                    await asyncio.sleep(min(CLAIM_QUERY_THROTTLE_MAX_SECONDS, claim_throttle_until - monotonic()))
                queue_size = int(recipient_queue.qsize())
                queue_high_watermark = int(
                    (runtime or {}).get("prefetch_queue_high_watermark", 0)
                    or calculate_prefetch_queue_high_watermark(float((runtime or {}).get("rate_limit_per_second", 0) or 0))
                )
                refill_low_watermark = max(
                    CLAIM_PREFETCH_MIN_LIMIT,
                    min(queue_high_watermark, int(queue_high_watermark * 0.55)),
                )
                if queue_size >= queue_high_watermark:
                    if runtime is not None:
                        runtime["recipient_queue_size"] = queue_size
                        runtime["prefetch_paused_high_watermark"] = queue_high_watermark
                    await asyncio.sleep(PREFETCH_PAUSE_SLEEP_SECONDS)
                    continue
                prefetch_empty_seconds = prefetch_queue_empty_seconds(runtime) if runtime is not None else 0.0
                if queue_size >= refill_low_watermark and prefetch_empty_seconds <= 0:
                    if runtime is not None:
                        runtime["recipient_queue_size"] = queue_size
                        runtime["prefetch_refill_low_watermark"] = refill_low_watermark
                    await asyncio.sleep(PREFETCH_PAUSE_SLEEP_SECONDS)
                    continue

                runtime = _task_runtime_metrics.get(task_id)
                if runtime is None:
                    exit_reason = "运行时指标不存在"
                    logger.warning("任务 %s 预取协程：运行时指标不存在，退出", task_id)
                    break

                writer_ref = _get_send_log_writer(task_id)
                writer_backlog = int(writer_ref.queue_size) if writer_ref is not None else int(runtime.get("result_queue_size", 0) or 0)
                writer_flush_p99_ms = float(runtime.get("writer_flush_p99_ms", 0) or 0)
                writer_backlog_pause_active = (
                    writer_backlog >= CLAIM_WRITER_BACKLOG_PAUSE_THRESHOLD
                    or (writer_backlog >= CLAIM_WRITER_BACKLOG_RESUME_THRESHOLD and writer_flush_p99_ms >= 1000.0)
                )
                if writer_backlog_pause_active:
                    runtime["claim_paused_for_writer_backlog"] = writer_backlog
                    runtime["claim_paused_for_writer_flush_p99_ms"] = round(writer_flush_p99_ms, 2)
                    await asyncio.sleep(CLAIM_WRITER_BACKLOG_SLEEP_SECONDS)
                    continue
                runtime["claim_paused_for_writer_flush_p99_ms"] = round(writer_flush_p99_ms, 2)
                if writer_backlog <= CLAIM_WRITER_BACKLOG_RESUME_THRESHOLD or writer_flush_p99_ms < 1000.0:
                    runtime["claim_paused_for_writer_backlog"] = 0
                target_fill = min(int(recipient_queue.maxsize), queue_high_watermark)
                free_slots = max(0, target_fill - queue_size)
                if free_slots <= 0:
                    await asyncio.sleep(PREFETCH_PAUSE_SLEEP_SECONDS)
                    continue

                cursor_row = int(
                    runtime.get("claim_cursor_row", 0) or runtime.get("pending_claim_cursor_row", 0) or 0
                )
                cursor_id = int(
                    runtime.get("claim_cursor_id", 0) or runtime.get("pending_claim_cursor_id", 0) or 0
                )
                now_value = monotonic()
                if cached_task is None or now_value - last_task_refresh_at >= TASK_CONFIG_REFRESH_INTERVAL_SECONDS:
                    cached_task = await _load_task_for_loop_refresh(task_id)
                    last_task_refresh_at = now_value
                current_task = cached_task
                if current_task is None:
                    exit_reason = "任务已不存在"
                    logger.error("任务 %s 预取协程：任务已不存在", task_id)
                    break

                runtime_prefetch_limit = int(runtime.get("effective_prefetch_limit", 0) or 0)
                effective_batch = min(
                    CLAIM_SINGLE_FETCH_MAX,
                    max(CLAIM_PREFETCH_MIN_LIMIT, normalized_batch_size, runtime_prefetch_limit),
                )
                if backpressure_state is not None:
                    effective_batch = min(
                        CLAIM_SINGLE_FETCH_MAX,
                        max(CLAIM_PREFETCH_MIN_LIMIT, adjusted_prefetch_batch(effective_batch, backpressure_state)),
                    )
                deficit = max(1, target_fill - queue_size)
                if bool(runtime.get("claim_query_timed_out", False)):
                    timeout_count = int(runtime.get("claim_fast_timeout_count", 0) or 0)
                    shrink_divisor = min(10, max(2, timeout_count + 1))
                    effective_batch = max(50, min(int(effective_batch), RECIPIENT_FAST_CLAIM_PAGE_SIZE // shrink_divisor))
                effective_batch = max(1, min(int(effective_batch), RECIPIENT_MEMORY_LOAD_MAX_BATCH, max(1, free_slots), deficit))

                recipients, new_cursor, new_cursor_id, has_more, claim_error, candidate_count = await _load_recipients_keyset_page(
                    current_task,
                    cursor_row,
                    cursor_id,
                    effective_batch,
                    inflight_emails,
                )
                runtime["claim_cursor_row"] = new_cursor
                runtime["claim_cursor_id"] = new_cursor_id
                runtime["pending_claim_cursor_row"] = new_cursor
                runtime["pending_claim_cursor_id"] = new_cursor_id
                runtime["last_claim_size"] = len(recipients)
                runtime["recipient_queue_size"] = int(recipient_queue.qsize())
                logger.debug(
                    "任务 %s 预取结果 cursor=(%s,%s) loaded=%s candidates=%s fetch_limit=%s has_more=%s prefetch_eof=%s queue_depth=%s",
                    task_id,
                    new_cursor,
                    new_cursor_id,
                    len(recipients),
                    candidate_count,
                    effective_batch,
                    has_more,
                    bool(runtime.get("prefetch_eof_confirmed", False)),
                    int(recipient_queue.qsize()),
                )
                if recipients:
                    runtime["claim_query_timed_out"] = False
                    runtime["claim_fast_timeout_count"] = 0
                    consecutive_empty_or_reserved = 0
                    stalled_empty_cursor_count = 0
                    last_empty_cursor_row = None

                if claim_error:
                    consecutive_failures += 1
                    runtime["prefetch_failed"] = True
                    runtime["prefetch_eof"] = False
                    runtime["prefetch_eof_confirmed"] = False
                    runtime["prefetch_consecutive_failures"] = consecutive_failures
                    runtime["prefetch_last_failure_at"] = monotonic()
                    logger.warning(
                        "任务 %s 预取收件人失败 attempt=%s/%s cursor_row=%s reason=%s",
                        task_id,
                        consecutive_failures,
                        PREFETCH_MAX_CONSECUTIVE_FAILURES,
                        new_cursor,
                        claim_error,
                    )
                    if consecutive_failures > 3:
                        cooldown_seconds = min(60.0, max(5.0, PREFETCH_RETRY_DELAY_SECONDS * consecutive_failures))
                        runtime["prefetch_paused_after_failures"] = consecutive_failures
                        runtime["prefetch_cooldown_seconds"] = round(cooldown_seconds, 2)
                        logger.warning("任务 %s 预取连续失败，游标暂不推进，等待数据库恢复", task_id)
                        logger.error(
                            "任务 %s 收件人预取连续失败 %s 次，暂停 %.1fs 后继续重试；不发送 EOF、不提前结束 reason=%s",
                            task_id,
                            consecutive_failures,
                            cooldown_seconds,
                            claim_error,
                        )
                        await asyncio.sleep(cooldown_seconds)
                        continue
                    await asyncio.sleep(min(PREFETCH_BACKOFF_MAX_SECONDS, PREFETCH_RETRY_DELAY_SECONDS * consecutive_failures))
                    continue

                consecutive_failures = 0

                current_claim_batch = recipients
                current_queued_recipients = []

                if not recipients:
                    if not has_more:
                        runtime["prefetch_exhausted"] = True
                        auditor_wake = runtime.get("completion_auditor_wake_event")
                        if isinstance(auditor_wake, asyncio.Event):
                            auditor_wake.set()
                        logger.info("任务 %s keyset 预取到达物理末尾 cursor=(%s,%s)，已通知 CompletionAuditor", task_id, new_cursor, new_cursor_id)
                        await _ensure_eof_sentinel()
                        break
                    await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                    continue

                runtime["prefetch_cooldown_seconds"] = 0.0
                _record_prefetch_fetch(runtime, len(recipients))
                try:
                    recipients = await _preallocate_queued_recipient_metadata(
                        current_task,
                        recipients,
                        url_pool_cache,
                        send_log_id_allocator,
                        inflight_emails,
                    )
                    runtime["prefetch_preallocated_count"] = int(runtime.get("prefetch_preallocated_count", 0) or 0) + len(recipients)
                except Exception as exc:
                    _rollback_prefetch_cursor_for_unqueued(runtime, task_id, recipients)
                    _release_inflight_recipients(inflight_emails, recipients)
                    current_claim_batch = []
                    runtime["prefetch_preallocation_failures"] = int(runtime.get("prefetch_preallocation_failures", 0) or 0) + 1
                    logger.warning(
                        "任务 %s 预取元数据分配失败，已回滚游标并重试 recipients=%s error_type=%s error=%s",
                        task_id,
                        len(recipients),
                        type(exc).__name__,
                        exc,
                    )
                    await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                    continue
                current_claim_batch = recipients
                queued_recipients: list[Recipient] = []
                skipped_already_reserved = 0
                for recipient in recipients:
                    if stop_event.is_set():
                        break
                    if recipient_queue.full():
                        await asyncio.sleep(PREFETCH_PAUSE_SLEEP_SECONDS)
                    if stop_event.is_set():
                        break
                    await recipient_queue.put(recipient)
                    queued_recipients.append(recipient)
                    current_queued_recipients = list(queued_recipients)
                    runtime["recipient_queue_size"] = int(recipient_queue.qsize())
                # 未成功 put 的已占用收件人必须释放；仅 stop 时回滚游标（completed 跳过不得回滚）。
                queued_id_set = {id(item) for item in queued_recipients}
                unqueued = [item for item in recipients if id(item) not in queued_id_set]
                if unqueued:
                    if stop_event.is_set():
                        _rollback_prefetch_cursor_for_unqueued(runtime, task_id, unqueued)
                    _release_inflight_recipients(inflight_emails, unqueued)
                current_queued_recipients = list(queued_recipients)
                if skipped_already_reserved:
                    runtime["prefetch_skipped_already_reserved"] = int(runtime.get("prefetch_skipped_already_reserved", 0) or 0) + skipped_already_reserved
                    logger.warning(
                        "任务 %s 预取入队阶段拦截重复收件人 count=%s cursor_row=%s",
                        task_id,
                        skipped_already_reserved,
                        runtime.get("claim_cursor_row", 0),
                    )

                _touch_task_progress(task_id)
                if not has_more:
                    runtime["prefetch_exhausted"] = True
                    auditor_wake = runtime.get("completion_auditor_wake_event")
                    if isinstance(auditor_wake, asyncio.Event):
                        auditor_wake.set()
                    logger.info(
                        "任务 %s 最后一批收件人已全部入队 loaded=%s fetch_limit=%s cursor=(%s,%s) queue_depth=%s，确认预取 EOF",
                        task_id,
                        len(queued_recipients),
                        effective_batch,
                        int(runtime.get("claim_cursor_row", 0) or 0),
                        int(runtime.get("claim_cursor_id", 0) or 0),
                        int(recipient_queue.qsize()),
                    )
                    await _ensure_eof_sentinel()
                    break
            except asyncio.CancelledError:
                raise
            except SQLAlchemyError as exc:
                # 数据库失败不代表物理 EOF：保持当前游标、清除 EOF 状态并退避重试。
                consecutive_failures += 1
                runtime_db = _task_runtime_metrics.get(task_id)
                if runtime_db is not None:
                    runtime_db["prefetch_failed"] = True
                    runtime_db["prefetch_eof"] = False
                    runtime_db["prefetch_eof_confirmed"] = False
                    runtime_db["prefetch_consecutive_failures"] = consecutive_failures
                    runtime_db["prefetch_last_failure_at"] = monotonic()
                logger.exception(
                    "任务 %s 预取数据库异常，游标暂不推进，等待数据库恢复 attempt=%s error_type=%s：",
                    task_id,
                    consecutive_failures,
                    type(exc).__name__,
                )
                if consecutive_failures > 3:
                    cooldown_seconds = min(60.0, max(5.0, PREFETCH_RETRY_DELAY_SECONDS * consecutive_failures))
                    if runtime_db is not None:
                        runtime_db["prefetch_cooldown_seconds"] = round(cooldown_seconds, 2)
                    logger.warning("任务 %s 预取连续失败，游标暂不推进，等待数据库恢复", task_id)
                    await asyncio.sleep(cooldown_seconds)
                else:
                    await asyncio.sleep(min(PREFETCH_BACKOFF_MAX_SECONDS, PREFETCH_RETRY_DELAY_SECONDS * consecutive_failures))
                continue
            except Exception as exc:
                if _is_recipient_claim_transient_error(exc):
                    _mark_claim_soft_retry(task_id, exc)
                    soft_retry_count = 1
                    runtime_soft = _task_runtime_metrics.get(task_id)
                    if runtime_soft is not None:
                        soft_retry_count = int(runtime_soft.get("claim_fast_timeout_count", 1) or 1)
                    backoff = min(PREFETCH_BACKOFF_MAX_SECONDS, PREFETCH_RETRY_DELAY_SECONDS * max(1, soft_retry_count))
                    logger.warning(
                        "任务 %s 预取协程瞬时异常（软重试） soft_retry=%s backoff=%.2fs：%s",
                        task_id,
                        soft_retry_count,
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)
                    continue
                consecutive_failures += 1
                logger.exception(
                    "任务 %s 预取协程异常 attempt=%s/%s：%s",
                    task_id,
                    consecutive_failures,
                    PREFETCH_MAX_CONSECUTIVE_FAILURES,
                    exc,
                )
                if consecutive_failures >= max(5, PREFETCH_MAX_CONSECUTIVE_FAILURES):
                    message = f"收件人预取连续异常 {consecutive_failures} 次：{type(exc).__name__}: {exc}"
                    runtime_retry = _task_runtime_metrics.get(task_id)
                    if runtime_retry is not None:
                        runtime_retry["prefetch_last_unhandled_error"] = message
                        runtime_retry["prefetch_consecutive_failures"] = consecutive_failures
                    logger.exception(
                        "任务 %s 预取循环异常达到退避阈值，保持运行并延长重试；不下发 EOF cursor=%s last_error=%s",
                        task_id,
                        (runtime_retry or {}).get("claim_cursor_row", 0),
                        message,
                    )
                    backoff = min(60.0, max(5.0, PREFETCH_RETRY_DELAY_SECONDS * consecutive_failures))
                    await asyncio.sleep(backoff)
                    continue
                await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS * consecutive_failures)
    finally:
        if stop_event.is_set() and exit_reason == "未知":
            exit_reason = "收到停止信号"
        runtime_ref = _task_runtime_metrics.get(task_id)
        if current_claim_batch:
            queued_id_set = {id(item) for item in current_queued_recipients}
            unqueued = [item for item in current_claim_batch if id(item) not in queued_id_set]
            if unqueued:
                _rollback_prefetch_cursor_for_unqueued(runtime_ref, task_id, unqueued)
                _release_inflight_recipients(inflight_emails, unqueued)
        # 预取循环因任务删除、运行时状态丢失或未捕获路径退出时，绝不能把“协程结束”当 EOF。
        # 这正是 loaded=0 后发送 Worker 收到哨兵并提前完成的风险路径。
        if not eof_sent and not stop_event.is_set():
            message = fatal_error_message or "收件人预取未确认到达列表末尾即退出"
            _mark_prefetch_failed(message)
            await _ensure_eof_sentinel(mark_eof=False)
        eof_confirmed = bool((runtime_ref or {}).get("prefetch_eof_confirmed", False)) if runtime_ref is not None else False
        cursor_snapshot = int((runtime_ref or {}).get("claim_cursor_row", 0) or 0) if runtime_ref is not None else 0
        logger.warning(
            "任务 %s 预取循环退出，原因=%s，EOF=%s，游标=%s",
            task_id,
            exit_reason,
            eof_confirmed,
            cursor_snapshot,
        )


async def _record_monitor_result(
    task_id: int,
    email: str,
    success: bool,
    response_text: str,
    attempt_count: int,
    trigger_count: int,
    egress_ip: str | None = None,
    sender_email: str | None = None,
    provider_message_id: str | None = None,
    status_override: str | None = None,
) -> bool:
    normalized_email = str(email or "").strip()
    if not normalized_email:
        return False
    status = str(status_override or ("monitor_sent" if success else "monitor_failed"))
    raw_response = str(response_text or "")
    trigger_marker = _monitor_trigger_marker(trigger_count)
    if success and not raw_response.strip():
        raw_response = "SendGrid 已接收请求，响应内容为空"
    if trigger_marker not in raw_response:
        raw_response = f"{raw_response}；{trigger_marker}" if raw_response else trigger_marker
    error_message = "" if success else normalize_send_log_error(raw_response)
    now_value = datetime.utcnow()
    proxy_info, outbound_ip = _runtime_send_proxy_snapshot(task_id)
    runtime = _task_runtime_metrics.get(int(task_id))
    sender_email = _resolve_log_sender_email(task_id, sender_email)
    normalized_provider_message_id = str(provider_message_id or "").strip()[:255]
    if success and not normalized_provider_message_id:
        logger.error(
            "任务 %s 监控邮件缺少 SendGrid 消息 ID，禁止写入成功日志 email=%s trigger_count=%s",
            task_id,
            normalized_email,
            trigger_count,
        )
        success = False
        status = "monitor_failed"
        error_message = "SendGrid 返回 202 但缺少 X-Message-Id，无法确认真实受理"
        raw_response = error_message
    if success and not sender_email:
        logger.error(
            "任务 %s 监控抽样缺少发件人快照，拒绝写入成功日志以便触发告警 email=%s trigger_count=%s",
            task_id,
            normalized_email,
            trigger_count,
        )
        success = False
        status = "monitor_failed"
        error_message = "监控邮件发送成功但发件人快照缺失，请检查通道发件人配置"
        raw_response = error_message
    record = {
        "task_id": task_id,
        "recipient_id": None,
        "email": normalized_email,
        "status": status,
        "error": error_message,
        "error_message": error_message,
        "response_text": raw_response[:4000],
        "attempt_count": int(attempt_count or 0),
        "monitor_trigger_count": max(0, int(trigger_count or 0)),
        "provider_message_id": normalized_provider_message_id,
        "sent_at": now_value if success else None,
        # 监控成功与失败都必须使用 log_type=monitor；状态字段负责区分结果。
        # 数据库约束要求带 monitor_trigger_count 的记录必须属于 monitor 类型。
        "log_type": "monitor",
        "sender_email": sender_email,
        "egress_ip": (outbound_ip or egress_ip or "")[:64],
        "proxy_info": proxy_info[:255],
        "outbound_ip": outbound_ip,
        "created_at": now_value,
    }
    writer = _get_send_log_writer(task_id)
    if writer is None:
        logger.error(
            "任务 %s 监控抽样日志写入器未初始化，发送结果未落库 email=%s status=%s trigger_count=%s",
            task_id,
            normalized_email,
            status,
            trigger_count,
        )
        return False
    try:
        # 监控详情是触发恢复和逐邮箱去重的数据库真相，不能使用普通详情日志的可丢弃策略。
        # CompactOutcome 不具备逐邮箱里程碑去重所需的全部字段，因此监控结果只写详情日志；
        # 避免先写入一条 recipient_id=NULL 的无效 CompactOutcome，造成完成计数和关键队列污染。
        enqueued = await writer.enqueue_detail(DetailLog(record))
        if not enqueued:
            deadline = monotonic() + RESULT_ENQUEUE_TIMEOUT_SECONDS
            while monotonic() < deadline and not enqueued:
                await asyncio.sleep(0.05)
                enqueued = await writer.enqueue_detail(DetailLog(record))
        if not enqueued:
            raise RuntimeError("监控 SendLog 详情队列持续满，无法保存监控发送结果")
        if runtime is not None:
            runtime["monitor_sent"] = int(runtime.get("monitor_sent", 0)) + (1 if success else 0)
            runtime["monitor_failed"] = int(runtime.get("monitor_failed", 0)) + (0 if success else 1)
        logger.info(
            "任务 %s 回测邮件结果已写入队列 email=%s status=%s trigger_count=%s log_type=monitor",
            task_id,
            normalized_email,
            status,
            trigger_count,
        )
        return True
    except Exception as exc:
        _log_sampled_task_warning(
            f"monitor_persist:{task_id}",
            "任务 %s 监控抽样日志入队失败 email=%s status=%s error_type=%s error=%s",
            task_id,
            normalized_email,
            status,
            type(exc).__name__,
            exc,
        )
        return False


async def batch_enqueue_results(
    task_id: int,
    items: list[PendingEnqueueResult],
    inflight_emails: set[str] | None = None,
    *,
    chunk_size: int = RESULT_ENQUEUE_CHUNK_SIZE,
) -> None:
    if not items:
        return
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None:
        runtime["api_recipient_send_count"] = int(runtime.get("api_recipient_send_count", 0) or 0) + len(items)
    writer = _get_send_log_writer(task_id)
    if writer is None:
        logger.error("任务 %s SendLog 写入器未初始化，无法批量记录 %s 条发送结果", task_id, len(items))
        for item in items:
            _discard_inflight_recipient(inflight_emails, item.recipient)
        return
    now_value = datetime.utcnow()
    records: list[dict] = []
    for item in items:
        result_item = QueuedSendResult(
            task_id=task_id,
            recipient_id=int(item.recipient.id or 0),
            email=item.recipient.email,
            success=item.success,
            response_text=item.response_text,
            attempt_count=item.attempt_count,
            sender_email=item.sender_email,
            send_log_id=item.send_log_id,
            status_override=item.status_override,
        )
        records.append(_build_send_log_row_dict(task_id, result_item, now_value))
    if runtime is not None:
        _apply_batch_enqueue_runtime_updates(runtime, task_id, items)
    for chunk_start in range(0, len(records), chunk_size):
        chunk_records = records[chunk_start:chunk_start + chunk_size]
        chunk_items = items[chunk_start:chunk_start + chunk_size]
        try:
            enqueued_count = await writer.enqueue_many(chunk_records)
        except Exception as exc:
            logger.error(
                "任务 %s SendLog 批量入队异常 records=%s error_type=%s error=%s",
                task_id,
                len(chunk_records),
                type(exc).__name__,
                exc,
            )
            enqueued_count = 0
        if enqueued_count < len(chunk_records):
            if runtime is not None:
                runtime["result_queue_backpressure_count"] = int(runtime.get("result_queue_backpressure_count", 0)) + (len(chunk_records) - enqueued_count)
            for dropped_item in chunk_items[enqueued_count:]:
                _discard_inflight_recipient(inflight_emails, dropped_item.recipient)
        if chunk_start + chunk_size < len(records):
            await asyncio.sleep(0)
    if runtime is not None:
        queue_size = int(writer.queue_size)
        runtime["result_queue_size"] = queue_size
        runtime["result_queue_max_observed"] = max(queue_size, int(runtime.get("result_queue_max_observed", 0)))


async def _enqueue_result(
    task_id: int,
    recipient: Recipient,
    success: bool,
    response_text: str,
    attempt_count: int,
    egress_ip: str | None = None,
    inflight_emails: set[str] | None = None,
    *,
    sender_email: str | None = None,
    send_log_id: int | None = None,
    skip_runtime_counters: bool = False,
) -> None:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is not None and not skip_runtime_counters:
        runtime["processed"] = int(runtime.get("processed", 0)) + 1
        runtime["success"] = int(runtime.get("success", 0)) + (1 if success else 0)
        runtime["sent_count"] = max(
            0,
            int(runtime.get("initial_sent_count", 0) or 0) + int(runtime.get("success", 0) or 0),
        )
        runtime["failed"] = int(runtime.get("failed", 0)) + (0 if success else 1)
        now_progress = monotonic()
        runtime["last_send_at"] = now_progress
        runtime["last_send_log_enqueued_at"] = now_progress
        runtime["outstanding_writes"] = int(runtime.get("outstanding_writes", 0)) + 1
        runtime["api_recipient_send_count"] = int(runtime.get("api_recipient_send_count", 0) or 0) + 1
        if success:
            runtime["max_successful_regular_row_number"] = max(
                int(runtime.get("max_successful_regular_row_number", 0) or 0),
                int(getattr(recipient, "row_number", 0) or 0),
            )
        _record_throughput_sample(runtime, success=success)
        _touch_task_progress(task_id, now_progress)
        _touch_task_send_progress(task_id, now_progress)
    writer = _get_send_log_writer(task_id)
    if writer is None:
        logger.error("任务 %s SendLog 写入器未初始化，无法记录发送结果 email=%s", task_id, recipient.email)
        _discard_inflight_recipient(inflight_emails, recipient)
        if runtime is not None and not skip_runtime_counters:
            runtime["outstanding_writes"] = max(0, int(runtime.get("outstanding_writes", 0)) - 1)
        return
    preallocated_send_log_id = int(send_log_id or getattr(recipient, "_mercury_send_log_id", 0) or 0) or None
    result_item = QueuedSendResult(
        task_id=task_id,
        recipient_id=int(recipient.id or 0),
        email=recipient.email,
        success=success,
        response_text=response_text,
        attempt_count=attempt_count,
        egress_ip=egress_ip,
        sender_email=str(sender_email or "").strip() or None,
        send_log_id=preallocated_send_log_id,
    )
    record = _build_send_log_row_dict(task_id, result_item, datetime.utcnow())
    try:
        enqueued = await writer.enqueue(record)
    except Exception as exc:
        logger.error(
            "任务 %s SendLog 入队异常 email=%s error_type=%s error=%s",
            task_id,
            recipient.email,
            type(exc).__name__,
            exc,
        )
        enqueued = False
    if not enqueued:
        if runtime is not None:
            runtime["result_queue_backpressure_count"] = int(runtime.get("result_queue_backpressure_count", 0)) + 1
        placeholder_ok = await _enqueue_send_log_failure_placeholder(
            task_id,
            record,
            error_message="发送日志写入队列已满，已记入失败以防重复发送",
        )
        if not placeholder_ok:
            _discard_inflight_recipient(inflight_emails, recipient)
            if runtime is not None and not skip_runtime_counters:
                runtime["outstanding_writes"] = max(0, int(runtime.get("outstanding_writes", 0)) - 1)
    if runtime is not None:
        queue_size = int(writer.queue_size)
        runtime["result_queue_size"] = queue_size
        runtime["result_queue_max_observed"] = max(queue_size, int(runtime.get("result_queue_max_observed", 0)))


def _build_send_log_row_dict(task_id: int, item: QueuedSendResult, now_value: datetime) -> dict:
    raw_response = str(item.response_text or "")
    sender_email = str(item.sender_email or "").strip()
    if item.success:
        status = "sent"
        sent_at = now_value
        error_message = ""
        if not raw_response.strip():
            raw_response = "SendGrid 已接收请求，响应内容为空"
    elif item.status_override in {"unknown", "delivery_unknown"}:
        status = "delivery_unknown"
        sent_at = None
        error_message = "HTTP 请求未收到响应，投递状态未知；需人工排查，禁止自动重试"
    else:
        status = "bounced" if is_hard_bounce_error(raw_response) else "failed"
        sent_at = None
        error_message = normalize_send_log_error(raw_response)
    proxy_info, outbound_ip = _runtime_send_proxy_snapshot(task_id)
    egress_compat = str(outbound_ip or item.egress_ip or "")[:64]
    return {
        "id": int(item.send_log_id or 0) or None,
        "task_id": task_id,
        "recipient_id": item.recipient_id,
        "email": item.email,
        "sender_email": _resolve_log_sender_email(task_id, sender_email),
        "status": status,
        "retry_state": "awaiting_confirmation" if status == "delivery_unknown" else "none",
        "abort_retry": bool(status in {"bounced", "unknown"}),
        "error": error_message,
        "error_message": error_message,
        "response_text": raw_response[:4000],
        "attempt_count": item.attempt_count,
        "log_type": "regular",
        "sent_at": sent_at,
        "egress_ip": egress_compat,
        "proxy_info": proxy_info[:255],
        "outbound_ip": outbound_ip,
        "created_at": now_value,
    }


def _result_log_fields(item: QueuedSendResult, now_value: datetime) -> tuple[str, datetime | None, str]:
    if item.success:
        return "sent", now_value, ""
    if item.status_override in {"unknown", "delivery_unknown"}:
        return "delivery_unknown", None, "HTTP 请求未收到响应，投递状态未知；需人工排查，禁止自动重试"
    return "failed", None, normalize_send_log_error(item.response_text)


def _discard_inflight_recipient(inflight_emails: set[str] | None, recipient: Recipient) -> None:
    if inflight_emails is None:
        return
    for key in _recipient_handled_keys(recipient):
        inflight_emails.discard(key)


def _discard_inflight_email(inflight_emails: set[str] | None, email: str) -> None:
    normalized = _normalize_inflight_email(email)
    if inflight_emails is not None and normalized:
        inflight_emails.discard(f"email:{normalized}")


def _discard_inflight_record(inflight_emails: set[str] | None, record: dict) -> None:
    if inflight_emails is None:
        return
    recipient_id = int(record.get("recipient_id") or 0)
    if recipient_id > 0:
        inflight_emails.discard(f"id:{recipient_id}")
    _discard_inflight_email(inflight_emails, str(record.get("email") or ""))


async def _enqueue_send_log_failure_placeholder(
    task_id: int,
    record: dict,
    *,
    error_message: str,
) -> bool:
    writer = _get_send_log_writer(task_id)
    if writer is None:
        logger.error(
            "任务 %s 无法写入失败占位日志（写入器未初始化）email=%s",
            task_id,
            record.get("email"),
        )
        return False
    placeholder = dict(record)
    placeholder["status"] = "failed"
    placeholder["retry_state"] = "none"
    placeholder["abort_retry"] = False
    placeholder["error"] = error_message
    placeholder["error_message"] = error_message
    placeholder["sent_at"] = None
    try:
        enqueued = await writer.enqueue(placeholder, allow_drop_callback=False)
        if not enqueued:
            logger.error(
                "任务 %s 失败占位日志入队失败 email=%s",
                task_id,
                record.get("email"),
            )
        return enqueued
    except Exception as exc:
        logger.error(
            "任务 %s 失败占位日志入队异常 email=%s error_type=%s error=%s",
            task_id,
            record.get("email"),
            type(exc).__name__,
            exc,
        )
        return False


async def _recover_dropped_success_send_log(task_id: int, record: dict) -> bool:
    """成功发送日志被环形缓冲丢弃时，禁止改写为 failed，尽量原样重入队。"""
    writer = _get_send_log_writer(task_id)
    if writer is None:
        return False
    success_record = dict(record)
    success_record["status"] = "sent"
    success_record["error"] = ""
    success_record["error_message"] = ""
    success_record["abort_retry"] = False
    for attempt in range(1, 4):
        try:
            if await writer.enqueue(success_record, allow_drop_callback=False):
                return True
        except Exception as exc:
            logger.warning(
                "任务 %s 成功发送日志重入队异常 attempt=%s email=%s error=%s",
                task_id,
                attempt,
                record.get("email"),
                exc,
            )
        await asyncio.sleep(0.05 * attempt)
    return False


def _record_writer_flush_metrics(
    task_id: int,
    stats: SendLogFlushStats,
    send_log_writer: AsyncBatchWriter | None,
) -> int:
    runtime = _task_runtime_metrics.get(task_id)
    if runtime is None:
        return 0
    runtime["writer_flush_count"] = int(runtime.get("writer_flush_count", 0)) + 1
    runtime["writer_last_batch_size"] = stats.batch_size
    runtime["writer_last_flush_ms"] = stats.flush_ms
    samples = runtime.get("writer_flush_ms_samples")
    if not isinstance(samples, deque):
        samples = deque(maxlen=WRITER_FLUSH_P99_SAMPLE_SIZE)
        runtime["writer_flush_ms_samples"] = samples
    samples.append(float(stats.flush_ms))
    sorted_samples = sorted(samples)
    p99_index = max(0, min(len(sorted_samples) - 1, math.ceil(len(sorted_samples) * 0.99) - 1))
    runtime["writer_flush_p99_ms"] = round(sorted_samples[p99_index], 2)
    runtime["result_queue_size"] = int(send_log_writer.queue_size) if send_log_writer is not None else stats.queue_size
    runtime["writer_queue_depth"] = runtime["result_queue_size"]
    runtime["writer_dropped_count"] = int(getattr(send_log_writer, "dropped_count", runtime.get("writer_dropped_count", 0)) or 0)
    if send_log_writer is not None and hasattr(send_log_writer, "flush_latency_percentiles"): 
        runtime["write_latency_percentiles"] = send_log_writer.flush_latency_percentiles()
    _record_phase_timing_sample(runtime, db_write_ms=float(stats.flush_ms))
    _touch_task_progress(task_id)
    _signal_claim_slots_available(task_id)
    return int(runtime.get("writer_flush_count", 0))


def _make_send_log_batch_flushed_callback(
    task_id: int,
    inflight_emails: set[str],
):
    async def _callback(records: list[dict], stats: SendLogFlushStats) -> None:
        for record in records:
            _discard_inflight_record(inflight_emails, record)
        _decrement_outstanding_writes(task_id, len(records))
        _record_writer_flush_metrics(task_id, stats, _get_send_log_writer(task_id))

    return _callback


def _record_http_429_metrics(
    task_id: int,
    runtime: dict[str, float | int],
    *,
    retry_after: int | str | None = None,
    reason: str = "429",
) -> None:
    normalized_reason = str(reason or "429").strip() or "429"
    if normalized_reason == "messaging_limit":
        runtime["sendgrid_messaging_limit_count"] = int(runtime.get("sendgrid_messaging_limit_count", 0)) + 1
    else:
        runtime["http_429_count"] = int(runtime.get("http_429_count", 0)) + 1
    now_value = monotonic()
    recent_times = runtime.get("http_429_recent_times")
    if not isinstance(recent_times, deque):
        recent_times = deque(maxlen=500)
        runtime["http_429_recent_times"] = recent_times
    recent_times.append(now_value)
    recent_events = runtime.get("http_429_recent_events")
    if not isinstance(recent_events, deque):
        recent_events = deque(maxlen=200)
        runtime["http_429_recent_events"] = recent_events
    retry_after_value = 0
    if retry_after is not None:
        try:
            retry_after_value = max(0, int(retry_after))
        except (TypeError, ValueError):
            retry_after_value = 0
    recent_events.append(
        {
            "at": now_value,
            "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "retry_after": retry_after_value,
            "reason": normalized_reason,
        }
    )
    last_logged_at = float(runtime.get("last_http_429_warn_logged_at", 0) or 0)
    if (now_value - last_logged_at) >= HTTP_429_WARN_LOG_INTERVAL_SECONDS:
        runtime["last_http_429_warn_logged_at"] = now_value
        if normalized_reason == "messaging_limit":
            logger.warning(
                "task %s 收到 SendGrid 发送限额响应（累计 %s 次）；建议降低速率、减少并发或等待额度恢复",
                task_id,
                runtime.get("sendgrid_messaging_limit_count", 0),
            )
        else:
            logger.warning(
                "task %s 收到 SendGrid 429 限流（累计 %s 次）；建议降低速率限制或 HTTP 批量大小以避免触发 SendGrid 限流",
                task_id,
                runtime.get("http_429_count", 0),
            )
    pid_controller = _task_pid_controllers.get(int(task_id))
    if pid_controller is not None and hasattr(pid_controller, "record_429_event"):
        pid_controller.record_429_event()


async def _alert_delivery_unknown_backlog(task_id: int) -> None:
    """投递未知只隔离对应收件人并告警，绝不改变主任务状态。"""
    await asyncio.sleep(DELIVERY_UNKNOWN_ALERT_AFTER_SECONDS)
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is None:
        return
    active_unknown = int(runtime.get("delivery_unknown_active", 0) or 0)
    if active_unknown < DELIVERY_UNKNOWN_ALERT_THRESHOLD:
        return
    runtime["delivery_unknown_alerted"] = active_unknown
    reason = (
        f"存在 {active_unknown} 个投递结果未知请求超过 {int(DELIVERY_UNKNOWN_ALERT_AFTER_SECONDS)} 秒，"
        "已隔离等待人工确认，主任务继续发送"
    )
    record_task_performance_alert(
        task_id,
        reason=reason,
        actual_rate=float(runtime.get("current_rate", 0) or 0),
        configured_rate=float(runtime.get("rate_limit_per_second", 0) or 0),
    )
    _log_sampled_task_warning(
        f"delivery_unknown_backlog:{task_id}",
        "任务 %s 投递未知积压告警 active_unknown=%s；仅隔离相关收件人，禁止自动暂停主任务",
        task_id,
        active_unknown,
    )


def _apply_batch_enqueue_runtime_updates(
    runtime: dict[str, float | int],
    task_id: int,
    items: list[PendingEnqueueResult],
) -> None:
    if not items:
        return
    success_count = sum(1 for item in items if item.success)
    unknown_count = sum(1 for item in items if item.status_override in {"unknown", "delivery_unknown"})
    fail_count = len(items) - success_count - unknown_count
    now_value = monotonic()
    runtime["processed"] = int(runtime.get("processed", 0)) + len(items)
    runtime["success"] = int(runtime.get("success", 0)) + success_count
    runtime["sent_count"] = max(
        0,
        int(runtime.get("initial_sent_count", 0) or 0) + int(runtime.get("success", 0) or 0),
    )
    runtime["failed"] = int(runtime.get("failed", 0)) + fail_count
    runtime["delivery_unknown"] = int(runtime.get("delivery_unknown", 0)) + unknown_count
    if unknown_count > 0:
        runtime["delivery_unknown_active"] = int(runtime.get("delivery_unknown_active", 0) or 0) + unknown_count
        runtime["delivery_unknown_oldest_at"] = float(runtime.get("delivery_unknown_oldest_at", 0) or now_value)
        existing_alert_task = _delivery_unknown_alert_tasks.get(int(task_id))
        if existing_alert_task is None or existing_alert_task.done():
            alert_task = asyncio.create_task(
                _alert_delivery_unknown_backlog(task_id),
                name=f"delivery-unknown-alert-watch-{task_id}",
            )
            _delivery_unknown_alert_tasks[int(task_id)] = alert_task

            def _finish_delivery_unknown_alert(completed: asyncio.Task, *, task_key: int = int(task_id)) -> None:
                if _delivery_unknown_alert_tasks.get(task_key) is completed:
                    _delivery_unknown_alert_tasks.pop(task_key, None)
                if not completed.cancelled():
                    completed.exception()

            alert_task.add_done_callback(_finish_delivery_unknown_alert)
    runtime["last_send_at"] = now_value
    runtime["last_send_log_enqueued_at"] = now_value
    runtime["outstanding_writes"] = int(runtime.get("outstanding_writes", 0)) + len(items)
    successful_rows = [int(getattr(item.recipient, "row_number", 0) or 0) for item in items if item.success]
    if successful_rows:
        runtime["max_successful_regular_row_number"] = max(
            int(runtime.get("max_successful_regular_row_number", 0) or 0),
            max(successful_rows),
        )
    processed_times = _ensure_timestamp_deque(runtime, "processed_recent_times")
    for _ in items:
        processed_times.append(now_value)
        _prune_recent_timestamps(processed_times, now_value, RATE_SAMPLE_RETENTION_SECONDS)
    if success_count > 0:
        success_times = _ensure_timestamp_deque(runtime, "success_recent_times")
        for _ in range(success_count):
            success_times.append(now_value)
            _prune_recent_timestamps(success_times, now_value, RATE_SAMPLE_RETENTION_SECONDS)
    _record_realtime_rate_events(
        runtime,
        success_count=success_count,
        processed_count=len(items),
        at=now_value,
    )
    _refresh_runtime_rate_metrics(runtime)
    _touch_task_progress(task_id, now_value)
    _touch_task_send_progress(task_id, now_value)


async def _log_recipient_stats(task: Task) -> int:
    runtime = _task_runtime_metrics.get(int(task.id or 0))
    if runtime is not None:
        total = int(getattr(task, "total_count", 0) or runtime.get("claimed_recipients_total", 0) or 0)
        processed = int(runtime.get("processed", 0) or 0)
        pending = max(0, total - processed)
        logger.debug(
            "task %s recipient stats from runtime: list_id=%s total=%s processed=%s pending=%s claim_cursor_row=%s",
            task.id,
            task.recipient_list_id,
            total,
            processed,
            pending,
            runtime.get("claim_cursor_row", 0),
        )
        return pending
    try:
        async with asyncio.timeout(5):
            async with AsyncSessionLocal() as db:
                sendable = await _count_task_sendable_recipients(db, task)
                processed = int(getattr(task, "sent_count", 0) or 0) + int(getattr(task, "failed_count", 0) or 0)
                pending = max(0, sendable - processed)
                logger.debug(
                    "task %s recipient stats fallback: list_id=%s sendable=%s processed=%s pending=%s",
                    task.id,
                    task.recipient_list_id,
                    sendable,
                    processed,
                    pending,
                )
                return pending
    except TimeoutError:
        logger.exception("task %s timed out while counting recipient stats for list_id=%s", task.id, task.recipient_list_id)
        raise


async def _sync_total_count(task_id: int) -> int:
    try:
        async with asyncio.timeout(20):
            async with AsyncSessionLocal() as db:
                logger.debug("task %s syncing total_count: loading task", task_id)
                task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
                if task is None:
                    return 0
                logger.debug("task %s syncing total_count: counting sendable recipients for list_id=%s", task_id, task.recipient_list_id)
                total = await _count_task_sendable_recipients(db, task)
                task.total_count = total
                task.updated_at = datetime.utcnow()
                await db.commit()
                logger.debug("task %s synced total_count=%s", task_id, total)
                return total
    except TimeoutError:
        logger.exception("task %s timed out while syncing total_count", task_id)
        raise


async def _mark_task_finished(task_id: int, reason: str = "无待发送收件人") -> None:
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            return
        processed = int(task.sent_count or 0) + int(task.failed_count or 0)
        total = int(task.total_count or 0)
        # 主循环只有在 prefetch_eof、收件人队列、在途发送和写入队列全部清空后才会调用这里。
        # finished 写入阶段不再执行补漏查询或回滚游标，避免校验查询超时/误判把已完成任务重新拉回 running/paused。
        normalized_reason = str(reason or "任务已完成").strip()
        if normalized_reason == "无待发送收件人" and total > 0 and processed >= total:
            normalized_reason = "全部收件人已处理完成"
        elif normalized_reason == "无待发送收件人" and processed > 0:
            normalized_reason = "当前可发送收件人已处理完成"
        safe_reason = sanitize_user_error_message(normalized_reason)
        task.status = "finished"
        task.run_message = safe_reason
        task.updated_at = datetime.utcnow()
        await db.commit()
        logger.debug("任务 %s 已结束：原因=%s 已处理=%s 总数=%s 已发送=%s 失败=%s", task_id, safe_reason, processed, total, task.sent_count or 0, task.failed_count or 0)
    notify_task_status_changed(task_id, "finished")


def _normalize_batch_sender(batch_sender: dict[str, str] | None) -> dict[str, str] | None:
    if batch_sender is None:
        return None
    email = str(batch_sender.get("email") or "").strip()
    if not email:
        return None
    return {
        "name": str(batch_sender.get("name") or ""),
        "email": email,
    }


def invalidate_task_runtime_config(task_id: int) -> None:
    runtime = _task_runtime_metrics.get(int(task_id))
    _invalidate_task_send_prepare_cache(runtime)
    if runtime is not None:
        runtime["configuration_invalidated"] = True
        runtime.pop("current_template_id", None)
        runtime.pop("current_template_name", None)
        runtime.pop("template_rotation_cursor", None)
    hooks = _task_shutdown_hooks.get(int(task_id))
    if hooks is not None:
        hooks.pop("cached_task", None)


def _invalidate_task_send_prepare_cache(runtime: dict[str, Any] | None) -> None:
    if runtime is None:
        return
    runtime.pop("static_content_cache_key", None)
    runtime.pop("cached_static_content", None)
    runtime.pop("batch_mail_shell_cache_key", None)
    runtime.pop("batch_mail_shell", None)


def _resolve_task_static_content(task: Task, runtime: dict[str, Any] | None) -> StaticMailContent:
    if task.template is None:
        raise ValueError("任务模板未加载")
    template_key = get_template_cache_key(task.template)
    if runtime is not None:
        cached_key = runtime.get("static_content_cache_key")
        cached_content = runtime.get("cached_static_content")
        if cached_key == template_key and isinstance(cached_content, StaticMailContent):
            return cached_content
    static_content = build_static_mail_content(task.template)
    if runtime is not None:
        runtime["static_content_cache_key"] = template_key
        runtime["cached_static_content"] = static_content
    return static_content


def _precompute_batch_dynamic_variables(template: Template, count: int) -> list[dict[str, str]]:
    if count <= 0 or template is None:
        return []
    batch_date = date_type.today().strftime("%Y-%m-%d")
    rules = _template_service_module._load_random_rules(template)
    variable_names = _template_service_module._collect_variable_names(
        str(template.subject or ""),
        str(template.html_body or ""),
        str(template.text_body or ""),
    )
    random_names = ["random"]
    for name in variable_names:
        if name == "random" or _template_service_module._RANDOM_VARIABLE_PATTERN.match(name):
            if name not in random_names:
                random_names.append(name)
    dynamic_values: list[dict[str, str]] = []
    for _ in range(count):
        recipient_dynamic = {"date": batch_date}
        for random_name in random_names:
            recipient_dynamic[random_name] = _template_service_module._generate_random_string(
                template,
                _template_service_module._rule_for_random_name(rules, random_name),
            )
        dynamic_values.append(recipient_dynamic)
    return dynamic_values


def _batch_mail_shell_cache_key(
    template_key: tuple[int | None, datetime | None],
    from_email: str,
    from_name: str,
    static_content: StaticMailContent,
) -> tuple[Any, ...]:
    return (
        template_key,
        str(from_email or "").strip(),
        str(from_name or ""),
        static_content.content_type,
        static_content.subject,
        static_content.content,
    )


def _build_batch_mail_shell(
    from_email: str,
    from_name: str,
    static_content: StaticMailContent,
    *,
    subject_override: str | None = None,
) -> Mail:
    normalized_from_email = str(from_email or "").strip()
    if not normalized_from_email:
        raise ValueError("发件人邮箱不能为空")
    subject = str(subject_override or static_content.subject or "").strip() or "MercuryMail"
    mail = Mail(from_email=Email(normalized_from_email, from_name or ""), subject=subject)
    if static_content.is_html:
        mail.add_content(Content("text/html", static_content.content))
    else:
        mail.add_content(Content("text/plain", static_content.content))
    return mail


def _resolve_batch_mail_shell(
    from_email: str,
    from_name: str,
    static_content: StaticMailContent,
    template_key: tuple[int | None, datetime | None],
    runtime: dict[str, Any] | None,
    *,
    subject_override: str | None = None,
) -> Mail:
    cache_key = _batch_mail_shell_cache_key(template_key, from_email, from_name, static_content)
    if subject_override:
        return _build_batch_mail_shell(
            from_email,
            from_name,
            static_content,
            subject_override=subject_override,
        )
    if runtime is not None:
        cached_key = runtime.get("batch_mail_shell_cache_key")
        cached_shell = runtime.get("batch_mail_shell")
        if cached_key == cache_key and cached_shell is not None:
            return cached_shell
    shell = _build_batch_mail_shell(from_email, from_name, static_content)
    if runtime is not None:
        runtime["batch_mail_shell_cache_key"] = cache_key
        runtime["batch_mail_shell"] = shell
    return shell


def _compose_batch_mail_from_shell(shell: Mail, recipients_data: list[dict[str, Any]]) -> Mail:
    mail = Mail(from_email=shell.from_email, subject=shell.subject)
    shell_contents = shell.contents
    if shell_contents:
        for content_block in shell_contents:
            mail.add_content(content_block)
    for item in recipients_data:
        recipient_email = str(item.get("email") or "").strip()
        if not recipient_email:
            continue
        personalization = Personalization()
        personalization.add_to(To(recipient_email))
        substitutions = item.get("substitutions") or {}
        if isinstance(substitutions, dict):
            for key, value in substitutions.items():
                substitution_key = str(key or "").strip()
                if substitution_key:
                    personalization.add_substitution(
                        Substitution(substitution_key, "" if value is None else str(value))
                    )
        headers = item.get("headers") or {}
        if isinstance(headers, dict):
            for header_key, header_value in headers.items():
                normalized_header_key = str(header_key or "").strip()
                if normalized_header_key:
                    personalization.add_header(
                        Header(normalized_header_key, "" if header_value is None else str(header_value))
                    )
        custom_args = dict(item.get("custom_args") or {}) if isinstance(item.get("custom_args"), dict) else {}
        custom_args.setdefault("task_id", str(item.get("task_id") or ""))
        custom_args.setdefault("recipient_id", str(item.get("recipient_id") or ""))
        custom_args.setdefault("send_log_id", str(item.get("send_log_id") or ""))
        custom_args.setdefault("log_type", str(item.get("log_type") or "regular"))
        if isinstance(custom_args, dict):
            for arg_key, arg_value in custom_args.items():
                normalized_arg_key = str(arg_key or "").strip()
                if normalized_arg_key:
                    personalization.add_custom_arg(
                        CustomArg(normalized_arg_key, "" if arg_value is None else str(arg_value))
                    )
        mail.add_personalization(personalization)
    if not mail.personalizations:
        raise ValueError("批量发送未包含有效收件人")
    return mail


def _build_unsubscribe_headers(unsubscribe_url: str) -> dict[str, str]:
    normalized_url = str(unsubscribe_url or "").strip()
    if not normalized_url:
        return {}
    return {
        "List-Unsubscribe": f"<{normalized_url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def _is_hard_bounce_response(status_code: int, error_message: str) -> bool:
    return is_hard_bounce_error(error_message, status_code=status_code)


def _is_send_infrastructure_soft_error(exc: BaseException) -> bool:
    """SenderPool/发送槽/客户端锁/超时等基础设施争用，应软重试而非整批记失败。"""
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return True
    message = str(exc or "").lower()
    markers = (
        "senderpool",
        "工作队列已满",
        "发送请求被拒绝",
        "发送槽等待超时",
        "客户端锁等待超时",
        "channel send slot",
        "queue full",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "连接池",
        "queuepool",
    )
    return any(marker in message for marker in markers)


def _build_sdk_batch_recipients_data(
    task: Task,
    recipients: list[Recipient],
    *,
    is_monitor: bool,
    unsubscribe_enabled: bool,
    picked_sender: dict[str, str],
    url_assignments: list[dict[str, str]],
    active_url_pool_id: int | None,
) -> tuple[list[dict], list[tuple[Recipient, str]]]:
    recipients_data: list[dict] = []
    preparation_failures: list[tuple[Recipient, str]] = []
    dynamic_variables = (
        _precompute_batch_dynamic_variables(task.template, len(recipients)) if not is_monitor else []
    )
    for index, recipient in enumerate(recipients):
        url_substitutions = url_assignments[index] if index < len(url_assignments) else _build_url_substitution_map("")
        if active_url_pool_id and not str(url_substitutions.get(SENDGRID_URL_SUBSTITUTION_KEY, "") or "").strip():
            preparation_failures.append((recipient, URL_POOL_EMPTY_ERROR))
            continue
        if is_monitor:
            variable_bundle = _monitor_variables(
                recipient.email,
                task,
                unsubscribe_enabled,
                sender=picked_sender,
                monitor_recipient=recipient,
            )
        else:
            variable_bundle = _recipient_variables(recipient, task, unsubscribe_enabled, sender=picked_sender)
            if index < len(dynamic_variables):
                variable_bundle.render_variables.update(dynamic_variables[index])
        substitutions = build_recipient_substitutions(task.template, variable_bundle.render_variables)
        substitutions.update(url_substitutions)
        substitutions.update(variable_bundle.substitution_unsubscribe)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "任务 %s 构建收件人 Personalization email=%s substitutions=%s headers=%s url_pool_id=%s unsubscribe_enabled=%s",
                getattr(task, "id", ""),
                recipient.email,
                sorted(substitutions.keys()),
                bool(variable_bundle.unsubscribe_url),
                active_url_pool_id,
                unsubscribe_enabled,
            )
        recipients_data.append(
            {
                "email": recipient.email,
                "task_id": int(task.id or 0),
                "recipient_id": int(recipient.id or 0),
                "log_type": "monitor" if is_monitor else "regular",
                "substitutions": substitutions,
                "headers": _build_unsubscribe_headers(variable_bundle.unsubscribe_url),
            }
        )
    return recipients_data, preparation_failures


async def _send_monitor_single(
    task: Task,
    recipient: Recipient,
    *,
    monitor_trigger_count: int = 0,
    monitor_rate_limiter_holder: RateLimiterHolder | None = None,
    url_pool_cache: TaskUrlPoolCache | None = None,
    worker_concurrency: int | None = None,
    batch_sender: dict[str, str] | None = None,
    sync_sendgrid_manager: SyncSendGridClientManager | None = None,
) -> None:
    """监控抽样：SDK 单 Personalization 发送，主题「回测 - X封」，不计入任务 sent_count。"""
    current_status = str(_task_status_cache.get(int(task.id or 0)) or "").strip().lower()
    if current_status in {"paused", "stopped", "failed", "aborted", "blocked"}:
        logger.info(
            "任务 %s 状态=%s，取消尚未发起 HTTP 的监控邮件 email=%s trigger_count=%s",
            task.id,
            current_status,
            recipient.email,
            monitor_trigger_count,
        )
        return
    normalized_email = str(recipient.email or "").strip()
    if not normalized_email:
        return
    if await _monitor_email_already_resolved(task.id, normalized_email, monitor_trigger_count):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "task %s monitor email already resolved email=%s trigger_count=%s",
                task.id,
                normalized_email,
                monitor_trigger_count,
            )
        return
    if task.template is None or task.channel is None:
        await _record_monitor_result(task.id, normalized_email, False, "任务模板或通道未加载", 0, monitor_trigger_count)
        return
    if sync_sendgrid_manager is None:
        await _record_monitor_result(task.id, normalized_email, False, "SendGrid SDK 客户端未初始化", 0, monitor_trigger_count)
        return
    if task.channel.proxy_id and not bool(task.channel.use_proxy):
        task.channel.use_proxy = True
    runtime = _task_runtime_metrics.get(task.id)
    task_paused = False
    sdk_inflight_tracked = False
    if runtime is not None:
        runtime["httpx_inflight"] = int(runtime.get("httpx_inflight", 0)) + 1
        runtime["active_http_inflight"] = runtime["httpx_inflight"]
        sdk_inflight_tracked = True
    try:
        logger.info("任务 %s 开始向 %s 发送回测邮件，触发计数=%s", task.id, normalized_email, monitor_trigger_count)
        picked_sender = _normalize_batch_sender(batch_sender)
        if picked_sender is None:
            picked_sender = await _pick_task_sender(task)
            _set_task_current_sender(task.id, picked_sender)
        unsubscribe_enabled = _task_unsubscribe_enabled(task)
        static_content = _resolve_task_static_content(task, runtime)
        if not str(static_content.content or "").strip():
            await _record_monitor_result(task.id, normalized_email, False, "模板渲染后的邮件正文为空", 0, monitor_trigger_count)
            return
        variable_bundle = _monitor_variables(
            normalized_email,
            task,
            unsubscribe_enabled,
            sender=picked_sender,
            monitor_recipient=recipient,
        )
        active_url_pool_id = int(task.url_pool_id or 0)
        if active_url_pool_id > 0:
            try:
                monitor_url_assignments = await _assign_urls_to_recipients(
                    [recipient],
                    url_pool_cache,
                    active_url_pool_id,
                    task.id,
                )
                url_substitutions = monitor_url_assignments[0] if monitor_url_assignments else _build_url_substitution_map("")
            except Exception as exc:
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    False,
                    sanitize_user_error_message(f"监控邮件 URL 池分配失败：{exc}"),
                    0,
                    monitor_trigger_count,
                )
                return
            if not str(url_substitutions.get(SENDGRID_URL_SUBSTITUTION_KEY, "") or "").strip():
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    False,
                    URL_POOL_EMPTY_ERROR,
                    0,
                    monitor_trigger_count,
                )
                return
        else:
            url_substitutions = _build_url_substitution_map("")
        substitutions = build_recipient_substitutions(task.template, variable_bundle.render_variables)
        substitutions.update(url_substitutions)
        substitutions.update(variable_bundle.substitution_unsubscribe)
        mail = build_batch_mail(
            from_email=picked_sender["email"],
            from_name=picked_sender.get("name") or "",
            static_content=static_content,
            recipients_data=[
                {
                    "email": normalized_email,
                    "substitutions": substitutions,
                    "headers": _build_unsubscribe_headers(variable_bundle.unsubscribe_url),
                    "custom_args": {
                        "task_id": str(task.id),
                        "recipient_id": "0",
                        "log_type": "monitor",
                        "monitor_trigger_count": str(max(0, int(monitor_trigger_count or 0))),
                    },
                }
            ],
            subject_override=f"回测 - {max(0, int(monitor_trigger_count or 0))}封",
        )
        max_attempts = max(MONITOR_SDK_MIN_ATTEMPTS, int(task.retry_count or 0) + 1)
        last_body = ""
        for attempt in range(1, max_attempts + 1):
            current_status = str(_task_status_cache.get(int(task.id or 0)) or "").strip().lower()
            if current_status in {"paused", "stopped", "failed", "aborted", "blocked"}:
                logger.info(
                    "任务 %s 状态=%s，停止监控邮件后续尝试 email=%s trigger_count=%s next_attempt=%s",
                    task.id,
                    current_status,
                    normalized_email,
                    monitor_trigger_count,
                    attempt,
                )
                return
            if monitor_rate_limiter_holder is not None and monitor_rate_limiter_holder.limiter is not None:
                await acquire_rate_token(monitor_rate_limiter_holder.limiter)
            try:
                logger.info(
                    "监控邮件 SDK 调用开始，收件人=%s，触发计数=%s",
                    [normalized_email],
                    monitor_trigger_count,
                )
                batch_result = await asyncio.wait_for(
                    send_batch_mail_async(sync_sendgrid_manager, mail),
                    timeout=MONITOR_SEND_TIMEOUT_SECONDS,
                )
                logger.info("监控邮件 SDK 调用完成，状态码=%s", int(batch_result.status_code or 0))
            except asyncio.TimeoutError:
                last_body = f"监控邮件 SenderPool HTTP 请求超时（超过 {MONITOR_SEND_TIMEOUT_SECONDS:.1f} 秒），投递状态未知"
                _log_sampled_task_warning(
                    f"monitor_timeout:{task.id}",
                    "监控邮件发送超时，任务=%s，收件人=%s，触发计数=%s，状态=delivery_unknown，禁止自动重试；任务主发送链路继续运行",
                    task.id,
                    normalized_email,
                    monitor_trigger_count,
                )
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    False,
                    last_body,
                    attempt,
                    monitor_trigger_count,
                    sender_email=str(picked_sender.get("email") or "").strip(),
                    status_override="delivery_unknown",
                )
                return
            except DeliveryUnknownError as exc:
                last_body = sanitize_user_error_message(f"监控邮件 SDK 请求结果未知：{exc}")
                _log_sampled_task_warning(
                    f"monitor_delivery_unknown:{task.id}",
                    "监控邮件 SDK 调用失败，任务=%s，收件人=%s，触发计数=%s，投递状态未知，禁止自动重试，错误=%s",
                    task.id,
                    normalized_email,
                    monitor_trigger_count,
                    last_body,
                )
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    False,
                    last_body,
                    attempt,
                    monitor_trigger_count,
                    sender_email=str(picked_sender.get("email") or "").strip(),
                    status_override="delivery_unknown",
                )
                return
            except Exception as exc:
                last_body = sanitize_user_error_message(f"监控邮件 SDK 调用异常：{exc}")
                logger.error(
                    "监控邮件 SDK 调用失败，任务=%s，收件人=%s，触发计数=%s，第 %s/%s 次，错误=%s",
                    task.id,
                    normalized_email,
                    monitor_trigger_count,
                    attempt,
                    max_attempts,
                    last_body,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(min(2 ** (attempt - 1), 10))
                    continue
                break
            maybe_warn_send_threadpool_pressure(worker_concurrency)
            http_send_ms = round(float(getattr(batch_result, "delay_ms", 0.0) or 0.0), 2)
            if runtime is not None:
                _record_http_send_latency_metrics(
                    runtime,
                    http_send_ms,
                    recipient_count=1,
                    channel_id=int(task.channel_id or 0),
                    success=bool(batch_result.success),
                )
                _record_http_completion(runtime)
                _record_http_send_health_metrics(runtime, http_send_ms=http_send_ms, result_body=batch_result.body or "")
            last_body = batch_result.body or ""
            per_result = (batch_result.per_recipient_results or {}).get(normalized_email) or {}
            if not per_result and batch_result.per_recipient_results:
                per_result = next(iter(batch_result.per_recipient_results.values()), {})
            success = bool(per_result.get("success", batch_result.success))
            error_message = str(per_result.get("error") or batch_result.body or "")
            provider_message_id = str(getattr(batch_result, "provider_message_id", "") or "").strip()
            if success and int(batch_result.status_code or 0) == 202 and provider_message_id:
                logger.info(
                    "任务 %s 监控邮件已由 SendGrid SDK 真实受理 email=%s trigger_count=%s status_code=%s sg_message_id=%s",
                    task.id,
                    normalized_email,
                    monitor_trigger_count,
                    batch_result.status_code,
                    provider_message_id,
                )
                _schedule_record_send_outcome(task.channel.id, True)
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    True,
                    batch_result.body,
                    attempt,
                    monitor_trigger_count,
                    sender_email=str(picked_sender.get("email") or "").strip(),
                    provider_message_id=provider_message_id,
                )
                return
            if success:
                error_message = (
                    f"SendGrid SDK 未返回可确认的 202 与消息 ID：status_code={int(batch_result.status_code or 0)}，"
                    f"sg_message_id={provider_message_id or '空'}"
                )
                last_body = error_message
                logger.error(
                    "任务 %s 监控邮件未获真实受理确认 email=%s trigger_count=%s error=%s",
                    task.id,
                    normalized_email,
                    monitor_trigger_count,
                    error_message,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(min(2 ** (attempt - 1), 10))
                    continue
                break
            status_code = int(per_result.get("status_code") or batch_result.status_code or 0)
            if is_sendgrid_messaging_limit_error(error_message):
                sync_sendgrid_manager.invalidate_client(reason="monitor_messaging_limit_response")
                if runtime is not None:
                    _record_http_429_metrics(
                        task.id,
                        runtime,
                        retry_after=batch_result.retry_after,
                        reason="messaging_limit",
                    )
            response_headers = dict(getattr(batch_result, "response_headers", {}) or {})
            if _is_sendgrid_auth_failure(status_code, error_message, response_headers) and await _confirm_sendgrid_auth_failure(
                task, status_code, error_message
            ): 
                task_paused = await _handle_task_send_auth_failure(task, status_code, error_message)
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    False,
                    "发送遇确认的 401/403，任务已暂停" if task_paused else "发送遇确认的 401/403，已降速重试",
                    attempt,
                    monitor_trigger_count,
                )
                return
            if _is_hard_bounce_response(status_code, error_message):
                _schedule_record_send_outcome(task.channel.id, False)
                await _record_monitor_result(
                    task.id,
                    normalized_email,
                    False,
                    error_message or "永久性投递失败",
                    attempt,
                    monitor_trigger_count,
                )
                return
            if attempt < max_attempts:
                retry_after_seconds = 0
                if batch_result.retry_after:
                    try:
                        retry_after_seconds = max(0, min(int(batch_result.retry_after), 300))
                    except ValueError:
                        retry_after_seconds = 0
                backoff_seconds = retry_after_seconds or min((2 ** (attempt - 1)) + random.uniform(0, 1), 60)
                if batch_result.status_code == 429:
                    backoff_seconds = max(backoff_seconds, 10)
                await asyncio.sleep(backoff_seconds)
        _schedule_record_send_outcome(task.channel.id, False)
        logger.warning("任务 %s 回测邮件发送失败 email=%s trigger_count=%s response=%s", task.id, normalized_email, monitor_trigger_count, (last_body or "监控邮件发送失败")[:500])
        await _record_monitor_result(
            task.id,
            normalized_email,
            False,
            last_body or "监控邮件发送失败",
            max_attempts,
            monitor_trigger_count,
        )
    except Exception as exc:
        _log_sampled_task_warning(
            f"send_monitor_single:{task.id}",
            "任务 %s 监控单封发送失败 email=%s trigger_count=%s error_type=%s error=%s",
            task.id,
            normalized_email,
            monitor_trigger_count,
            type(exc).__name__,
            exc,
        )
        await _record_monitor_result(
            task.id,
            normalized_email,
            False,
            sanitize_user_error_message(f"监控邮件发送失败：{exc}"),
            0,
            monitor_trigger_count,
        )
    finally:
        if sdk_inflight_tracked and runtime is not None:
            runtime["httpx_inflight"] = max(0, int(runtime.get("httpx_inflight", 0)) - 1)
            runtime["active_http_inflight"] = runtime["httpx_inflight"]


async def _send_batch(
    task: Task,
    recipients: list[Recipient],
    *,
    is_monitor: bool = False,
    monitor_trigger_count: int = 0,
    rate_limiter_holder: RateLimiterHolder | None = None,
    monitor_rate_limiter_holder: RateLimiterHolder | None = None,
    url_pool_cache: TaskUrlPoolCache | None = None,
    worker_concurrency: int | None = None,
    rate_limit: float = 0.0,
    batch_sender: dict[str, str] | None = None,
    sync_sendgrid_manager: SyncSendGridClientManager | None = None,
    send_log_id_allocator: SendLogIdAllocator | None = None,
    allow_partial_retry: bool = True,
    inflight_emails: set[str] | None = None,
    skip_rate_token_acquire: bool = False,
    rate_worker_id: int | None = None,
) -> None:
    if not recipients:
        return
    task = _task_with_selected_template(task, _task_runtime_metrics.get(task.id))
    if is_monitor:
        for recipient in recipients:
            await _send_monitor_single(
                task,
                recipient,
                monitor_trigger_count=monitor_trigger_count,
                monitor_rate_limiter_holder=monitor_rate_limiter_holder,
                url_pool_cache=url_pool_cache,
                worker_concurrency=worker_concurrency,
                batch_sender=batch_sender,
                sync_sendgrid_manager=sync_sendgrid_manager,
            )
        return
    active_inflight = inflight_emails if inflight_emails is not None else _get_task_inflight_emails(task.id)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "任务 %s SDK 批量发送 recipients=%s partial_retry=%s inflight_reserved=%s",
            task.id,
            len(recipients),
            allow_partial_retry,
            len(active_inflight or []),
        )
    if task.template is None or task.channel is None:
        missing_error = "任务模板或通道未加载"
        for recipient in recipients:
            await _enqueue_result(task.id, recipient, False, missing_error, 0, inflight_emails=active_inflight)
        return
    if sync_sendgrid_manager is None:
        missing_manager_error = "SendGrid SDK 客户端未初始化，批量发送中止"
        for recipient in recipients:
            await _enqueue_result(task.id, recipient, False, missing_manager_error, 0, inflight_emails=active_inflight)
        return
    if task.channel.proxy_id and not bool(task.channel.use_proxy):
        task.channel.use_proxy = True
    try:
        picked_sender = _normalize_batch_sender(batch_sender)
        if picked_sender is None:
            picked_sender = await pick_next_sender(
                task.channel,
                rotation_enabled=_task_sender_rotation_enabled(task),
            )
            _set_task_current_sender(task.id, picked_sender)
        unsubscribe_enabled = bool(task.unsubscribe_enabled)
        build_runtime = _task_runtime_metrics.get(task.id)
        static_content = _resolve_task_static_content(task, build_runtime)
        if not str(static_content.content or "").strip():
            empty_error = "模板渲染后的邮件正文为空，请检查模板内容和变量"
            for recipient in recipients:
                await _enqueue_result(task.id, recipient, False, empty_error, 0, inflight_emails=active_inflight)
            return
        active_url_pool_id = task.url_pool_id
        url_assignments: list[dict[str, str]] = []
        deduped_recipients: list[Recipient] = []
        seen_batch_recipient_keys: set[str] = set()
        duplicate_in_batch = 0
        for recipient in recipients:
            dedupe_key = _inflight_recipient_key(recipient)
            if dedupe_key and dedupe_key in seen_batch_recipient_keys:
                duplicate_in_batch += 1
                continue
            if dedupe_key:
                seen_batch_recipient_keys.add(dedupe_key)
            deduped_recipients.append(recipient)
        if duplicate_in_batch:
            build_runtime = _task_runtime_metrics.get(task.id)
            if build_runtime is not None:
                build_runtime["send_batch_duplicate_recipient_dropped"] = int(build_runtime.get("send_batch_duplicate_recipient_dropped", 0) or 0) + duplicate_in_batch
            logger.error(
                "任务 %s 发送前拦截同批重复收件人 duplicate_count=%s original=%s deduped=%s",
                task.id,
                duplicate_in_batch,
                len(recipients),
                len(deduped_recipients),
            )
        recipients = deduped_recipients
        if not recipients:
            return
        url_assignments = [
            dict(getattr(recipient, "_mercury_url_substitutions", None) or _build_url_substitution_map(""))
            for recipient in recipients
        ]
        if active_url_pool_id:
            missing_url_recipients = [
                recipient
                for recipient, assignment in zip(recipients, url_assignments)
                if not str(assignment.get(SENDGRID_URL_SUBSTITUTION_KEY, "") or "").strip()
            ]
            if missing_url_recipients:
                logger.warning(
                    "任务 %s 发送批次发现缺少 URL 分配，跳过并记录失败 missing=%s total=%s",
                    task.id,
                    len(missing_url_recipients),
                    len(recipients),
                )
                for recipient in missing_url_recipients:
                    await _enqueue_result(
                        task.id,
                        recipient,
                        False,
                        "收件人缺少预取阶段分配的 URL",
                        0,
                        inflight_emails=active_inflight,
                        send_log_id=getattr(recipient, "_mercury_send_log_id", None),
                    )
                missing_url_ids = {id(recipient) for recipient in missing_url_recipients}
                recipients = [recipient for recipient in recipients if id(recipient) not in missing_url_ids]
                url_assignments = [
                    dict(getattr(recipient, "_mercury_url_substitutions", None) or _build_url_substitution_map(""))
                    for recipient in recipients
                ]
                if not recipients:
                    return
        loop = asyncio.get_running_loop()
        build_executor = _get_global_render_threadpool()
        recipients_data, preparation_failures = await loop.run_in_executor(
            build_executor,
            lambda: _build_sdk_batch_recipients_data(
                task,
                recipients,
                is_monitor=False,
                unsubscribe_enabled=unsubscribe_enabled,
                picked_sender=picked_sender,
                url_assignments=url_assignments,
                active_url_pool_id=int(active_url_pool_id) if active_url_pool_id else None,
            ),
        )
        for failed_recipient, failure_message in preparation_failures:
            await _enqueue_result(
                task.id,
                failed_recipient,
                False,
                failure_message,
                0,
                inflight_emails=active_inflight,
            )
        if not recipients_data:
            return
        failure_emails = {
            str(item[0].email or "").strip().lower()
            for item in preparation_failures
        }
        sendable_recipients = [
            recipient
            for recipient in recipients
            if str(recipient.email or "").strip().lower() not in failure_emails
        ]
        if not recipients_data:
            return
        send_log_ids = {
            str(int(recipient.id or 0)): int(getattr(recipient, "_mercury_send_log_id", 0) or 0)
            for recipient in sendable_recipients
        }
        missing_send_log_ids = [key for key, value in send_log_ids.items() if not value]
        if missing_send_log_ids:
            allocation_error = "收件人缺少预取阶段分配的 SendLog ID"
            logger.error(
                "任务 %s 发送批次缺少预分配 SendLog ID missing=%s recipients=%s",
                task.id,
                len(missing_send_log_ids),
                len(sendable_recipients),
            )
            for recipient in sendable_recipients:
                await _enqueue_result(task.id, recipient, False, allocation_error, 0, inflight_emails=active_inflight)
            return
        for item in recipients_data:
            recipient_key = str(item.get("recipient_id") or "").strip()
            send_log_id = send_log_ids.get(recipient_key)
            custom_args = dict(item.get("custom_args") or {}) if isinstance(item.get("custom_args"), dict) else {}
            custom_args["task_id"] = str(task.id)
            custom_args["log_type"] = str(item.get("log_type") or "regular")
            custom_args["recipient_id"] = str(item.get("recipient_id") or "")
            if send_log_id:
                custom_args["send_log_id"] = str(send_log_id)
            item["custom_args"] = custom_args
        _reserve_inflight_recipients(active_inflight, sendable_recipients)
        build_batch_started_at = monotonic()
        template_key = get_template_cache_key(task.template)
        mail_shell = _resolve_batch_mail_shell(
            picked_sender["email"],
            picked_sender.get("name") or "",
            static_content,
            template_key,
            build_runtime,
        )
        mail = await loop.run_in_executor(
            build_executor,
            _compose_batch_mail_from_shell,
            mail_shell,
            recipients_data,
        )
        if build_runtime is not None:
            batch_build_time_ms = round((monotonic() - build_batch_started_at) * 1000, 2)
            build_runtime["batch_build_time_ms"] = batch_build_time_ms
            build_runtime["last_build_batch_ms"] = batch_build_time_ms
    except Exception as exc:
        _log_sampled_task_warning(
            f"send_batch_prepare:{task.id}",
            "任务 %s SDK 批量邮件准备失败 recipients=%s error_type=%s error=%s",
            task.id,
            len(recipients),
            type(exc).__name__,
            exc,
        )
        prepare_error = sanitize_user_error_message(f"准备批量邮件失败：{exc}")
        for recipient in recipients:
            await _enqueue_result(task.id, recipient, False, prepare_error, 0, inflight_emails=active_inflight)
        return

    runtime = _task_runtime_metrics.get(task.id)
    active_send_slots = len(sendable_recipients)
    active_send_slot_held = False
    sdk_inflight_tracked = False
    if runtime is not None and active_send_slots > 0:
        await _increment_active_sends(runtime, task.id, active_send_slots)
        active_send_slot_held = True
    if runtime is not None:
        runtime["httpx_inflight"] = int(runtime.get("httpx_inflight", 0)) + 1
        sdk_inflight_tracked = True

    configured_max_attempts = max(1, int(settings.send_temporary_max_retries) + 1)
    max_attempts = min(max(1, int(task.retry_count or 0) + 1), configured_max_attempts)
    task_paused = False
    single_recipient_mode = len(sendable_recipients) == 1
    attempt_limit = max_attempts if single_recipient_mode and not allow_partial_retry else 1

    try:
        for attempt in range(1, attempt_limit + 1):
            current_status = str(_task_status_cache.get(task.id) or getattr(task, "status", "") or "").strip().lower()
            if current_status in {"paused", "stopped", "failed", "finished", "aborted"} or int(task.id) in _task_shutdown_requested:
                _rollback_unsent_recipients_for_resume(
                    runtime,
                    int(task.id),
                    sendable_recipients,
                    active_inflight,
                    reason=f"SDK 批量发送前检测到任务状态={current_status or 'shutdown'}",
                )
                return
            rate_tokens_consumed = bool(skip_rate_token_acquire)
            http_429_attempt = 0
            batch_result = None
            http_send_ms = 0.0
            connection_reused = None
            infrastructure_retry_attempts = 0
            infrastructure_retry_started_at = monotonic()
            while True:
                if not rate_tokens_consumed:
                    preprocess_ms = float((runtime or {}).get("last_build_batch_ms", 0) or 0)
                    wait_token_started_at = monotonic()
                    if rate_limiter_holder is not None:
                        # 所有 Worker 共享任务级单调时钟令牌桶，避免双层排队与每 Worker 突发叠加。
                        await acquire_worker_rate_tokens(
                            rate_limiter_holder,
                            rate_worker_id,
                            len(sendable_recipients),
                        )
                    wait_token_ms = round((monotonic() - wait_token_started_at) * 1000, 2)
                    rate_tokens_consumed = True
                else:
                    preprocess_ms = float((runtime or {}).get("last_build_batch_ms", 0) or 0)
                    wait_token_ms = 0.0
                current_status = str(_task_status_cache.get(task.id) or getattr(task, "status", "") or "").strip().lower()
                if current_status in {"paused", "stopped", "failed", "finished", "aborted"} or int(task.id) in _task_shutdown_requested:
                    _rollback_unsent_recipients_for_resume(
                        runtime,
                        int(task.id),
                        sendable_recipients,
                        active_inflight,
                        reason=f"SDK 令牌等待后检测到任务状态={current_status or 'shutdown'}",
                    )
                    return
                try:
                    limiter = (runtime or {}).get("concurrency_semaphore")
                    if not isinstance(limiter, DynamicConcurrencyLimiter):
                        raise RuntimeError("任务本地并发限制器未初始化")
                    async with limiter.slot():
                        http_send_started_at = monotonic()
                        if runtime is not None:
                            runtime["send_batch_http_started_at"] = round(http_send_started_at, 6)
                            runtime["send_batch_worker_concurrency"] = int(worker_concurrency or 0)
                            runtime["local_concurrency_limit"] = limiter.limit
                            runtime["local_concurrency_inflight"] = limiter.inflight
                        batch_result = await send_batch_mail_async(sync_sendgrid_manager, mail)
                except DeliveryUnknownError as exc:
                    unknown_message = sanitize_user_error_message(str(exc))
                    if runtime is not None:
                        runtime["transport_degraded_until"] = monotonic() + max(30.0, AUTO_TUNE_INTERVAL_SECONDS * 3.0)
                    unknown_items = [
                        PendingEnqueueResult(
                            recipient=recipient,
                            success=False,
                            response_text=unknown_message,
                            attempt_count=attempt,
                            sender_email=str(picked_sender.get("email") or "").strip() or None,
                            send_log_id=send_log_ids.get(str(int(getattr(recipient, "id", 0) or 0))),
                            status_override="delivery_unknown",
                        )
                        for recipient in sendable_recipients
                    ]
                    await batch_enqueue_results(task.id, unknown_items, inflight_emails=active_inflight)
                    await _persist_claim_cursor_after_http_batch(task.id)
                    logger.warning(
                        "任务 %s 批次进入待确认队列 recipients=%s，需人工确认；未配置业务幂等键，禁止自动重试",
                        task.id,
                        len(sendable_recipients),
                    )
                    return
                except Exception as exc:
                    if _is_send_infrastructure_soft_error(exc):
                        infrastructure_retry_attempts += 1
                        elapsed_retry = monotonic() - infrastructure_retry_started_at
                        remaining_budget = INFRASTRUCTURE_SOFT_RETRY_TOTAL_SECONDS - elapsed_retry
                        if infrastructure_retry_attempts >= INFRASTRUCTURE_SOFT_RETRY_MAX_ATTEMPTS or remaining_budget <= 0:
                            failure_message = sanitize_user_error_message(
                                f"发送基础设施连续失败，已在 {elapsed_retry:.1f} 秒重试预算内尝试 {infrastructure_retry_attempts} 次：{str(exc) or type(exc).__name__}"
                            )
                            failure_items = [
                                PendingEnqueueResult(
                                    recipient=recipient,
                                    success=False,
                                    response_text=failure_message,
                                    attempt_count=attempt,
                                    sender_email=str(picked_sender.get("email") or "").strip() or None,
                                    send_log_id=send_log_ids.get(str(int(getattr(recipient, "id", 0) or 0))),
                                )
                                for recipient in sendable_recipients
                            ]
                            await batch_enqueue_results(task.id, failure_items, inflight_emails=active_inflight)
                            _log_sampled_task_warning(
                                f"infrastructure_retry_exhausted:{task.id}",
                                "任务 %s 基础设施软重试预算耗尽，当前批次已标记失败 recipients=%s attempts=%s elapsed=%.1fs error_type=%s error=%s",
                                task.id,
                                len(sendable_recipients),
                                infrastructure_retry_attempts,
                                elapsed_retry,
                                type(exc).__name__,
                                str(exc) or "无错误详情",
                            )
                            return
                        retry_backoff = min(remaining_budget, 2.0 * (2 ** (infrastructure_retry_attempts - 1)))
                        _log_sampled_task_warning(
                            f"infrastructure_soft_retry:{task.id}",
                            "任务 %s 发送基础设施瞬时失败，%.1f 秒后软重试 attempt=%s/%s total_budget=%ss error_type=%s error=%s",
                            task.id,
                            retry_backoff,
                            infrastructure_retry_attempts,
                            INFRASTRUCTURE_SOFT_RETRY_MAX_ATTEMPTS,
                            int(INFRASTRUCTURE_SOFT_RETRY_TOTAL_SECONDS),
                            type(exc).__name__,
                            str(exc) or "无错误详情",
                        )
                        await asyncio.sleep(retry_backoff)
                        continue
                    if not isinstance(exc, ValueError):
                        raise
                    error_message = sanitize_user_error_message(str(exc))
                    _rollback_unsent_recipients_for_resume(
                        runtime,
                        int(task.id),
                        sendable_recipients,
                        active_inflight,
                        reason=f"发送请求构建失败，未向 SendGrid 发出请求：{error_message[:200]}",
                    )
                    logger.error(
                        "任务 %s 发送请求构建失败，当前批次未记为投递失败并等待修复后重试：收件人数=%s，错误=%s",
                        task.id,
                        len(sendable_recipients),
                        error_message,
                    )
                    await _mark_task_paused(task.id, f"发送请求构建失败，请检查模板、发件人和通道配置：{error_message}")
                    return
                maybe_warn_send_threadpool_pressure(worker_concurrency)
                http_finished_at = monotonic()
                http_send_ms = round(float(getattr(batch_result, "delay_ms", 0.0) or 0.0), 2)
                if runtime is not None:
                    runtime["send_batch_http_finished_at"] = round(http_finished_at, 6)
                    if logger.isEnabledFor(logging.DEBUG) or bool(getattr(settings, "perf_debug", False)):
                        logger.info(
                            "任务 %s HTTP批次窗口 recipients=%s started=%.6f finished=%.6f http_ms=%s slot_wait_ms=%s httpx_inflight=%s worker_concurrency=%s",
                            task.id,
                            len(sendable_recipients),
                            http_send_started_at,
                            http_finished_at,
                            http_send_ms,
                            runtime.get("last_local_concurrency_wait_ms", 0),
                            runtime.get("httpx_inflight", 0),
                            worker_concurrency,
                        )
                connection_reused = batch_result.connection_reused
                batch_is_messaging_limit = is_sendgrid_messaging_limit_error(batch_result.body)
                response_headers = dict(getattr(batch_result, "response_headers", {}) or {})
                send_auth_error_type = str(getattr(batch_result, "sendgrid_error_type", "") or "") or classify_sendgrid_auth_error(
                    int(batch_result.status_code or 0),
                    str(batch_result.body or ""),
                    response_headers,
                )
                auth_confirmed = False
                if int(batch_result.status_code or 0) in (401, 403):
                    response_origin = str(
                        getattr(batch_result, "response_origin", "unknown") or "unknown"
                    )
                    if response_origin == "unknown":
                        response_origin = classify_sendgrid_response_origin(
                            response_headers,
                            str(batch_result.body or ""),
                        )
                    api_key = str(getattr(task.channel, "api_key", "") or "").strip()
                    api_key_fingerprint = (
                        hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]
                        if api_key
                        else "未配置"
                    )
                    logger.error(
                        "任务 %s Mail Send 请求被拒绝 status=%s origin=%s error_type=%s channel_id=%s sender=%s recipients=%s proxy_id=%s api_key_fingerprint=%s response=%s",
                        task.id,
                        int(batch_result.status_code or 0),
                        response_origin,
                        send_auth_error_type or "unknown",
                        task.channel_id,
                        str(picked_sender.get("email") or "").strip(),
                        len(sendable_recipients),
                        getattr(task.channel, "proxy_id", None),
                        api_key_fingerprint,
                        sanitize_user_error_message(str(batch_result.body or ""))[:1000],
                    )
                    if runtime is not None:
                        runtime["last_send_auth_status"] = int(batch_result.status_code or 0)
                        runtime["last_send_auth_sender"] = str(picked_sender.get("email") or "").strip()
                        runtime["last_send_auth_response"] = sanitize_user_error_message(str(batch_result.body or ""))[:1000]

                    auth_confirmed = False
                    is_direct_sendgrid_response = response_origin in {"sendgrid", "unknown"}
                    canonical_sendgrid_auth_error = _is_canonical_sendgrid_mail_auth_error(
                        int(batch_result.status_code or 0),
                        str(batch_result.body or ""),
                    )
                    if canonical_sendgrid_auth_error:
                        auth_confirmed = True
                    elif is_direct_sendgrid_response and send_auth_error_type in _SENDGRID_ACCOUNT_AUTH_ERROR_TYPES:
                        auth_confirmed = await _confirm_sendgrid_auth_failure(
                            task,
                            int(batch_result.status_code or 0),
                            str(batch_result.body or ""),
                        )
                    elif not is_direct_sendgrid_response:
                        logger.error(
                            "任务 %s 收到代理侧认证响应，不暂停 SendGrid 通道；请检查代理认证配置 channel_id=%s origin=%s",
                            task.id,
                            task.channel_id,
                            response_origin,
                        )
                    if auth_confirmed:
                        logger.error(
                            "通道 %s 认证失败（%s），任务自动暂停 task_id=%s 原因=SendGrid 认证失败",
                            task.channel_id,
                            int(batch_result.status_code or 0),
                            task.id,
                        )
                    if send_auth_error_type not in _SENDGRID_SEND_CONFIG_ERROR_TYPES and not auth_confirmed:
                        sync_sendgrid_manager.invalidate_client(reason="unconfirmed_auth_response_rebuild")
                        if runtime is not None:
                            runtime["sendgrid_client_invalidations"] = int(runtime.get("sendgrid_client_invalidations", 0) or 0) + 1
                        logger.warning(
                            "任务 %s 发送接口返回 %s 但鉴权复核未确认失效，跳过 SDK 层批次重放以避免重复发送风险 channel_id=%s recipients=%s",
                            task.id,
                            int(batch_result.status_code or 0),
                            task.channel_id,
                            len(sendable_recipients),
                        )
                if int(batch_result.status_code or 0) in (401, 403) and auth_confirmed:
                    task_paused = await _handle_task_send_auth_failure(
                        task,
                        int(batch_result.status_code or 0),
                        str(batch_result.body or ""),
                    )
                    _rollback_unsent_recipients_for_resume(
                        runtime,
                        int(task.id),
                        sendable_recipients,
                        active_inflight,
                        reason="SendGrid 认证失败，任务已自动暂停，收件人等待认证恢复后继续发送",
                    )
                    return
                if batch_result.status_code == 429 or batch_is_messaging_limit:
                    if batch_is_messaging_limit:
                        sync_sendgrid_manager.invalidate_client(reason="sendgrid_messaging_limit_response")
                    if runtime is not None:
                        _record_http_429_metrics(
                            task.id,
                            runtime,
                            retry_after=batch_result.retry_after,
                            reason="messaging_limit" if batch_is_messaging_limit else "429",
                        )
                    retry_after_seconds = 0
                    if batch_result.retry_after:
                        try:
                            retry_after_seconds = max(0, min(int(batch_result.retry_after), 300))
                        except ValueError:
                            retry_after_seconds = 0
                    backoff_seconds = retry_after_seconds or min(10 + random.uniform(0, 2), 60)
                    if http_429_attempt + 1 < HTTP_429_BATCH_MAX_RETRIES:
                        http_429_attempt += 1
                        logger.warning(
                            "任务 %s SDK 批量发送触发 %s channel_id=%s recipients=%s attempt=%s/%s retry_after=%s backoff=%s",
                            task.id,
                            "SendGrid 发送限额" if batch_is_messaging_limit else "429 限流",
                            task.channel_id,
                            len(sendable_recipients),
                            http_429_attempt,
                            HTTP_429_BATCH_MAX_RETRIES,
                            batch_result.retry_after,
                            backoff_seconds,
                        )
                        if active_send_slot_held and runtime is not None:
                            await _decrement_active_sends(runtime, task.id, active_send_slots)
                            active_send_slot_held = False
                        await asyncio.sleep(backoff_seconds)
                        if runtime is not None and not active_send_slot_held:
                            await _increment_active_sends(runtime, task.id, active_send_slots)
                            active_send_slot_held = True
                        continue
                break

            if runtime is not None:
                _record_phase_timing_sample(
                    runtime,
                    wait_token_ms=wait_token_ms,
                    http_request_ms=http_send_ms,
                    preprocess_ms=preprocess_ms,
                )
                _record_http_send_latency_metrics(
                    runtime,
                    http_send_ms,
                    recipient_count=len(sendable_recipients),
                    channel_id=int(task.channel_id or 0),
                    success=bool(batch_result.success),
                )
                _record_http_completion(runtime)
                _record_http_send_health_metrics(runtime, http_send_ms=http_send_ms, result_body=batch_result.body or "")
                # 多任务下代理 RTT 升高很常见；SDK 线程内已按失败 client 精确失效，
                # 外层不再对 status_code=0 做二次全局失效，避免旧失败批次关闭后续新建健康客户端。
                if int(batch_result.status_code or 0) == 0:
                    runtime["transport_failed_batch_count"] = int(runtime.get("transport_failed_batch_count", 0) or 0) + 1
                    runtime["transport_degraded_until"] = monotonic() + max(30.0, AUTO_TUNE_INTERVAL_SECONDS * 3.0)
                    runtime["slow_http_streak"] = 0
                elif http_send_ms >= SEND_HTTP_CRITICAL_WARN_MS:
                    runtime["slow_http_streak"] = int(runtime.get("slow_http_streak", 0) or 0) + 1
                else:
                    runtime["slow_http_streak"] = 0
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "任务 %s SDK 批量 HTTP 发送 channel_id=%s recipients=%s http_send_ms=%s status_code=%s connection_reused=%s",
                    task.id,
                    task.channel_id,
                    len(sendable_recipients),
                    http_send_ms,
                    batch_result.status_code,
                    connection_reused,
                )
            if not batch_result.success:
                logger.warning(
                    "任务 %s SDK 批量发送失败 channel_id=%s recipients=%s status_code=%s body=%s",
                    task.id,
                    task.channel_id,
                    len(sendable_recipients),
                    batch_result.status_code,
                    str(batch_result.body or "")[:500],
                )
            if http_send_ms >= SEND_HTTP_WARN_LOG_MS:
                logger.warning(
                    "任务 %s SDK 批量发送偏慢 channel_id=%s recipients=%s attempt=%s http_send_ms=%s status_code=%s success=%s",
                    task.id,
                    task.channel_id,
                    len(sendable_recipients),
                    attempt,
                    http_send_ms,
                    batch_result.status_code,
                    batch_result.success,
                )

            if not batch_result.success and int(batch_result.status_code or 0) == 0:
                _rollback_unsent_recipients_for_resume(
                    runtime,
                    int(task.id),
                    sendable_recipients,
                    active_inflight,
                    reason=f"SDK 传输失败未到达 SendGrid，不写失败日志，等待任务恢复后重发：{sanitize_user_error_message(str(batch_result.body or ''))[:200]}",
                )
                return

            per_recipient_results = batch_result.per_recipient_results or {}
            if not per_recipient_results:
                per_recipient_results = {
                    item["email"]: {
                        "success": batch_result.success,
                        "status_code": batch_result.status_code,
                        "error": "" if batch_result.success else str(batch_result.body or ""),
                    }
                    for item in recipients_data
                }

            attempt_failures: list[Recipient] = []
            pending_enqueue_items: list[PendingEnqueueResult] = []
            batch_success_count = 0
            batch_failure_count = 0
            batch_has_success = False
            batch_limit_message = ""
            auth_failure_confirmed: bool | None = None
            substitution_by_email = _index_recipient_substitutions(recipients_data)
            for recipient in sendable_recipients:
                recipient_key = str(recipient.email or "").strip().lower()
                recipient_id_key = str(int(getattr(recipient, "id", 0) or 0))
                batch_sender_email = str(picked_sender.get("email") or getattr(task.channel, "sender_email", "") or "").strip()
                result_item = per_recipient_results.get(recipient.email) or per_recipient_results.get(str(recipient.email or "").lower())
                if result_item is None:
                    normalized_lookup = {str(key or "").lower(): value for key, value in per_recipient_results.items()}
                    result_item = normalized_lookup.get(str(recipient.email or "").strip().lower(), {})
                success = bool(result_item.get("success"))
                status_code = int(result_item.get("status_code") or batch_result.status_code or 0)
                error_message = str(result_item.get("error") or batch_result.body or "")
                if success:
                    batch_has_success = True
                    batch_success_count += 1
                    pending_enqueue_items.append(
                        PendingEnqueueResult(
                            recipient=recipient,
                            success=True,
                            response_text=batch_result.body,
                            attempt_count=attempt,
                            sender_email=batch_sender_email or None,
                            send_log_id=send_log_ids.get(recipient_id_key),
                        )
                    )
                    continue
                if is_sendgrid_messaging_limit_error(error_message):
                    batch_failure_count += 1
                    batch_limit_message = normalize_send_log_error(error_message)
                    pending_enqueue_items.append(
                        PendingEnqueueResult(
                            recipient=recipient,
                            success=False,
                            response_text=batch_limit_message,
                            attempt_count=attempt,
                            sender_email=batch_sender_email or None,
                            send_log_id=send_log_ids.get(recipient_id_key),
                        )
                    )
                    continue
                if _is_sendgrid_auth_failure(status_code, error_message, response_headers):
                    if auth_failure_confirmed is None:
                        auth_failure_confirmed = await _confirm_sendgrid_auth_failure(task, status_code, error_message)
                    if auth_failure_confirmed:
                        task_paused = await _handle_task_send_auth_failure(task, status_code, error_message)
                        if task_paused:
                            _rollback_unsent_recipients_for_resume(
                                runtime,
                                int(task.id),
                                sendable_recipients,
                                active_inflight,
                                reason="SendGrid API Key 鉴权失败，当前批次未投递并等待修复后恢复",
                            )
                            return
                        attempt_failures.append(recipient)
                        continue
                    error_message = _auth_unconfirmed_send_error_message(status_code, error_message)
                if _is_hard_bounce_response(status_code, error_message):
                    batch_failure_count += 1
                    pending_enqueue_items.append(
                        PendingEnqueueResult(
                            recipient=recipient,
                            success=False,
                            response_text=error_message or "永久性投递失败",
                            attempt_count=attempt,
                            sender_email=batch_sender_email or None,
                            send_log_id=send_log_ids.get(recipient_key),
                        )
                    )
                    continue
                attempt_failures.append(recipient)

            if batch_success_count or batch_failure_count:
                _schedule_record_send_outcomes(task.channel.id, success_count=batch_success_count, failure_count=batch_failure_count)
            await batch_enqueue_results(
                task.id,
                pending_enqueue_items,
                inflight_emails=active_inflight,
            )
            if batch_limit_message:
                pause_message = batch_limit_message or CHANNEL_LIMIT_PAUSE_MESSAGE
                logger.error(
                    "任务 %s 检测到 SendGrid 额度耗尽，立即暂停任务避免持续失败 channel_id=%s message=%s",
                    task.id,
                    task.channel_id,
                    pause_message,
                )
                await _mark_task_paused(task.id, pause_message)
                return
            if batch_has_success:
                _schedule_monitor_trigger(task.id, reason="batch_success")

            _record_result_queue_peak_after_batch(runtime, _get_send_log_writer(task.id))
            await _persist_claim_cursor_after_http_batch(task.id)

            if task_paused:
                return
            if not attempt_failures:
                return

            if attempt < attempt_limit and len(attempt_failures) == len(sendable_recipients):
                retry_after_seconds = 0
                if batch_result.retry_after:
                    try:
                        retry_after_seconds = max(0, min(int(batch_result.retry_after), 300))
                    except ValueError:
                        retry_after_seconds = 0
                backoff_seconds = retry_after_seconds or min((2 ** (attempt - 1)) + random.uniform(0, 1), 60)
                if active_send_slot_held and runtime is not None:
                    await _decrement_active_sends(runtime, task.id, active_send_slots)
                    active_send_slot_held = False
                await asyncio.sleep(backoff_seconds)
                if runtime is not None and not active_send_slot_held:
                    await _increment_active_sends(runtime, task.id, active_send_slots)
                    active_send_slot_held = True
                continue
            batch_failure_count += len(attempt_failures)
            if allow_partial_retry and len(sendable_recipients) > 1:
                retry_batch_size = get_send_http_batch_size(task)
                retry_batches = chunk_recipients_for_sdk_batch(attempt_failures, retry_batch_size)
                if runtime is not None:
                    runtime["partial_retry_batch_count"] = int(runtime.get("partial_retry_batch_count", 0) or 0) + len(retry_batches)
                    runtime["partial_retry_recipient_count"] = int(runtime.get("partial_retry_recipient_count", 0) or 0) + len(attempt_failures)
                logger.warning(
                    "任务 %s 部分失败收件人将继续按 SDK 批量重试 recipients=%s retry_batches=%s batch_size=%s，禁止退化为逐封发送",
                    task.id,
                    len(attempt_failures),
                    len(retry_batches),
                    retry_batch_size,
                )
                if active_send_slot_held and runtime is not None:
                    await _decrement_active_sends(runtime, task.id, active_send_slots)
                    active_send_slot_held = False
                for retry_batch in retry_batches:
                    await _send_batch(
                        task,
                        retry_batch,
                        rate_limiter_holder=rate_limiter_holder,
                        monitor_rate_limiter_holder=monitor_rate_limiter_holder,
                        url_pool_cache=url_pool_cache,
                        worker_concurrency=worker_concurrency,
                        rate_limit=rate_limit,
                        batch_sender=picked_sender,
                        sync_sendgrid_manager=sync_sendgrid_manager,
                        send_log_id_allocator=send_log_id_allocator,
                        allow_partial_retry=False,
                        inflight_emails=active_inflight,
                    )
                return
            failure_enqueue_items = [
                PendingEnqueueResult(
                    recipient=recipient,
                    success=False,
                    response_text=str(
                        per_recipient_results.get(recipient.email, {}).get("error")
                        or batch_result.body
                        or "发送失败"
                    ),
                    attempt_count=attempt_limit,
                    sender_email=batch_sender_email or None,
                    send_log_id=send_log_ids.get(str(int(getattr(recipient, "id", 0) or 0))),
                )
                for recipient in attempt_failures
            ]
            if failure_enqueue_items:
                _schedule_record_send_outcomes(task.channel.id, failure_count=len(failure_enqueue_items))
                await batch_enqueue_results(
                    task.id,
                    failure_enqueue_items,
                    inflight_emails=active_inflight,
                )
    finally:
        if sdk_inflight_tracked and runtime is not None:
            runtime["httpx_inflight"] = max(0, int(runtime.get("httpx_inflight", 0)) - 1)
        if active_send_slot_held and runtime is not None:
            await _decrement_active_sends(runtime, task.id, active_send_slots)


@dataclass
class WarmupBatchSendItem:
    email: str
    success: bool
    status_code: int
    body: str
    sender_email: str = ""


def _build_warmup_static_content(subject: str, text_body: str, html_body: str) -> StaticMailContent:
    subject_value = str(subject or "").strip() or "MercuryMail"
    html_value = str(html_body or "").strip()
    text_value = str(text_body or "").strip()
    if html_value:
        return StaticMailContent(subject=subject_value, html=html_value, text=text_value, content_type="html")
    if text_value:
        return StaticMailContent(subject=subject_value, html="", text=text_value, content_type="text")
    raise ValueError("养号邮件正文为空")


async def send_warmup_sdk_batch(
    channel: Channel,
    recipients: list,
    *,
    subject: str,
    text_body: str,
    html_body: str,
    sync_sendgrid_manager: SyncSendGridClientManager,
    batch_sender: dict[str, str] | None = None,
    rotation_enabled: bool = False,
) -> list[WarmupBatchSendItem]:
    """养号 SDK 批量发送：与任务 _send_batch 共用 build_batch_mail + send_batch_mail_async。"""
    if not recipients:
        return []
    if channel.proxy_id and not bool(channel.use_proxy):
        channel.use_proxy = True
    picked_sender = _normalize_batch_sender(batch_sender)
    if picked_sender is None:
        picked_sender = await pick_next_sender(channel, rotation_enabled=rotation_enabled)
    static_content = _build_warmup_static_content(subject, text_body, html_body)
    recipients_data: list[dict[str, object]] = []
    normalized_recipients: list[tuple[object, str]] = []
    for recipient in recipients:
        email = str(getattr(recipient, "email", "") or "").strip()
        if not email:
            continue
        recipient_id = int(getattr(recipient, "id", 0) or 0)
        unsubscribe_url = build_unsubscribe_url(settings.public_base_url, email, recipient_id, None)
        custom_args = {
            "task_id": "0",
            "recipient_id": str(recipient_id),
            "log_type": "warmup",
        }
        recipients_data.append(
            {
                "email": email,
                "substitutions": {},
                "headers": _build_unsubscribe_headers(unsubscribe_url),
                "custom_args": custom_args,
            }
        )
        normalized_recipients.append((recipient, email))
    if not recipients_data:
        return []
    mail = build_batch_mail(
        from_email=picked_sender["email"],
        from_name=picked_sender.get("name") or "",
        static_content=static_content,
        recipients_data=recipients_data,
    )
    batch_result = await send_batch_mail_async(sync_sendgrid_manager, mail)
    items: list[WarmupBatchSendItem] = []
    for _recipient, email in normalized_recipients:
        per_result = (batch_result.per_recipient_results or {}).get(email) or {}
        if not per_result and batch_result.per_recipient_results:
            per_result = next(iter(batch_result.per_recipient_results.values()), {})
        success = bool(per_result.get("success", batch_result.success))
        status_code = int(per_result.get("status_code") or batch_result.status_code or 0)
        error_message = str(per_result.get("error") or batch_result.body or "")
        items.append(
            WarmupBatchSendItem(
                email=email,
                success=success,
                status_code=status_code,
                body=(batch_result.body or "") if success else error_message,
                sender_email=str(picked_sender.get("email") or "").strip(),
            )
        )
    return items


async def verify_task_capacity(task: Task, channel: Channel) -> TaskCapacityProof:
    """证明任务目标速率所需 HTTP 资源；容量不足只告警，不阻止启动。"""
    task_id = int(getattr(task, "id", 0) or 0)
    rate = max(0.0, min(get_effective_second_rate_limit(task), 100000.0))
    batch_size = max(1, int(get_send_http_batch_size(task) or 1))
    effective_rtt = max(0.01, float(await resolve_task_proxy_rtt_seconds(channel)))
    required_http_rps = rate / float(batch_size)
    required_http_inflight = max(1, int(math.ceil(required_http_rps * effective_rtt * 1.5)))
    explicit_task_max = int(getattr(task, "max_concurrency", 0) or 0)
    physical_limit = get_per_task_max_effective_concurrency()
    local_limit = min(explicit_task_max, physical_limit) if explicit_task_max > 0 else physical_limit
    global_limit = 0
    available_global = 0
    constrained: list[str] = []
    capacity_inflight = min(required_http_inflight, local_limit)
    if required_http_inflight > local_limit:
        constrained.append("任务本地并发上限")
    estimated_rate_limit = max(0.0, capacity_inflight / (effective_rtt * 1.5) * batch_size)
    proof = TaskCapacityProof(
        task_id=task_id,
        configured_rate=rate,
        batch_size=batch_size,
        effective_rtt=effective_rtt,
        required_http_rps=required_http_rps,
        required_http_inflight=required_http_inflight,
        global_concurrency_limit=global_limit,
        available_global_concurrency=available_global,
        estimated_rate_limit=min(rate, estimated_rate_limit) if constrained else rate,
        constrained_resources=tuple(constrained),
    )
    if constrained:
        logger.warning(
            "任务 %s 配置速率 %s 封/秒在当前资源下可能无法完全达到，实际可达上限约 %s 封/秒",
            task_id,
            round(rate, 2),
            round(proof.estimated_rate_limit, 2),
        )
        logger.warning(
            "任务 %s 容量证明详情：http_rps=%.2f effective_rtt=%.3fs required_http_inflight=%s task_local_limit=%s global_limit=%s available_global=%s constrained=%s",
            task_id,
            required_http_rps,
            effective_rtt,
            required_http_inflight,
            local_limit,
            global_limit,
            available_global,
            "、".join(constrained),
        )
    else:
        logger.info(
            "任务 %s 容量证明通过：http_rps=%.2f effective_rtt=%.3fs required_http_inflight=%s task_local_limit=%s available_global=%s",
            task_id,
            required_http_rps,
            effective_rtt,
            required_http_inflight,
            local_limit,
            available_global,
        )
    return proof


async def run_task(task_id: int) -> None:
    logger.debug("任务 %s 运行器已启动", task_id)
    _suppress_gc_for_task()
    lock = None
    send_log_writer: AsyncBatchWriter | None = None
    url_pool_cache: TaskUrlPoolCache | None = None
    task_channel_id: int | None = None
    sync_sendgrid_manager: SyncSendGridClientManager | None = None
    monitor_sendgrid_manager: SyncSendGridClientManager | None = None
    monitor_sender_pool_task_id = MONITOR_SENDER_POOL_TASK_ID_OFFSET + int(task_id)
    recipient_queue: asyncio.Queue[Recipient | None] | None = None
    prefetch_task: asyncio.Task | None = None
    prefetch_guard_task: asyncio.Task | None = None
    completion_auditor_task: asyncio.Task | None = None
    prefetch_stop_event: asyncio.Event | None = None
    send_outcome_worker_acquired = False
    runtime: dict[str, Any] | None = None
    workers: list[asyncio.Task] = []
    monitor_workers: list[asyncio.Task] = []
    worker_scale_supervisor_task: asyncio.Task | None = None
    auto_tune_task: asyncio.Task | None = None
    backpressure_task: asyncio.Task | None = None
    heartbeat_task: asyncio.Task | None = None
    realtime_snapshot_task: asyncio.Task | None = None
    try:
        logger.debug("任务 %s 正在加载任务及关联数据", task_id)
        task = await _load_task(task_id)
        if task is None:
            logger.error("任务 %s 不存在", task_id)
            return
        if task.template is not None:
            await asyncio.to_thread(warm_up_template, task.template)
        task_channel_id = int(task.channel_id)
        run_generation = max(1, int(getattr(task, "run_generation", 0) or 0))
        logger.debug("任务 %s 已加载：状态=%s 模板ID=%s 收件人池ID=%s 通道ID=%s", task_id, task.status, task.template_id, task.recipient_list_id, task.channel_id)
        validation_error = await _validate_task_ready(task)
        if validation_error:
            await _mark_task_failed(task_id, validation_error)
            return
        try:
            await verify_writer_prerequisites_ready()
        except Exception as exc:
            logger.exception("任务 %s 启动前 SendLog 写入链路校验失败", task_id)
            await _mark_task_failed(
                task_id,
                f"发送日志数据库结构未就绪，任务未进入发送以避免已发送但无法落库：{sanitize_user_error_message(str(exc))}",
            )
            return
        await _reconcile_task_claim_cursor_from_send_logs(task_id)
        task = await _load_task(task_id)
        if task is None:
            logger.error("任务 %s 在领取游标校准后仍不存在", task_id)
            return
        if await is_warmup_lock_held(task.channel_id):
            if task_id in _scheduled_dispatcher_tasks:
                await _defer_scheduled_task_start(task_id, SCHEDULED_CHANNEL_BUSY_REASON)
                return
            await _mark_task_failed(task_id, "该通道已有养号计划在运行")
            return
        try:
            await _acquire_send_outcome_worker(task_id=task_id, channel_id=int(task.channel_id or 0))
            send_outcome_worker_acquired = True
        except Exception as exc:
            logger.warning(
                "任务 %s 发送结果队列初始化异常，发送链路继续运行并将在首个结果到达时自动重建 channel_id=%s error=%s",
                task_id,
                task.channel_id,
                sanitize_user_error_message(str(exc)),
                exc_info=True,
            )
            send_outcome_worker_acquired = False
        if bool(getattr(settings, "allow_parallel_tasks_per_channel", False)):
            logger.warning("任务 %s 已启用同通道多任务并行模式 channel_id=%s，发送并发由本地 PID 动态调节控制", task_id, task.channel_id)
        else:
            logger.warning("任务 %s 未启用同通道多任务并行，通道 %s 的任务会被独占锁串行化", task_id, task.channel_id)
            try:
                lock = await acquire_task_lock(task.channel_id, task_id, ttl_seconds=120, timeout_seconds=5)
            except RedisLockUnavailableError as exc:
                if task_id in _scheduled_dispatcher_tasks:
                    await _defer_scheduled_task_start(task_id, exc.message)
                    return
                await _mark_task_failed(task_id, exc.message)
                return
            except Exception as exc:
                if task_id in _scheduled_dispatcher_tasks:
                    await _defer_scheduled_task_start(task_id, f"获取发送任务锁失败：{exc}")
                    return
                await _mark_task_failed(task_id, f"获取发送任务锁失败：{exc}")
                return
            if lock is None:
                if task_id in _scheduled_dispatcher_tasks:
                    await _defer_scheduled_task_start(task_id, SCHEDULED_CHANNEL_BUSY_REASON)
                    return
                await _mark_task_failed(task_id, CHANNEL_TASK_BUSY_MESSAGE)
                return
        configured_per_second_rate = max(0.0, min(get_effective_second_rate_limit(task), 100000.0))
        if configured_per_second_rate <= 0:
            await _mark_task_failed(task_id, "任务发送速率必须大于 0，请设置任务 rate_limit 或通道 second_limit")
            return
        channel_id = int(getattr(task, "channel_id", 0) or 0)
        channel_active_task_count = 1
        per_second_rate = configured_per_second_rate
        logger.info(
            "任务 %s 已创建独立任务级限速目标 configured_rate=%.1f effective_rate=%.1f channel_id=%s",
            task_id,
            configured_per_second_rate,
            per_second_rate,
            channel_id,
        )
        send_http_batch_size = get_send_http_batch_size(task)
        capacity_proof = await verify_task_capacity(task, task.channel)
        proxy_rtt_seconds = capacity_proof.effective_rtt
        configured_concurrency = int(task.concurrency or 0)
        configured_auto_concurrency = bool(getattr(task, "auto_concurrency", False))
        auto_concurrency_enabled = bool(settings.enable_pid_concurrency_tuning)
        if auto_concurrency_enabled and not configured_auto_concurrency:
            logger.warning(
                "任务 %s 使用旧版固定并发配置 concurrency=%s；当前发送架构已自动接管为任务本地 PID 并发，原值仅作为启动下限",
                task_id,
                configured_concurrency,
            )
        pipeline_plan = build_send_pipeline_plan(
            rate_limit=per_second_rate,
            send_http_batch_size=send_http_batch_size,
            rtt_seconds=proxy_rtt_seconds,
            configured_concurrency=configured_concurrency,
            auto_concurrency_enabled=auto_concurrency_enabled,
        )
        effective_concurrency = pipeline_plan.effective_concurrency
        min_concurrency = pipeline_plan.min_concurrency
        initial_concurrency = min(min_concurrency, effective_concurrency)
        required_concurrency_for_rate = calculate_task_capacity_requirement(
            rate_limit=per_second_rate,
            send_http_batch_size=send_http_batch_size,
            rtt_seconds=proxy_rtt_seconds,
        )
        if effective_concurrency < required_concurrency_for_rate:
            logger.warning(
                "任务 %s 配置速率 %s 封/秒在当前资源下可能无法完全达到，实际可达上限约 %s 封/秒",
                task_id,
                round(per_second_rate, 2),
                round(min(per_second_rate, capacity_proof.estimated_rate_limit), 2),
            )
        configured_task_max = int(getattr(task, "max_concurrency", 0) or 0)
        if configured_task_max > 0:
            task_max_concurrency = configured_task_max
        else:
            # Task 当前没有持久化 max_concurrency 字段。默认硬编码 128 会在高 RTT 或小批量时
            # 无声截断已计算出的容量下限，使 PID 永远无法扩到目标速率所需并发。
            configured_default_max = max(1, int(os.getenv("TASK_DEFAULT_MAX_CONCURRENCY", "128") or 128))
            task_max_concurrency = max(configured_default_max, effective_concurrency, required_concurrency_for_rate)
        task_max_concurrency = min(task_max_concurrency, get_per_task_max_effective_concurrency())
        initial_local_concurrency = max(1, min(int(effective_concurrency or 1), task_max_concurrency))
        local_concurrency_limiter = DynamicConcurrencyLimiter(initial_local_concurrency, task_max_concurrency)
        _scheduled_dispatcher_tasks.discard(task_id)
        effective_concurrency = initial_local_concurrency
        concurrency = normalize_task_concurrency(effective_concurrency)
        allocated_startup_workers, startup_reservation_error = await allocate_task_worker_slots(task_id, concurrency)
        if allocated_startup_workers <= 0:
            await _mark_task_failed(
                task_id,
                startup_reservation_error or "本机发送并发容量不足，无法启动任务",
            )
            return
        if allocated_startup_workers < concurrency:
            logger.warning(
                "任务 %s 启动并发因本机容量限制从 %s 降为 %s",
                task_id,
                concurrency,
                allocated_startup_workers,
            )
            concurrency = allocated_startup_workers
            effective_concurrency = allocated_startup_workers
            initial_local_concurrency = allocated_startup_workers
            await local_concurrency_limiter.set_limit(allocated_startup_workers)
        logger.info(
            "任务 %s 本地并发控制已初始化：初始并发=%s，上限=%s，目标速率=%.1f封/秒，探测RTT=%.2f秒，批次=%s",
            task_id,
            initial_local_concurrency,
            task_max_concurrency,
            per_second_rate,
            proxy_rtt_seconds,
            send_http_batch_size,
        )
        prefetch_limit = max(
            CLAIM_PREFETCH_MIN_LIMIT,
            min(
                RECIPIENT_MEMORY_LOAD_MAX_BATCH,
                max(pipeline_plan.prefetch_limit, send_http_batch_size * max(2, effective_concurrency) * 20),
            ),
        )
        steady_state_queue_seconds = 300.0
        prefetch_queue_high_watermark = max(
            PREFETCH_QUEUE_MIN_HIGH_WATERMARK,
            min(
                RECIPIENT_QUEUE_MAXSIZE,
                max(prefetch_limit * max(4, effective_concurrency), int(math.ceil(per_second_rate * steady_state_queue_seconds))),
            ),
        )
        logger.info(
            "任务 %s 发送管线计划：target_rate=%s/s http_batch=%s http_rps=%s rtt=%ss effective_concurrency=%s min_concurrency=%s prefetch_limit=%s prefetch_high_watermark=%s result_chunk=%s local_pid_concurrency=%s",
            task_id,
            pipeline_plan.target_rate,
            pipeline_plan.send_http_batch_size,
            pipeline_plan.http_rps,
            round(pipeline_plan.rtt_seconds, 3),
            pipeline_plan.effective_concurrency,
            pipeline_plan.min_concurrency,
            pipeline_plan.prefetch_limit,
            pipeline_plan.prefetch_high_watermark,
            pipeline_plan.result_enqueue_chunk_size,
            pipeline_plan.local_pid_concurrency,
        )
        logger.info(
            "任务 %s %s并发数为 %s（初始计算值 %s，自适应=%s，RTT=%ss，速率=%s，批量=%s）",
            task_id,
            "自动设置" if auto_concurrency_enabled else "固定",
            concurrency,
            initial_concurrency,
            "开启" if auto_concurrency_enabled else "关闭",
            round(proxy_rtt_seconds, 3),
            per_second_rate,
            send_http_batch_size,
        )
        ensure_send_threadpool_capacity(get_active_workers_total())
        recommended_concurrency, concurrency_warning = resolve_task_concurrency_guidance(
            concurrency,
            per_second_rate,
            send_http_batch_size,
        )
        if recommended_concurrency and per_second_rate > 0:
            threshold = max(1, int(recommended_concurrency * 0.8))
            if concurrency < threshold:
                logger.warning(
                    "任务 %s 当前任务并发数 (%s) 远低于推荐值 (%s)，可能无法达到目标速率。请使用「检测代理延迟」功能获取推荐值。",
                    task_id,
                    concurrency,
                    recommended_concurrency,
                )
        if concurrency_warning:
            logger.warning("任务 %s %s", task_id, concurrency_warning)
        # 正式发送仅使用任务独立的 SyncSendGridClientManager。不要再创建未参与发送的
        # channel_manager AsyncClient，否则其后台保活会额外占用代理连接并污染健康判断。
        total = int(task.total_count or 0)
        logger.debug("任务 %s 发送循环已启动，待发送收件人总数=%s", task_id, total)
        last_channel_proxy_fingerprint = _build_task_channel_proxy_fingerprint(task.channel)
        channel_proxy_reload_pending = False
        httpx_client_swap_lock = asyncio.Lock()
        logger.debug(
            "task %s async send loop channel_id=%s concurrency=%s",
            task_id,
            task.channel_id,
            concurrency,
        )
        http_recipients_per_request = send_http_batch_size
        estimated_http_rps = estimate_http_requests_per_second(per_second_rate, send_http_batch_size)
        logger.info(
            "任务 %s SendGrid SDK 批量模式 recipients_per_request=%s estimated_http_rps=%s rate_limit=%s concurrency=%s",
            task_id,
            http_recipients_per_request,
            estimated_http_rps,
            per_second_rate,
            concurrency,
        )
        try:
            sync_sendgrid_manager = SyncSendGridClientManager(
                task.channel,
                timeout=SEND_EMAIL_HTTP_BUDGET_SECONDS,
                effective_concurrency=min(
                    get_per_task_max_effective_concurrency(),
                    max(effective_concurrency * 2, effective_concurrency + 2),
                ),
            )
            sync_sendgrid_manager.owner_task_id = int(task_id)
            # 监控发送使用独立 SDK Manager 与独立 SenderPool，避免主发送高并发占满队列后
            # 监控邮件持续排队、超时或在任务收尾阶段被取消。
            monitor_sendgrid_manager = SyncSendGridClientManager(
                task.channel,
                timeout=SEND_EMAIL_HTTP_BUDGET_SECONDS,
                effective_concurrency=max(1, min(5, len(await _parse_monitor_emails(task)) or 1)),
            )
            monitor_sendgrid_manager.owner_task_id = monitor_sender_pool_task_id
        except Exception as exc:
            await _mark_task_failed(task_id, f"初始化 SendGrid SDK 客户端失败：{exc}")
            return
        logger.info("任务 %s 批次大小固定为 %s 封/批，运行中不可变", task_id, send_http_batch_size)
        rate_limiter_holder = RateLimiterHolder(
            per_second_rate=per_second_rate,
            limiter=build_rate_limiter(per_second_rate),
            worker_count=max(1, effective_concurrency),
            task_id=task_id,
        )
        monitor_per_second_rate = get_monitor_rate_limit(per_second_rate)
        monitor_rate_limiter_holder = RateLimiterHolder(
            per_second_rate=monitor_per_second_rate,
            limiter=build_rate_limiter(monitor_per_second_rate),
            task_id=task_id,
        )
        for label, holder in (("发送", rate_limiter_holder), ("监控抽样", monitor_rate_limiter_holder)):
            limiter_error = validate_rate_limiter_holder(holder, label)
            if limiter_error:
                await _mark_task_failed(task_id, limiter_error)
                return
        claim_batch_size = max(CLAIM_PREFETCH_MIN_LIMIT, min(RECIPIENT_MEMORY_LOAD_MAX_BATCH, prefetch_limit))
        result_enqueue_chunk_size = int(pipeline_plan.result_enqueue_chunk_size)
        if settings.enable_pid_concurrency_tuning:
            pid_max_concurrency = task_max_concurrency
            _task_pid_controllers[task_id] = create_task_pid_controller(max_concurrency=pid_max_concurrency)
            runtime_for_capacity = _task_runtime_metrics.get(task_id)
            if runtime_for_capacity is not None:
                runtime_for_capacity["capacity_required_http_rps"] = capacity_proof.required_http_rps
                runtime_for_capacity["capacity_required_http_inflight"] = capacity_proof.required_http_inflight
                runtime_for_capacity["capacity_pid_max_concurrency"] = pid_max_concurrency
            _task_queue_aware_states[task_id] = QueueAwareConcurrencyState()
        monitor_interval = int(task.monitor_interval if task.monitor_interval is not None else 100)
        monitor_emails = await _parse_monitor_emails(task)
        has_monitor_sampling = _is_monitor_sampling_enabled(monitor_interval, monitor_emails)
        if monitor_interval <= 0:
            logger.info(
                "task %s monitor sampling disabled because monitor_interval=%s",
                task_id,
                monitor_interval,
            )
        elif not monitor_emails:
            logger.debug(
                "task %s monitor sampling disabled because monitor_emails is empty",
                task_id,
            )
        active_url_pool_id = task.url_pool_id
        if active_url_pool_id:
            url_pool_cache = await start_task_url_pool_cache(task_id, int(active_url_pool_id))
        send_log_id_block_size = max(
            send_http_batch_size * max(10, effective_concurrency * 4),
            min(200_000, max(10_000, int((per_second_rate or send_http_batch_size) * 180))),
        )
        send_log_id_allocator = SendLogIdAllocator(task_id, block_size=send_log_id_block_size)
        try:
            await send_log_id_allocator.warmup(min(send_log_id_block_size, max(send_http_batch_size, total or send_http_batch_size)))
            logger.info(
                "任务 %s SendLog ID 池预热完成 block_size=%s cached=%s",
                task_id,
                send_log_id_block_size,
                send_log_id_allocator.cached_count,
            )
        except Exception as exc:
            await _mark_task_failed(task_id, f"初始化 SendLog ID 池失败：{exc}")
            return
        inflight_emails: set[str] = set()
        _task_inflight_emails[task_id] = inflight_emails
        initial_regular_send_log_count = await _count_regular_send_logs(task_id)
        if initial_regular_send_log_count < 0:
            initial_regular_send_log_count = 0
        recipient_queue_maxsize = max(
            PREFETCH_QUEUE_MIN_HIGH_WATERMARK,
            min(RECIPIENT_QUEUE_MAXSIZE, max(prefetch_queue_high_watermark * 2, int(math.ceil(per_second_rate * 600.0)) if per_second_rate > 0 else RECIPIENT_QUEUE_MAXSIZE)),
        )
        recipient_queue = asyncio.Queue(maxsize=recipient_queue_maxsize)

        def _resize_recipient_queue_capacity(active_workers: int, batch_size: int) -> None:
            nonlocal recipient_queue

            requested_size = max(10_000, int(active_workers) * max(1, int(batch_size)) * 10)
            requested_size = min(RECIPIENT_QUEUE_MAXSIZE, requested_size)
            old_queue = recipient_queue
            old_size = old_queue.maxsize
            if old_size == requested_size:
                return

            # 预取协程和完整性守卫在启动时持有 Queue 对象引用，运行中绝不能替换对象；
            # 否则预取会写入旧队列而 Worker 从新队列读取，造成整批收件人永久滞留。
            new_size = max(requested_size, old_queue.qsize())
            if old_size == new_size:
                return
            old_queue._maxsize = new_size
            if new_size > old_size:
                putters = getattr(old_queue, "_putters", None)
                wakeup_next = getattr(old_queue, "_wakeup_next", None)
                if putters is not None and callable(wakeup_next):
                    wakeup_next(putters)

            hooks = _task_shutdown_hooks.get(task_id)
            if hooks is not None:
                hooks["recipient_queue"] = old_queue
            runtime = _task_runtime_metrics.get(task_id)
            if runtime is not None:
                runtime["recipient_queue"] = old_queue
                runtime["recipient_queue_maxsize"] = new_size
                runtime["recipient_queue_size"] = old_queue.qsize()
            logger.info(
                "任务 %s 收件人队列容量已调整为 %s（原容量 %s）",
                task_id,
                new_size,
                old_size,
            )

        _resize_recipient_queue_capacity(effective_concurrency, send_http_batch_size)
        prefetch_stop_event = asyncio.Event()
        monitor_worker_count = max(1, min(len(monitor_emails) or 1, 5))
        monitor_queue: asyncio.Queue[tuple[Task, Recipient, int, dict[str, str], str] | None] = asyncio.Queue(maxsize=max(monitor_worker_count * 2, 1))
        runtime_started_at = monotonic()
        persisted_success_count, persisted_failed_count = await _get_persisted_regular_result_counts(task_id)
        initial_sent_count = max(int(task.sent_count or 0), persisted_success_count)
        initial_failed_count = max(int(task.failed_count or 0), persisted_failed_count)
        claim_cursor_row = int(getattr(task, "last_claimed_row_number", 0) or 0)
        claim_cursor_id = int(getattr(task, "last_claimed_recipient_id", 0) or 0)
        startup_min_row = 0
        startup_max_row = 0
        try:
            async with RecipientClaimSessionLocal() as cursor_db:
                bounds_result = await cursor_db.execute(
                    select(
                        func.min(Recipient.row_number),
                        func.max(Recipient.row_number),
                    ).where(
                        Recipient.list_id == int(task.recipient_list_id or 0),
                        Recipient.row_number > 0,
                        Recipient.email.is_not(None),
                        func.btrim(Recipient.email) != "",
                        Recipient.is_valid.is_(True),
                        func.coalesce(Recipient.is_unsubscribed, False).is_(False),
                    )
                )
                startup_min_value, startup_max_value = bounds_result.one()
                startup_min_row = int(startup_min_value or 0)
                startup_max_row = int(startup_max_value or 0)
        except Exception as exc:
            logger.warning(
                "任务 %s 启动时读取收件人行号边界失败，继续使用持久游标 cursor=%s error_type=%s",
                task_id,
                claim_cursor_row,
                type(exc).__name__,
            )
        if startup_min_row > 0 and (claim_cursor_row <= 0 or claim_cursor_row < startup_min_row - 1):
            logger.warning(
                "任务 %s 检测到启动游标异常 cursor=%s list_id=%s min_row=%s，已校准为%s",
                task_id,
                claim_cursor_row,
                int(task.recipient_list_id or 0),
                startup_min_row,
                startup_min_row - 1,
            )
            claim_cursor_row = startup_min_row - 1
            task.last_claimed_row_number = claim_cursor_row
            await _persist_claim_cursor_to_db(task_id, claim_cursor_row, force=True, allow_decrease=True)
        claim_slots_wake_event = asyncio.Event()
        monitor_trigger_idle_event = asyncio.Event()
        monitor_trigger_idle_event.set()
        completion_auditor_wake_event = asyncio.Event()
        completion_auditor_finished_event = asyncio.Event()
        initial_monitor_trigger_count = 0
        initial_monitor_success_bucket = 0
        if has_monitor_sampling:
            existing_monitor_milestone = await _get_latest_monitor_sent_milestone(
                task_id,
                monitor_emails,
                monitor_interval,
                initial_sent_count,
            )
            if existing_monitor_milestone > 0:
                initial_monitor_trigger_count = existing_monitor_milestone
            initial_monitor_success_bucket = initial_monitor_trigger_count // max(1, monitor_interval)
            logger.info(
                "任务 %s 监控抽样初始化：success_count=%s failed_count=%s latest_monitor_milestone=%s monitor_interval=%s monitor_email_count=%s",
                task_id,
                initial_sent_count,
                initial_failed_count,
                initial_monitor_trigger_count,
                monitor_interval,
                len(monitor_emails),
            )
        _task_runtime_metrics[task_id] = {
            "task_id": task_id,
            "run_generation": run_generation,
            "trace_id": f"task-{task_id}-run-{run_generation}",
            "channel_id": task_channel_id,
            "started_at": runtime_started_at,
            "last_batch_started_at": runtime_started_at,
            "last_batch_finished_at": runtime_started_at,
            "last_batch_duration_ms": 0,
            "last_batch_size": 0,
            "batch_count": 0,
            "enqueued": 0,
            "processed": 0,
            "success": 0,
            "failed": 0,
            "api_recipient_send_count": 0,
            "initial_regular_send_log_count": initial_regular_send_log_count,
            "active_sends": 0,
            WORKER_LOCAL_BATCH_INFLIGHT_KEY: 0,
            "admitted_workers": int(_task_active_workers.get(task_id, concurrency)),
            "active_http_inflight": 0,
            "active_sends_updated_at": 0.0,
            "outstanding_writes": 0,
            "claim_cursor_row": claim_cursor_row,
            "claim_cursor_id": claim_cursor_id,
            "last_claim_ms": 0.0,
            "last_claim_size": 0,
            "claimed_recipients_total": 0,
            "claim_success_cycles": 0,
            "last_claim_summary_at": runtime_started_at,
            "claim_count": 0,
            "claim_persist_pending_batches": 0,
            "pending_claim_cursor_row": claim_cursor_row,
            "pending_claim_cursor_id": claim_cursor_id,
            "startup_cursor_row": claim_cursor_row,
            "startup_cursor_id": claim_cursor_id,
            "persisted_cursor_row": claim_cursor_row,
            "persisted_cursor_id": claim_cursor_id,
            "last_send_at": runtime_started_at,
            "rate_limit_per_second": per_second_rate,
            "rate_limiter_mode": settings.task_rate_limiter_mode,
            "configured_rate_rps": configured_per_second_rate,
            "effective_target_rate_rps": per_second_rate,
            "channel_capacity_rps": 0.0,
            "channel_active_task_count": channel_active_task_count,
            "channel_fair_share_rps": 0.0,
            "batch_size": max(1, int(task.batch_size or 1)),
            "claim_batch_size": claim_batch_size,
            "result_enqueue_chunk_size": result_enqueue_chunk_size,
            "pipeline_http_rps": pipeline_plan.http_rps,
            "pipeline_local_pid_concurrency": pipeline_plan.local_pid_concurrency,
            "http_recipients_per_request": http_recipients_per_request,
            "send_http_batch_size": send_http_batch_size,
            "concurrency_semaphore": local_concurrency_limiter,
            "local_concurrency_limit": initial_local_concurrency,
            "local_concurrency_max": task_max_concurrency,
            "send_http_batch_enabled": True,
            "http_batch_persist_pending_count": 0,
            "estimated_http_requests_per_second": estimated_http_rps,
            "concurrency": concurrency,
            "effective_concurrency": concurrency,
            "pid_target_concurrency": concurrency,
            "configured_concurrency": configured_concurrency,
            "redis_coordination_mode": "normal",
            "redis_coordination_degraded": 0,
            "min_concurrency": min_concurrency,
            "initial_concurrency": initial_concurrency,
            "required_concurrency_for_rate": required_concurrency_for_rate,
            "auto_concurrency_enabled": auto_concurrency_enabled,
            "auto_concurrency": concurrency,
            "concurrency_reason": "",
            "last_concurrency_adjust_at": 0.0,
            "prefetch_limit": prefetch_limit,
            "base_prefetch_limit": prefetch_limit,
            "effective_prefetch_limit": prefetch_limit,
            "prefetch_queue_high_watermark": prefetch_queue_high_watermark,
            "recipient_queue_maxsize": recipient_queue_maxsize,
            "prefetch_paused_high_watermark": 0,
            "proxy_rtt_seconds": round(proxy_rtt_seconds, 4),
            "recommended_concurrency": int(recommended_concurrency) if recommended_concurrency else 0,
            "concurrency_warning": str(concurrency_warning or ""),
            "initial_sent_count": initial_sent_count,
            "sent_count": initial_sent_count,
            "initial_failed_count": initial_failed_count,
            "total_count": max(0, int(task.total_count or 0)),
            "status": str(task.status or "running"),
            "last_heartbeat": wall_time(),
            "max_concurrency": task_max_concurrency,
            "http_completions_last_1s": 0,
            "slow_http_count": 0,
            "http_timeout_count": 0,
            "monitor_interval": max(1, monitor_interval) if has_monitor_sampling else monitor_interval,
            "last_monitor_trigger_count": initial_monitor_trigger_count,
            "last_monitor_success_bucket": initial_monitor_success_bucket,
            "has_monitor_emails": has_monitor_sampling,
            "monitor_trigger_pending": False,
            "monitor_completion_pending": False,
            "monitor_trigger_idle_event": monitor_trigger_idle_event,
            "monitor_sent": 0,
            "monitor_failed": 0,
            "monitor_email_count": len(monitor_emails),
            "monitor_local_inflight": set(),
            "recipient_queue_size": 0,
            "prefetch_eof": False,
            "prefetch_eof_confirmed": False,
            "prefetch_eof_empty_streak": 0,
            "prefetch_eof_required_empty_streak": max(PREFETCH_EOF_EMPTY_CONFIRM_STREAK, PREFETCH_STALLED_EMPTY_EOF_THRESHOLD),
            "result_queue_size": 0,
            "result_queue_max_observed": 0,
            "result_queue_backpressure_count": 0,
            "writer_flush_count": 0,
            "writer_last_batch_size": 0,
            "writer_last_flush_ms": 0.0,
            "writer_flush_batch_size": get_result_flush_batch_size_default(),
            "writer_flush_interval_seconds": RESULT_FLUSH_INTERVAL_SECONDS_DEFAULT,
            "writer_turbo_active": False,
            "global_concurrency_occupied": get_global_concurrency_occupied(),
            "last_progress_at": runtime_started_at,
            "last_send_progress_at": runtime_started_at,
            "last_send_log_enqueued_at": runtime_started_at,
            "send_progress_watch_processed": persisted_success_count + persisted_failed_count,
            "max_successful_regular_row_number": claim_cursor_row,
            "claim_consecutive_failures": 0,
            "writer_restart_count": 0,
            "claim_slots_wake_event": claim_slots_wake_event,
            "recipient_list_max_row_number": startup_max_row,
            "completion_auditor_wake_event": completion_auditor_wake_event,
            "completion_auditor_finished_event": completion_auditor_finished_event,
            "prefetch_exhausted": False,
            "prefetch_task_exhausted": False,
            "task_finished": False,
            "main_loop_prev_iter_at": runtime_started_at,
            "main_loop_expected_interval": 0.0,
            "event_loop_latency_ms": 0.0,
            "main_loop_work_ms": 0.0,
            "event_loop_tasks_count": 0,
            "httpx_client_reload_count": 0,
            "httpx_inflight": 0,
            "last_build_batch_ms": 0.0,
            "last_http_send_ms": 0.0,
            "avg_http_send_ms": 0.0,
            "http_send_p95_ms": 0.0,
            "channel_http_send_p95_ms": 0.0,
            "http_send_p90_ms": 0.0,
            "channel_http_send_p90_ms": 0.0,
            "batch_efficiency": 0.0,
            "batch_size_optimization_hint": "",
            "suggested_send_http_batch_size": 0,
            "last_logged_batch_size_hint": "",
            "last_batch_size_hint_logged_at": 0.0,
            "http_send_p99_ms": 0.0,
            "result_queue_peak_after_batch": 0,
            "http_send_ms_samples": deque(maxlen=HTTP_SEND_P99_SAMPLE_SIZE),
            "http_429_count": 0,
            "last_http_429_warn_logged_at": 0.0,
            "writer_flush_p99_ms": 0.0,
            "writer_flush_ms_samples": deque(maxlen=WRITER_FLUSH_P99_SAMPLE_SIZE),
            "processed_recent_times": deque(),
            "success_recent_times": deque(),
            "worker_retire_count": 0,
            "worker_retire_lock": asyncio.Lock(),
            "last_dynamic_rtt_probe_at": runtime_started_at,
            "phase_timing_samples": deque(maxlen=1000),
            "prefetch_fetch_events": deque(maxlen=500),
            "prefetch_queue_history": deque(maxlen=120),
            "backpressure_alert": "",
            "prefetch_queue_empty_since": 0.0,
            "write_dropped_total": 0,
            "http_429_recent_events": deque(maxlen=200),
            "http_rps_history": deque(maxlen=60),
            "low_rate_streak_started_at": 0.0,
        }
        runtime = _task_runtime_metrics.get(task_id)
        if runtime is None:
            runtime = {"task_id": task_id, "started_at": runtime_started_at}
            _task_runtime_metrics[task_id] = runtime
            logger.warning(
                "任务 %s 运行时指标注册后读取为空，已重新初始化，任务将继续进入发送循环",
                task_id,
            )
        _sync_runtime_send_proxy_fields(task_id, task.channel)
        if task.template is not None:
            _resolve_task_static_content(task, runtime)
        logger.info(
            "任务 %s 启动发送配置：用户并发=%s，有效并发=%s，初始前馈并发=%s，最小并发=%s，速率=%s 封/秒，HTTP批量=%s，RTT=%ss，本地 PID 并发=%s，预取高水位=%s",
            task_id,
            concurrency,
            effective_concurrency,
            initial_concurrency,
            min_concurrency,
            rate_limiter_holder.per_second_rate,
            send_http_batch_size,
            round(proxy_rtt_seconds, 3),
            _task_runtime_metrics.get(task_id, {}).get("effective_concurrency", effective_concurrency) if _task_runtime_metrics.get(task_id) else effective_concurrency,
            prefetch_queue_high_watermark,
        )
        logger.debug(
            "task %s loop config: concurrency=%s effective_concurrency=%s rate_limit_per_second=%s monitor_rate_limit_per_second=%s retry_count=%s monitor_interval=%s monitor_email_count=%s monitor_emails=%s",
            task_id,
            concurrency,
            effective_concurrency,
            rate_limiter_holder.per_second_rate,
            monitor_rate_limiter_holder.per_second_rate,
            task.retry_count,
            monitor_interval,
            len(monitor_emails),
            monitor_emails,
        )
        writer_batch_size = max(get_result_flush_batch_size_turbo(), result_enqueue_chunk_size)
        writer_queue_maxsize = max(RESULT_QUEUE_MAX_SIZE, writer_batch_size * 100, 500_000)
        send_log_writer = AsyncBatchWriter(
            task_id,
            batch_size=writer_batch_size,
            max_flush_interval=RESULT_FLUSH_INTERVAL_SECONDS_TURBO,
            queue_maxsize=writer_queue_maxsize,
            on_batch_flushed=_make_send_log_batch_flushed_callback(task_id, inflight_emails),
        )
        runtime_for_writer = _task_runtime_metrics.get(task_id)
        if runtime_for_writer is not None:
            runtime_for_writer["writer_batch_size"] = writer_batch_size
            runtime_for_writer["writer_queue_maxsize"] = writer_queue_maxsize
            runtime_for_writer["writer_flush_interval_seconds"] = RESULT_FLUSH_INTERVAL_SECONDS_TURBO
        await send_log_writer.start()
        if not send_log_writer.is_alive():
            message = "SendLog 写入器初始化失败，任务已中断，请检查数据库连接与 send_logs 索引迁移"
            logger.error("任务 %s %s", task_id, message)
            await _mark_task_failed(task_id, message)
            return
        worker_control_queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=16)
        next_worker_index = concurrency
        workers.clear()
        _task_shutdown_hooks[task_id] = {
            "send_log_writer": send_log_writer,
            "recipient_queue": recipient_queue,
            "prefetch_task": None,
            "completion_auditor_task": None,
            "prefetch_stop_event": prefetch_stop_event,
            "monitor_queue": monitor_queue,
            "monitor_workers": monitor_workers,
            "monitor_trigger_lock": asyncio.Lock(),
            "cached_task": task,
            "send_workers": workers,
            "worker_control_queue": worker_control_queue,
            "next_worker_index": next_worker_index,
        }

        async def ensure_result_writer_alive() -> None:
            if send_log_writer is None:
                return
            if send_log_writer.is_alive():
                return
            runtime_ref = _task_runtime_metrics.get(task_id)
            restart_count = int((runtime_ref or {}).get("writer_restart_count", 0)) + 1
            if restart_count > WRITER_MAX_RESTART_COUNT:
                raise RuntimeError("SendLog 异步写入协程多次异常退出")
            if runtime_ref is not None:
                runtime_ref["writer_restart_count"] = restart_count
            logger.error(
                "任务 %s SendLog 写入协程异常退出，正在重启 attempt=%s",
                task_id,
                restart_count,
            )
            await send_log_writer.restart()

        def update_batch_metrics(enqueued_count: int) -> None:
            # last_batch_size 按入队条数累加；收件人供给层已改为启动游标 + 内存队列。
            runtime = _task_runtime_metrics.get(task_id)
            if runtime is None:
                return
            configured_batch_size = max(1, int(runtime.get("batch_size", 1)))
            runtime["enqueued"] = int(runtime.get("enqueued", 0)) + enqueued_count
            runtime["last_batch_size"] = int(runtime.get("last_batch_size", 0)) + enqueued_count
            if int(runtime.get("last_batch_size", 0)) >= configured_batch_size:
                now_value = monotonic()
                started_value = float(runtime.get("last_batch_started_at", now_value))
                runtime["batch_count"] = int(runtime.get("batch_count", 0)) + 1
                runtime["last_batch_duration_ms"] = max(0, int((now_value - started_value) * 1000))
                runtime["last_batch_finished_at"] = now_value
                runtime["last_batch_started_at"] = now_value
                runtime["last_batch_size"] = 0

        async def spawn_send_worker() -> bool:
            nonlocal next_worker_index
            hooks_ref = _task_shutdown_hooks.get(task_id) or {}
            worker_list = hooks_ref.get("send_workers")
            if not isinstance(worker_list, list):
                return False
            worker_index = next_worker_index
            next_worker_index += 1
            hooks_ref["next_worker_index"] = next_worker_index
            startup_future = asyncio.get_running_loop().create_future()
            worker_task = asyncio.create_task(
                send_worker(worker_index, startup_future),
                name=f"task-send-worker-{task_id}-{worker_index}",
            )
            worker_list.append(worker_task)
            try:
                await asyncio.wait_for(startup_future, timeout=10.0)
            except Exception as exc:
                logger.warning(
                    "任务 %s 动态发送 Worker 启动失败 index=%s error_type=%s error=%s",
                    task_id,
                    worker_index,
                    type(exc).__name__,
                    exc,
                )
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
                try:
                    worker_list.remove(worker_task)
                except ValueError:
                    pass
                return False
            live_count = count_live_worker_tasks(worker_list)
            runtime_ref = _task_runtime_metrics.get(task_id)
            if runtime_ref is not None:
                runtime_ref["effective_concurrency"] = live_count
                runtime_ref["admitted_workers"] = int(_task_active_workers.get(task_id, live_count))
            if sync_sendgrid_manager is not None:
                sync_sendgrid_manager.effective_concurrency = max(
                    live_count,
                    int(getattr(sync_sendgrid_manager, "effective_concurrency", 0) or 0),
                )
            sender_pool = get_sender_pool(task_id, workers=live_count)
            if live_count > int(sender_pool.aggregate_stats().get("workers", 0) or 0):
                sender_pool.resize(live_count)
            return True

        async def worker_scale_supervisor() -> None:
            while True:
                signal = await worker_control_queue.get()
                try:
                    if signal is None:
                        return
                    spawn_count = max(0, int(signal))
                    spawned = 0
                    for _ in range(spawn_count):
                        try:
                            spawned_ok = await spawn_send_worker()
                        except Exception as exc:
                            logger.warning(
                                "任务 %s 本地 Worker 扩容失败 remaining=%s error_type=%s error=%s",
                                task_id,
                                spawn_count - spawned,
                                type(exc).__name__,
                                exc,
                            )
                            break
                        if not spawned_ok:
                            logger.warning("任务 %s 本地 Worker 启动失败，待启动数量=%s", task_id, spawn_count - spawned)
                            break
                        spawned += 1
                finally:
                    worker_control_queue.task_done()

        async def request_worker_scale_up(delta: int) -> int:
            normalized_delta = max(0, int(delta))
            if normalized_delta <= 0:
                return 0
            runtime_ref = _task_runtime_metrics.get(task_id) or {}
            limiter = runtime_ref.get("concurrency_semaphore")
            if not isinstance(limiter, DynamicConcurrencyLimiter):
                logger.error("任务 %s 本地并发限制器不存在，无法扩容", task_id)
                return 0
            current_limit = limiter.limit
            target_limit = await limiter.set_limit(current_limit + normalized_delta)
            actual_delta = max(0, target_limit - current_limit)
            runtime_ref["local_concurrency_limit"] = target_limit
            if actual_delta > 0:
                await worker_control_queue.put(actual_delta)
            return actual_delta

        async def request_worker_scale_down(delta: int) -> int:
            normalized_delta = max(0, int(delta))
            if normalized_delta <= 0:
                return 0
            runtime_ref = _task_runtime_metrics.get(task_id)
            if runtime_ref is None:
                return 0
            retire_lock = runtime_ref.get("worker_retire_lock")
            if not isinstance(retire_lock, asyncio.Lock):
                return 0
            limiter = runtime_ref.get("concurrency_semaphore")
            if not isinstance(limiter, DynamicConcurrencyLimiter):
                logger.error("任务 %s 本地并发限制器不存在，无法缩容", task_id)
                return 0
            current_limit = limiter.limit
            target_limit = await limiter.set_limit(current_limit - normalized_delta)
            actual_delta = max(0, current_limit - target_limit)
            async with retire_lock:
                runtime_ref["worker_retire_count"] = int(runtime_ref.get("worker_retire_count", 0)) + actual_delta
                runtime_ref["local_concurrency_limit"] = target_limit
            return actual_delta

        async def apply_auto_tune_scaling(new_target: int, reason: str, current_live: int) -> None:
            runtime_ref = _task_runtime_metrics.get(task_id)
            if runtime_ref is None:
                return
            runtime_rate_limit = float(runtime_ref.get("rate_limit_per_second", per_second_rate) or per_second_rate)
            runtime_batch_size = int(runtime_ref.get("send_http_batch_size", send_http_batch_size) or send_http_batch_size)
            runtime_rtt_seconds = resolve_runtime_effective_http_rtt_seconds(runtime_ref, proxy_rtt_seconds)
            guaranteed_floor = calculate_task_capacity_requirement(
                rate_limit=runtime_rate_limit,
                send_http_batch_size=runtime_batch_size,
                rtt_seconds=runtime_rtt_seconds,
            )
            configured_max = max(1, int(runtime_ref.get("max_concurrency", local_concurrency_limiter.max_limit) or local_concurrency_limiter.max_limit))
            bounded_target = min(
                configured_max,
                max(guaranteed_floor, int(new_target)),
            )
            if bounded_target > current_live:
                scale_step = max(WORKER_SCALE_MAX_STEP, math.ceil(current_live * 0.50))
                bounded_target = min(bounded_target, current_live + scale_step)
            if bounded_target < guaranteed_floor:
                runtime_ref["concurrency_capacity_block_reason"] = (
                    f"目标速率所需并发下限 {guaranteed_floor} 超过当前硬上限 {bounded_target}"
                )
                logger.error(
                    "任务 %s 并发容量不足：目标速率所需下限=%s，但当前硬上限=%s",
                    task_id,
                    guaranteed_floor,
                    bounded_target,
                )
            if bounded_target > current_live:
                reserved_target, reservation_error = await ensure_task_concurrency_reservation(task_id, bounded_target)
                if reservation_error is not None or reserved_target <= current_live:
                    logger.warning(
                        "任务 %s PID 扩容资源预留失败 current=%s target=%s error=%s",
                        task_id,
                        current_live,
                        bounded_target,
                        reservation_error or "本地并发容量不足",
                    )
                    return
                allowed_target = min(bounded_target, reserved_target)
                scaled = await request_worker_scale_up(allowed_target - current_live)
                if scaled <= 0:
                    await _release_reserved_worker_slots_and_sync(task_id, max(0, reserved_target - current_live))
                    return
                actual_target = current_live + scaled
            elif bounded_target < current_live:
                await request_worker_scale_down(current_live - bounded_target)
                actual_target = bounded_target
            else:
                return
            adjust_at = monotonic()
            runtime_ref["last_concurrency_adjust_at"] = adjust_at
            runtime_ref["concurrency_reason"] = reason
            runtime_ref["auto_concurrency"] = actual_target
            logger.info(
                "任务 %s 自适应调整并发数：%s -> %s，原因：%s",
                task_id,
                current_live,
                actual_target,
                reason,
            )
            runtime_ref["effective_concurrency"] = actual_target
            runtime_ref["pid_target_concurrency"] = actual_target
            hooks_after_scale = _task_shutdown_hooks.get(task_id) or {}
            live_workers_after_scale = hooks_after_scale.get("send_workers")
            logger.info(
                "任务 %s PID 并发目标已应用 current=%s target=%s active_workers=%s degraded=%s",
                task_id,
                current_live,
                actual_target,
                count_live_worker_tasks(live_workers_after_scale if isinstance(live_workers_after_scale, list) else None),
                bool(runtime_ref.get("redis_worker_count_degraded", 0)),
            )
            _resize_recipient_queue_capacity(actual_target, int(runtime_ref.get("send_http_batch_size", send_http_batch_size) or send_http_batch_size))
            runtime_ref["recipient_queue_maxsize"] = recipient_queue.maxsize

        async def auto_tune_concurrency_loop() -> None:
            try:
                while True:
                    await asyncio.sleep(AUTO_TUNE_INTERVAL_SECONDS)
                    if task_id in _task_shutdown_requested:
                        return
                    runtime_ref = _task_runtime_metrics.get(task_id)
                    if runtime_ref is None:
                        return
                    hooks_ref = _task_shutdown_hooks.get(task_id) or {}
                    worker_list = hooks_ref.get("send_workers")
                    if not isinstance(worker_list, list):
                        return
                    live_worker_count = count_live_worker_tasks(worker_list)
                    current_live = max(live_worker_count, local_concurrency_limiter.limit)
                    runtime_ref["active_workers"] = live_worker_count
                    runtime_ref["effective_concurrency"] = current_live
                    rate_limit_value = max(
                        0.0,
                        float(runtime_ref.get("rate_limit_per_second", per_second_rate) or per_second_rate),
                    )
                    tune_result = None
                    pid_controller = _task_pid_controllers.get(task_id)
                    queue_state = _task_queue_aware_states.setdefault(task_id, QueueAwareConcurrencyState())
                    if settings.enable_pid_concurrency_tuning and pid_controller is not None:
                        _, avg_rate = _refresh_runtime_rate_metrics(runtime_ref, SUCCESS_RATE_WINDOW_SECONDS)
                        recent_429_count = count_recent_429_events(runtime_ref, AUTO_TUNE_WINDOW_SECONDS)
                        pid_raw = pid_controller.evaluate(
                            target_rate=rate_limit_value,
                            actual_rate=avg_rate,
                            current_concurrency=current_live,
                            recent_429_count=recent_429_count,
                            task_id=task_id,
                        )
                        if pid_raw is not None and pid_raw[0] > current_live:
                            pid_gate_failures: list[str] = []
                            hooks_snapshot = _task_shutdown_hooks.get(task_id) or {}
                            queue_snapshot = hooks_snapshot.get("recipient_queue")
                            writer_snapshot = hooks_snapshot.get("send_log_writer")
                            active_task_count = max(1, len(_task_runtime_metrics))
                            relaxed_ratio = 1.2 if active_task_count <= 2 else 1.0
                            gate_batch_size = max(1, int(runtime_ref.get("send_http_batch_size", send_http_batch_size) or send_http_batch_size))
                            required_depth = max(gate_batch_size * max(1, current_live) * 2, 1000)
                            required_depth = max(1, int(required_depth / relaxed_ratio))
                            queue_depth = queue_snapshot.qsize() if isinstance(queue_snapshot, asyncio.Queue) else 0
                            claim_p95 = float(runtime_ref.get("claim_query_p95_ms", runtime_ref.get("db_claim_p95_ms", 0)) or 0)
                            prefetch_scale_blocked = (
                                claim_p95 >= PID_PREFETCH_SCALE_BLOCK_P95_MS
                                and queue_depth < required_depth * PID_PREFETCH_SCALE_REQUIRED_QUEUE_MULTIPLIER
                            )
                            limiter_reserved_ahead = float(runtime_ref.get("rate_limiter_reserved_ahead_seconds", 0) or 0)
                            rolling_sdk_rate = float(runtime_ref.get("sdk_success_rate_10s", runtime_ref.get("success_rate", 0)) or 0)
                            target_rate = max(0.0, float(runtime_ref.get("rate_limit_per_second", 0) or 0))
                            token_reservation_saturated = (
                                limiter_reserved_ahead >= 5.0
                                and (target_rate <= 0 or rolling_sdk_rate >= target_rate * 0.9)
                            )
                            if token_reservation_saturated:
                                pid_gate_failures.append(
                                    f"限速器已有 {limiter_reserved_ahead:.1f} 秒发送预约且 SDK 速率已达目标90%，继续扩容只会增加等待协程"
                                )
                            if prefetch_scale_blocked:
                                pid_gate_failures.append(
                                    f"预取查询 P95={claim_p95:.0f}ms 且队列深度={queue_depth}低于扩容安全水位={required_depth * PID_PREFETCH_SCALE_REQUIRED_QUEUE_MULTIPLIER}"
                                )
                            if is_confirmed_prefetch_eof(runtime_ref):
                                runtime_ref["pid_empty_queue_cycles"] = 0
                                runtime_ref["pid_upscale_block_reasons"] = "任务已确认 EOF，停止 PID 调节"
                                await asyncio.sleep(PID_INTERVAL_SECONDS)
                                continue
                            if queue_depth <= 0:
                                empty_cycles = int(runtime_ref.get("pid_empty_queue_cycles", 0) or 0) + 1
                                runtime_ref["pid_empty_queue_cycles"] = empty_cycles
                            else:
                                empty_cycles = 0
                                runtime_ref["pid_empty_queue_cycles"] = 0
                            if empty_cycles >= 3:
                                pid_gate_failures.append("收件人队列持续为空，等待预取恢复")
                                last_empty_log_at = float(runtime_ref.get("pid_empty_queue_last_log_at", 0) or 0)
                                if monotonic() - last_empty_log_at >= 30.0:
                                    logger.warning("任务 %s 收件人队列持续为空，暂停 PID 上调，等待预取恢复", task_id)
                                    runtime_ref["pid_empty_queue_last_log_at"] = monotonic()
                            # 预取查询慢时不允许用增加 Worker 的方式掩盖供给瓶颈；只有队列充足后才检查较宽松的低水位门禁。
                            if queue_depth < required_depth and claim_p95 > 5000 * relaxed_ratio:
                                pid_gate_failures.append(f"低水位下收件人预取查询 P95={claim_p95:.0f}ms")
                            if queue_depth < required_depth and int(runtime_ref.get("claim_consecutive_timeouts", 0) or 0) > 4:
                                pid_gate_failures.append(f"低水位下收件人预取连续超时={int(runtime_ref.get('claim_consecutive_timeouts', 0))} 次")
                            sender_wait_p95 = float(
                                runtime_ref.get(
                                    "sender_pool_queue_wait_ms_p95",
                                    runtime_ref.get("sender_queue_wait_p95_ms", 0),
                                )
                                or 0
                            )
                            if sender_wait_p95 > 1000 * relaxed_ratio:
                                runtime_ref["sender_pool_capacity_pressure"] = 1
                                logger.warning(
                                    "任务 %s SenderPool 排队偏高 P95=%.0fms，将先扩充执行池并继续由 PID 提升并发",
                                    task_id,
                                    sender_wait_p95,
                                )
                            if isinstance(writer_snapshot, AsyncBatchWriter) and writer_snapshot.queue_size > 30_000:
                                pid_gate_failures.append(f"日志写入队列深度={writer_snapshot.queue_size}")
                            if recent_429_count > 0:
                                pid_gate_failures.append("近期存在 SendGrid 限流")
                            if pid_gate_failures:
                                target_gap_ratio = (rate_limit_value - avg_rate) / max(rate_limit_value, 1.0)
                                delivery_unknown_active = int(runtime_ref.get("delivery_unknown_active", 0) or 0)
                                transport_degraded_until = float(runtime_ref.get("transport_degraded_until", 0) or 0)
                                transport_degraded = delivery_unknown_active > 0 or monotonic() < transport_degraded_until
                                if transport_degraded:
                                    pid_gate_failures.append(
                                        f"发送传输异常（投递未知={delivery_unknown_active}，冷却剩余={max(0, int(transport_degraded_until - monotonic()))}秒）"
                                    )
                                allow_degraded_upscale = (
                                    bool(getattr(settings, "pid_allow_degraded_upscale", True))
                                    and target_gap_ratio >= 0.20
                                    and recent_429_count <= 0
                                    and not transport_degraded
                                    and not prefetch_scale_blocked
                                    and not token_reservation_saturated
                                )
                                if allow_degraded_upscale and empty_cycles < 3 and queue_depth > 0 and pid_raw is not None and pid_raw[0] > current_live:
                                    degraded_step = max(1, min(16, int(getattr(settings, "pid_degraded_upscale_step", 2) or 2)))
                                    degraded_target = min(pid_raw[0], current_live + degraded_step)
                                    logger.warning(
                                        "PID 门禁存在但实际速率明显不足，允许实验性小步上调 task_id=%s current=%s target=%s 原因=%s",
                                        task_id,
                                        current_live,
                                        degraded_target,
                                        "、".join(pid_gate_failures),
                                    )
                                    pid_raw = (degraded_target, f"实际速率低于目标80%，门禁降级为小步试探：{degraded_target}")
                                    runtime_ref["pid_upscale_block_reasons"] = "、".join(pid_gate_failures)
                                else:
                                    logger.warning(
                                        "PID 因条件不满足而暂停上调 task_id=%s current=%s proposed=%s 原因=%s",
                                        task_id,
                                        current_live,
                                        pid_raw[0] if pid_raw is not None else current_live,
                                        "、".join(pid_gate_failures),
                                    )
                                    runtime_ref["pid_upscale_block_reasons"] = "、".join(pid_gate_failures)
                                    pid_raw = None
                        hooks_ref = _task_shutdown_hooks.get(task_id) or {}
                        recipient_queue_ref = hooks_ref.get("recipient_queue")
                        writer_ref = hooks_ref.get("send_log_writer")
                        prefetch_depth = int(recipient_queue_ref.qsize()) if isinstance(recipient_queue_ref, asyncio.Queue) else 0
                        write_depth = int(writer_ref.queue_size) if isinstance(writer_ref, AsyncBatchWriter) else 0
                        base_prefetch = int(runtime_ref.get("base_prefetch_limit", runtime_ref.get("prefetch_limit", 0)) or 0)
                        sender_stats = get_sender_pool(task_id, workers=local_concurrency_limiter.max_limit).aggregate_stats()
                        tune_result, queue_state, effective_prefetch_limit = refine_pid_concurrency_target(
                            pid_result=pid_raw,
                            current_concurrency=current_live,
                            target_rate=rate_limit_value,
                            actual_rate=avg_rate,
                            prefetch_queue_depth=prefetch_depth,
                            write_queue_depth=write_depth,
                            queue_state=queue_state,
                            base_prefetch_limit=base_prefetch,
                            send_http_batch_size=gate_batch_size,
                            sender_queue_wait_p95_ms=float(
                                runtime_ref.get(
                                    "sender_pool_queue_wait_ms_p95",
                                    runtime_ref.get("sender_queue_wait_p95_ms", 0),
                                )
                                or 0
                            ),
                            db_claim_p95_ms=float(runtime_ref.get("claim_query_p95_ms", runtime_ref.get("db_claim_p95_ms", runtime_ref.get("recipient_load_p95_ms", 0))) or 0),
                            local_concurrency_wait_p95_ms=float(runtime_ref.get("local_concurrency_wait_p95_ms", 0) or 0),
                            http_p95_ms=max(
                                float(runtime_ref.get("channel_http_send_p95_ms", 0) or 0),
                                float(runtime_ref.get("http_send_p95_ms", 0) or 0),
                            ),
                            rate_limiter_reserved_ahead_seconds=float(runtime_ref.get("rate_limiter_reserved_ahead_seconds", 0) or 0),
                            sender_alive_workers=int(sender_stats.get("alive_workers", 0) or 0),
                            sender_workers=int(sender_stats.get("workers", 0) or 0),
                            task_id=task_id,
                            enable_queue_aware=settings.enable_queue_aware_concurrency,
                        )
                        _task_queue_aware_states[task_id] = queue_state
                        runtime_ref["effective_prefetch_limit"] = effective_prefetch_limit
                        runtime_ref["prefetch_limit_boosted"] = int(queue_state.prefetch_limit_boosted)
                        if tune_result is not None:
                            runtime_ref["concurrency_reason"] = tune_result[1]
                    else:
                        tune_result = evaluate_auto_tune_concurrency(
                            runtime_ref,
                            live_workers=current_live,
                            rate_limit=rate_limit_value,
                            per_task_max=int(
                                runtime_ref.get("max_concurrency", local_concurrency_limiter.max_limit)
                                or local_concurrency_limiter.max_limit
                            ),
                            last_adjust_at=float(runtime_ref.get("last_concurrency_adjust_at", 0) or 0),
                        )
                    if tune_result is None:
                        runtime_ref["pid_last_no_scale_reason"] = str(runtime_ref.get("pid_upscale_block_reasons") or "PID 当前未提出并发调整")
                        continue
                    new_target, reason = tune_result
                    runtime_ref["pid_proposed_concurrency"] = int(new_target)
                    runtime_ref["pid_proposed_reason"] = str(reason)
                    await apply_auto_tune_scaling(new_target, reason, current_live)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("任务 %s 自适应并发调度协程异常", task_id)

        async def backpressure_monitor_loop() -> None:
            state: BackpressureState | None = None
            try:
                while True:
                    await asyncio.sleep(BACKPRESSURE_CHECK_INTERVAL_SECONDS)
                    if task_id in _task_shutdown_requested:
                        return
                    runtime_ref = _task_runtime_metrics.get(task_id)
                    hooks_ref = _task_shutdown_hooks.get(task_id) or {}
                    writer = hooks_ref.get("send_log_writer")
                    queue_ref = hooks_ref.get("recipient_queue")
                    if runtime_ref is None:
                        return
                    prefetch_depth = int(queue_ref.qsize()) if isinstance(queue_ref, asyncio.Queue) else 0
                    write_depth = int(writer.queue_size) if isinstance(writer, AsyncBatchWriter) else 0
                    history = runtime_ref.get("prefetch_queue_history")
                    if isinstance(history, deque):
                        history.append({"ts": monotonic(), "depth": prefetch_depth})
                    state = evaluate_backpressure(
                        prefetch_depth=prefetch_depth,
                        write_depth=write_depth,
                        base_prefetch_batch=prefetch_limit,
                        previous=state,
                    )
                    runtime_ref["backpressure_alert"] = state.alert_message
                    if isinstance(writer, AsyncBatchWriter):
                        writer.set_write_paused(state.write_paused)
                    if state.alert_message:
                        _, current_rate = _refresh_runtime_rate_metrics(runtime_ref)
                        record_task_performance_alert(
                            task_id,
                            reason=state.alert_message,
                            actual_rate=current_rate,
                            configured_rate=float(runtime_ref.get("rate_limit_per_second", per_second_rate) or per_second_rate),
                        )
                    cached_task = hooks_ref.get("cached_task")
                    if isinstance(cached_task, Task):
                        await check_send_rate_performance_alert(task_id, cached_task, runtime_ref)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("任务 %s 背压监控协程异常", task_id)

        async def apply_dynamic_worker_scaling(
            *,
            channel_task: Task,
            worker_tasks: list[asyncio.Task],
            previous_rtt_seconds: float,
            new_rtt_seconds: float,
        ) -> None:
            runtime_ref = _task_runtime_metrics.get(task_id)
            if runtime_ref is None or channel_task is None:
                return
            configured_rate = max(0.0, min(get_effective_second_rate_limit(channel_task), 100000.0))
            per_second_rate = configured_rate
            runtime_ref["configured_rate_rps"] = configured_rate
            runtime_ref["rate_limit_per_second"] = per_second_rate
            runtime_ref["effective_target_rate_rps"] = per_second_rate
            runtime_ref["channel_active_task_count"] = 1
            runtime_ref["channel_fair_share_rps"] = 0.0
            runtime_ref["channel_capacity_rps"] = 0.0
            send_http_batch_size = get_send_http_batch_size(channel_task)
            min_con = cap_task_effective_concurrency(
                calculate_min_concurrency(per_second_rate, send_http_batch_size, new_rtt_seconds)
            )
            runtime_ref["min_concurrency"] = min_con
            runtime_ref["proxy_rtt_seconds"] = round(float(new_rtt_seconds), 4)
            configured_cap = int(
                runtime_ref.get("max_concurrency", local_concurrency_limiter.max_limit)
                or local_concurrency_limiter.max_limit
            )
            current_live = count_live_worker_tasks(worker_tasks)
            new_target = calculate_dynamic_worker_target(current_live, min_con, configured_cap)
            if new_target is None:
                return
            if bool(runtime_ref.get("auto_concurrency_enabled", False)) and new_target < current_live:
                runtime_ref["dynamic_rtt_scale_down_skipped"] = int(runtime_ref.get("dynamic_rtt_scale_down_skipped", 0) or 0) + 1
                runtime_ref["concurrency_reason"] = "自动并发模式下跳过 RTT 探测触发的缩容，由 PID 根据实际吞吐和 429 统一调节"
                return
            if new_target > current_live:
                delta = new_target - current_live
                reserved_target, reservation_error = await ensure_task_concurrency_reservation(task_id, new_target)
                if reservation_error is not None or reserved_target <= current_live:
                    logger.warning(
                        "任务 %s RTT 前馈扩容资源预留失败 current=%s target=%s error=%s",
                        task_id,
                        current_live,
                        new_target,
                        reservation_error or "本地并发容量不足",
                    )
                    return
                scaled = await request_worker_scale_up(min(delta, reserved_target - current_live))
                if scaled <= 0:
                    return
                logger.info(
                    "代理延迟变更，调整并发数：%s -> %s",
                    current_live,
                    current_live + scaled,
                )
            else:
                delta = current_live - new_target
                await request_worker_scale_down(delta)
                logger.info(
                    "代理延迟变更，调整并发数：%s -> %s",
                    current_live,
                    new_target,
                )
            runtime_ref["effective_concurrency"] = count_live_worker_tasks(worker_tasks)
            runtime_ref["admitted_workers"] = int(_task_active_workers.get(task_id, runtime_ref["effective_concurrency"]))
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "task %s dynamic worker scaling rtt=%s->%s min_con=%s target=%s live=%s",
                    task_id,
                    round(previous_rtt_seconds, 4),
                    round(new_rtt_seconds, 4),
                    min_con,
                    new_target,
                    runtime_ref["effective_concurrency"],
                )

        def _take_available_recipients_for_batch(
            batch_recipients: list[Recipient],
            batch_size: int,
        ) -> bool:
            """Drain immediately available recipients so a backlog never waits for batch-fill timeout."""
            saw_eof = False
            while len(batch_recipients) < batch_size:
                try:
                    queued_item = recipient_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                recipient_queue.task_done()
                if queued_item is None:
                    saw_eof = True
                    if batch_recipients:
                        # 当前 Worker 已拿到末尾部分批次时，必须把哨兵放回队尾；否则发送完
                        # 该批次后会再次阻塞在 queue.get()，主循环也会永久等待 Worker 退出。
                        recipient_queue.put_nowait(None)
                        break
                    continue
                batch_recipients.append(queued_item)
            return saw_eof

        async def send_worker(worker_index: int, startup_future: asyncio.Future[None]) -> None:
            try:
                if not startup_future.done():
                    startup_future.set_result(None)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "task %s 发送 Worker 已启动 worker_index=%s channel_id=%s",
                        task_id,
                        worker_index,
                        task.channel_id,
                    )
                while True:
                    if await _try_claim_worker_retire(task_id):
                        # 仅允许空闲且不是最后一个的 Worker 退出。预取的瞬时空窗不能让所有
                        # Worker 消失，否则后续补进队列的收件人无人消费，任务可能错误收尾或卡死。
                        live_workers_before_retire = count_live_worker_tasks(workers)
                        can_retire = recipient_queue.empty() and (
                            live_workers_before_retire > 1
                            or is_confirmed_prefetch_eof(_task_runtime_metrics.get(task_id))
                        )
                        if can_retire:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(
                                    "task %s 发送 Worker %s 收到缩容信号并在空闲时退出",
                                    task_id,
                                    worker_index,
                                )
                            return
                        runtime_retire = _task_runtime_metrics.get(task_id)
                        if runtime_retire is not None:
                            retire_lock = runtime_retire.get("worker_retire_lock")
                            if isinstance(retire_lock, asyncio.Lock):
                                async with retire_lock:
                                    runtime_retire["worker_retire_count"] = int(
                                        runtime_retire.get("worker_retire_count", 0) or 0
                                    ) + 1
                            runtime_retire["worker_retire_deferred_for_delivery"] = int(
                                runtime_retire.get("worker_retire_deferred_for_delivery", 0) or 0
                            ) + 1
                        logger.info(
                            "任务 %s 发送 Worker %s 收到缩容信号，但队列非空或仍需保留最后一个 Worker，延后退出",
                            task_id,
                            worker_index,
                        )
                    batch_recipients: list[Recipient] = []
                    hooks_ref = _task_shutdown_hooks.get(task_id) or {}
                    cached_task_snapshot = hooks_ref.get("cached_task")
                    batch_http_size = send_http_batch_size
                    queue_wait_started_at = monotonic()
                    saw_eof_sentinel = _take_available_recipients_for_batch(batch_recipients, batch_http_size)
                    batch_fill_deadline = 0.0
                    while len(batch_recipients) < batch_http_size and not saw_eof_sentinel:
                        current_status = str(_task_status_cache.get(task_id) or "").strip().lower()
                        if current_status in {"paused", "stopped", "failed", "finished", "aborted"} or task_id in _task_shutdown_requested:
                            if batch_recipients:
                                _rollback_unsent_recipients_for_resume(
                                    _task_runtime_metrics.get(task_id),
                                    task_id,
                                    batch_recipients,
                                    inflight_emails,
                                    reason=f"发送 Worker {worker_index} 检测到任务状态={current_status or 'shutdown'}",
                                )
                            return
                        try:
                            if batch_recipients:
                                if batch_fill_deadline <= 0:
                                    batch_fill_deadline = monotonic() + SEND_WORKER_BATCH_FILL_MAX_WAIT_SECONDS
                                timeout_seconds = max(0.0, batch_fill_deadline - monotonic())
                                if timeout_seconds <= 0:
                                    break
                                item = await asyncio.wait_for(recipient_queue.get(), timeout=timeout_seconds)
                            else:
                                item = await recipient_queue.get()
                        except asyncio.TimeoutError:
                            if batch_recipients:
                                break
                            await asyncio.sleep(min(0.01, SEND_WORKER_BATCH_FILL_MAX_WAIT_SECONDS))
                            continue
                        if item is None:
                            recipient_queue.task_done()
                            raw_queue = getattr(recipient_queue, "_queue", None)
                            has_real_recipient_after_sentinel = bool(
                                raw_queue is not None and any(queued_item is not None for queued_item in raw_queue)
                            )
                            if has_real_recipient_after_sentinel:
                                logger.warning(
                                    "task %s 发送 Worker %s 收到结束标记但队列仍有真实收件人，将继续消费",
                                    task_id,
                                    worker_index,
                                )
                                continue
                            if not batch_recipients:
                                if logger.isEnabledFor(logging.DEBUG):
                                    logger.debug("task %s 发送 Worker %s 收到结束标记并退出", task_id, worker_index)
                                return
                            # 当前 Worker 会先发送已取得的末尾部分批次；把哨兵归还队尾，保证
                            # 本 Worker 完成批次后及其他仍存活 Worker 都能收到退出信号。
                            recipient_queue.put_nowait(None)
                            saw_eof_sentinel = True
                            break
                        batch_recipients.append(item)
                        recipient_queue.task_done()
                        saw_eof_sentinel = _take_available_recipients_for_batch(batch_recipients, batch_http_size)

                    if not batch_recipients:
                        if saw_eof_sentinel:
                            return
                        continue
                    logger.info(
                        "任务 %s 本批实际发送 %s 封，配置批次大小 %s",
                        task_id,
                        len(batch_recipients),
                        batch_http_size,
                    )

                    local_batch_tracked = False
                    runtime_local_batch = _task_runtime_metrics.get(task_id)
                    _increment_worker_local_batch_inflight(runtime_local_batch, len(batch_recipients))
                    local_batch_tracked = True
                    current_status = str(_task_status_cache.get(task_id) or "").strip().lower()
                    if current_status in {"paused", "stopped", "failed", "finished", "aborted"} or task_id in _task_shutdown_requested:
                        _rollback_unsent_recipients_for_resume(
                            _task_runtime_metrics.get(task_id),
                            task_id,
                            batch_recipients,
                            inflight_emails,
                            reason=f"发送 Worker {worker_index} 发起 SDK 前检测到任务状态={current_status or 'shutdown'}",
                        )
                        if local_batch_tracked:
                            _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                            local_batch_tracked = False
                        return

                    runtime_ref = _task_runtime_metrics.get(task_id)
                    if runtime_ref is not None:
                        maybe_log_send_pipeline_perf(
                            task_id,
                            runtime_ref,
                            recipient_queue=recipient_queue,
                            send_log_writer=send_log_writer,
                        )

                    task_snapshot = cached_task_snapshot if isinstance(cached_task_snapshot, Task) else await _load_task(task_id)
                    if task_snapshot is None:
                        logger.error("任务 %s 发送 Worker 无法加载任务快照，跳过 %s 个收件人", task_id, len(batch_recipients))
                        _rollback_unsent_recipients_for_resume(
                            _task_runtime_metrics.get(task_id),
                            task_id,
                            batch_recipients,
                            inflight_emails,
                            reason=f"发送 Worker {worker_index} 无法加载任务快照",
                        )
                        if local_batch_tracked:
                            _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                            local_batch_tracked = False
                        continue
                    snapshot_status = str(getattr(task_snapshot, "status", "") or "").strip().lower()
                    if snapshot_status != "running":
                        _rollback_unsent_recipients_for_resume(
                            _task_runtime_metrics.get(task_id),
                            task_id,
                            batch_recipients,
                            inflight_emails,
                            reason=f"发送 Worker {worker_index} 加载任务快照后发现状态={snapshot_status or 'unknown'}",
                        )
                        if local_batch_tracked:
                            _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                            local_batch_tracked = False
                        return

                    _update_claim_cursor_from_recipients(task_id, batch_recipients)
                    update_batch_metrics(len(batch_recipients))
                    runtime_ref = _task_runtime_metrics.get(task_id)
                    if runtime_ref is not None:
                        queue_wait_ms = round((monotonic() - queue_wait_started_at) * 1000, 2)
                        runtime_ref["recipient_queue_size"] = int(recipient_queue.qsize())
                        runtime_ref["last_recipient_queue_wait_ms"] = queue_wait_ms
                        if queue_wait_ms >= 500 and int(recipient_queue.qsize()) == 0:
                            runtime_ref["recipient_queue_starvation_count"] = int(runtime_ref.get("recipient_queue_starvation_count", 0) or 0) + 1
                            logger.warning(
                                "任务 %s 发送 Worker %s 等待首个收件人偏长 wait_ms=%s batch=%s queue_size=0 prefetch_eof=%s",
                                task_id,
                                worker_index,
                                queue_wait_ms,
                                len(batch_recipients),
                                bool(runtime_ref.get("prefetch_eof", False)),
                            )
                    live_worker_count = int(
                        (runtime_ref or {}).get(
                            "effective_concurrency",
                            count_live_worker_tasks(workers),
                        )
                        or concurrency
                    )

                    try:
                        current_status = str(_task_status_cache.get(task_id) or "").strip().lower()
                        if current_status in {"paused", "stopped", "failed", "finished", "aborted"} or task_id in _task_shutdown_requested:
                            _rollback_unsent_recipients_for_resume(
                                _task_runtime_metrics.get(task_id),
                                task_id,
                                batch_recipients,
                                inflight_emails,
                                reason=f"发送 Worker {worker_index} 选取发件人前检测到任务状态={current_status or 'shutdown'}",
                            )
                            if local_batch_tracked:
                                _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                                local_batch_tracked = False
                            return
                        fixed_sender_snapshot = _task_fixed_sender(task_snapshot)
                        batch_sender_snapshot = fixed_sender_snapshot or await batch_pick_next_sender(
                            task_snapshot.channel,
                            rotation_enabled=_task_sender_rotation_enabled(task_snapshot),
                        )
                        current_status = str(_task_status_cache.get(task_id) or "").strip().lower()
                        if current_status in {"paused", "stopped", "failed", "finished", "aborted"} or task_id in _task_shutdown_requested:
                            _rollback_unsent_recipients_for_resume(
                                _task_runtime_metrics.get(task_id),
                                task_id,
                                batch_recipients,
                                inflight_emails,
                                reason=f"发送 Worker {worker_index} 选取发件人后检测到任务状态={current_status or 'shutdown'}",
                            )
                            if local_batch_tracked:
                                _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                                local_batch_tracked = False
                            return
                        _set_task_current_sender(task_id, batch_sender_snapshot)
                    except Exception as exc:
                        logger.warning(
                            "任务 %s 发送 Worker 选取发件人失败 recipients=%s error=%s",
                            task_id,
                            len(batch_recipients),
                            exc,
                        )
                        for failed_recipient in batch_recipients:
                            await _enqueue_result(
                                task_id,
                                failed_recipient,
                                False,
                                f"选取发件人失败：{exc}",
                                0,
                                inflight_emails=inflight_emails,
                            )
                        if local_batch_tracked:
                            _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                            local_batch_tracked = False
                        continue

                    try:
                        await _send_batch(
                            task_snapshot,
                            batch_recipients,
                            rate_limiter_holder=rate_limiter_holder,
                            url_pool_cache=url_pool_cache,
                            worker_concurrency=live_worker_count,
                            rate_limit=per_second_rate,
                            batch_sender=batch_sender_snapshot,
                            sync_sendgrid_manager=sync_sendgrid_manager,
                            send_log_id_allocator=send_log_id_allocator,
                            inflight_emails=inflight_emails,
                            rate_worker_id=worker_index,
                        )
                        if runtime_ref is not None:
                            runtime_ref["infrastructure_failure_streak"] = 0
                        batch_sender_email = str(batch_sender_snapshot.get("email") or "").strip()
                        if batch_sender_email:
                            if logger.isEnabledFor(logging.DEBUG) or bool(getattr(settings, "perf_debug", False)):
                                rotation_label = "已启用" if _task_sender_rotation_enabled(task_snapshot) else "未启用"
                                logger.debug(
                                    "任务 %s 批次发送完成 收件人数=%s 发件人=%s 轮询=%s",
                                    task_id,
                                    len(batch_recipients),
                                    batch_sender_email,
                                    rotation_label,
                                )
                                batch_senders = runtime_ref.get("batch_sender_emails") if runtime_ref is not None else None
                                if not isinstance(batch_senders, set):
                                    batch_senders = set()
                                batch_senders.add(batch_sender_email)
                                if runtime_ref is not None:
                                    runtime_ref["batch_sender_emails"] = batch_senders
                                    logger.debug(
                                        "任务 %s 累计批次发件人=%s",
                                        task_id,
                                        sorted(batch_senders),
                                    )
                    except Exception as exc:
                        # 多任务争用 SenderPool/发送槽/客户端锁时，整批记失败会让 SDK 失败数持续虚增。
                        if _is_send_infrastructure_soft_error(exc):
                            runtime_soft = _task_runtime_metrics.get(task_id)
                            failure_streak = int((runtime_soft or {}).get("infrastructure_failure_streak", 0) or 0) + 1
                            if runtime_soft is not None:
                                runtime_soft["infrastructure_failure_streak"] = failure_streak
                                runtime_soft["infrastructure_last_error"] = f"{type(exc).__name__}: {str(exc) or '无错误详情'}"[:500]
                                # 本地队列、线程池或客户端锁繁忙不是远端过载证据。这里缩容会形成
                                # “资源瞬时繁忙 -> 并发下降 -> 吞吐更低 -> PID 与缩容互相打架”的持续降速环。
                                # 仅 429 由 PID 触发安全缩容；基础设施软错误保持并发并安全回滚重调度。
                                runtime_soft["infrastructure_concurrency_held"] = int(
                                    runtime_soft.get("effective_concurrency", live_worker_count) or live_worker_count
                                )
                            backoff = min(
                                INFRASTRUCTURE_RETRY_MAX_SECONDS,
                                INFRASTRUCTURE_RETRY_BASE_SECONDS * (2 ** min(4, max(0, failure_streak - 1))) + random.uniform(0.0, 1.0),
                            )
                            _log_sampled_task_warning(
                                f"send_infrastructure_soft:{task_id}",
                                "任务 %s SDK 发送基础设施瞬时失败，当前批次已安全回滚，退避 %.2f 秒后由队列重新调度 recipients=%s failure_streak=%s error_type=%s error=%s",
                                task_snapshot.id,
                                backoff,
                                len(batch_recipients),
                                failure_streak,
                                type(exc).__name__,
                                str(exc) or "无错误详情",
                            )
                            _rollback_unsent_recipients_for_resume(
                                _task_runtime_metrics.get(task_id),
                                task_id,
                                batch_recipients,
                                inflight_emails,
                                reason=f"发送 Worker {worker_index} SDK 批量发送超时/基础设施繁忙",
                            )
                            if local_batch_tracked:
                                _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                                local_batch_tracked = False
                            await asyncio.sleep(backoff)
                            continue
                        if "Lock is not acquired" in str(exc):
                            logger.error(
                                "任务 %s 锁状态异常，锁名称=发送Worker批次锁，recipients=%s，堆栈=%s",
                                task_snapshot.id,
                                len(batch_recipients),
                                "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                            )
                        else:
                            _log_sampled_task_warning(
                                f"batch_worker_crash:{task_snapshot.id}",
                                "任务 %s SDK 批量发送 Worker 崩溃 recipients=%s error_type=%s error=%s",
                                task_snapshot.id,
                                len(batch_recipients),
                                type(exc).__name__,
                                exc,
                            )
                        failure_items = [
                            PendingEnqueueResult(
                                recipient=recipient_snapshot,
                                success=False,
                                response_text=f"SDK 批量 Worker 异常：{exc}",
                                attempt_count=0,
                            )
                            for recipient_snapshot in batch_recipients
                        ]
                        await batch_enqueue_results(
                            task_snapshot.id,
                            failure_items,
                            inflight_emails=inflight_emails,
                            chunk_size=int((_task_runtime_metrics.get(task_snapshot.id) or {}).get("result_enqueue_chunk_size", RESULT_ENQUEUE_CHUNK_SIZE) or RESULT_ENQUEUE_CHUNK_SIZE),
                        )
                    finally:
                        if local_batch_tracked:
                            _decrement_worker_local_batch_inflight(_task_runtime_metrics.get(task_id), len(batch_recipients))
                            local_batch_tracked = False
                    if saw_eof_sentinel:
                        logger.debug(
                            "任务 %s 发送 Worker %s 已完成 EOF 前最后部分批次 recipients=%s，现退出",
                            task_id,
                            worker_index,
                            len(batch_recipients),
                        )
                        return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not startup_future.done():
                    startup_future.set_exception(exc)
                await _pause_task_for_unexpected_exception(
                    task_id,
                    exc,
                    source=f"发送 Worker {worker_index}",
                )
            finally:
                await _release_task_worker_slot(task_id)
                runtime_ref = _task_runtime_metrics.get(task_id)
                if runtime_ref is not None:
                    runtime_ref["active_workers"] = int(_task_active_workers.get(task_id, 0))

        async def monitor_worker(worker_index: int, startup_future: asyncio.Future[None]) -> None:
            try:
                if not startup_future.done():
                    startup_future.set_result(None)
                logger.debug(
                    "task %s sdk monitor batch worker started worker_index=%s channel_id=%s",
                    task_id,
                    worker_index,
                    task.channel_id,
                )
                while True:
                    item = await monitor_queue.get()
                    if item is None:
                        monitor_queue.task_done()
                        break
                    (
                        task_snapshot,
                        recipient_snapshot,
                        monitor_trigger_count_snapshot,
                        monitor_batch_sender,
                        monitor_claim_key,
                    ) = item
                    try:
                        current_status = str(_task_status_cache.get(task_id) or "").strip().lower()
                        if current_status in {"paused", "stopped", "failed", "aborted", "blocked"}:
                            logger.info(
                                "任务 %s 状态=%s，丢弃尚未开始的监控邮件 email=%s trigger_count=%s；恢复后按数据库成功记录补发",
                                task_id,
                                current_status,
                                recipient_snapshot.email,
                                monitor_trigger_count_snapshot,
                            )
                            continue
                        await _send_monitor_single(
                            task_snapshot,
                            recipient_snapshot,
                            monitor_trigger_count=monitor_trigger_count_snapshot,
                            monitor_rate_limiter_holder=monitor_rate_limiter_holder,
                            url_pool_cache=url_pool_cache,
                            worker_concurrency=concurrency,
                            batch_sender=monitor_batch_sender,
                            sync_sendgrid_manager=monitor_sendgrid_manager,
                        )
                    except Exception as exc:
                        _log_sampled_task_warning(
                            f"monitor_batch_worker_crash:{task_snapshot.id}",
                            "任务 %s SDK 监控 Worker 崩溃 trigger=%s trigger_count=%s error_type=%s error=%s",
                            task_snapshot.id,
                            recipient_snapshot.email,
                            monitor_trigger_count_snapshot,
                            type(exc).__name__,
                            exc,
                        )
                        await _record_monitor_result(
                            task_snapshot.id,
                            recipient_snapshot.email,
                            False,
                            f"SDK 监控 Worker 异常：{exc}",
                            0,
                            monitor_trigger_count_snapshot,
                        )
                    finally:
                        runtime_monitor = _task_runtime_metrics.get(task_id)
                        local_monitor_inflight = (runtime_monitor or {}).get("monitor_local_inflight")
                        if isinstance(local_monitor_inflight, set):
                            local_monitor_inflight.discard(
                                (
                                    max(0, int(monitor_trigger_count_snapshot or 0)),
                                    str(recipient_snapshot.email or "").strip().lower(),
                                )
                            )
                        if monitor_claim_key:
                            try:
                                await asyncio.wait_for(
                                    redis_background_client.delete(monitor_claim_key),
                                    timeout=REDIS_OPERATION_TIMEOUT_SECONDS,
                                )
                            except Exception:
                                logger.warning(
                                    "任务 %s 释放监控抢占标记失败 key=%s，将等待 TTL；数据库去重仍会阻止重复发送",
                                    task_id,
                                    monitor_claim_key,
                                    exc_info=True,
                                )
                        monitor_queue.task_done()
            except Exception as exc:
                if not startup_future.done():
                    startup_future.set_exception(exc)
                raise

        send_worker_startups = [asyncio.get_running_loop().create_future() for _ in range(concurrency)]
        for index in range(concurrency):
            workers.append(
                asyncio.create_task(
                    send_worker(index, send_worker_startups[index]),
                    name=f"task-send-worker-{task_id}-{index}",
                )
            )
        worker_scale_supervisor_task = asyncio.create_task(
            worker_scale_supervisor(),
            name=f"task-worker-scale-supervisor-{task_id}",
        )
        if auto_concurrency_enabled:
            auto_tune_task = asyncio.create_task(
                auto_tune_concurrency_loop(),
                name=f"task-auto-tune-concurrency-{task_id}",
            )
        backpressure_task = asyncio.create_task(
            backpressure_monitor_loop(),
            name=f"task-backpressure-monitor-{task_id}",
        )
        hooks_after_workers = _task_shutdown_hooks.get(task_id)
        if hooks_after_workers is not None:
            hooks_after_workers["worker_scale_supervisor"] = worker_scale_supervisor_task
            if auto_tune_task is not None:
                hooks_after_workers["auto_tune_task"] = auto_tune_task
            hooks_after_workers["backpressure_task"] = backpressure_task
        monitor_worker_startups = [asyncio.get_running_loop().create_future() for _ in range(monitor_worker_count)]
        monitor_workers.clear()
        monitor_workers.extend(
            asyncio.create_task(monitor_worker(index, monitor_worker_startups[index]), name=f"task-monitor-worker-{task_id}-{index}")
            for index in range(monitor_worker_count)
        )
        hooks_monitor_workers = _task_shutdown_hooks.get(task_id)
        if hooks_monitor_workers is not None:
            hooks_monitor_workers["monitor_workers"] = monitor_workers
        startup_failure: Exception | None = None
        try:
            await asyncio.gather(*send_worker_startups, *monitor_worker_startups)
        except Exception as exc:
            startup_failure = exc
        if startup_failure is not None:
            logger.exception(
                "任务 %s Worker 启动失败 channel_id=%s error_type=%s error=%s",
                task_id,
                task.channel_id,
                type(startup_failure).__name__,
                startup_failure,
            )
            for worker in workers:
                worker.cancel()
            for worker in monitor_workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            await asyncio.gather(*monitor_workers, return_exceptions=True)
            if send_log_writer is not None:
                await send_log_writer.flush_and_stop(timeout=WRITER_DRAIN_TIMEOUT_SECONDS)
            await _pause_task_for_unexpected_exception(
                task_id,
                startup_failure,
                source="发送 Worker 启动阶段",
            )
            return

        completion_auditor_task = asyncio.create_task(
            _completion_auditor_loop(
                task_id,
                {
                    "recipient_queue": recipient_queue,
                    "inflight_emails": inflight_emails,
                    "stop_event": prefetch_stop_event,
                    "url_pool_cache": url_pool_cache,
                    "send_log_id_allocator": send_log_id_allocator,
                },
            ),
            name=f"task-completion-auditor-{task_id}",
        )
        prefetch_task = asyncio.create_task(
            _prefetch_recipients_loop(
                task_id,
                recipient_queue,
                inflight_emails,
                prefetch_limit,
                prefetch_stop_event,
                url_pool_cache,
                send_log_id_allocator,
            ),
            name=f"task-recipient-prefetch-{task_id}",
        )
        hooks_after_prefetch = _task_shutdown_hooks.get(task_id)
        if hooks_after_prefetch is not None:
            hooks_after_prefetch["prefetch_task"] = prefetch_task
            hooks_after_prefetch["completion_auditor_task"] = completion_auditor_task
        logger.info(
            "任务 %s keyset 预取与 CompletionAuditor 已启动 prefetch_batch_size=%s queue_maxsize=%s",
            task_id,
            prefetch_limit,
            RECIPIENT_QUEUE_MAXSIZE,
        )

        def mark_channel_proxy_reload_if_needed(channel_task: Task) -> None:
            nonlocal channel_proxy_reload_pending
            if channel_task.channel is None:
                return
            new_fingerprint = _build_task_channel_proxy_fingerprint(channel_task.channel)
            if new_fingerprint != last_channel_proxy_fingerprint:
                channel_proxy_reload_pending = True
                logger.warning(
                    "task %s channel proxy fingerprint changed pending_reload old=%s new=%s",
                    task_id,
                    last_channel_proxy_fingerprint,
                    new_fingerprint,
                )

        async def try_apply_httpx_client_reload(channel_task: Task) -> str:
            nonlocal last_channel_proxy_fingerprint, channel_proxy_reload_pending
            if not channel_proxy_reload_pending or channel_task.channel is None:
                return "skipped"
            async with httpx_client_swap_lock:
                if not channel_proxy_reload_pending or channel_task.channel is None:
                    return "skipped"
                new_fingerprint = _build_task_channel_proxy_fingerprint(channel_task.channel)
                if new_fingerprint == last_channel_proxy_fingerprint:
                    channel_proxy_reload_pending = False
                    return "skipped"
                try:
                    await reload_httpx_client(
                        channel_task.channel,
                        worker_concurrency=concurrency,
                        rate_limit=per_second_rate,
                        owner_task_id=task_id,
                    )
                    if sync_sendgrid_manager is not None:
                        sync_sendgrid_manager.update_channel(channel_task.channel)
                        sync_sendgrid_manager.invalidate_client(reason="proxy_reload")
                    last_channel_proxy_fingerprint = new_fingerprint
                    channel_proxy_reload_pending = False
                    runtime_snapshot = _task_runtime_metrics.get(task_id) or {}
                    runtime_snapshot["httpx_client_reload_count"] = int(runtime_snapshot.get("httpx_client_reload_count", 0)) + 1
                    logger.debug(
                        "task %s reloaded httpx client after channel proxy change fingerprint=%s",
                        task_id,
                        new_fingerprint,
                    )
                    _signal_claim_slots_available(task_id)
                    return "applied"
                except Exception as exc:
                    logger.error(
                        "task %s 代理热更新失败，任务已暂停，请手动检查代理配置后恢复 error_type=%s",
                        task_id,
                        type(exc).__name__,
                    )
                    if sync_sendgrid_manager is not None:
                        sync_sendgrid_manager.update_channel(channel_task.channel)
                        sync_sendgrid_manager.invalidate_client(reason="proxy_reload_failed")
                    channel_id = int(getattr(channel_task, "channel_id", 0) or 0)
                    if channel_id > 0:
                        try:
                            await force_close_httpx_client(channel_id, owner_task_id=task_id)
                        except Exception as close_exc:
                            logger.error(
                                "task %s force close httpx client after proxy reload failure channel_id=%s error_type=%s",
                                task_id,
                                channel_id,
                                type(close_exc).__name__,
                            )
                    channel_proxy_reload_pending = False
                    runtime_reload = _task_runtime_metrics.get(task_id)
                    if runtime_reload is not None:
                        runtime_reload["proxy_reload_failed"] = 1
                        runtime_reload["proxy_reload_retry_after"] = monotonic() + 30.0
                    logger.error(
                        "任务 %s 代理热更新失败，继续沿用当前客户端发送并在 30 秒后重试热更新；禁止因基础设施热更新失败暂停任务",
                        task_id,
                    )
                    return "deferred"

        async def maybe_apply_proxy_reload_in_idle_window(channel_task: Task | None) -> str:
            nonlocal cached_task
            if channel_task is None:
                return "skipped"
            reload_result = await try_apply_httpx_client_reload(channel_task)
            if reload_result == "failed":
                return "failed"
            if reload_result == "applied":
                refreshed_task = await _load_task(task_id)
                if refreshed_task is not None:
                    cached_task = refreshed_task
            return reload_result

        async def discard_eof_sentinels_preserving_recipients(reason: str) -> int:
            drained_sentinels = 0
            retained_recipients: list[Recipient] = []
            while True:
                try:
                    queued_item = recipient_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                recipient_queue.task_done()
                if queued_item is None:
                    drained_sentinels += 1
                    continue
                retained_recipients.append(queued_item)
            for retained in retained_recipients:
                await recipient_queue.put(retained)
            if drained_sentinels:
                logger.warning(
                    "任务 %s %s：已丢弃残留 EOF 哨兵 count=%s retained=%s",
                    task_id,
                    reason,
                    drained_sentinels,
                    len(retained_recipients),
                )
            return drained_sentinels

        async def ensure_prefetch_and_workers_running(reason: str) -> None:
            nonlocal prefetch_task, completion_auditor_task
            runtime_ref = _task_runtime_metrics.get(task_id)
            if runtime_ref is not None:
                runtime_ref["prefetch_exhausted"] = False
                runtime_ref["prefetch_task_exhausted"] = False
                runtime_ref["task_finished"] = False
                runtime_ref["prefetch_failed"] = False
                runtime_ref["prefetch_error"] = ""
                runtime_ref["last_prefetch_recovery_reason"] = reason
                runtime_ref["prefetch_recovery_count"] = int(runtime_ref.get("prefetch_recovery_count", 0) or 0) + 1
                _touch_task_progress(task_id)
            await discard_eof_sentinels_preserving_recipients(reason)
            if prefetch_stop_event is not None and prefetch_stop_event.is_set():
                prefetch_stop_event.clear()
            if completion_auditor_task is None or completion_auditor_task.done():
                completion_auditor_task = asyncio.create_task(
                    _completion_auditor_loop(
                        task_id,
                        {
                            "recipient_queue": recipient_queue,
                            "inflight_emails": inflight_emails,
                            "stop_event": prefetch_stop_event,
                            "url_pool_cache": url_pool_cache,
                            "send_log_id_allocator": send_log_id_allocator,
                        },
                    ),
                    name=f"task-completion-auditor-{task_id}-recovery",
                )
                hooks_auditor_recovery = _task_shutdown_hooks.get(task_id)
                if hooks_auditor_recovery is not None:
                    hooks_auditor_recovery["completion_auditor_task"] = completion_auditor_task
                logger.warning("任务 %s CompletionAuditor 未运行，已重新启动", task_id)
            if prefetch_task is None or prefetch_task.done():
                prefetch_task = asyncio.create_task(
                    _prefetch_recipients_loop(
                        task_id,
                        recipient_queue,
                        inflight_emails,
                        prefetch_limit,
                        prefetch_stop_event,
                        url_pool_cache,
                        send_log_id_allocator,
                    ),
                    name=f"task-recipient-prefetch-{task_id}-recovery",
                )
                hooks_recovery = _task_shutdown_hooks.get(task_id)
                if hooks_recovery is not None:
                    hooks_recovery["prefetch_task"] = prefetch_task
                logger.warning("任务 %s 检测到发送停滞，重新启动预取协程", task_id)
            live_workers_now = count_live_worker_tasks(workers)
            target_workers = max(
                1,
                int((runtime_ref or {}).get("worker_count", 0) or (runtime_ref or {}).get("effective_concurrency", 0) or concurrency or 1),
            )
            if live_workers_now < target_workers:
                need_spawn = max(1, target_workers - live_workers_now)
                logger.warning(
                    "任务 %s %s：发送 Worker 不足，存活=%s 目标=%s，将补启=%s",
                    task_id,
                    reason,
                    live_workers_now,
                    target_workers,
                    need_spawn,
                )
                for _ in range(need_spawn):
                    try:
                        spawned_ok = await spawn_send_worker()
                    except Exception as spawn_exc:
                        logger.warning(
                            "任务 %s 紧急恢复补启发送 Worker 失败 error_type=%s error=%s",
                            task_id,
                            type(spawn_exc).__name__,
                            spawn_exc,
                        )
                        break
                    if not spawned_ok:
                        break
            live_monitor_workers_now = count_live_worker_tasks(monitor_workers)
            if live_monitor_workers_now < monitor_worker_count:
                missing_monitor_workers = monitor_worker_count - live_monitor_workers_now
                logger.warning(
                    "任务 %s %s：监控 Worker 不足，存活=%s 目标=%s，将补启=%s",
                    task_id,
                    reason,
                    live_monitor_workers_now,
                    monitor_worker_count,
                    missing_monitor_workers,
                )
                for _ in range(missing_monitor_workers):
                    monitor_index = len(monitor_workers)
                    startup_future = asyncio.get_running_loop().create_future()
                    monitor_task = asyncio.create_task(
                        monitor_worker(monitor_index, startup_future),
                        name=f"task-monitor-worker-{task_id}-recovery-{monitor_index}",
                    )
                    monitor_workers.append(monitor_task)
                    try:
                        await asyncio.wait_for(startup_future, timeout=10.0)
                    except Exception as monitor_exc:
                        logger.error(
                            "任务 %s 补启监控 Worker 失败 index=%s error_type=%s error=%s",
                            task_id,
                            monitor_index,
                            type(monitor_exc).__name__,
                            monitor_exc,
                        )
                        monitor_task.cancel()
                        await asyncio.gather(monitor_task, return_exceptions=True)
                        break
                hooks_monitor_recovery = _task_shutdown_hooks.get(task_id)
                if hooks_monitor_recovery is not None:
                    hooks_monitor_recovery["monitor_workers"] = monitor_workers
                if runtime_ref is not None:
                    runtime_ref["monitor_worker_missing"] = False
                    runtime_ref["monitor_trigger_pending"] = True
                _schedule_monitor_trigger(task_id, reason="monitor_worker_recovered")

        async def maybe_recover_send_stall(current_task_for_guard: Task) -> bool:
            runtime_ref = _task_runtime_metrics.get(task_id)
            if runtime_ref is None:
                return False
            if is_confirmed_prefetch_eof(runtime_ref):
                return False
            queue_size = int(recipient_queue.qsize())
            active_sends_ref = int(runtime_ref.get("active_sends", 0) or 0)
            worker_local_inflight = int(runtime_ref.get(WORKER_LOCAL_BATCH_INFLIGHT_KEY, 0) or 0)
            outstanding_writes_ref = int(runtime_ref.get("outstanding_writes", 0) or 0)
            pending_result_queue_ref = int(send_log_writer.pending_count) if send_log_writer is not None else 0
            live_workers_ref = count_live_worker_tasks(workers)
            if queue_size > 0 and live_workers_ref <= 0 and active_sends_ref <= 0 and worker_local_inflight <= 0:
                logger.error(
                    "任务 %s 收件人队列仍有 %s 条但发送 Worker 已全部退出，立即补启 Worker",
                    task_id,
                    queue_size,
                )
                await ensure_prefetch_and_workers_running("队列非空但发送 Worker 全部退出")
                _touch_task_send_progress(task_id, monotonic())
                return True
            if queue_size > 0 or active_sends_ref > 0 or worker_local_inflight > 0 or outstanding_writes_ref > 0 or pending_result_queue_ref > 0:
                return False
            now_for_stall = monotonic()
            _update_send_progress_watch(task_id, runtime_ref, now_for_stall)
            last_send_progress_at = float(runtime_ref.get("last_send_progress_at", now_for_stall) or now_for_stall)
            stall_seconds = max(0.0, now_for_stall - last_send_progress_at)
            if stall_seconds < 60.0:
                return False
            runtime_ref["last_send_stall_seconds"] = round(stall_seconds, 1)
            runtime_ref["last_send_stall_detected_at"] = now_for_stall
            if stall_seconds >= 120.0:
                runtime_ref["status"] = "paused"
                runtime_ref["stalled"] = True
                runtime_ref["status_reason"] = "发送连续停滞超过 120 秒，已自动暂停并保留现场"
                logger.warning("任务 %s 发送连续停滞超过 120 秒，已自动暂停并保留现场", task_id)
                await _mark_task_paused(task_id, "发送连续停滞超过 120 秒，已自动暂停并保留现场")
                _task_shutdown_requested.add(task_id)
                return True
            if bool(runtime_ref.get("prefetch_exhausted", False)):
                auditor_wake = runtime_ref.get("completion_auditor_wake_event")
                if isinstance(auditor_wake, asyncio.Event):
                    auditor_wake.set()
                runtime_ref["completion_auditor_reason"] = "发送停滞，已请求立即审计"
                return True
            logger.warning(
                "任务 %s 检测到发送停滞，强制重启预取协程",
                task_id,
            )
            logger.warning(
                "任务 %s 主循环检测到发送进度停滞 %.1f 秒，执行紧急恢复",
                task_id,
                stall_seconds,
            )
            rewind_floor = _claim_cursor_rollback_floor(runtime_ref)
            runtime_ref["claim_cursor_row"], runtime_ref["claim_cursor_id"] = rewind_floor
            runtime_ref["pending_claim_cursor_row"], runtime_ref["pending_claim_cursor_id"] = rewind_floor
            await ensure_prefetch_and_workers_running("发送进度停滞紧急恢复")
            _touch_task_send_progress(task_id, monotonic())
            return True

        finish_reason = "无待发送收件人"
        should_mark_finished = False
        cached_task: Task | None = task
        last_config_refresh_at = monotonic()
        status_wake_event = ensure_task_status_watch(task_id, str(task.status or "running"))
        last_status_db_poll_at = monotonic()
        claim_cursor_row = int(getattr(task, "last_claimed_row_number", 0) or 0)
        last_loop_diagnostic_at = monotonic()
        last_perf_metrics_log_at = monotonic()
        last_task_status_log_at = monotonic()
        last_monitor_poll_at = monotonic()
        last_proxy_rtt_probe_at = monotonic()
        last_rate_monitor_check_at = monotonic()
        last_state_snapshot_at = monotonic()

        async def runtime_heartbeat_loop() -> None:
            while True:
                runtime_ref = _task_runtime_metrics.get(task_id)
                if runtime_ref is None:
                    return
                runtime_ref["last_heartbeat"] = wall_time()
                await asyncio.sleep(5.0)

        heartbeat_task = asyncio.create_task(runtime_heartbeat_loop(), name=f"task-runtime-heartbeat-{task_id}")

        async def runtime_realtime_snapshot_loop() -> None:
            """独立于主循环每秒发布跨进程指标，避免 DB/收尾阻塞让前端看到陈旧速率。"""
            while True:
                await publish_task_runtime_snapshot(task_id)
                await asyncio.sleep(TASK_STATE_SNAPSHOT_INTERVAL_SECONDS)

        realtime_snapshot_task = asyncio.create_task(
            runtime_realtime_snapshot_loop(),
            name=f"task-runtime-realtime-snapshot-{task_id}",
        )
        while True:
            await ensure_result_writer_alive()
            loop_runtime = _task_runtime_metrics.get(task_id)
            if (
                loop_runtime is not None
                and settings.perf_debug
                and monotonic() - last_perf_metrics_log_at >= PERF_METRICS_LOG_INTERVAL_SECONDS
            ):
                _record_http_rps_snapshot(loop_runtime)
                _log_per_second_performance_metrics(task_id, loop_runtime)
                last_perf_metrics_log_at = monotonic()
            if loop_runtime is not None and monotonic() - last_task_status_log_at >= TASK_STATUS_LOG_INTERVAL_SECONDS:
                _log_task_runtime_status(task_id, int(task.channel_id or 0), loop_runtime)
                last_task_status_log_at = monotonic()
            if loop_runtime is not None and monotonic() - last_loop_diagnostic_at >= MAIN_LOOP_DIAGNOSTIC_INTERVAL_SECONDS:
                _log_event_loop_diagnostics(task_id, loop_runtime)
                last_loop_diagnostic_at = monotonic()
            status, last_status_db_poll_at = await _resolve_loop_task_status(
                task_id,
                status_wake_event,
                last_status_db_poll_at,
                runtime=loop_runtime,
            )
            if status in {"paused", "stopped", "failed", "finished", "aborted", "blocked"}:
                finish_reason = f"状态已变更为 {status}"
                logger.debug("任务 %s 主循环因状态变更退出，status=%s", task_id, status)
                if status == "finished":
                    runtime_snapshot = _task_runtime_metrics.get(task_id) or {}
                    regular_success_count = _get_monitor_success_count(task_id, runtime_snapshot) if runtime_snapshot else 0
                    should_mark_finished = regular_success_count > 0
                else:
                    should_mark_finished = False
                break
            if task_id in _task_shutdown_requested:
                finish_reason = "收到停机请求"
                logger.warning("任务 %s 收到停机请求，主循环即将退出", task_id)
                should_mark_finished = False
                break
            now_value = monotonic()
            iter_started_at = now_value
            if cached_task is None or now_value - last_config_refresh_at >= TASK_CONFIG_REFRESH_INTERVAL_SECONDS:
                previous_template_key = (
                    get_template_cache_key(cached_task.template)
                    if cached_task is not None and cached_task.template is not None
                    else None
                )
                cached_task = await _load_task_for_loop_refresh(task_id)
                last_config_refresh_at = now_value
                if cached_task is not None:
                    monitor_interval = int(
                        cached_task.monitor_interval if cached_task.monitor_interval is not None else 100
                    )
                    monitor_emails = await _parse_monitor_emails(cached_task)
                    has_monitor_sampling = _is_monitor_sampling_enabled(monitor_interval, monitor_emails)
                    runtime_refresh = _task_runtime_metrics.get(task_id)
                    refreshed_template_key = (
                        get_template_cache_key(cached_task.template)
                        if cached_task.template is not None
                        else None
                    )
                    if runtime_refresh is not None and refreshed_template_key != previous_template_key:
                        _invalidate_task_send_prepare_cache(runtime_refresh)
                    if runtime_refresh is not None:
                        was_monitor_enabled = bool(runtime_refresh.get("has_monitor_emails", False))
                        runtime_refresh["monitor_interval"] = (
                            max(1, monitor_interval) if has_monitor_sampling else monitor_interval
                        )
                        runtime_refresh["has_monitor_emails"] = has_monitor_sampling
                        if was_monitor_enabled and not has_monitor_sampling:
                            runtime_refresh["monitor_trigger_pending"] = False
                            logger.info(
                                "任务 %s 监控抽样已禁用（monitor_interval=%s）",
                                task_id,
                                monitor_interval,
                            )
                    hooks_refresh = _task_shutdown_hooks.get(task_id)
                    if hooks_refresh is not None:
                        hooks_refresh["cached_task"] = cached_task
                    mark_channel_proxy_reload_if_needed(cached_task)
            current_task = cached_task
            if current_task is None:
                finish_reason = "任务已删除或不可用"
                logger.warning("任务 %s 主循环因任务不可用退出", task_id)
                _finalize_main_loop_iteration(task_id, iter_started_at)
                break
            runtime = _task_runtime_metrics.get(task_id) or {}
            # 运行期 RTT 直接来自真实 Mail Send 批次延迟。禁止插入 /v3/scopes 健康探测，
            # 避免探测请求与发送争抢代理连接、带宽和服务端排队容量。
            if now_value - last_proxy_rtt_probe_at >= PROXY_RTT_DYNAMIC_PROBE_INTERVAL_SECONDS:
                last_proxy_rtt_probe_at = now_value
                observed_http_ms = max(
                    float(runtime.get("http_send_p95_ms", 0) or 0),
                    float(runtime.get("channel_http_send_p95_ms", 0) or 0),
                )
                if observed_http_ms > 0:
                    previous_rtt_seconds = float(runtime.get("proxy_rtt_seconds", proxy_rtt_seconds) or proxy_rtt_seconds)
                    measured_rtt_seconds = max(0.05, observed_http_ms / 1000.0)
                    smoothed_rtt_seconds = smooth_runtime_rtt_seconds(previous_rtt_seconds, measured_rtt_seconds)
                    runtime["last_dynamic_rtt_probe_at"] = now_value
                    runtime["last_measured_proxy_rtt_seconds"] = round(float(measured_rtt_seconds), 4)
                    if bool(runtime.get("auto_concurrency_enabled", False)):
                        await apply_dynamic_worker_scaling(
                            channel_task=current_task,
                            worker_tasks=workers,
                            previous_rtt_seconds=previous_rtt_seconds,
                            new_rtt_seconds=smoothed_rtt_seconds,
                        )
                else:
                    runtime["proxy_rtt_seconds"] = round(float(smoothed_rtt_seconds), 4)
                    rtt_probe_configured_rate = max(0.0, min(get_effective_second_rate_limit(current_task), 100000.0))
                    rtt_probe_rate = rtt_probe_configured_rate
                    runtime["configured_rate_rps"] = rtt_probe_configured_rate
                    runtime["rate_limit_per_second"] = rtt_probe_rate
                    runtime["effective_target_rate_rps"] = rtt_probe_rate
                    runtime["channel_active_task_count"] = 1
                    runtime["channel_fair_share_rps"] = 0.0
                    runtime["channel_capacity_rps"] = 0.0
                    refreshed_min_concurrency = calculate_min_concurrency(
                        rtt_probe_rate,
                        get_send_http_batch_size(current_task),
                        smoothed_rtt_seconds,
                    )
                    runtime["min_concurrency"] = cap_task_effective_concurrency(refreshed_min_concurrency)
            if now_value - last_rate_monitor_check_at >= RATE_MONITOR_CHECK_INTERVAL_SECONDS:
                last_rate_monitor_check_at = now_value
                try:
                    await check_send_rate_performance_alert(task_id, current_task, runtime)
                except Exception:
                    logger.exception("任务 %s 发送速率性能告警检查失败", task_id)
            if bool(runtime.get("has_monitor_emails", False)):
                monitor_poll_elapsed = now_value - last_monitor_poll_at
                if monitor_poll_elapsed >= MONITOR_TRIGGER_POLL_INTERVAL_SECONDS:
                    last_monitor_poll_at = now_value
                    if count_live_worker_tasks(monitor_workers) < monitor_worker_count:
                        await ensure_prefetch_and_workers_running("监控轮询检测到 Worker 缺失")
                    try:
                        await _maybe_trigger_monitor_emails(task_id)
                    except Exception:
                        logger.exception("任务 %s 监控触发失败", task_id)
            _record_main_loop_scheduling_delay(runtime, now_value)
            current_sent_count = int(runtime.get("initial_sent_count", 0)) + int(runtime.get("success", 0))
            if has_reached_channel_limit(current_sent_count, getattr(current_task.channel, "daily_limit", None)):
                finish_reason = _FINISH_REASON_CHANNEL_DAILY_LIMIT
                should_mark_finished = False
                logger.warning("任务 %s 通道 %s 已达日限额=%s", task_id, current_task.channel_id, getattr(current_task.channel, "daily_limit", None))
                _finalize_main_loop_iteration(task_id, iter_started_at)
                break
            if has_reached_channel_limit(current_sent_count, getattr(current_task.channel, "hourly_limit", None)):
                finish_reason = _FINISH_REASON_CHANNEL_HOURLY_LIMIT
                should_mark_finished = False
                logger.warning("任务 %s 通道 %s 已达小时限额=%s", task_id, current_task.channel_id, getattr(current_task.channel, "hourly_limit", None))
                _finalize_main_loop_iteration(task_id, iter_started_at)
                break
            refreshed_configured_rate = max(0.0, min(get_effective_second_rate_limit(current_task), 100000.0))
            refreshed_rate = refreshed_configured_rate
            runtime = _task_runtime_metrics.get(task_id)
            local_effective_concurrency = max(
                1,
                int((runtime or {}).get("effective_concurrency", (runtime or {}).get("active_workers", 1)) or 1),
            )
            if rate_limiter_holder.refresh(refreshed_rate):
                runtime = _task_runtime_metrics.get(task_id)
                if runtime is not None:
                    runtime["rate_limit_per_second"] = rate_limiter_holder.per_second_rate
                    runtime["configured_rate_rps"] = refreshed_configured_rate
                    runtime["effective_target_rate_rps"] = refreshed_rate
                    runtime["local_effective_concurrency"] = local_effective_concurrency
            else:
                runtime = _task_runtime_metrics.get(task_id)
                if runtime is not None:
                    runtime["configured_rate_rps"] = refreshed_configured_rate
                    runtime["effective_target_rate_rps"] = refreshed_rate
                    runtime["local_effective_concurrency"] = local_effective_concurrency
                logger.debug(
                    "task %s rate limit hot-reloaded per_second_rate=%s",
                    task_id,
                    rate_limiter_holder.per_second_rate,
                )
            refreshed_monitor_rate = get_monitor_rate_limit(rate_limiter_holder.per_second_rate)
            if monitor_rate_limiter_holder.refresh(refreshed_monitor_rate):
                runtime = _task_runtime_metrics.get(task_id)
                if runtime is not None:
                    runtime["monitor_rate_limit_per_second"] = monitor_rate_limiter_holder.per_second_rate
            runtime = _task_runtime_metrics.get(task_id) or {}
            if now_value - last_state_snapshot_at >= TASK_STATE_SNAPSHOT_INTERVAL_SECONDS:
                should_log_state_snapshot = now_value - float(runtime.get("last_state_log_at", 0.0) or 0.0) >= TASK_STATE_LOG_INTERVAL_SECONDS
                snapshot_cursor = int(runtime.get("claim_cursor_row", 0) or runtime.get("pending_claim_cursor_row", 0) or 0)
                snapshot_queue_size = int(recipient_queue.qsize())
                snapshot_sent = _runtime_processed_count(runtime)
                snapshot_total = int(getattr(current_task, "total_count", 0) or runtime.get("total_count", 0) or 0)
                snapshot_remaining = max(0, snapshot_total - snapshot_sent) if snapshot_total > 0 else 0
                snapshot_eof = is_confirmed_prefetch_eof(runtime)
                snapshot_workers = count_live_worker_tasks(workers)
                if should_log_state_snapshot:
                    logger.warning(
                        "任务 %s 状态快照：游标=%s，队列深度=%s，已发送=%s，剩余=%s，预取EOF=%s，活跃Worker=%s",
                        task_id,
                        snapshot_cursor,
                        snapshot_queue_size,
                        snapshot_sent,
                        snapshot_remaining,
                        snapshot_eof,
                        snapshot_workers,
                    )
                    runtime["last_state_log_at"] = now_value
                await publish_task_runtime_snapshot(task_id)
                last_state_snapshot_at = now_value
            if lock is not None and lock.lost_event.is_set():
                logger.error("任务 %s 通道锁已丢失，任务将标记失败", task_id)
                await _mark_task_failed(task_id, CHANNEL_LOCK_LOST_MESSAGE)
                finish_reason = CHANNEL_LOCK_LOST_MESSAGE
                should_mark_finished = False
                _finalize_main_loop_iteration(task_id, iter_started_at)
                break
            _update_stall_progress_watch(task_id, runtime, recipient_queue, send_log_writer, now_value)
            _update_send_progress_watch(task_id, runtime, now_value)
            recovered_from_stall = await maybe_recover_send_stall(current_task)
            if recovered_from_stall:
                runtime["main_loop_expected_interval"] = PREFETCH_RETRY_DELAY_SECONDS
                await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                _finalize_main_loop_iteration(task_id, iter_started_at)
                continue
            stall_message = _check_main_loop_stall(task_id, runtime, recipient_queue, send_log_writer, now_value)
            if stall_message:
                runtime["main_loop_stall_detected_count"] = int(runtime.get("main_loop_stall_detected_count", 0) or 0) + 1
                runtime["main_loop_stall_last_message"] = stall_message[:500]
                _log_sampled_task_warning(
                    f"main_loop_stall_recovery:{task_id}",
                    "任务 %s 主循环停滞，保持 running 并启动自愈，禁止因瞬时基础设施阻塞标记失败：%s",
                    task_id,
                    stall_message,
                )
                await ensure_prefetch_and_workers_running("主循环停滞自愈")
                _touch_task_progress(task_id, now_value)
                runtime["main_loop_expected_interval"] = PREFETCH_RETRY_DELAY_SECONDS
                await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                _finalize_main_loop_iteration(task_id, iter_started_at)
                continue
            runtime["recipient_queue_size"] = int(recipient_queue.qsize())
            _update_prefetch_queue_empty_watch(runtime, int(recipient_queue.qsize()), now_value)
            runtime["result_queue_size"] = int(send_log_writer.queue_size)
            runtime["global_concurrency_occupied"] = get_global_concurrency_occupied()
            runtime["admitted_workers"] = int(_task_active_workers.get(task_id, runtime.get("active_workers", 0)))
            live_workers_now = count_live_worker_tasks(workers)
            runtime["effective_concurrency"] = live_workers_now
            runtime["auto_concurrency"] = live_workers_now
            limiter_snapshot = runtime.get("concurrency_semaphore")
            if isinstance(limiter_snapshot, DynamicConcurrencyLimiter):
                runtime["local_concurrency_limit"] = limiter_snapshot.limit
                runtime["local_concurrency_inflight"] = limiter_snapshot.inflight
            if channel_proxy_reload_pending:
                reload_result = await maybe_apply_proxy_reload_in_idle_window(cached_task)
                if reload_result == "failed":
                    finish_reason = "代理热更新失败"
                    should_mark_finished = False
                    _finalize_main_loop_iteration(task_id, iter_started_at)
                    break
                cached_task = await _load_task(task_id) or cached_task
                current_task = cached_task
                if current_task is None:
                    finish_reason = "任务已不存在"
                    logger.error("任务 %s 任务记录已不存在", task_id)
                    _finalize_main_loop_iteration(task_id, iter_started_at)
                    break
                runtime["main_loop_expected_interval"] = MAIN_LOOP_IDLE_SLEEP_SECONDS
                _finalize_main_loop_iteration(task_id, iter_started_at)
                continue
            if prefetch_task is not None and prefetch_task.done():
                runtime["prefetch_task_exhausted"] = True
            if prefetch_task is not None and prefetch_task.done() and not bool(runtime.get("prefetch_eof_confirmed", False)):
                prefetch_error = None
                try:
                    prefetch_error = prefetch_task.exception()
                except asyncio.CancelledError:
                    prefetch_error = "cancelled"
                runtime["prefetch_failed"] = False
                runtime["prefetch_error"] = str(
                    prefetch_error or runtime.get("prefetch_error") or "收件人预取未确认 EOF 即退出"
                )
                logger.warning(
                    "任务 %s 预取协程未确认 EOF 即退出，将执行紧急恢复：%s",
                    task_id,
                    runtime["prefetch_error"],
                )
                await ensure_prefetch_and_workers_running("预取协程未确认 EOF 退出")
                runtime["main_loop_expected_interval"] = PREFETCH_RETRY_DELAY_SECONDS
                await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                _finalize_main_loop_iteration(task_id, iter_started_at)
                continue
            if bool(runtime.get("prefetch_failed", False)):
                prefetch_error_message = str(runtime.get("prefetch_error") or "收件人预取失败，任务已终止以避免漏发")
                logger.error("任务 %s 收件人预取异常退出，将立即重启预取协程：%s", task_id, prefetch_error_message)
                await ensure_prefetch_and_workers_running("预取协程异常退出自愈")
                runtime["main_loop_expected_interval"] = PREFETCH_RETRY_DELAY_SECONDS
                await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                _finalize_main_loop_iteration(task_id, iter_started_at)
                continue
            active_sends = int(runtime.get("active_sends", 0))
            worker_local_batch_inflight = int(runtime.get(WORKER_LOCAL_BATCH_INFLIGHT_KEY, 0) or 0)
            outstanding_writes = int(runtime.get("outstanding_writes", 0))
            pending_result_queue = int(send_log_writer.pending_count) if send_log_writer is not None else 0
            # EOF 是预取器确认的物理列表末尾；task_finished 只代表审计/收尾结果，不能反过来作为 EOF 信号。
            prefetch_eof = is_confirmed_prefetch_eof(runtime)
            last_log_enqueued_at = float(runtime.get("last_send_log_enqueued_at", runtime.get("last_send_progress_at", now_value)) or now_value)
            if (
                recipient_queue.empty()
                and not prefetch_eof
                and not bool(runtime.get("prefetch_task_exhausted", False))
                and now_value - last_log_enqueued_at >= PREFETCH_IDLE_RECOVERY_SECONDS
            ):
                logger.error(
                    "任务 %s 超过 %s 秒没有新的发送日志写入，队列为空但预取EOF未确认，强制重启预取循环",
                    task_id,
                    int(PREFETCH_IDLE_RECOVERY_SECONDS),
                )
                await ensure_prefetch_and_workers_running("队列空且无发送日志写入超过60秒")
                runtime["last_send_log_enqueued_at"] = now_value
                runtime["main_loop_expected_interval"] = PREFETCH_RETRY_DELAY_SECONDS
                await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                _finalize_main_loop_iteration(task_id, iter_started_at)
                continue
            if prefetch_eof and recipient_queue.empty() and active_sends > 0 and worker_local_batch_inflight == 0 and int(runtime.get("httpx_inflight", 0) or 0) <= 0 and outstanding_writes == 0 and pending_result_queue == 0:
                runtime["stale_active_sends_reset_total"] = int(runtime.get("stale_active_sends_reset_total", 0) or 0) + active_sends
                runtime["stale_active_sends_last_value"] = active_sends
                runtime["active_sends"] = 0
                active_sends = 0
                _touch_task_progress(task_id, now_value)
                logger.warning(
                    "任务 %s 收尾阶段检测到 active_sends 残留但无 HTTP/写入/队列在途，已清零以避免已完成任务被误判失败 stale_active_sends=%s",
                    task_id,
                    runtime["stale_active_sends_last_value"],
                )
            if prefetch_eof and recipient_queue.empty() and active_sends == 0 and worker_local_batch_inflight == 0 and outstanding_writes == 0 and pending_result_queue == 0:
                if send_log_writer is not None:
                    await send_log_writer.flush(timeout=WRITER_DRAIN_TIMEOUT_SECONDS)
                    pending_result_queue = int(send_log_writer.pending_count)
                    outstanding_writes = int(runtime.get("outstanding_writes", 0) or 0)
                    if pending_result_queue > 0 or outstanding_writes > 0:
                        logger.warning(
                            "任务 %s 完成前等待 SendLog 写入清空 outstanding=%s queue=%s",
                            task_id,
                            outstanding_writes,
                            pending_result_queue,
                        )
                        await asyncio.sleep(MAIN_LOOP_IDLE_SLEEP_SECONDS)
                        runtime["main_loop_expected_interval"] = MAIN_LOOP_IDLE_SLEEP_SECONDS
                        _finalize_main_loop_iteration(task_id, iter_started_at)
                        continue
                live_workers_for_finish = count_live_worker_tasks(workers)
                if live_workers_for_finish > 0:
                    runtime["finish_waiting_workers"] = live_workers_for_finish
                    logger.warning(
                        "任务 %s 已 EOF 且队列/写入为空，主动停止剩余发送 Worker=%s 后进入完成收尾",
                        task_id,
                        live_workers_for_finish,
                    )
                    for worker in workers:
                        if not worker.done():
                            worker.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                    runtime["effective_concurrency"] = 0
                    runtime["active_workers"] = 0
                try:
                    await flush_pending_claim_cursor(task_id)
                    runtime["final_claim_cursor_persisted"] = True
                except Exception as cursor_exc:
                    runtime["final_claim_cursor_persisted"] = False
                    logger.warning(
                        "任务 %s 完成前持久化游标失败，为避免状态不一致，本轮不标记完成 error_type=%s error=%s",
                        task_id,
                        type(cursor_exc).__name__,
                        cursor_exc,
                    )
                    await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                    _finalize_main_loop_iteration(task_id, iter_started_at)
                    continue
                processed_runtime = _runtime_processed_count(runtime)
                total_runtime = int(getattr(current_task, "total_count", 0) or runtime.get("total_count", 0) or 0)
                if not bool(runtime.get("task_finished", False)):
                    auditor_wake = runtime.get("completion_auditor_wake_event")
                    if isinstance(auditor_wake, asyncio.Event):
                        auditor_wake.set()
                    await asyncio.sleep(MAIN_LOOP_IDLE_SLEEP_SECONDS)
                    _finalize_main_loop_iteration(task_id, iter_started_at)
                    continue
                persisted_success, persisted_failed = await _get_persisted_regular_result_counts(task_id)
                ledger_processed = max(processed_runtime, int(persisted_success) + int(persisted_failed))
                if total_runtime > 0 and ledger_processed < total_runtime:
                    runtime["completion_total_mismatch_count"] = int(runtime.get("completion_total_mismatch_count", 0) or 0) + 1
                    runtime["prefetch_eof"] = False
                    runtime["prefetch_eof_confirmed"] = False
                    rewind_floor = _claim_cursor_rollback_floor(runtime)
                    runtime["claim_cursor_row"], runtime["claim_cursor_id"] = rewind_floor
                    runtime["pending_claim_cursor_row"], runtime["pending_claim_cursor_id"] = rewind_floor
                    logger.warning(
                        "任务 %s EOF 已确认但处理数小于总数 processed=%s total=%s，拒绝标记 finished，回滚游标到 %s 恢复预取",
                        task_id,
                        ledger_processed,
                        total_runtime,
                        rewind_floor,
                    )
                    await ensure_prefetch_and_workers_running("完成前处理数小于总数")
                    runtime["main_loop_expected_interval"] = PREFETCH_RETRY_DELAY_SECONDS
                    await asyncio.sleep(PREFETCH_RETRY_DELAY_SECONDS)
                    _finalize_main_loop_iteration(task_id, iter_started_at)
                    continue
                processed_runtime = ledger_processed
                runtime["completion_unfinished_count"] = 0
                runtime["prefetch_eof"] = True
                runtime["prefetch_eof_confirmed"] = True
                if int(runtime.get("delivery_unknown", 0) or 0) > 0:
                    finish_reason = "存在投递状态未知收件人，已进入待确认队列，需人工排查"
                else:
                    finish_reason = "全部收件人已处理完成" if total_runtime > 0 and processed_runtime >= total_runtime else "当前可发送收件人已处理完成"
                should_mark_finished = True
                logger.info("任务 %s 所有收件人已处理，准备标记完成", task_id)
                logger.info(
                    "任务 %s 预取确认到达列表末尾，总收件人数=%s，已发送=%s",
                    task_id,
                    total_runtime,
                    processed_runtime,
                )
                logger.info("任务 %s 所有收件人已发送完毕，准备退出", task_id)
                logger.info(
                    "任务 %s 收件人预取、发送与落库均已完成 processed=%s total=%s reason=%s",
                    task_id,
                    processed_runtime,
                    total_runtime,
                    finish_reason,
                )
                _finalize_main_loop_iteration(task_id, iter_started_at)
                break
            if recipient_queue.empty() and not prefetch_eof:
                await asyncio.sleep(PREFETCH_PAUSE_SLEEP_SECONDS)
                runtime["main_loop_expected_interval"] = PREFETCH_PAUSE_SLEEP_SECONDS
            elif recipient_queue.empty() and prefetch_eof:
                await asyncio.sleep(MAIN_LOOP_IDLE_SLEEP_SECONDS)
                runtime["main_loop_expected_interval"] = MAIN_LOOP_IDLE_SLEEP_SECONDS
            else:
                runtime["main_loop_expected_interval"] = 0.0
            _finalize_main_loop_iteration(task_id, iter_started_at)

        runtime_for_shutdown = _task_runtime_metrics.get(task_id)
        if str(finish_reason).startswith("状态已变更为 paused") or str(finish_reason).startswith("状态已变更为 stopped"):
            if prefetch_stop_event is not None:
                prefetch_stop_event.set()
            if prefetch_task is not None and not prefetch_task.done():
                try:
                    await asyncio.wait_for(prefetch_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("任务 %s 暂停/停止时预取协程停止超时，将取消预取并回滚已入队收件人", task_id)
                    prefetch_task.cancel()
                    await asyncio.gather(prefetch_task, return_exceptions=True)
            if completion_auditor_task is not None and not completion_auditor_task.done():
                completion_auditor_task.cancel()
                await asyncio.gather(completion_auditor_task, return_exceptions=True)
            await _drain_unsent_recipient_queue_for_resume(
                task_id,
                recipient_queue,
                inflight_emails,
                runtime_for_shutdown,
                reason=finish_reason,
            )
            if runtime_for_shutdown is not None:
                runtime_for_shutdown["claim_cursor_sealed_for_shutdown"] = True
            try:
                await _cancel_claim_cursor_writer(task_id)
                await _persist_runtime_claim_cursor_now(task_id, runtime_for_shutdown)
            except Exception:
                logger.exception("任务 %s 暂停/停止时持久化回滚游标失败", task_id)
        else:
            try:
                await flush_pending_claim_cursor(task_id)
            except Exception:
                logger.exception("任务 %s 刷写待持久化游标失败", task_id)

        async def _graceful_shutdown_active_sends() -> None:
            logger.debug("task %s 等待在途发送完成; reason=%s", task_id, finish_reason)
            if prefetch_stop_event is not None:
                prefetch_stop_event.set()
            if prefetch_task is not None and not prefetch_task.done():
                try:
                    await asyncio.wait_for(prefetch_task, timeout=WRITER_DRAIN_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    logger.warning("任务 %s 预取协程停止超时", task_id)
                    prefetch_task.cancel()
            if prefetch_guard_task is not None and not prefetch_guard_task.done():
                prefetch_guard_task.cancel()
                await asyncio.gather(prefetch_guard_task, return_exceptions=True)
            shutdown_deadline = monotonic() + GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS
            while int((_task_runtime_metrics.get(task_id) or {}).get("active_sends", 0)) > 0:
                if monotonic() >= shutdown_deadline:
                    break
                await asyncio.sleep(PREFETCH_PAUSE_SLEEP_SECONDS)
            if should_mark_finished:
                await _run_monitor_completion_trigger(task_id)
            else:
                await _maybe_trigger_monitor_emails(task_id)
            logger.debug("task %s 等待监控队列清空; reason=%s", task_id, finish_reason)
            try:
                await asyncio.wait_for(monitor_queue.join(), timeout=MONITOR_DRAIN_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                logger.error(
                    "任务 %s 监控队列清空超时 timeout=%ss pending=%s；主发送结果不受影响",
                    task_id,
                    MONITOR_DRAIN_TIMEOUT_SECONDS,
                    monitor_queue.qsize(),
                )
            live_worker_count = count_live_worker_tasks(workers)
            for _ in range(live_worker_count):
                try:
                    recipient_queue.put_nowait(None)
                except asyncio.QueueFull:
                    await recipient_queue.put(None)
            try:
                worker_control_queue.put_nowait(None)
            except asyncio.QueueFull:
                await worker_control_queue.put(None)
            if worker_scale_supervisor_task is not None and not worker_scale_supervisor_task.done():
                await asyncio.gather(worker_scale_supervisor_task, return_exceptions=True)
            if auto_tune_task is not None and not auto_tune_task.done():
                auto_tune_task.cancel()
                await asyncio.gather(auto_tune_task, return_exceptions=True)
            if backpressure_task is not None and not backpressure_task.done():
                backpressure_task.cancel()
                await asyncio.gather(backpressure_task, return_exceptions=True)
            for _ in monitor_workers:
                await monitor_queue.put(None)
            await asyncio.gather(*workers, return_exceptions=True)
            await asyncio.gather(*monitor_workers, return_exceptions=True)
            if send_log_writer is not None:
                await send_log_writer.flush_and_stop(timeout=WRITER_DRAIN_TIMEOUT_SECONDS)
                logger.info(
                    "任务 %s 监控链路收尾完成 configured=%s sent=%s failed=%s last_trigger=%s",
                    task_id,
                    int((_task_runtime_metrics.get(task_id) or {}).get("monitor_email_count", 0) or 0),
                    int((_task_runtime_metrics.get(task_id) or {}).get("monitor_sent", 0) or 0),
                    int((_task_runtime_metrics.get(task_id) or {}).get("monitor_failed", 0) or 0),
                    int((_task_runtime_metrics.get(task_id) or {}).get("last_monitor_trigger_count", 0) or 0),
                )
            else:
                logger.error("任务 %s SendLog 写入器未初始化，跳过优雅刷盘", task_id)

        graceful_total_timeout = GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS + MONITOR_DRAIN_TIMEOUT_SECONDS
        try:
            await asyncio.wait_for(_graceful_shutdown_active_sends(), timeout=graceful_total_timeout)
        except asyncio.TimeoutError:
            try:
                await _maybe_trigger_monitor_emails(task_id)
            except Exception:
                logger.exception("任务 %s 优雅停机超时清理前触发监控抽样失败", task_id)
            logger.error(
                "task %s graceful shutdown timed out after %ss; stopping workers and draining writer",
                task_id,
                graceful_total_timeout,
            )
            for worker in workers:
                worker.cancel()
            for worker in monitor_workers:
                worker.cancel()
            auxiliary_tasks = []
            if prefetch_task is not None and not prefetch_task.done():
                prefetch_task.cancel()
                auxiliary_tasks.append(prefetch_task)
            if prefetch_guard_task is not None and not prefetch_guard_task.done():
                prefetch_guard_task.cancel()
                auxiliary_tasks.append(prefetch_guard_task)
            if worker_scale_supervisor_task is not None and not worker_scale_supervisor_task.done():
                worker_scale_supervisor_task.cancel()
                auxiliary_tasks.append(worker_scale_supervisor_task)
            if auto_tune_task is not None and not auto_tune_task.done():
                auto_tune_task.cancel()
                auxiliary_tasks.append(auto_tune_task)
            if backpressure_task is not None and not backpressure_task.done():
                backpressure_task.cancel()
                auxiliary_tasks.append(backpressure_task)
            await asyncio.gather(*workers, return_exceptions=True)
            await asyncio.gather(*monitor_workers, return_exceptions=True)
            if auxiliary_tasks:
                await asyncio.gather(*auxiliary_tasks, return_exceptions=True)
            if send_log_writer is not None:
                await _drain_result_writer_for_task(task_id, timeout=WRITER_DRAIN_TIMEOUT_SECONDS)
        runtime = _task_runtime_metrics.get(task_id)
        if runtime is not None and int(runtime.get("last_batch_size", 0)) > 0:
            now_value = monotonic()
            started_value = float(runtime.get("last_batch_started_at", now_value))
            runtime["batch_count"] = int(runtime.get("batch_count", 0)) + 1
            runtime["last_batch_duration_ms"] = max(0, int((now_value - started_value) * 1000))
            runtime["last_batch_finished_at"] = now_value
        if should_mark_finished:
            await _mark_task_finished(task_id, finish_reason)
        elif finish_reason in {_FINISH_REASON_CHANNEL_DAILY_LIMIT, _FINISH_REASON_CHANNEL_HOURLY_LIMIT}:
            await _mark_task_paused(task_id, CHANNEL_LIMIT_PAUSE_MESSAGE)
        try:
            await _log_recipient_stats(task)
        except Exception:
            logger.exception("task %s finished but failed to write auxiliary recipient stats", task_id)
    except (TimeoutError, asyncio.TimeoutError) as exc:
        logger.warning(
            "任务 %s 操作超时（任务运行主循环），已自动重试/降级处理，任务将继续运行 error_type=%s",
            task_id,
            type(exc).__name__,
        )
        runtime = _task_runtime_metrics.get(task_id)
        if runtime is not None:
            runtime["task_timeout_recovery_count"] = int(runtime.get("task_timeout_recovery_count", 0) or 0) + 1
            runtime["last_timeout_operation"] = "任务运行主循环"
        try:
            await _mark_task_scheduled_waiting(task_id, "系统资源暂时繁忙，任务已自动延迟重试")
        except Exception:
            logger.exception("任务 %s 超时后的暂停状态写入失败，保留运行器清理流程", task_id)
    except asyncio.CancelledError:
        try:
            await _maybe_trigger_monitor_emails(task_id)
        except Exception:
            logger.exception("任务 %s 取消期间触发监控抽样失败", task_id)
        logger.warning("任务 %s 运行协程被取消", task_id)
        raise
    except Exception as exc:
        try:
            await _maybe_trigger_monitor_emails(task_id)
        except Exception:
            logger.exception("任务 %s 运行器异常后触发监控抽样失败", task_id)
        await _pause_task_for_unexpected_exception(task_id, exc, source="任务主循环")
    finally:
        try:
            await _maybe_trigger_monitor_emails(task_id)
        except Exception:
            logger.exception("任务 %s 运行器清理期间触发监控抽样失败", task_id)
        try:
            await flush_pending_claim_cursor(task_id)
        except Exception:
            logger.exception("任务 %s 清理阶段刷写游标失败", task_id)
        await release_task_concurrency(task_id)
        clear_task_performance_alert(task_id)
        if send_outcome_worker_acquired:
            try:
                await _release_send_outcome_worker()
            except Exception:
                logger.exception("任务 %s 发送结果 Worker 释放失败", task_id)
        if prefetch_stop_event is not None:
            prefetch_stop_event.set()
        if prefetch_task is not None and not prefetch_task.done():
            try:
                await asyncio.wait_for(prefetch_task, timeout=5.0)
            except asyncio.TimeoutError:
                prefetch_task.cancel()
                await asyncio.gather(prefetch_task, return_exceptions=True)
            except asyncio.CancelledError:
                prefetch_task.cancel()
                await asyncio.gather(prefetch_task, return_exceptions=True)
                raise
            except Exception:
                logger.exception("任务 %s 预取协程清理失败", task_id)
        if prefetch_guard_task is not None and not prefetch_guard_task.done():
            prefetch_guard_task.cancel()
            await asyncio.gather(prefetch_guard_task, return_exceptions=True)
        remaining_send_workers = [worker for worker in workers if not worker.done()]
        remaining_monitor_workers = [worker for worker in monitor_workers if not worker.done()]
        remaining_auxiliary_tasks = [
            worker
            for worker in (worker_scale_supervisor_task, auto_tune_task, backpressure_task, heartbeat_task, realtime_snapshot_task)
            if worker is not None and not worker.done()
        ]
        for worker in (*remaining_send_workers, *remaining_monitor_workers, *remaining_auxiliary_tasks):
            worker.cancel()
        if remaining_send_workers or remaining_monitor_workers or remaining_auxiliary_tasks:
            await asyncio.gather(
                *remaining_send_workers,
                *remaining_monitor_workers,
                *remaining_auxiliary_tasks,
                return_exceptions=True,
            )
        if send_log_writer is not None:
            try:
                await send_log_writer.flush_and_stop(timeout=WRITER_DRAIN_TIMEOUT_SECONDS)
            except Exception:
                logger.exception("任务 %s SendLog 写入器关闭失败", task_id)
        try:
            runtime_final = _task_runtime_metrics.get(task_id) or {}
            api_send_count = int(runtime_final.get("api_recipient_send_count", 0) or 0)
            initial_log_count = int(runtime_final.get("initial_regular_send_log_count", 0) or 0)
            send_log_count = await _count_regular_send_logs(task_id)
            expected_log_count = initial_log_count + api_send_count
            if send_log_count >= 0 and send_log_count != expected_log_count:
                logger.error(
                    "任务 %s 发送次数与 SendLog 不一致：本轮API发送=%s 启动时SendLog=%s 结束时SendLog=%s 期望=%s processed=%s",
                    task_id,
                    api_send_count,
                    initial_log_count,
                    send_log_count,
                    expected_log_count,
                    int(runtime_final.get("processed", 0) or 0),
                )
            else:
                logger.info(
                    "任务 %s 发送次数核对通过：本轮API发送=%s SendLog=%s（启动时=%s）",
                    task_id,
                    api_send_count,
                    send_log_count,
                    initial_log_count,
                )
        except Exception:
            logger.exception("任务 %s 发送次数核对失败", task_id)
        await release_task_lock(lock)
        if sync_sendgrid_manager is not None:
            try:
                sync_sendgrid_manager.close()
            except Exception:
                logger.exception("任务 %s 关闭 SendGrid 同步客户端失败", task_id)
        if monitor_sendgrid_manager is not None:
            try:
                monitor_sendgrid_manager.close()
            except Exception:
                logger.exception("任务 %s 关闭监控 SendGrid 同步客户端失败", task_id)
        try:
            shutdown_task_sender_pool(task_id)
            shutdown_task_sender_pool(monitor_sender_pool_task_id)
        except Exception:
            logger.exception("任务 %s 关闭独立 SenderPool 失败", task_id)
        logger.debug("任务 %s 运行协程已退出", task_id)
        await close_task_url_pool_cache(task_id)
        current = _running_tasks.get(task_id)
        if current is not None and current.done():
            _running_tasks.pop(task_id, None)
        _task_inflight_emails.pop(task_id, None)
        _task_shutdown_hooks.pop(task_id, None)
        _task_shutdown_requested.discard(int(task_id))
        _task_runtime_metrics.pop(task_id, None)
        _task_pid_controllers.pop(task_id, None)
        _task_queue_aware_states.pop(task_id, None)
        _clear_unsubscribe_cache_for_task(task_id)
        clear_task_status_watch(task_id)
        _restore_gc_for_task()


async def reset_task_progress_for_start(
    task_id: int,
    *,
    clear_logs: bool = True,
    preserve_status: bool = False,
) -> bool:
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            return False
        existing_regular_count = int(
            (
                await db.execute(
                    select(func.count())
                    .select_from(SendLog)
                    .where(SendLog.task_id == task_id, func.coalesce(SendLog.log_type, "regular") == "regular")
                )
            ).scalar_one()
            or 0
        )
        if clear_logs and existing_regular_count > 0:
            logger.warning(
                "task %s 已存在 %s 条热表常规发送日志，本次启动不会清空进度，避免重复发送；请使用显式重置功能重新发送",
                task_id,
                existing_regular_count,
            )
            clear_logs = False
        if clear_logs:
            await db.execute(delete(SendLog).where(SendLog.task_id == task_id))
            task.sent_count = 0
            task.failed_count = 0
            task.last_claimed_row_number = 0
            task.last_claimed_recipient_id = 0
        task.updated_at = datetime.utcnow()
        await db.commit()
        logger.debug(
            "task %s progress reset for start: clear_logs=%s sent=%s failed=%s cursor=%s",
            task_id,
            clear_logs,
            task.sent_count,
            task.failed_count,
            task.last_claimed_row_number,
        )
        return True


async def _mark_task_scheduled_waiting(
    task_id: int,
    message: str,
    *,
    scheduled_at: datetime | None = None,
    timezone: str | None = None,
) -> None:
    safe_message = sanitize_user_error_message(message)
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(Task).where(Task.id == int(task_id)))).scalar_one_or_none()
        if task is None:
            return
        task.status = "scheduled"
        task.run_message = safe_message
        if scheduled_at is not None:
            task.scheduled_at = scheduled_at
        if timezone is not None:
            task.timezone = timezone
        task.updated_at = datetime.utcnow()
        await db.commit()
    notify_task_status_changed(int(task_id), "scheduled")


def cancel_scheduled_start_retry(task_id: int) -> None:
    """任务暂停/停止时取消定时启动等待重试。"""
    task_key = int(task_id)
    _scheduled_start_retries.pop(task_key, None)
    _scheduled_dispatcher_tasks.discard(task_key)
    _scheduler_claimed_tasks.discard(task_key)


async def _try_claim_scheduler_task(task_id: int) -> bool:
    task_key = int(task_id)
    async with _scheduler_claimed_tasks_lock:
        if task_key in _scheduler_claimed_tasks:
            return False
        current = _running_tasks.get(task_key)
        if current is not None and not current.done():
            return False
        _scheduler_claimed_tasks.add(task_key)
        return True


async def _release_scheduler_task_claim(task_id: int) -> None:
    async with _scheduler_claimed_tasks_lock:
        _scheduler_claimed_tasks.discard(int(task_id))


def _ensure_scheduled_retry_snapshot(
    task_id: int,
    scheduled_at: datetime | None,
    timezone: str | None,
) -> ScheduledStartRetryState:
    task_key = int(task_id)
    entry = _scheduled_start_retries.get(task_key)
    if entry is None:
        entry = ScheduledStartRetryState(task_id=task_key)
        _scheduled_start_retries[task_key] = entry
    if entry.scheduled_at is None and scheduled_at is not None:
        entry.scheduled_at = scheduled_at
    if entry.timezone is None and timezone is not None:
        entry.timezone = str(timezone or DEFAULT_TASK_TIMEZONE)
    return entry


def _build_scheduled_retry_message(reason: str, attempts: int, max_attempts: int, interval_seconds: float) -> str:
    normalized_reason = str(reason or SCHEDULED_CHANNEL_BUSY_REASON).strip() or SCHEDULED_CHANNEL_BUSY_REASON
    return (
        f"{normalized_reason}，将于 {int(interval_seconds)} 秒后重试"
        f"（第 {attempts}/{max_attempts} 次）"
    )


async def _is_scheduled_start_waiting(task_id: int) -> bool:
    entry = _scheduled_start_retries.get(int(task_id))
    if entry is None:
        return False
    return monotonic() < float(entry.next_retry_at or 0)


async def _is_channel_busy_for_scheduled_send(channel_id: int, task_id: int) -> bool:
    channel_key = int(channel_id)
    if await is_warmup_lock_held(channel_key):
        return True
    if bool(getattr(settings, "allow_parallel_tasks_per_channel", False)):
        return False
    if await is_task_lock_held(channel_key):
        return True
    probe_lock = await try_acquire_task_lock(channel_key, int(task_id))
    if probe_lock is None:
        return True
    await release_task_lock(probe_lock)
    return False


async def _defer_scheduled_task_start(task_id: int, reason: str, lock: object | None = None) -> None:
    _scheduled_dispatcher_tasks.discard(int(task_id))
    await _release_scheduler_task_claim(int(task_id))
    if lock is not None:
        await release_task_lock(lock)
    entry = _scheduled_start_retries.get(int(task_id))
    await _enqueue_scheduled_start_retry(
        int(task_id),
        reason,
        scheduled_at=entry.scheduled_at if entry else None,
        timezone=entry.timezone if entry else None,
    )


async def _enqueue_scheduled_start_retry(
    task_id: int,
    reason: str,
    *,
    scheduled_at: datetime | None = None,
    timezone: str | None = None,
) -> None:
    task_key = int(task_id)
    interval_seconds = get_scheduled_start_retry_interval_seconds()
    max_attempts = get_scheduled_start_retry_max_attempts()
    entry = _ensure_scheduled_retry_snapshot(task_key, scheduled_at, timezone)
    entry.attempts += 1
    entry.reason = str(reason or "").strip() or SCHEDULED_CHANNEL_BUSY_REASON
    if entry.attempts > max_attempts:
        _scheduled_start_retries.pop(task_key, None)
        _scheduled_dispatcher_tasks.discard(task_key)
        if SCHEDULED_CHANNEL_BUSY_REASON in entry.reason or "通道" in entry.reason:
            await _mark_task_failed(task_key, "任务启动失败：通道被占用，重试耗尽")
            logger.error(
                "任务 %s 定时启动重试次数已用尽（%s 次）：通道被占用",
                task_key,
                max_attempts,
            )
        else:
            await _mark_task_aborted(
                task_key,
                f"定时启动失败：{entry.reason}（已重试 {max_attempts} 次）",
            )
            logger.error(
                "任务 %s 定时启动重试次数已用尽（%s 次），已标记为 aborted",
                task_key,
                max_attempts,
            )
        return
    entry.next_retry_at = monotonic() + interval_seconds
    _scheduled_start_retries[task_key] = entry
    retry_message = _build_scheduled_retry_message(entry.reason, entry.attempts, max_attempts, interval_seconds)
    await _mark_task_scheduled_waiting(
        task_key,
        retry_message,
        scheduled_at=entry.scheduled_at,
        timezone=entry.timezone,
    )
    logger.warning(
        "任务 %s 定时启动等待重试（%s/%s）：%s",
        task_key,
        entry.attempts,
        max_attempts,
        entry.reason,
    )


async def _atomic_claim_task_start(task_id: int) -> bool:
    """以单条 UPDATE ... RETURNING 抢占启动权；未返回行表示其他实例已获胜。"""
    task_key = int(task_id)
    now_utc = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        claimed_id = (
            await db.execute(
                update(Task)
                .where(
                    Task.id == task_key,
                    Task.status.in_(TASK_STARTABLE_STATUSES),
                    Task.deleted_at.is_(None),
                )
                .values(
                    status="starting",
                    runner_instance_id=INSTANCE_ID,
                    runner_lease_until=None,
                    run_generation=func.coalesce(Task.run_generation, 0) + 1,
                    updated_at=now_utc,
                )
                .returning(Task.id)
            )
        ).scalar_one_or_none()
        if claimed_id is None:
            await db.rollback()
            return False
        await db.commit()
    notify_task_status_changed(task_key, "starting")
    logger.info("任务启动权原子抢占成功 task_id=%s instance_id=%s", task_key, INSTANCE_ID)
    return True


async def _activate_task_runner_lease(task_id: int) -> bool:
    task_key = int(task_id)
    now_utc = datetime.utcnow()
    lease_until = now_utc + timedelta(seconds=TASK_RUNNER_LEASE_SECONDS)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            update(Task)
            .where(
                Task.id == task_key,
                Task.status == "starting",
                Task.runner_instance_id == INSTANCE_ID,
            )
            .values(
                status="running",
                scheduled_at=None,
                runner_lease_until=lease_until,
                updated_at=now_utc,
            )
        )
        if int(result.rowcount or 0) != 1:
            await db.rollback()
            return False
        await db.commit()
    notify_task_status_changed(task_key, "running")
    return True


async def _renew_task_runner_lease_loop(task_id: int) -> None:
    task_key = int(task_id)
    transient_failures = 0
    while True:
        try:
            await asyncio.sleep(TASK_RUNNER_LEASE_RENEW_INTERVAL_SECONDS)
            now_utc = datetime.utcnow()
            lease_until = now_utc + timedelta(seconds=TASK_RUNNER_LEASE_SECONDS)
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_key,
                        Task.status == "running",
                        Task.runner_instance_id == INSTANCE_ID,
                    )
                    .values(runner_lease_until=lease_until, updated_at=now_utc)
                )
                if int(result.rowcount or 0) == 1:
                    await db.commit()
                    if transient_failures:
                        logger.info("任务 Runner 租约续约已恢复 task_id=%s failures=%s", task_key, transient_failures)
                    transient_failures = 0
                    continue
                await db.rollback()
            async with AsyncSessionLocal() as verify_db:
                ownership = (
                    await verify_db.execute(
                        select(Task.status, Task.runner_instance_id).where(Task.id == task_key)
                    )
                ).one_or_none()
            if ownership is None:
                logger.error("任务 Runner 所属任务已删除，停止当前实例 task_id=%s instance_id=%s", task_key, INSTANCE_ID)
                _task_shutdown_requested.add(task_key)
                return
            status_value, owner_value = ownership
            if str(status_value or "").strip().lower() != "running" or str(owner_value or "") != INSTANCE_ID:
                logger.error(
                    "任务 Runner 所有权已被确认转移，停止当前实例 task_id=%s instance_id=%s current_status=%s current_owner=%s",
                    task_key,
                    INSTANCE_ID,
                    status_value,
                    owner_value,
                )
                _task_shutdown_requested.add(task_key)
                return
            transient_failures += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            transient_failures += 1
            runtime = _task_runtime_metrics.get(task_key)
            if runtime is not None:
                runtime["runner_lease_transient_failures"] = transient_failures
                runtime["runner_lease_last_error"] = f"{type(exc).__name__}: {exc}"[:300]
            _log_sampled_task_warning(
                f"runner_lease:{task_key}",
                "任务 %s Runner 租约续约瞬时失败 attempt=%s/%s error_type=%s error=%s；本地 Runner 继续运行",
                task_key,
                transient_failures,
                TASK_RUNNER_LEASE_MAX_TRANSIENT_FAILURES,
                type(exc).__name__,
                str(exc) or "无错误详情",
            )
        if transient_failures >= TASK_RUNNER_LEASE_MAX_TRANSIENT_FAILURES:
            logger.error(
                "任务 %s Runner 租约连续续约失败 %s 次，但未确认所有权转移；为避免基础设施抖动导致停发，当前实例继续运行并持续重试",
                task_key,
                transient_failures,
            )
            transient_failures = TASK_RUNNER_LEASE_MAX_TRANSIENT_FAILURES


async def _rollback_task_start_claim(task_id: int, *, fallback_status: str, message: str = "") -> bool:
    normalized_fallback = str(fallback_status or "failed").strip().lower() or "failed"
    values: dict[str, object] = {
        "status": normalized_fallback,
        "runner_instance_id": None,
        "runner_lease_until": None,
        "updated_at": datetime.utcnow(),
    }
    if message:
        values["run_message"] = sanitize_user_error_message(message)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            update(Task)
            .where(
                Task.id == int(task_id),
                Task.status == "starting",
                Task.runner_instance_id == INSTANCE_ID,
            )
            .values(**values)
        )
        if int(result.rowcount or 0) != 1:
            await db.rollback()
            return False
        await db.commit()
    notify_task_status_changed(int(task_id), normalized_fallback)
    return True


async def _set_task_status_running(task_id: int) -> bool:
    if not await _atomic_claim_task_start(task_id):
        return False
    return await _activate_task_runner_lease(task_id)


async def _launch_scheduled_task_runner(task_id: int, *, rollback_status: str = "scheduled") -> bool:
    """启动已由数据库原子抢占的定时任务 runner。"""
    task_key = int(task_id)
    if not await reset_task_progress_for_start(task_key, clear_logs=False, preserve_status=True):
        logger.error("任务调度器重置进度失败 task_id=%s", task_key)
        await _rollback_task_start_claim(task_key, fallback_status="failed", message="任务启动前进度校准失败")
        return False
    try:
        started = await start_task_runner(
            task_key,
            from_scheduled_dispatcher=True,
            startup_claimed=True,
            rollback_status=rollback_status,
        )
    except GlobalConcurrencyLimitError as exc:
        await _rollback_task_start_claim(task_key, fallback_status=rollback_status, message=exc.message)
        await _defer_scheduled_task_start(task_key, exc.message)
        return False
    except ChannelNotReadyError as exc:
        _scheduled_dispatcher_tasks.discard(task_key)
        _scheduled_start_retries.pop(task_key, None)
        await _rollback_task_start_claim(task_key, fallback_status="failed", message=exc.message)
        return False
    except Exception as exc:
        await _rollback_task_start_claim(task_key, fallback_status="failed", message=str(exc))
        raise
    if not started:
        _scheduled_dispatcher_tasks.discard(task_key)
        await _rollback_task_start_claim(task_key, fallback_status=rollback_status, message="运行协程未创建")
        logger.warning("任务 %s 定时调度未能启动运行协程（可能已在运行）", task_key)
    return bool(started)


async def _process_scheduled_start_retries() -> int:
    started = 0
    now_value = monotonic()
    interval_seconds = get_scheduled_start_retry_interval_seconds()
    max_attempts = get_scheduled_start_retry_max_attempts()
    pending_ids = [
        task_id
        for task_id, entry in list(_scheduled_start_retries.items())
        if now_value >= float(entry.next_retry_at or 0)
    ]
    for task_id in pending_ids:
        entry = _scheduled_start_retries.get(int(task_id))
        if entry is None:
            continue
        if not await _try_claim_scheduler_task(int(task_id)):
            continue
        async with AsyncSessionLocal() as db:
            row = (
                await db.execute(
                    select(Task.id, Task.channel_id, Task.status).where(Task.id == int(task_id))
                )
            ).one_or_none()
        if row is None:
            _scheduled_start_retries.pop(int(task_id), None)
            await _release_scheduler_task_claim(int(task_id))
            continue
        _, channel_id, task_status = row
        if str(task_status or "") not in {"scheduled", "pending"}:
            _scheduled_start_retries.pop(int(task_id), None)
            await _release_scheduler_task_claim(int(task_id))
            continue
        if channel_id is None:
            _scheduled_start_retries.pop(int(task_id), None)
            await _release_scheduler_task_claim(int(task_id))
            await _mark_task_failed(int(task_id), "通道不可用，无法启动任务")
            continue
        channel_key = int(channel_id)
        if await _is_channel_busy_for_scheduled_send(channel_key, int(task_id)):
            await _enqueue_scheduled_start_retry(
                int(task_id),
                SCHEDULED_CHANNEL_BUSY_REASON,
                scheduled_at=entry.scheduled_at,
                timezone=entry.timezone,
            )
            await _release_scheduler_task_claim(int(task_id))
            logger.info(
                "任务 %s 定时启动重试：通道 %s 仍被占用，%s 秒后再次尝试（第 %s/%s 次）",
                task_id,
                channel_key,
                int(interval_seconds),
                min(entry.attempts + 1, max_attempts),
                max_attempts,
            )
            continue
        health_error = await ensure_channel_ready_for_task_start(channel_key)
        if health_error:
            _scheduled_start_retries.pop(int(task_id), None)
            await _release_scheduler_task_claim(int(task_id))
            await _mark_task_failed(int(task_id), health_error)
            logger.error("任务 %s 定时启动重试被拒绝：%s", task_id, health_error)
            continue
        if not await _atomic_claim_task_start(int(task_id)):
            await _release_scheduler_task_claim(int(task_id))
            logger.warning("任务 %s 定时启动重试时未获得数据库启动权", task_id)
            continue
        try:
            runner_started = await _launch_scheduled_task_runner(int(task_id), rollback_status=str(task_status or "scheduled"))
        except Exception:
            await _release_scheduler_task_claim(int(task_id))
            raise
        if runner_started:
            _scheduled_start_retries.pop(int(task_id), None)
            started += 1
        else:
            await _release_scheduler_task_claim(int(task_id))
    return started


async def schedule_due_tasks() -> int:
    started = await _process_scheduled_start_retries()
    now_utc = datetime.now(timezone.utc)
    schedulable_statuses = ("scheduled", "pending")
    channels_reserved_this_round: set[int] = set()

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Task.id, Task.channel_id, Task.scheduled_at, Task.timezone)
            .where(
                Task.status.in_(schedulable_statuses),
                Task.scheduled_at.is_not(None),
                Task.deleted_at.is_(None),
            )
            .order_by(Task.scheduled_at.asc(), Task.id.asc())
        )
        due_candidates = list(result.all())

    claimed_task_ids: list[int] = []
    for task_id, channel_id, scheduled_at, task_timezone in due_candidates:
        due_utc = scheduled_local_to_utc(scheduled_at, task_timezone)
        next_run_utc = next_task_run_utc(scheduled_at, task_timezone, now_utc)
        if not is_task_due_now(scheduled_at, task_timezone, now_utc):
            logger.debug(
                "定时发送任务尚未到达启动时间 task_id=%s scheduled_at_local=%s timezone=%s due_utc=%s next_run_utc=%s",
                task_id,
                scheduled_at,
                task_timezone or DEFAULT_TASK_TIMEZONE,
                due_utc.isoformat(),
                next_run_utc.isoformat() if next_run_utc else "",
            )
            continue
        if not await _try_claim_scheduler_task(int(task_id)):
            continue
        if await _is_scheduled_start_waiting(int(task_id)):
            await _release_scheduler_task_claim(int(task_id))
            continue
        if channel_id is None:
            await _release_scheduler_task_claim(int(task_id))
            await _mark_scheduled_task_failed(int(task_id), "通道不可用，无法启动任务")
            continue
        channel_key = int(channel_id)
        health_error = await ensure_channel_ready_for_task_start(channel_key)
        if health_error:
            await _release_scheduler_task_claim(int(task_id))
            await _mark_scheduled_task_failed(int(task_id), health_error)
            continue
        if channel_key in channels_reserved_this_round or await _is_channel_busy_for_scheduled_send(channel_key, int(task_id)):
            _ensure_scheduled_retry_snapshot(int(task_id), scheduled_at, task_timezone)
            await _enqueue_scheduled_start_retry(
                int(task_id),
                SCHEDULED_CHANNEL_BUSY_REASON,
                scheduled_at=scheduled_at,
                timezone=task_timezone,
            )
            await _release_scheduler_task_claim(int(task_id))
            continue

        _ensure_scheduled_retry_snapshot(int(task_id), scheduled_at, task_timezone)

        if not await _atomic_claim_task_start(int(task_id)):
            await _release_scheduler_task_claim(int(task_id))
            continue
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(Task)
                .where(Task.id == int(task_id), Task.status == "starting", Task.runner_instance_id == INSTANCE_ID)
                .values(scheduled_at=None, updated_at=datetime.utcnow())
            )
            await db.commit()

        channels_reserved_this_round.add(channel_key)
        claimed_task_ids.append(int(task_id))
        logger.info(
            "定时调度已认领任务 task_id=%s channel_id=%s scheduled_at_local=%s timezone=%s due_utc=%s",
            task_id,
            channel_key,
            scheduled_at,
            task_timezone or DEFAULT_TASK_TIMEZONE,
            due_utc.isoformat(),
        )

    for task_id in claimed_task_ids:
        try:
            runner_started = await _launch_scheduled_task_runner(task_id, rollback_status="scheduled")
        except Exception:
            await _release_scheduler_task_claim(task_id)
            raise
        if runner_started:
            started += 1
        else:
            await _release_scheduler_task_claim(task_id)
    return started


async def start_scheduler_loop(poll_interval_seconds: int | None = None) -> None:
    from app.core.database import assert_database_tables_ready

    await assert_database_tables_ready()
    configured_interval = poll_interval_seconds if poll_interval_seconds is not None else getattr(settings, "scheduled_start_retry_interval_seconds", 60)
    interval = max(5, min(15, int(configured_interval or 60)))
    logger.debug("任务定时调度循环已启动，轮询间隔=%s 秒", interval)
    while True:
        try:
            started = await schedule_due_tasks()
            if started:
                logger.debug("定时调度本轮已启动 %s 个任务", started)
        except asyncio.CancelledError:
            logger.debug("任务定时调度循环已取消")
            raise
        except Exception:
            logger.exception("任务定时调度循环发生未预期异常")
        await asyncio.sleep(interval)


def get_mutable_task_runtime(task_id: int) -> dict[str, float | int] | None:
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is None:
        return None
    return runtime


def _runtime_snapshot_redis_key(task_id: int) -> str:
    return f"mercury:task_runtime_snapshot:{int(task_id)}"


async def publish_task_runtime_snapshot(task_id: int) -> dict[str, float | int]:
    snapshot = get_task_runtime_metrics(int(task_id))
    if not snapshot:
        return {}
    snapshot = _copy_runtime_snapshot(snapshot)
    try:
        await safe_redis_op(
            redis_background_client.setex(
                _runtime_snapshot_redis_key(task_id),
                max(90, int(TASK_STATE_SNAPSHOT_INTERVAL_SECONDS * 4)),
                json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")),
            ),
            fallback_value=None,
            op_name=f"发布任务 runtime 快照 task={int(task_id)}",
            timeout_seconds=min(0.5, REDIS_OPERATION_TIMEOUT_SECONDS),
            log_warning=False,
        )
    except Exception:
        logger.debug("任务 %s runtime 快照发布失败", task_id, exc_info=True)
    return snapshot


async def runtime_snapshot_sync_loop() -> None:
    """API/worker 进程共享 runtime 快照；只使用后台 Redis 客户端，不触碰发送槽。"""
    while True:
        try:
            async for key in redis_background_client.scan_iter(match="mercury:task_runtime_snapshot:*", count=200):
                raw = await safe_redis_op(
                    redis_background_client.get(key),
                    fallback_value=None,
                    op_name="同步任务 runtime 快照",
                    timeout_seconds=min(0.5, REDIS_OPERATION_TIMEOUT_SECONDS),
                    log_warning=False,
                )
                if not raw:
                    continue
                decoded = json.loads(raw)
                if not isinstance(decoded, dict):
                    continue
                task_key = int(str(key).rsplit(":", 1)[-1])
                if task_key not in _task_runtime_metrics:
                    _task_runtime_shared_snapshots[task_key] = decoded
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("runtime 共享快照同步暂时失败", exc_info=True)
        await asyncio.sleep(0.5)


async def get_shared_task_runtime_metrics(task_id: int) -> dict[str, float | int]:
    task_key = int(task_id)
    local = get_task_runtime_metrics(task_key)
    if local:
        return local
    try:
        raw = await safe_redis_op(
            redis_background_client.get(_runtime_snapshot_redis_key(task_key)),
            fallback_value=None,
            op_name=f"读取任务 runtime 快照 task={task_key}",
            timeout_seconds=min(0.5, REDIS_OPERATION_TIMEOUT_SECONDS),
            log_warning=False,
        )
        if raw:
            decoded = json.loads(raw)
            if isinstance(decoded, dict):
                _task_runtime_shared_snapshots[task_key] = decoded
                return _copy_runtime_snapshot(decoded)
    except Exception:
        logger.debug("任务 %s runtime 共享快照读取失败", task_key, exc_info=True)
    return {}


def _copy_runtime_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not snapshot:
        return {}
    copied: dict[str, Any] = {}
    for key, value in snapshot.items():
        if isinstance(value, (str, int, float, bool, type(None))):
            copied[key] = value
        elif isinstance(value, dict):
            copied[key] = _copy_runtime_snapshot(value)
        elif isinstance(value, (list, tuple, set, deque)):
            copied[key] = [
                _copy_runtime_snapshot(item) if isinstance(item, dict) else item
                for item in value
                if isinstance(item, (str, int, float, bool, type(None), dict))
            ]
        elif isinstance(value, (asyncio.Future, asyncio.Task, asyncio.Event, asyncio.Lock, asyncio.Queue)):
            continue
    return copied


def get_task_runtime_metrics(task_id: int) -> dict[str, float | int]:
    task_key = int(task_id)
    runtime = _task_runtime_metrics.get(task_key)
    if runtime is None:
        return _copy_runtime_snapshot(_task_runtime_shared_snapshots.get(task_key, {}))
    snapshot = {
        key: value
        for key, value in runtime.items()
        if key not in {
            "claim_slots_wake_event",
            "processed_recent_times",
            "success_recent_times",
            "realtime_rate_tracker",
            "monitor_local_inflight",
            "http_completion_times",
            "phase_timing_samples",
            "prefetch_fetch_events",
            "http_429_recent_events",
            "http_429_recent_times",
            "http_rps_history",
            "http_send_ms_samples",
            "writer_flush_ms_samples",
            "active_sends_lock",
            "worker_retire_lock",
        }
    }
    snapshot = _copy_runtime_snapshot(snapshot)
    now_value = monotonic()
    started_value = float(snapshot.get("started_at", now_value))
    elapsed_seconds = max(0.0, now_value - started_value)
    snapshot["elapsed_seconds"] = elapsed_seconds
    realtime_sample = _sample_realtime_rate(runtime, at=now_value)
    instant_success_rate = float(realtime_sample["success_rate"])
    instant_current_rate = float(realtime_sample["processed_rate"])
    rolling_success_rate, rolling_current_rate = _compute_rolling_rates(runtime)
    snapshot["success_rate"] = rolling_success_rate
    snapshot["current_rate"] = instant_current_rate
    snapshot["sdk_current_rate_1s"] = instant_current_rate
    snapshot["sdk_success_rate_1s"] = instant_success_rate
    snapshot["sdk_processed_last_1s"] = max(0, int(realtime_sample["processed_count"]))
    snapshot["sdk_success_last_1s"] = max(0, int(realtime_sample["success_count"]))
    snapshot["sdk_realtime_window_seconds"] = float(realtime_sample["window_seconds"])
    snapshot["sdk_realtime_sampled_at"] = float(realtime_sample["sampled_at"])
    snapshot["sdk_raw_processed_rate_5s"] = float(realtime_sample["raw_processed_rate"])
    snapshot["sdk_raw_success_rate_5s"] = float(realtime_sample["raw_success_rate"])
    snapshot["current_rate_10s"] = rolling_current_rate
    snapshot["sdk_current_rate_10s"] = rolling_current_rate
    snapshot["sdk_success_rate_10s"] = rolling_success_rate
    snapshot["inflight"] = max(0, int(snapshot.get("active_sends", 0)))
    snapshot["outstanding_writes"] = max(0, int(snapshot.get("outstanding_writes", 0)))
    snapshot["admitted_workers"] = int(_task_active_workers.get(task_id, snapshot.get("admitted_workers", 0)))
    snapshot["active_http_inflight"] = max(0, int(snapshot.get("httpx_inflight", 0)))
    snapshot["executing_tasks"] = get_executing_tasks_total()
    global_active_workers = get_admitted_workers_total()
    snapshot["process_active_send_workers"] = global_active_workers
    snapshot["global_admitted_workers"] = global_active_workers
    snapshot["global_concurrency_occupied"] = global_active_workers
    snapshot["global_warmup_concurrency_reserved"] = get_warmup_concurrency_reserved_count()
    snapshot["global_concurrency_limit"] = 0
    snapshot["global_concurrency_admission_enabled"] = False
    snapshot["event_loop_latency_ms"] = float(snapshot.get("event_loop_latency_ms", 0.0))
    snapshot["scheduling_delay_ms"] = float(snapshot.get("event_loop_latency_ms", 0.0))
    snapshot["main_loop_work_ms"] = float(snapshot.get("main_loop_work_ms", 0.0))
    snapshot["event_loop_tasks_count"] = int(snapshot.get("event_loop_tasks_count", 0))
    snapshot["instant_rate_1s"] = _compute_instant_http_rate(runtime)
    snapshot["live_success_count"] = int(snapshot.get("initial_sent_count", 0)) + int(snapshot.get("success", 0))
    snapshot["live_fail_count"] = int(snapshot.get("initial_failed_count", 0)) + int(snapshot.get("failed", 0))
    snapshot["last_429_count_per_minute"] = count_http_429_last_minute(runtime)
    snapshot["runtime_snapshot_instance"] = INSTANCE_ID
    snapshot["runtime_snapshot_at"] = round(monotonic(), 3)
    # 运行时字典包含 Queue、Event、Task 和 Future 等进程内对象；共享快照只能发布 JSON 标量/容器。
    safe_snapshot = _copy_runtime_snapshot(snapshot)
    _task_runtime_shared_snapshots[task_key] = safe_snapshot
    return safe_snapshot


def count_http_429_last_minute(runtime: dict[str, float | int] | None) -> int:
    if runtime is None:
        return 0
    recent_times = runtime.get("http_429_recent_times")
    if not isinstance(recent_times, deque):
        return 0
    now_value = monotonic()
    cutoff = now_value - 60.0
    return sum(1 for timestamp in recent_times if float(timestamp) >= cutoff)


def get_task_admitted_workers(task_id: int) -> int:
    runtime = _task_runtime_metrics.get(int(task_id))
    if runtime is not None:
        return int(runtime.get("admitted_workers", _task_active_workers.get(int(task_id), 0)))
    return int(_task_active_workers.get(int(task_id), 0))


def is_task_runner_active(task_id: int) -> bool:
    current = _running_tasks.get(task_id)
    return current is not None and not current.done()


_CHANNEL_UNAUTHORIZED_TASK_STOP_IN_PROGRESS: set[int] = set()


async def abort_task_for_channel_unauthorized(task_id: int) -> bool:
    """通道 API 密钥失效时停止发送任务（含运行中 Runner）。"""
    normalized_id = int(task_id or 0)
    if normalized_id <= 0:
        return False
    if normalized_id in _CHANNEL_UNAUTHORIZED_TASK_STOP_IN_PROGRESS:
        return False
    _CHANNEL_UNAUTHORIZED_TASK_STOP_IN_PROGRESS.add(normalized_id)
    try:
        status = str(await _get_task_status(normalized_id) or "").strip().lower()
        if status in {"completed", "failed", "aborted", "finished", "draft", "cancelled", "stopped"}:
            return False
        if status not in {"running", "paused"}:
            return False

        logger.error("通道 API 密钥已失效，自动停止任务：%s", normalized_id)
        cancel_scheduled_start_retry(normalized_id)
        await _mark_task_aborted(normalized_id, "通道 API 密钥已失效，自动停止任务")
        if is_task_runner_active(normalized_id):
            asyncio.create_task(
                cancel_task_runner(normalized_id),
                name=f"cancel-task-{normalized_id}-channel-unauthorized",
            )
        return True
    finally:
        _CHANNEL_UNAUTHORIZED_TASK_STOP_IN_PROGRESS.discard(normalized_id)


async def _recover_legacy_time_error_task(task_id: int) -> bool:
    """兼容曾被 datetime.time 名称冲突误标失败的旧任务。"""
    task_key = int(task_id)
    legacy_signature = "datetime.time' has no attribute 'time"
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            update(Task)
            .where(
                Task.id == task_key,
                Task.status == "failed",
                Task.run_message.contains(legacy_signature),
                Task.deleted_at.is_(None),
            )
            .values(
                status="pending",
                run_message="检测到旧版本时间调用异常，已自动恢复为待启动状态",
                runner_instance_id=None,
                runner_lease_until=None,
                updated_at=datetime.utcnow(),
            )
        )
        recovered = int(result.rowcount or 0) > 0
        if recovered:
            await db.commit()
        else:
            await db.rollback()
    if recovered:
        logger.warning("任务 %s 检测到旧版本 datetime.time 调用异常，已自动重置为待启动状态并保留发送进度", task_key)
        notify_task_status_changed(task_key, "pending")
    return recovered


async def start_task_runner(
    task_id: int,
    force: bool = False,
    concurrency: int | None = None,
    from_scheduled_dispatcher: bool = False,
    reset_progress_on_start: bool = False,
    startup_claimed: bool = False,
    rollback_status: str | None = None,
) -> bool:
    fallback_status = str(rollback_status or "failed").strip().lower() or "failed"
    await _recover_legacy_time_error_task(task_id)
    current = _running_tasks.get(task_id)
    if current is not None and not current.done():
        if not force:
            logger.debug("任务 %s 运行协程已在运行中", task_id)
            return False
        logger.warning("任务 %s 强制重启运行协程", task_id)
        if not await wait_for_task_runner_exit(task_id, RUNNER_EXIT_WAIT_SECONDS):
            logger.warning("任务 %s 等待旧运行协程退出超时，将强制取消", task_id)
            await cancel_task_runner(task_id)
    elif current is not None:
        _running_tasks.pop(task_id, None)
    if not startup_claimed:
        if not await _atomic_claim_task_start(task_id):
            logger.info("任务启动权原子抢占失败，其他实例或请求已处理 task_id=%s", task_id)
            return False
        startup_claimed = True
    if reset_progress_on_start:
        reset_ok = await reset_task_progress_for_start(
            task_id,
            clear_logs=True,
            preserve_status=True,
        )
        if not reset_ok:
            logger.error("任务 %s 手动启动前重置发送进度失败：任务不存在", task_id)
            await _rollback_task_start_claim(task_id, fallback_status="failed", message="任务启动前进度重置失败")
            return False
        logger.info("任务 %s 手动启动前已清空历史 SendLog 并重置领取游标", task_id)
    if concurrency is None:
        async with AsyncSessionLocal() as db:
            loaded = (
                await db.execute(
                    select(Task.concurrency, Task.rate_limit, Task.send_http_batch_size).where(Task.id == task_id)
                )
            ).one_or_none()
        if loaded is None:
            if startup_claimed:
                await _rollback_task_start_claim(task_id, fallback_status=fallback_status, message="任务不存在")
            return False
        loaded_concurrency, loaded_rate_limit, loaded_batch_size = loaded
        concurrency = normalize_task_concurrency(loaded_concurrency)
        recommended, guidance_warning = resolve_task_concurrency_guidance(
            concurrency,
            float(loaded_rate_limit or 0),
            resolve_send_http_batch_size_value(loaded_batch_size),
        )
        if guidance_warning:
            logger.warning(
                "task %s start preflight concurrency guidance recommended=%s: %s",
                task_id,
                recommended,
                guidance_warning,
            )
    else:
        async with AsyncSessionLocal() as db:
            loaded = (
                await db.execute(
                    select(Task.rate_limit, Task.send_http_batch_size).where(Task.id == task_id)
                )
            ).one_or_none()
        if loaded is None:
            if startup_claimed:
                await _rollback_task_start_claim(task_id, fallback_status=fallback_status, message="任务不存在")
            return False
        loaded_rate_limit, loaded_batch_size = loaded
        recommended, guidance_warning = resolve_task_concurrency_guidance(
            concurrency,
            float(loaded_rate_limit or 0),
            resolve_send_http_batch_size_value(loaded_batch_size),
        )
        if guidance_warning:
            logger.warning(
                "task %s start preflight concurrency guidance recommended=%s: %s",
                task_id,
                recommended,
                guidance_warning,
            )
    capped_concurrency = cap_task_effective_concurrency(concurrency)
    try:
        await refresh_recipient_statistics_for_task_start(task_id)
        async with AsyncSessionLocal() as db:
            channel_id = (await db.execute(select(Task.channel_id).where(Task.id == task_id))).scalar_one_or_none()
        if channel_id is not None:
            health_error = await ensure_channel_ready_for_task_start(int(channel_id))
            if health_error:
                raise ChannelNotReadyError(health_error)
            register_task_channel(task_id, int(channel_id))
            await warn_if_channel_shares_proxy_endpoint(int(channel_id))
    except ChannelNotReadyError as exc:
        await _release_scheduler_task_claim(task_id)
        await _rollback_task_start_claim(task_id, fallback_status=fallback_status, message=exc.message)
        raise
    except Exception as exc:
        await _release_scheduler_task_claim(task_id)
        await _rollback_task_start_claim(task_id, fallback_status=fallback_status, message=str(exc))
        logger.exception("任务 %s 启动前通道/代理检查失败", task_id)
        raise TaskServiceUnavailableError("启动前通道或代理检查失败，请稍后重试") from exc
    if from_scheduled_dispatcher:
        _scheduled_dispatcher_tasks.add(task_id)
    if not await _activate_task_runner_lease(task_id):
        await _release_scheduler_task_claim(task_id)
        await _rollback_task_start_claim(task_id, fallback_status=fallback_status, message="无法切换为运行中状态")
        logger.warning("任务 %s 无法从 starting 切换为 running，启动权可能已失效", task_id)
        return False
    await _release_scheduler_task_claim(task_id)
    task = asyncio.create_task(run_task(task_id), name=f"mercury-task-runner-{task_id}")
    _running_tasks[task_id] = task
    lease_task = asyncio.create_task(
        _renew_task_runner_lease_loop(task_id),
        name=f"mercury-task-lease-{task_id}",
    )
    _task_lease_tasks[task_id] = lease_task
    _scheduled_start_retries.pop(task_id, None)
    task.add_done_callback(lambda finished_task: _cleanup_runner(task_id, finished_task))
    logger.debug(
        "task %s background asyncio task created concurrency=%s process_active_send_workers=%s global_admission=false",
        task_id,
        capped_concurrency,
        get_active_workers_total(),
    )
    return True
