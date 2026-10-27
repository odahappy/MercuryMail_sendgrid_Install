import asyncio
import json
import logging
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from starlette import status
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, computed_field, field_validator
from sqlalchemy import delete as sql_delete, exists, func, select
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import ensure_export_operation_no_audit, require_admin, require_admin_token_only
from app.core.database import AsyncSessionLocal, get_db, is_database_pool_pressure_error, maybe_warn_database_pool_pressure
from app.core.redis import redis_client
from app.models.models import Channel, Proxy, Recipient, RecipientList, SendLog, Task, Template, Unsubscribe, URLPool
from app.services.locker import CHANNEL_TASK_BUSY_MESSAGE, RedisLockUnavailableError, is_task_lock_held, is_warmup_lock_held
from app.services.recipient_service import normalize_recipient_row_numbers
from app.services.channel_manager import build_concurrency_warning, parse_sender_emails, peek_last_sender, recommend_task_concurrency
from app.services.connection_manager import validate_sendgrid_api_key_proxy_consistency
from app.services.export_service import (
    ExportScopeError,
    ExportTaskNotFoundError,
    build_task_failed_sql,
    build_task_success_sql,
    build_task_unset_sql,
    dispatch_smart_export,
)
from app.utils.export import ExportCopyError, format_export_error_message
from app.utils.task_status import resolve_task_status_label
from app.services.task_stats_service import build_task_perf, build_task_stats, list_task_failures
from app.services.channel_health import ensure_channel_ready_for_task_start
from app.services.dashboard_stats_service import invalidate_dashboard_stats_cache
from app.services.template_service import validate_task_url_pool_binding
from app.services.task_runner import (
    ChannelNotReadyError,
    GlobalConcurrencyLimitError,
    DEFAULT_SEND_HTTP_BATCH_SIZE,
    cancel_scheduled_start_retry,
    cancel_task_runner,
    cap_task_effective_concurrency,
    check_global_concurrency_limit,
    estimate_http_requests_per_second,
    get_send_http_batch_size,
    get_task_concurrency_guidance_from_runtime,
    get_task_performance_alert,
    get_task_runtime_metrics,
    cleanup_task_runtime_state,
    is_task_due_now,
    is_task_runner_active,
    notify_task_status_changed,
    parse_monitor_emails_from_task,
    reconcile_task_resume_state,
    resolve_send_http_batch_size_value,
    schedule_due_tasks,
    start_task_runner,
    TaskServiceUnavailableError,
    validate_send_http_batch_config,
    validate_task_concurrency_config,
    wait_for_task_runner_exit,
)

router = APIRouter(prefix="/api/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)
_TASK_STATS_CACHE_TTL_SECONDS = 5.0
_TASK_PERF_CACHE_TTL_SECONDS = 5.0
_task_stats_cache: dict[int, tuple[float, dict]] = {}
_task_perf_cache: dict[int, tuple[float, dict]] = {}


def _active_task_clause():
    """仅包含未软删除任务；关联资源已删除的历史任务仍可只读展示。"""
    return Task.deleted_at.is_(None)


def _ensure_task_visible(task: Task | None) -> Task:
    if task is None or task.deleted_at is not None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在")
    return task


def _resolve_admin_operator(admin_payload: dict | None) -> str:
    if not isinstance(admin_payload, dict):
        return "未知"
    username = str(admin_payload.get("sub") or "").strip()
    if username:
        return username
    user_id = admin_payload.get("user_id")
    if user_id is not None:
        return f"管理员#{user_id}"
    return "管理员"


async def _ensure_task_channel_valid(db: AsyncSession, task: Task) -> None:
    channel_id = int(task.channel_id or 0)
    if channel_id <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务未绑定有效通道")
    channel = await db.get(Channel, channel_id)
    if channel is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务绑定的通道不存在或已删除")
    if not bool(channel.enabled) or not bool(channel.is_active):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务绑定的通道已禁用")


def _validate_task_startable(task: Task) -> None:
    current = str(task.status or "draft").strip().lower()
    if current == "running":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=TASK_ALREADY_RUNNING_MESSAGE)
    if current == "starting":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="任务正在启动中，请稍后重试")
    if current in {"finished", "completed"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已完成，无法重新启动")
    if current == "aborted":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已终止，无法重新启动")
    if current == "cancelled":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已取消，无法重新启动")
    if current in {"paused", "stopped"} and int(task.sent_count or 0) > 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="任务已有发送进度，请使用继续/恢复接口断点续发",
        )
    if current not in {"draft", "scheduled", "failed", "stopped", "paused"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"当前状态为「{resolve_task_status_label(current)}」，无法启动任务",
        )


def _batch_start_skip_reason(current_status: str) -> str | None:
    """返回批量启动不可执行时的明确中文原因；None 表示允许启动。"""
    normalized = str(current_status or "").strip().lower()
    if normalized in {"draft", "scheduled", "pending", "paused", "stopped", "failed"}:
        return None
    if normalized == "running":
        return "任务已在运行中"
    if normalized == "starting":
        return "任务正在启动中，请稍后重试"
    if normalized in {"finished", "completed"}:
        return "任务已结束，无法启动"
    if normalized == "aborted":
        return "任务已终止，无法启动"
    if normalized == "cancelled":
        return "任务已取消，无法启动"
    return f"当前状态为「{resolve_task_status_label(normalized or '未知')}」，无法启动任务"


def _validate_task_pausable(task: Task) -> None:
    current = str(task.status or "").strip().lower()
    if current != "running":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"当前状态为「{resolve_task_status_label(current)}」，仅运行中的任务可以暂停",
        )


def _validate_task_stoppable(task: Task) -> None:
    current = str(task.status or "").strip().lower()
    if current == "stopped":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已停止，无需重复操作")
    if current in {"finished", "completed"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已完成，无法停止")
    if current == "aborted":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已终止，无法停止")
    if current == "draft":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="草稿任务尚未启动，无法停止")
    if current == "cancelled":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务已取消，无法停止")
    if current not in {"running", "paused", "scheduled", "pending", "failed"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"当前状态为「{resolve_task_status_label(current)}」，无法停止任务",
        )


DEFAULT_TASK_TIMEZONE = "Asia/Shanghai"
SUPPORTED_TASK_TIMEZONES = {"Asia/Shanghai", "Asia/Tokyo"}
RECIPIENT_LIST_OCCUPIED_MESSAGE = "该收件人池已被其他运行中的任务使用，请选择其他收件人池。"
TASK_STATUS_QUERY_TIMEOUT_SECONDS = 5
TASK_STATUS_SENDER_PEEK_TIMEOUT_SECONDS = 2
OCCUPYING_TASK_STATUSES = {"running", "paused"}
TASK_ALREADY_RUNNING_MESSAGE = "任务已在运行中，请勿重复启动"
TASK_DB_BUSY_MESSAGE = "数据库繁忙，请稍后重试"
TASK_REDIS_BUSY_MESSAGE = "锁服务暂时不可用，请稍后重试"
_task_status_response_cache: dict[int, dict[str, object]] = {}


def _cache_task_status_response(task_id: int, response: "TaskStatusResponse") -> None:
    try:
        _task_status_response_cache[task_id] = response.model_dump()
    except AttributeError:
        _task_status_response_cache[task_id] = response.model_dump()


def _build_timeout_fallback_status(task_id: int, error_message: str) -> "TaskStatusResponse":
    cached = dict(_task_status_response_cache.get(task_id) or {})
    runtime = get_task_runtime_metrics(task_id)
    live_success_count = int(
        runtime.get("live_success_count", cached.get("live_success_count", cached.get("success_count", 0)))
    )
    live_fail_count = int(
        runtime.get("live_fail_count", cached.get("live_fail_count", cached.get("fail_count", 0)))
    )
    success_count = int(cached.get("success_count", live_success_count))
    fail_count = int(cached.get("fail_count", live_fail_count))
    processed_count = int(cached.get("processed_count", success_count + fail_count))
    total_recipients = int(cached.get("total_recipients", 0))
    progress_percent = float(cached.get("progress_percent", 0))
    if total_recipients > 0 and processed_count > 0:
        progress_percent = round((processed_count / total_recipients) * 100, 2)
    return TaskStatusResponse(
        task_id=task_id,
        status="unknown",
        success_count=success_count,
        fail_count=fail_count,
        live_success_count=live_success_count,
        live_fail_count=live_fail_count,
        processed_count=processed_count,
        total_recipients=total_recipients,
        progress_percent=progress_percent,
        current_rate=float(runtime.get("current_rate", cached.get("current_rate", 0))),
        instant_rate_1s=float(runtime.get("instant_rate_1s", cached.get("instant_rate_1s", 0))),
        success_rate=float(runtime.get("success_rate", cached.get("success_rate", 0))),
        batch_count=int(runtime.get("batch_count", cached.get("batch_count", 0))),
        last_batch_size=int(runtime.get("last_batch_size", cached.get("last_batch_size", 0))),
        last_claim_size=int(runtime.get("last_claim_size", cached.get("last_claim_size", 0))),
        current_batch_size=int(runtime.get("last_claim_size", cached.get("current_batch_size", 0))),
        last_batch_duration_ms=int(runtime.get("last_batch_duration_ms", cached.get("last_batch_duration_ms", 0))),
        last_claim_ms=float(runtime.get("last_claim_ms", cached.get("last_claim_ms", 0))),
        last_http_send_ms=float(runtime.get("last_http_send_ms", cached.get("last_http_send_ms", 0))),
        last_build_batch_ms=float(runtime.get("last_build_batch_ms", cached.get("last_build_batch_ms", 0))),
        http_send_p99_ms=float(runtime.get("http_send_p99_ms", cached.get("http_send_p99_ms", 0))),
        result_queue_peak_after_batch=int(
            runtime.get("result_queue_peak_after_batch", cached.get("result_queue_peak_after_batch", 0))
        ),
        avg_http_send_ms=float(runtime.get("avg_http_send_ms", cached.get("avg_http_send_ms", 0))),
        batch_efficiency=float(runtime.get("batch_efficiency", cached.get("batch_efficiency", 0))),
        batch_size_optimization_hint=str(
            runtime.get("batch_size_optimization_hint", cached.get("batch_size_optimization_hint", ""))
        ),
        suggested_send_http_batch_size=int(
            runtime.get("suggested_send_http_batch_size", cached.get("suggested_send_http_batch_size", 0))
        ),
        slow_http_count=int(runtime.get("slow_http_count", cached.get("slow_http_count", 0))),
        http_timeout_count=int(runtime.get("http_timeout_count", cached.get("http_timeout_count", 0))),
        rate_limit=float(cached.get("rate_limit", 0)),
        elapsed_seconds=float(runtime.get("elapsed_seconds", cached.get("elapsed_seconds", 0))),
        inflight_count=int(runtime.get("inflight", cached.get("inflight_count", 0))),
        outstanding_writes=int(runtime.get("outstanding_writes", cached.get("outstanding_writes", 0))),
        inflight_current=int(runtime.get("inflight", cached.get("inflight_current", 0))),
        async_send_active_requests=int(
            runtime.get("async_send_active_requests", cached.get("async_send_active_requests", 0))
        ),
        result_queue_size=int(runtime.get("result_queue_size", cached.get("result_queue_size", 0))),
        writer_last_flush_ms=float(runtime.get("writer_last_flush_ms", cached.get("writer_last_flush_ms", 0))),
        writer_flush_p99_ms=float(runtime.get("writer_flush_p99_ms", cached.get("writer_flush_p99_ms", 0))),
        http_429_count=int(runtime.get("http_429_count", cached.get("http_429_count", 0))),
        result_queue_backpressure_count=int(
            runtime.get("result_queue_backpressure_count", cached.get("result_queue_backpressure_count", 0))
        ),
        rate_limit_per_second=float(runtime.get("rate_limit_per_second", cached.get("rate_limit_per_second", 0))),
        avg_batch_rate=float(cached.get("avg_batch_rate", 0)),
        global_concurrency_reserved=int(
            runtime.get("global_concurrency_reserved", cached.get("global_concurrency_reserved", 0))
        ),
        global_concurrency_occupied=int(
            runtime.get("global_concurrency_occupied", cached.get("global_concurrency_occupied", 0))
        ),
        global_concurrency_limit=int(
            runtime.get("global_concurrency_limit", cached.get("global_concurrency_limit", 0))
        ),
        event_loop_latency_ms=float(runtime.get("event_loop_latency_ms", cached.get("event_loop_latency_ms", 0))),
        main_loop_work_ms=float(runtime.get("main_loop_work_ms", cached.get("main_loop_work_ms", 0))),
        scheduling_delay_ms=float(
            runtime.get("scheduling_delay_ms", cached.get("scheduling_delay_ms", 0))
        ),
        event_loop_tasks_count=int(
            runtime.get("event_loop_tasks_count", cached.get("event_loop_tasks_count", 0))
        ),
        current_sender_name=str(runtime.get("current_sender_name", cached.get("current_sender_name", ""))),
        current_sender_email=str(runtime.get("current_sender_email", cached.get("current_sender_email", ""))),
        error_message=error_message,
        run_message=str(cached.get("run_message", "")),
        status_reason=str(cached.get("status_reason", cached.get("run_message", ""))),
        recommended_concurrency=cached.get("recommended_concurrency"),
        concurrency_warning=cached.get("concurrency_warning"),
        http_recipients_per_request=int(
            cached.get("http_recipients_per_request", cached.get("send_http_batch_size", DEFAULT_SEND_HTTP_BATCH_SIZE))
        ),
        estimated_http_requests_per_second=float(
            cached.get("estimated_http_requests_per_second", runtime.get("estimated_http_requests_per_second", 0))
        ),
        send_http_batch_size=int(cached.get("send_http_batch_size", DEFAULT_SEND_HTTP_BATCH_SIZE)),
        send_http_batch_enabled=bool(
            cached.get("send_http_batch_enabled", runtime.get("send_http_batch_enabled", False))
        ),
        effective_concurrency=int(
            runtime.get("effective_concurrency", cached.get("effective_concurrency", cached.get("concurrency", 0)))
        ),
        configured_concurrency=int(
            runtime.get("configured_concurrency", cached.get("configured_concurrency", 0))
        ),
        min_concurrency=int(runtime.get("min_concurrency", cached.get("min_concurrency", 0))),
        prefetch_limit=int(runtime.get("prefetch_limit", cached.get("prefetch_limit", 0))),
        proxy_rtt_ms=round(
            float(runtime.get("proxy_rtt_seconds", cached.get("proxy_rtt_seconds", 0))) * 1000.0,
            1,
        ),
    )


def _is_transient_task_control_db_error(exc: Exception) -> bool:
    from app.core.database import _is_transient_db_error

    return isinstance(exc, (OperationalError, DBAPIError, asyncio.TimeoutError)) or is_database_pool_pressure_error(exc) or _is_transient_db_error(exc)


def _raise_task_control_http(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, GlobalConcurrencyLimitError):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=exc.message) from exc
    if isinstance(exc, ChannelNotReadyError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message) from exc
    if isinstance(exc, TaskServiceUnavailableError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=exc.message) from exc
    if isinstance(exc, RedisLockUnavailableError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=TASK_REDIS_BUSY_MESSAGE) from exc
    if _is_transient_task_control_db_error(exc):
        maybe_warn_database_pool_pressure(force=True)
        logger.warning(
            "task control transient database error error_type=%s error=%s",
            type(exc).__name__,
            exc,
        )
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=TASK_DB_BUSY_MESSAGE) from exc
    logger.exception("任务控制接口发生未捕获异常 error_type=%s error=%s", type(exc).__name__, exc)
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="任务启动服务异常，请稍后重试或查看后端日志") from exc


async def _get_recipient_import_progress_status(recipient_list_id: int) -> tuple[str, str]:
    try:
        progress = await redis_client.hgetall(f"recipient_progress:{int(recipient_list_id)}")
    except Exception:
        logger.warning("读取收件人导入进度失败 list_id=%s", recipient_list_id, exc_info=True)
        return "", ""
    if not progress:
        return "", ""
    return str(progress.get("status") or "").strip().lower(), str(progress.get("error") or "").strip()


async def count_sendable_recipients(db: AsyncSession, recipient_list_id: int) -> int:
    normalized_list_id = int(recipient_list_id or 0)
    fixed_row_numbers = await normalize_recipient_row_numbers(db, normalized_list_id)
    if fixed_row_numbers > 0:
        await db.flush()
    unsubscribed_exists = exists(select(1).where(Unsubscribe.email == Recipient.email))
    return int(
        (
            await db.execute(
                select(func.count())
                .select_from(Recipient)
                .where(
                    Recipient.list_id == normalized_list_id,
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


async def ensure_recipient_list_available(db: AsyncSession, recipient_list_id: int, current_task_id: int | None = None) -> int:
    normalized_list_id = int(recipient_list_id or 0)
    if normalized_list_id <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务未绑定有效收件人池")
    recipient_list = await db.get(RecipientList, normalized_list_id)
    if recipient_list is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="收件人池不存在")
    progress_status, progress_error = await _get_recipient_import_progress_status(normalized_list_id)
    if progress_status in {"queued", "processing", "importing"}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="收件人池仍在导入中，请等待导入完成后再创建或启动任务")
    if progress_status == "failed":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=progress_error or "收件人池导入失败，请重新导入后再创建或启动任务")
    sendable_count = await count_sendable_recipients(db, normalized_list_id)
    if sendable_count <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="收件人池为空或没有可发送邮箱")
    cached_valid = int(recipient_list.valid_count or 0)
    cached_total = int(recipient_list.total_count or 0)
    if cached_valid != sendable_count or cached_total < sendable_count:
        recipient_list.valid_count = sendable_count
        recipient_list.total_count = max(cached_total, sendable_count)
        recipient_list.updated_at = datetime.utcnow()
        await db.flush()
    conditions = [Task.recipient_list_id == normalized_list_id, Task.status.in_(OCCUPYING_TASK_STATUSES)]
    if current_task_id is not None:
        conditions.append(Task.id != current_task_id)
    occupied_task_id = (await db.execute(select(Task.id).where(*conditions).limit(1))).scalar_one_or_none()
    if occupied_task_id is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=RECIPIENT_LIST_OCCUPIED_MESSAGE)
    return sendable_count


def normalize_task_timezone(value: str | None) -> str:
    timezone_name = (value or DEFAULT_TASK_TIMEZONE).strip() or DEFAULT_TASK_TIMEZONE
    if timezone_name not in SUPPORTED_TASK_TIMEZONES:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="不支持的任务时区")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="服务器不支持该任务时区") from exc
    return timezone_name


_MONITOR_EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


class TaskBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=160)
    template_id: int
    recipient_list_id: int
    channel_id: int
    url_pool_id: int | None = None
    concurrency: int = Field(default=1, ge=1, le=1000)
    auto_concurrency: bool = Field(
        default=False,
        description="是否启用自适应并发调整；启用后运行期由系统根据速率与网络动态优化并发数。",
    )
    batch_size: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="数据库领取批次大小（claim batch），控制每次从收件人表拉取的数量；不是 SendGrid HTTP 批量发送大小。",
    )
    send_http_batch_size: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="SendGrid SDK 每 HTTP 请求携带的收件人数（Substitution 批量模式）。",
    )
    rate_limit: float = Field(
        default=0.0,
        ge=0,
        le=100000,
        description="发送速率（封/秒）。批量模式下 HTTP 请求频率约为 rate_limit / send_http_batch_size。",
    )
    retry_count: int = Field(default=3, ge=0, le=10)
    monitor_emails: list[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("monitor_emails", "monitor_recipients"),
        serialization_alias="monitor_emails",
    )
    monitor_interval: int = Field(default=100, ge=0, le=1000000)

    @field_validator("monitor_emails")
    @classmethod
    def normalize_monitor_emails(cls, value):
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_email in value or []:
            email = str(raw_email or "").strip()
            if not email:
                continue
            if len(email) > 254 or not _MONITOR_EMAIL_PATTERN.fullmatch(email):
                raise ValueError(f"监控邮箱格式无效：{email[:80]}")
            dedupe_key = email.lower()
            if dedupe_key not in seen:
                normalized.append(email)
                seen.add(dedupe_key)
        if len(normalized) > 100:
            raise ValueError("监控邮箱最多允许 100 个")
        return normalized

    unsubscribe_enabled: bool = False
    sender_rotation_enabled: bool = Field(
        default=False,
        description="是否启用发件人轮询；需通道配置多个启用发件人时生效。",
    )
    scheduled_at: datetime | None = None
    timezone: str = DEFAULT_TASK_TIMEZONE


class TaskCreate(TaskBase):
    model_config = ConfigDict(title="TaskCreate")


class TaskUpdate(TaskBase):
    model_config = ConfigDict(title="TaskUpdate")


def _validate_task_batch_payload(payload: TaskBase) -> None:
    batch_config_error = validate_send_http_batch_config(payload.rate_limit, payload.send_http_batch_size)
    if batch_config_error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=batch_config_error)
    concurrency_config_error = validate_task_concurrency_config(payload.concurrency)
    if concurrency_config_error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=concurrency_config_error)


async def _check_task_start_concurrency(task_id: int, concurrency: int | None) -> None:
    capped_concurrency = cap_task_effective_concurrency(concurrency)
    concurrency_limit_error = await check_global_concurrency_limit(capped_concurrency, exclude_task_id=task_id)
    if concurrency_limit_error:
        from app.services.dashboard_events import record_global_concurrency_rejection

        await record_global_concurrency_rejection()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=concurrency_limit_error)


class ProxyOut(BaseModel):
    id: int
    name: str
    type: str
    protocol: str
    host: str
    port: int
    username: str = ""
    is_active: bool


class ChannelOut(BaseModel):
    id: int
    name: str
    use_proxy: bool
    proxy_id: int | None = None
    proxy_name: str = ""
    proxy_address: str = "直连"
    proxy: ProxyOut | None = None


class MaskedSenderOut(BaseModel):
    name: str
    email: str
    enabled: bool = True


class TaskOut(BaseModel):
    id: int
    name: str
    template_id: int | None = None
    template_name: str
    recipient_list_id: int | None = None
    recipient_list_name: str
    channel_id: int | None = None
    channel_name: str
    channel: ChannelOut | None = None
    channel_senders: list[MaskedSenderOut] = Field(default_factory=list)
    use_proxy: bool = False
    proxy_latency_ms: float | None = None
    proxy_name: str = ""
    proxy_address: str = "直连"
    url_pool_id: int | None = None
    url_pool_name: str | None = None
    status: str
    status_label: str = ""
    run_message: str = ""
    status_reason: str = ""
    concurrency: int
    auto_concurrency: bool = False
    batch_size: int
    send_http_batch_size: int = 50
    rate_limit: float
    retry_count: int
    monitor_emails: list[str]
    monitor_interval: int
    total_count: int
    sent_count: int
    failed_count: int
    unsubscribe_enabled: bool
    sender_rotation_enabled: bool = False
    scheduled_at: datetime | None = None
    timezone: str = DEFAULT_TASK_TIMEZONE
    created_at: datetime
    updated_at: datetime
    recommended_concurrency: int | None = None
    concurrency_warning: str | None = None
    http_recipients_per_request: int = 50
    estimated_http_requests_per_second: float = 0


class TaskListResponse(BaseModel):
    items: list[TaskOut]
    total: int
    page: int
    page_size: int


class BulkStartRequest(BaseModel):
    task_ids: list[int] = Field(..., min_length=1, max_length=100)


class BulkStartItem(BaseModel):
    task_id: int
    success: bool
    status: str
    runner_started: bool = False
    message: str = ""

    @computed_field
    @property
    def status_label(self) -> str:
        return resolve_task_status_label(self.status)


class BulkStartResponse(BaseModel):
    items: list[BulkStartItem]
    started_count: int
    failed_count: int


class BatchTaskRequest(BaseModel):
    task_ids: list[int] = Field(..., min_length=1, max_length=100)

    @field_validator("task_ids")
    @classmethod
    def normalize_task_ids(cls, value):
        normalized = list(dict.fromkeys(int(task_id) for task_id in value if int(task_id) > 0))
        if not normalized:
            raise ValueError("请至少选择一个有效任务")
        return normalized


class BatchTaskFailure(BaseModel):
    task_id: int
    reason: str


class BatchTaskResponse(BaseModel):
    success: list[int]
    failed: list[BatchTaskFailure]
    skipped: list[BatchTaskFailure]


class TaskStatusResponse(BaseModel):
    task_id: int
    status: str
    status_label: str = ""
    success_count: int
    fail_count: int
    live_success_count: int = 0
    live_fail_count: int = 0
    processed_count: int
    total_recipients: int
    progress_percent: float
    current_rate: float = 0
    instant_rate_1s: float = 0
    success_rate: float = 0
    batch_count: int = 0
    last_batch_size: int = 0
    last_claim_size: int = 0  # 当前批次大小：最近一次 Claim 实际领取数量
    current_batch_size: int = 0
    last_batch_duration_ms: int = 0
    last_claim_ms: float = 0
    last_http_send_ms: float = 0
    last_build_batch_ms: float = 0
    http_send_p99_ms: float = 0
    result_queue_peak_after_batch: int = 0
    avg_http_send_ms: float = 0
    batch_efficiency: float = 0
    batch_size_optimization_hint: str = ""
    suggested_send_http_batch_size: int = 0
    slow_http_count: int = 0
    http_timeout_count: int = 0
    rate_limit: float = 0
    elapsed_seconds: float = 0
    inflight_count: int = 0
    outstanding_writes: int = 0
    inflight_current: int = 0
    async_send_active_requests: int = 0
    result_queue_size: int = 0
    writer_last_flush_ms: float = 0
    writer_flush_p99_ms: float = 0
    result_queue_backpressure_count: int = 0
    http_429_count: int = 0
    rate_limit_per_second: float = 0
    avg_batch_rate: float = 0
    global_concurrency_reserved: int = 0
    global_concurrency_occupied: int = 0
    global_concurrency_limit: int = 0
    event_loop_latency_ms: float = 0
    main_loop_work_ms: float = 0
    scheduling_delay_ms: float = 0
    event_loop_tasks_count: int = 0
    current_sender_name: str = ""
    current_sender_email: str = ""
    error_message: str = ""
    run_message: str = ""
    status_reason: str = ""
    recommended_concurrency: int | None = None
    concurrency_warning: str | None = None
    http_recipients_per_request: int = DEFAULT_SEND_HTTP_BATCH_SIZE
    estimated_http_requests_per_second: float = 0
    send_http_batch_size: int = DEFAULT_SEND_HTTP_BATCH_SIZE
    send_http_batch_enabled: bool = False
    effective_concurrency: int = 0
    configured_concurrency: int = 0
    min_concurrency: int = 0
    prefetch_limit: int = 0
    proxy_rtt_ms: float = 0
    current_rate_10s: float = 0
    sdk_current_rate_10s: float = 0
    sdk_current_rate_1s: float = 0
    sdk_success_rate_1s: float = 0
    sdk_processed_last_1s: int = 0
    sdk_success_last_1s: int = 0
    remaining_count: int = 0
    last_heartbeat: float = 0
    stalled: bool = False


class TaskPerformanceAlertResponse(BaseModel):
    alert_at: str | None = None
    reason: str | None = None
    actual_rate: float | None = None
    configured_rate: float | None = None
    threshold_rate: float | None = None


class TaskStatsResponse(BaseModel):
    total_recipients: int
    delivered: int
    soft_bounced_or_retrying: int
    hard_bounced: int
    unsubscribed: int
    pending: int
    success_sdk: int = 0
    failed_sdk: int = 0
    unsent_count: int = 0
    success: int = 0
    failed: int = 0
    soft_bounced: int = 0
    retrying: int = 0
    active_workers: int
    avg_http_latency_ms: float
    queue_prefetch_length: int
    queue_write_length: int
    last_429_count_per_minute: int


class Recent429Item(BaseModel):
    time: str
    retry_after: int = 0


class TaskPerfResponse(BaseModel):
    current_rate: float
    success_rate: float = 0
    rate_window_seconds: int = 10
    rate_window_success_count: int = 0
    rate_window_processed_count: int = 0
    realtime_rate: float = 0
    sdk_realtime_rate: float = 0
    avg_http_latency_ms: float
    active_workers: int
    configured_concurrency: int
    effective_concurrency: int = 0
    configured_user_concurrency: int = 0
    auto_concurrency_enabled: bool = False
    queue_prefetch_length: int
    queue_write_length: int
    last_429_count: int
    http_requests_per_second: float = 0
    send_http_batch_size: int = 50
    running: bool = False
    sdk_current_rate_1s: float = 0
    sdk_success_rate_1s: float = 0
    sdk_current_rate_10s: float = 0
    wait_token_ms: float = 0
    http_request_ms: float = 0
    db_write_ms: float = 0
    preprocess_ms: float = 0
    prefetch_queue_len: int = 0
    prefetch_fetch_count: int = 0
    prefetch_total_rows: int = 0
    write_queue_len: int = 0
    write_dropped: int = 0
    recent_429_list: list[Recent429Item] = []
    rate_limit_setting: float = 0
    max_theoretical_http_per_sec: float = 0
    suggested_action: str = ""
    http_rps_recent: list[float] = []
    prefetch_queue_empty_seconds: float = 0
    recent_429_count_2min: int = 0
    auto_concurrency: int = 0
    concurrency_reason: str = ""
    pid_history: list[dict] = Field(default_factory=list)
    pid_latest_reason: str = ""
    prefetch_queue_history: list = Field(default_factory=list)
    write_latency_percentiles: dict = Field(default_factory=dict)
    backpressure_alert: str = ""
    dynamic_rtt_scale_down_skipped: int = 0
    prefetch_limit_boosted: int = 0
    effective_prefetch_limit: int = 0
    inflight_email_count: int = 0
    last_claim_candidate_count: int = 0
    last_claim_skipped_reserved: int = 0
    last_claim_skipped_duplicate_candidate: int = 0
    last_claim_error: str = ""
    dynamic_rtt_scale_down_skipped: int = 0
    prefetch_limit_boosted: int = 0
    effective_prefetch_limit: int = 0


class TaskFailureItem(BaseModel):
    email: str
    error: str = ""
    created_at: datetime
    retry_count: int = 0
    sender_email: str | None = None


class TaskFailuresResponse(BaseModel):
    items: list[TaskFailureItem]
    total: int
    page: int
    page_size: int


def mask_sender_email(email: str) -> str:
    candidate = str(email or "").strip()
    if not candidate or "@" not in candidate:
        return candidate
    local_part, domain_part = candidate.split("@", 1)
    if len(local_part) <= 2:
        masked_local = f"{local_part[:1]}***" if local_part else "***"
    else:
        masked_local = f"{local_part[:2]}***{local_part[-1]}"
    return f"{masked_local}@{domain_part}"


def build_masked_channel_senders(channel: Channel | None) -> list[MaskedSenderOut]:
    if channel is None:
        return []
    senders: list[MaskedSenderOut] = []
    for item in parse_sender_emails(channel, active_only=False):
        email_value = str(item.get("email") or "").strip()
        if not email_value:
            continue
        senders.append(
            MaskedSenderOut(
                name=str(item.get("name") or "").strip(),
                email=mask_sender_email(email_value),
                enabled=bool(item.get("enabled", True)),
            )
        )
    if not senders and channel.sender_email:
        senders.append(
            MaskedSenderOut(
                name=str(channel.sender_name or "").strip(),
                email=mask_sender_email(str(channel.sender_email)),
                enabled=True,
            )
        )
    return senders


def resolve_task_proxy_latency_ms(task_id: int, task: Task) -> float | None:
    runtime = get_task_runtime_metrics(int(task_id))
    if runtime:
        proxy_rtt_ms = float(runtime.get("proxy_rtt_seconds", 0) or 0) * 1000.0
        if proxy_rtt_ms > 0:
            return round(proxy_rtt_ms, 1)
        avg_http_ms = float(runtime.get("avg_http_send_ms", 0) or 0)
        if avg_http_ms > 0 and is_task_runner_active(int(task_id)):
            return round(avg_http_ms, 1)
    return None


def parse_monitor_emails(value) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    try:
        parsed = json.loads(value or "[]")
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except (TypeError, json.JSONDecodeError):
        return []
    return []


def normalize_scheduled_at(value: datetime | None, timezone_name: str | None = None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        local_zone = ZoneInfo(normalize_task_timezone(timezone_name))
        return value.astimezone(local_zone).replace(tzinfo=None)
    return value.replace(tzinfo=None)


async def ensure_task_refs(db: AsyncSession, payload: TaskBase):
    template = (await db.execute(select(Template).where(Template.id == payload.template_id))).scalar_one_or_none()
    if template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="模板不存在")
    recipient_list = (await db.execute(select(RecipientList).where(RecipientList.id == payload.recipient_list_id))).scalar_one_or_none()
    if recipient_list is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="收件人池不存在")
    channel = (await db.execute(select(Channel).options(selectinload(Channel.proxy)).where(Channel.id == payload.channel_id))).scalar_one_or_none()
    if channel is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="通道不存在")
    if not bool(channel.enabled) or not bool(channel.is_active):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道已禁用")
    if channel.proxy_id and channel.proxy is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道绑定的代理不存在")
    if channel.proxy_id and channel.proxy is not None and not bool(channel.proxy.is_active):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道绑定的代理已禁用")
    if channel.proxy_id and not bool(channel.use_proxy):
        channel.use_proxy = True
        channel.updated_at = datetime.utcnow()
    consistency_error = await validate_sendgrid_api_key_proxy_consistency(channel)
    if consistency_error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=consistency_error)
    url_pool = None
    if payload.url_pool_id is not None:
        url_pool = (await db.execute(select(URLPool).where(URLPool.id == payload.url_pool_id))).scalar_one_or_none()
        if url_pool is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    return template, recipient_list, channel, url_pool


def effective_url_pool(task: Task):
    return getattr(task, "url_pool", None)


def _ensure_task_url_pool_ready(task: Task) -> None:
    error = validate_task_url_pool_binding(task)
    if error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error)


def proxy_to_out(proxy: Proxy | None) -> ProxyOut | None:
    if proxy is None:
        return None
    return ProxyOut(
        id=proxy.id,
        name=proxy.name or "代理",
        type=proxy.type or proxy.protocol or "HTTP",
        protocol=proxy.protocol or proxy.type or "HTTP",
        host=proxy.host or "",
        port=int(proxy.port or 0),
        username=proxy.username or "",
        is_active=bool(proxy.is_active),
    )


def mask_proxy_address(proxy: Proxy | None) -> str:
    if proxy is None:
        return "直连"
    scheme = "socks5" if str(proxy.type or proxy.protocol or "HTTP").upper() == "SOCKS5" else "http"
    auth = "***@" if (proxy.username or proxy.password) else ""
    host = str(proxy.host or "").strip()
    port = int(proxy.port or 0)
    return f"{scheme}://{auth}{host}:{port}" if host and port else "直连"


def channel_to_out(channel: Channel | None) -> ChannelOut | None:
    if channel is None:
        return None
    proxy = channel.proxy if bool(channel.use_proxy) else None
    proxy_address = mask_proxy_address(proxy)
    return ChannelOut(
        id=channel.id,
        name=channel.name or "-",
        use_proxy=bool(channel.use_proxy),
        proxy_id=channel.proxy_id if bool(channel.use_proxy) else None,
        proxy_name=proxy.name if proxy else "",
        proxy_address=proxy_address,
        proxy=proxy_to_out(proxy),
    )


async def task_to_out(
    task: Task,
    *,
    concurrency_warning: str | None = None,
    recommended_concurrency: int | None = None,
) -> TaskOut:
    pool = effective_url_pool(task)
    channel_out = channel_to_out(task.channel)
    rate_limit = float(task.rate_limit or 0)
    recommended = recommended_concurrency
    configured_batch_size = get_send_http_batch_size(task)
    configured_rate = float(task.rate_limit or 0)
    if recommended is None and rate_limit > 0:
        recommended = recommend_task_concurrency(rate_limit, send_http_batch_size=configured_batch_size)
    warning = concurrency_warning
    if warning is None and rate_limit > 0:
        warning = build_concurrency_warning(
            task.concurrency,
            rate_limit,
            send_http_batch_size=configured_batch_size,
        )
    masked_senders = build_masked_channel_senders(task.channel)
    proxy_latency_ms = resolve_task_proxy_latency_ms(int(task.id), task)
    return TaskOut(
        id=task.id,
        name=task.name,
        template_id=task.template_id,
        template_name=task.template.name if task.template else "模板已删除",
        recipient_list_id=task.recipient_list_id,
        recipient_list_name=task.recipient_list.name if task.recipient_list else "收件人池已删除",
        channel_id=task.channel_id,
        channel_name=task.channel.name if task.channel else "通道已删除",
        channel=channel_out,
        channel_senders=masked_senders,
        use_proxy=bool(channel_out.use_proxy) if channel_out else False,
        proxy_latency_ms=proxy_latency_ms,
        proxy_name=channel_out.proxy_name if channel_out else "",
        proxy_address=channel_out.proxy_address if channel_out else "直连",
        url_pool_id=pool.id if pool else None,
        url_pool_name=pool.name if pool else None,
        status=task.status or "draft",
        status_label=resolve_task_status_label(task.status),
        run_message=str(getattr(task, "run_message", "") or ""),
        status_reason=str(getattr(task, "run_message", "") or ""),
        concurrency=task.concurrency or 1,
        auto_concurrency=bool(getattr(task, "auto_concurrency", False)),
        batch_size=task.batch_size or 100,
        send_http_batch_size=get_send_http_batch_size(task),
        rate_limit=float(task.rate_limit or 0),
        retry_count=task.retry_count or 3,
        monitor_emails=await parse_monitor_emails_from_task(task),
        monitor_interval=task.monitor_interval if task.monitor_interval is not None else 100,
        total_count=task.total_count or 0,
        sent_count=task.sent_count or 0,
        failed_count=task.failed_count or 0,
        unsubscribe_enabled=bool(task.unsubscribe_enabled),
        sender_rotation_enabled=bool(getattr(task, "sender_rotation_enabled", False)),
        scheduled_at=task.scheduled_at,
        timezone=task.timezone or DEFAULT_TASK_TIMEZONE,
        created_at=task.created_at,
        updated_at=task.updated_at,
        recommended_concurrency=recommended,
        concurrency_warning=warning,
        http_recipients_per_request=configured_batch_size,
        estimated_http_requests_per_second=round(
            estimate_http_requests_per_second(configured_rate, configured_batch_size),
            2,
        ),
    )


@router.get("", response_model=TaskListResponse, dependencies=[Depends(require_admin)])
async def list_tasks(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    task_status: str | None = Query(default=None, alias="status"),
    db: AsyncSession = Depends(get_db),
):
    try:
        base_query = select(Task).where(_active_task_clause())
        count_query = select(func.count()).select_from(Task).where(_active_task_clause())
        if task_status:
            normalized_status = str(task_status or "").strip()
            if normalized_status:
                base_query = base_query.where(Task.status == normalized_status)
                count_query = count_query.where(Task.status == normalized_status)
        total = int((await db.execute(count_query)).scalar_one())
        result = await db.execute(
            base_query
            .options(
                selectinload(Task.template),
                selectinload(Task.url_pool),
                selectinload(Task.recipient_list),
                selectinload(Task.channel).selectinload(Channel.proxy),
            )
            .order_by(Task.created_at.desc(), Task.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        items = []
        for task in result.scalars().all():
            items.append(await task_to_out(task))
        return TaskListResponse(items=items, total=total, page=page, page_size=page_size)
    except Exception as exc:
        logger.exception("获取任务列表失败 page=%s page_size=%s status=%s", page, page_size, task_status)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


class TaskListItem(BaseModel):
    id: int
    name: str


@router.get("/list", response_model=list[TaskListItem], dependencies=[Depends(require_admin)])
async def list_tasks_simple(
    task_status: str = Query(default="all", alias="status"),
    db: AsyncSession = Depends(get_db),
):
    try:
        query = select(Task.id, Task.name).where(_active_task_clause()).order_by(Task.created_at.desc(), Task.id.desc())
        normalized_status = str(task_status or "all").strip().lower()
        if normalized_status and normalized_status != "all":
            query = query.where(Task.status == normalized_status)
        rows = (await db.execute(query)).all()
        return [TaskListItem(id=int(row.id), name=str(row.name or f"任务{row.id}")) for row in rows]
    except Exception as exc:
        logger.exception("获取简化任务列表失败 status=%s", task_status)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.post("", response_model=TaskOut, dependencies=[Depends(require_admin)])
async def create_task(payload: TaskCreate, db: AsyncSession = Depends(get_db)):
    try:
        _validate_task_batch_payload(payload)
        _, recipient_list, _, _ = await ensure_task_refs(db, payload)
        sendable_count = await ensure_recipient_list_available(db, payload.recipient_list_id)
        task_timezone = normalize_task_timezone(payload.timezone)
        scheduled_at = normalize_scheduled_at(payload.scheduled_at, task_timezone)
        task = Task(
            name=payload.name.strip(),
            template_id=payload.template_id,
            recipient_list_id=payload.recipient_list_id,
            channel_id=payload.channel_id,
            url_pool_id=payload.url_pool_id,
            concurrency=payload.concurrency,
            auto_concurrency=bool(payload.auto_concurrency),
            batch_size=payload.batch_size,
            send_http_batch_size=payload.send_http_batch_size,
            rate_limit=payload.rate_limit,
            retry_count=payload.retry_count,
            monitor_emails=json.dumps(payload.monitor_emails, ensure_ascii=False),
            monitor_emails_json=json.dumps(payload.monitor_emails, ensure_ascii=False),
            monitor_interval=payload.monitor_interval,
            monitor_count=1,
            unsubscribe_enabled=payload.unsubscribe_enabled,
            sender_rotation_enabled=bool(payload.sender_rotation_enabled),
            scheduled_at=scheduled_at,
            timezone=task_timezone,
            total_count=sendable_count,
            status="scheduled" if scheduled_at is not None else "draft",
        )
        db.add(task)
        await db.commit()
        result = await db.execute(select(Task).options(selectinload(Task.template), selectinload(Task.url_pool), selectinload(Task.recipient_list), selectinload(Task.channel).selectinload(Channel.proxy)).where(Task.id == task.id))
        created_task = result.scalar_one()
        warning = build_concurrency_warning(
            payload.concurrency,
            payload.rate_limit,
            send_http_batch_size=resolve_send_http_batch_size_value(getattr(payload, "send_http_batch_size", None)),
        )
        recommended = (
            recommend_task_concurrency(
                payload.rate_limit,
                send_http_batch_size=resolve_send_http_batch_size_value(getattr(payload, "send_http_batch_size", None)),
            )
            if float(payload.rate_limit or 0) > 0
            else None
        )
        if warning:
            logger.warning("创建任务 %s：%s", created_task.id, warning)
        return await task_to_out(created_task, concurrency_warning=warning, recommended_concurrency=recommended)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("创建任务失败 payload_name=%s template_id=%s recipient_list_id=%s channel_id=%s", getattr(payload, 'name', ''), getattr(payload, 'template_id', None), getattr(payload, 'recipient_list_id', None), getattr(payload, 'channel_id', None))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务器繁忙，请稍后重试") from exc



@router.get("/{task_id}", response_model=TaskOut, dependencies=[Depends(require_admin)])
async def get_task(task_id: int, db: AsyncSession = Depends(get_db)):
    try:
        task = (await db.execute(select(Task).options(selectinload(Task.template), selectinload(Task.url_pool), selectinload(Task.recipient_list), selectinload(Task.channel).selectinload(Channel.proxy)).where(Task.id == task_id))).scalar_one_or_none()
        _ensure_task_visible(task)
        return await task_to_out(task)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("获取任务详情失败 task_id=%s", task_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.put("/{task_id}", response_model=TaskOut, dependencies=[Depends(require_admin)])
async def update_task(task_id: int, payload: TaskUpdate, db: AsyncSession = Depends(get_db)):
    try:
        _validate_task_batch_payload(payload)
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在")
        _, recipient_list, _, _ = await ensure_task_refs(db, payload)
        sendable_count = None
        if task.status in OCCUPYING_TASK_STATUSES or payload.recipient_list_id != task.recipient_list_id:
            sendable_count = await ensure_recipient_list_available(db, payload.recipient_list_id, current_task_id=task_id)
        for field in [
            "name",
            "template_id",
            "recipient_list_id",
            "channel_id",
            "url_pool_id",
            "concurrency",
            "auto_concurrency",
            "batch_size",
            "send_http_batch_size",
            "rate_limit",
            "retry_count",
            "monitor_interval",
            "unsubscribe_enabled",
            "sender_rotation_enabled",
        ]:
            setattr(task, field, getattr(payload, field))
        task.timezone = normalize_task_timezone(payload.timezone)
        task.scheduled_at = normalize_scheduled_at(payload.scheduled_at, task.timezone)
        task.name = task.name.strip()
        task.monitor_emails = json.dumps(payload.monitor_emails, ensure_ascii=False)
        task.monitor_emails_json = json.dumps(payload.monitor_emails, ensure_ascii=False)
        if sendable_count is None:
            sendable_count = await count_sendable_recipients(db, int(payload.recipient_list_id))
        task.total_count = sendable_count
        if task.status in {"draft", "scheduled"}:
            task.status = "scheduled" if task.scheduled_at is not None else "draft"
        elif task.status in {"stopped", "failed"} and task.scheduled_at is not None:
            task.status = "scheduled"
        task.updated_at = datetime.utcnow()
        await db.commit()
        result = await db.execute(select(Task).options(selectinload(Task.template), selectinload(Task.url_pool), selectinload(Task.recipient_list), selectinload(Task.channel).selectinload(Channel.proxy)).where(Task.id == task_id))
        updated_task = result.scalar_one()
        warning = build_concurrency_warning(
            payload.concurrency,
            payload.rate_limit,
            send_http_batch_size=resolve_send_http_batch_size_value(getattr(payload, "send_http_batch_size", None)),
        )
        recommended = (
            recommend_task_concurrency(
                payload.rate_limit,
                send_http_batch_size=resolve_send_http_batch_size_value(getattr(payload, "send_http_batch_size", None)),
            )
            if float(payload.rate_limit or 0) > 0
            else None
        )
        if warning:
            logger.warning("更新任务 %s：%s", task_id, warning)
        return await task_to_out(updated_task, concurrency_warning=warning, recommended_concurrency=recommended)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("更新任务失败 task_id=%s", task_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


async def set_task_status(
    task_id: int,
    next_status: str,
    db: AsyncSession,
    *,
    operator: str = "未知",
):
    task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    _ensure_task_visible(task)
    old_status = str(task.status or "draft")
    normalized_next = str(next_status or "").strip().lower()
    if old_status == normalized_next:
        status_messages = {"paused": "任务已暂停", "running": "任务已启动", "stopped": "任务已停止", "finished": "任务已完成"}
        return {
            "success": True,
            "status": normalized_next,
            "message": status_messages.get(normalized_next, "任务状态未变化"),
            "run_message": str(getattr(task, "run_message", "") or ""),
        }
    if normalized_next in {"paused", "stopped", "draft", "cancelled"}:
        cancel_scheduled_start_retry(task_id)
    task.status = normalized_next
    if normalized_next == "paused":
        task.run_message = f"任务由{operator}手动暂停"
    elif normalized_next == "stopped":
        task.run_message = f"任务由{operator}手动停止，发送引擎将等待在途邮件落库后退出"
    elif normalized_next == "running":
        task.run_message = "任务正在发送中"
    elif normalized_next == "finished" and not str(getattr(task, "run_message", "") or "").strip():
        task.run_message = "任务已完成"
    task.updated_at = datetime.utcnow()
    await db.commit()
    notify_task_status_changed(task_id, normalized_next)
    logger.info(
        "任务 %s 状态变更：%s → %s，操作者=%s",
        task_id,
        old_status,
        normalized_next,
        operator,
    )
    status_messages = {"paused": "任务已暂停", "running": "任务已启动", "stopped": "任务已停止", "finished": "任务已完成"}
    return {
        "success": True,
        "status": normalized_next,
        "message": status_messages.get(normalized_next, "任务状态已更新"),
        "run_message": str(getattr(task, "run_message", "") or ""),
    }


async def reset_task_progress(task_id: int, db: AsyncSession, clear_logs: bool = True) -> Task:
    task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在")
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
            "任务 %s 已存在 %s 条常规发送日志，常规启动不会清空进度，避免重复发送；如需重发请使用显式重置功能",
            task_id,
            existing_regular_count,
        )
        clear_logs = False
    if clear_logs:
        await db.execute(sql_delete(SendLog).where(SendLog.task_id == task_id))
        task.sent_count = 0
        task.failed_count = 0
        task.last_claimed_row_number = 0
        task.last_claimed_recipient_id = 0
    task.status = "running"
    task.updated_at = datetime.utcnow()
    await db.commit()
    await db.refresh(task)
    notify_task_status_changed(task_id, "running")
    logger.warning(
        "任务 %s 发送进度已重置：clear_logs=%s status=running sent=%s failed=%s cursor=%s",
        task_id,
        clear_logs,
        task.sent_count,
        task.failed_count,
        task.last_claimed_row_number,
    )
    return task



@router.post("/{task_id}/start", dependencies=[Depends(require_admin)])
async def start_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    maybe_warn_database_pool_pressure()
    operator = _resolve_admin_operator(admin_payload)
    try:
        logger.info("任务 %s 手动启动预检开始，操作者=%s", task_id, operator)
        task = (await db.execute(select(Task).options(selectinload(Task.template)).where(Task.id == task_id))).scalar_one_or_none()
        _ensure_task_visible(task)
        logger.info("任务 %s 已加载，状态=%s 通道=%s 收件人池=%s 模板=%s", task_id, task.status, task.channel_id, task.recipient_list_id, task.template_id)
        _validate_task_startable(task)
        await _ensure_task_channel_valid(db, task)
        logger.info("任务 %s 通道基础校验通过 channel_id=%s", task_id, task.channel_id)
        _ensure_task_url_pool_ready(task)
        logger.info("任务 %s URL 池绑定校验通过 url_pool_id=%s", task_id, task.url_pool_id)
        if await is_task_lock_held(task.channel_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=CHANNEL_TASK_BUSY_MESSAGE)
        if await is_warmup_lock_held(task.channel_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该通道已有养号计划在运行")
        logger.info("任务 %s 通道锁预检通过 channel_id=%s", task_id, task.channel_id)
        health_error = await ensure_channel_ready_for_task_start(task.channel_id)
        if health_error:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=health_error)
        logger.info("任务 %s 通道健康检查通过 channel_id=%s", task_id, task.channel_id)
        batch_config_error = validate_send_http_batch_config(float(task.rate_limit or 0), get_send_http_batch_size(task))
        if batch_config_error:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=batch_config_error)
        logger.info("任务 %s 批量与速率配置校验通过 rate_limit=%s batch=%s", task_id, task.rate_limit, get_send_http_batch_size(task))
        await ensure_recipient_list_available(db, task.recipient_list_id, current_task_id=task_id)
        logger.info("任务 %s 收件人池校验通过 recipient_list_id=%s", task_id, task.recipient_list_id)
        await _check_task_start_concurrency(task_id, task.concurrency)
        logger.info("任务 %s 全局并发预检通过 concurrency=%s", task_id, task.concurrency)
        previous_status = str(task.status or "draft")
        try:
            runner_started = await start_task_runner(
                task_id,
                force=True,
                concurrency=task.concurrency,
                reset_progress_on_start=True,
                rollback_status=previous_status,
            )
        except GlobalConcurrencyLimitError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=exc.message) from exc
        except ChannelNotReadyError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message) from exc
        except TaskServiceUnavailableError as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=exc.message) from exc
        except TypeError as exc:
            logger.exception("任务 %s 启动发送引擎参数不兼容", task_id)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="发送引擎启动参数不兼容，请更新后端服务后重试") from exc
        if not runner_started:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="任务启动失败，可能已被其他实例占用或当前不可启动",
            )
        logger.info(
            "任务 %s 状态变更：%s → running，操作者=%s，runner_started=%s",
            task_id,
            previous_status,
            operator,
            runner_started,
        )
        return {"success": True, "status": "running", "runner_started": runner_started, "message": "任务已启动"}
    except Exception as exc:
        _raise_task_control_http(exc)


@router.post("/batch-start", response_model=BatchTaskResponse)
async def batch_start_tasks(
    payload: BatchTaskRequest,
    admin_payload: dict = Depends(require_admin),
):
    operator = _resolve_admin_operator(admin_payload)
    succeeded: list[int] = []
    failed: list[BatchTaskFailure] = []
    skipped: list[BatchTaskFailure] = []
    for task_id in payload.task_ids:
        async with AsyncSessionLocal() as db:
            task = (await db.execute(select(Task).options(selectinload(Task.template)).where(Task.id == task_id))).scalar_one_or_none()
            if task is None or task.deleted_at is not None:
                failed.append(BatchTaskFailure(task_id=task_id, reason="任务不存在"))
                continue
            current_status = str(task.status or "").strip().lower()
            skip_reason = _batch_start_skip_reason(current_status)
            if skip_reason is not None:
                skipped.append(BatchTaskFailure(task_id=task_id, reason=skip_reason))
                logger.info(
                    "管理员批量启动跳过任务 task_id=%s 状态=%s 操作者=%s 原因=%s",
                    task_id,
                    current_status or "未知",
                    operator,
                    skip_reason,
                )
                continue
            try:
                await _ensure_task_channel_valid(db, task)
                _ensure_task_url_pool_ready(task)
                if not task.template_id or task.template is None:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="任务模板不存在或已被删除")
                await ensure_recipient_list_available(db, task.recipient_list_id, current_task_id=task_id)
                health_error = await ensure_channel_ready_for_task_start(task.channel_id)
                if health_error:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=health_error)
                if await is_task_lock_held(task.channel_id):
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=CHANNEL_TASK_BUSY_MESSAGE)
                if await is_warmup_lock_held(task.channel_id):
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该通道已有养号计划在运行")
                await _check_task_start_concurrency(task_id, task.concurrency)
                runner_started = await start_task_runner(
                    task_id,
                    force=False,
                    concurrency=task.concurrency,
                    reset_progress_on_start=current_status == "pending",
                    rollback_status=current_status,
                )
                if not runner_started:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="任务启动失败，可能已被其他实例占用")
                succeeded.append(task_id)
                logger.info(
                    "管理员批量启动任务成功 task_id=%s 原状态=%s 操作者=%s 进度重置=%s",
                    task_id,
                    current_status,
                    operator,
                    current_status == "pending",
                )
            except HTTPException as exc:
                failed.append(BatchTaskFailure(task_id=task_id, reason=str(exc.detail)))
                logger.warning("管理员批量启动任务失败 task_id=%s 操作者=%s 原因=%s", task_id, operator, exc.detail)
            except (GlobalConcurrencyLimitError, ChannelNotReadyError, TaskServiceUnavailableError) as exc:
                reason = str(getattr(exc, "message", None) or "服务繁忙，请稍后重试")
                failed.append(BatchTaskFailure(task_id=task_id, reason=reason))
                logger.warning("管理员批量启动任务失败 task_id=%s 操作者=%s 原因=%s", task_id, operator, reason)
            except Exception:
                logger.exception("管理员批量启动任务异常 task_id=%s 操作者=%s", task_id, operator)
                failed.append(BatchTaskFailure(task_id=task_id, reason="服务繁忙，请稍后重试"))
    return BatchTaskResponse(success=succeeded, failed=failed, skipped=skipped)


@router.post("/batch-resume", response_model=BatchTaskResponse)
async def batch_resume_tasks(
    payload: BatchTaskRequest,
    admin_payload: dict = Depends(require_admin),
):
    operator = _resolve_admin_operator(admin_payload)
    succeeded: list[int] = []
    failed: list[BatchTaskFailure] = []
    skipped: list[BatchTaskFailure] = []
    for task_id in payload.task_ids:
        async with AsyncSessionLocal() as db:
            try:
                task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
                if task is None or task.deleted_at is not None:
                    failed.append(BatchTaskFailure(task_id=task_id, reason="任务不存在"))
                    continue
                current_status = str(task.status or "").strip().lower()
                if current_status not in {"paused", "stopped", "failed"}:
                    skipped.append(BatchTaskFailure(task_id=task_id, reason="任务状态不允许恢复，仅暂停、停止或失败中的任务可以继续"))
                    continue
                await resume_task(task_id=task_id, db=db, admin_payload=admin_payload)
                succeeded.append(task_id)
                logger.info("管理员批量继续任务成功 task_id=%s 操作者=%s", task_id, operator)
            except HTTPException as exc:
                await db.rollback()
                reason = str(exc.detail or "任务恢复失败")
                failed.append(BatchTaskFailure(task_id=task_id, reason=reason))
                logger.warning("管理员批量继续任务失败 task_id=%s 操作者=%s 原因=%s", task_id, operator, reason)
            except Exception as exc:
                await db.rollback()
                reason = str(getattr(exc, "message", None) or "服务繁忙，请稍后重试")
                failed.append(BatchTaskFailure(task_id=task_id, reason=reason))
                logger.exception("管理员批量继续任务异常 task_id=%s 操作者=%s", task_id, operator)
    return BatchTaskResponse(success=succeeded, failed=failed, skipped=skipped)


@router.post("/bulk-start", response_model=BulkStartResponse, dependencies=[Depends(require_admin)])
async def bulk_start_tasks(payload: BulkStartRequest):
    results: list[BulkStartItem] = []
    reserved_channels: set[int] = set()
    reserved_recipient_lists: set[int] = set()
    for task_id in payload.task_ids:
        async with AsyncSessionLocal() as db:
            task = (await db.execute(select(Task).options(selectinload(Task.template)).where(Task.id == task_id))).scalar_one_or_none()
            if task is None:
                results.append(BulkStartItem(task_id=task_id, success=False, status="missing", message="任务不存在"))
                continue
            if task.status in {"paused", "stopped"} and int(task.sent_count or 0) > 0:
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status, message="任务已有发送进度，请使用继续/恢复接口断点续发"))
                continue
            url_pool_error = validate_task_url_pool_binding(task)
            if url_pool_error:
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=url_pool_error))
                continue
            if task.channel_id in reserved_channels or await is_task_lock_held(task.channel_id):
                logger.warning("任务 %s 批量启动已跳过：通道 %s 正忙", task_id, task.channel_id)
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=CHANNEL_TASK_BUSY_MESSAGE))
                continue
            if await is_warmup_lock_held(task.channel_id):
                logger.warning("任务 %s 批量启动已跳过：通道 %s 正在养号", task_id, task.channel_id)
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message="该通道已有养号计划在运行"))
                continue
            health_error = await ensure_channel_ready_for_task_start(task.channel_id)
            if health_error:
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=health_error))
                continue
            batch_config_error = validate_send_http_batch_config(float(task.rate_limit or 0), get_send_http_batch_size(task))
            if batch_config_error:
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=batch_config_error))
                continue
            if task.recipient_list_id in reserved_recipient_lists:
                logger.warning(
                    "task %s bulk start skipped: recipient list %s reserved in current batch",
                    task_id,
                    task.recipient_list_id,
                )
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=RECIPIENT_LIST_OCCUPIED_MESSAGE))
                continue
            try:
                await ensure_recipient_list_available(db, task.recipient_list_id, current_task_id=task_id)
            except HTTPException as exc:
                logger.warning("任务 %s 批量启动已跳过：收件人池不可用，原因=%s", task_id, exc.detail)
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=str(exc.detail)))
                continue
            try:
                concurrency_limit_error = await check_global_concurrency_limit(task.concurrency, exclude_task_id=task_id)
            except TaskServiceUnavailableError as exc:
                logger.warning("任务 %s 批量启动已跳过：并发服务不可用，原因=%s", task_id, exc.message)
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=exc.message))
                continue
            if concurrency_limit_error:
                logger.warning("任务 %s 批量启动已跳过：%s", task_id, concurrency_limit_error)
                results.append(BulkStartItem(task_id=task_id, success=False, status=task.status or "draft", message=concurrency_limit_error))
                continue
            reserved_channels.add(task.channel_id)
            reserved_recipient_lists.add(task.recipient_list_id)
            previous_status = str(task.status or "draft")
        try:
            runner_started = await start_task_runner(
                task_id,
                force=True,
                concurrency=task.concurrency,
                reset_progress_on_start=True,
                rollback_status=previous_status,
            )
        except GlobalConcurrencyLimitError as exc:
            logger.warning("任务 %s 批量启动失败：%s", task_id, exc.message)
            reserved_channels.discard(task.channel_id)
            reserved_recipient_lists.discard(task.recipient_list_id)
            results.append(BulkStartItem(task_id=task_id, success=False, status=previous_status, message=exc.message))
            continue
        except ChannelNotReadyError as exc:
            logger.warning("任务 %s 批量启动失败：%s", task_id, exc.message)
            reserved_channels.discard(task.channel_id)
            reserved_recipient_lists.discard(task.recipient_list_id)
            results.append(BulkStartItem(task_id=task_id, success=False, status=previous_status, message=exc.message))
            continue
        except TaskServiceUnavailableError as exc:
            logger.warning("任务 %s 批量启动失败：%s", task_id, exc.message)
            reserved_channels.discard(task.channel_id)
            reserved_recipient_lists.discard(task.recipient_list_id)
            results.append(BulkStartItem(task_id=task_id, success=False, status=previous_status, message=exc.message))
            continue
        if not runner_started:
            reserved_channels.discard(task.channel_id)
            reserved_recipient_lists.discard(task.recipient_list_id)
            results.append(
                BulkStartItem(
                    task_id=task_id,
                    success=False,
                    status=previous_status,
                    message="任务启动失败，可能已被其他实例占用或当前不可启动",
                )
            )
            continue
        results.append(BulkStartItem(task_id=task_id, success=True, status="running", runner_started=runner_started, message="任务已启动"))
    await asyncio.sleep(0)
    started_count = sum(1 for item in results if item.success)
    return BulkStartResponse(items=results, started_count=started_count, failed_count=len(results) - started_count)


@router.post("/schedule/run-due", dependencies=[Depends(require_admin)])
async def run_due_scheduled_tasks():
    started = await schedule_due_tasks()
    return {"success": True, "started": started}


async def _batch_change_task_status(
    payload: BatchTaskRequest,
    *,
    action: str,
    operator: str,
) -> BatchTaskResponse:
    succeeded: list[int] = []
    failed: list[BatchTaskFailure] = []
    skipped: list[BatchTaskFailure] = []
    allowed_statuses = {"pause": {"running"}, "stop": {"running", "pending", "paused"}}[action]
    action_label = "暂停" if action == "pause" else "停止"
    next_status = "paused" if action == "pause" else "stopped"
    for task_id in payload.task_ids:
        async with AsyncSessionLocal() as db:
            try:
                task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
                if task is None or task.deleted_at is not None:
                    failed.append(BatchTaskFailure(task_id=task_id, reason="任务不存在"))
                    continue
                current_status = str(task.status or "").strip().lower()
                if current_status not in allowed_statuses:
                    skipped.append(BatchTaskFailure(task_id=task_id, reason=f"任务状态不允许{action_label}"))
                    continue
                if action == "pause":
                    _validate_task_pausable(task)
                else:
                    _validate_task_stoppable(task)
                await set_task_status(task_id, next_status, db, operator=operator)
                succeeded.append(task_id)
                logger.info("管理员批量%s任务成功 task_id=%s 操作者=%s", action_label, task_id, operator)
            except HTTPException as exc:
                failed.append(BatchTaskFailure(task_id=task_id, reason=str(exc.detail)))
                logger.warning("管理员批量%s任务失败 task_id=%s 操作者=%s 原因=%s", action_label, task_id, operator, exc.detail)
            except Exception:
                await db.rollback()
                logger.exception("管理员批量%s任务异常 task_id=%s 操作者=%s", action_label, task_id, operator)
                failed.append(BatchTaskFailure(task_id=task_id, reason="服务繁忙，请稍后重试"))
    return BatchTaskResponse(success=succeeded, failed=failed, skipped=skipped)


@router.post("/batch-pause", response_model=BatchTaskResponse)
async def batch_pause_tasks(
    payload: BatchTaskRequest,
    admin_payload: dict = Depends(require_admin),
):
    return await _batch_change_task_status(payload, action="pause", operator=_resolve_admin_operator(admin_payload))


@router.post("/batch-stop", response_model=BatchTaskResponse)
async def batch_stop_tasks(
    payload: BatchTaskRequest,
    admin_payload: dict = Depends(require_admin),
):
    return await _batch_change_task_status(payload, action="stop", operator=_resolve_admin_operator(admin_payload))


@router.post("/{task_id}/pause", dependencies=[Depends(require_admin)])
async def pause_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    maybe_warn_database_pool_pressure()
    try:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        _ensure_task_visible(task)
        _validate_task_pausable(task)
        operator = _resolve_admin_operator(admin_payload)
        return await set_task_status(task_id, "paused", db, operator=operator)
    except Exception as exc:
        _raise_task_control_http(exc)


@router.post("/{task_id}/resume", dependencies=[Depends(require_admin)])
async def resume_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    maybe_warn_database_pool_pressure()
    operator = _resolve_admin_operator(admin_payload)
    try:
        task = (await db.execute(select(Task).options(selectinload(Task.template)).where(Task.id == task_id))).scalar_one_or_none()
        _ensure_task_visible(task)
        if task.status == "running":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=TASK_ALREADY_RUNNING_MESSAGE)
        if task.status == "starting":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="任务正在启动中，请稍后重试")
        if task.status not in {"paused", "stopped", "failed"}:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="仅暂停、已停止或失败的任务可以恢复发送")
        previous_status = task.status or "paused"
        await _ensure_task_channel_valid(db, task)
        _ensure_task_url_pool_ready(task)
        if not await wait_for_task_runner_exit(task_id):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="任务正在收尾中，请稍后重试",
            )
        if is_task_runner_active(task_id):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="任务正在收尾中，请稍后重试",
            )
        if await is_task_lock_held(task.channel_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=CHANNEL_TASK_BUSY_MESSAGE)
        if await is_warmup_lock_held(task.channel_id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该通道已有养号计划在运行")
        health_error = await ensure_channel_ready_for_task_start(task.channel_id)
        if health_error:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=health_error)
        batch_config_error = validate_send_http_batch_config(float(task.rate_limit or 0), get_send_http_batch_size(task))
        if batch_config_error:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=batch_config_error)
        await reconcile_task_resume_state(task_id)
        await db.refresh(task)
        if str(task.status or "").strip().lower() == "running":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=TASK_ALREADY_RUNNING_MESSAGE)
        if task.scheduled_at is not None and not is_task_due_now(task.scheduled_at, task.timezone):
            task.status = "scheduled"
            task.updated_at = datetime.utcnow()
            await db.commit()
            notify_task_status_changed(task_id, "scheduled")
            return {
                "success": True,
                "status": "scheduled",
                "runner_started": False,
                "message": "任务已恢复为定时等待，将在指定时间自动启动",
            }
        await ensure_recipient_list_available(db, task.recipient_list_id, current_task_id=task_id)
        await _check_task_start_concurrency(task_id, task.concurrency)
        old_status = previous_status
        try:
            runner_started = await start_task_runner(
                task_id,
                force=False,
                concurrency=task.concurrency,
                reset_progress_on_start=False,
                rollback_status=previous_status,
            )
        except GlobalConcurrencyLimitError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=exc.message) from exc
        except ChannelNotReadyError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message) from exc
        if not runner_started:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="任务恢复失败，可能已被其他实例占用或当前不可启动",
            )
        logger.info("任务 %s 状态变更：%s → running，操作者=%s", task_id, old_status, operator)
        return {"success": True, "status": "running", "runner_started": runner_started, "message": "任务已继续"}
    except Exception as exc:
        _raise_task_control_http(exc)


@router.post("/{task_id}/stop", dependencies=[Depends(require_admin)])
async def stop_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    maybe_warn_database_pool_pressure()
    try:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        _ensure_task_visible(task)
        _validate_task_stoppable(task)
        operator = _resolve_admin_operator(admin_payload)
        result = await set_task_status(task_id, "stopped", db, operator=operator)
        result["graceful_shutdown"] = is_task_runner_active(task_id)
        result["message"] = "任务已停止，发送引擎将等待在途邮件落库后退出"
        result["run_message"] = str(result.get("run_message") or f"任务由{operator}手动停止，发送引擎将等待在途邮件落库后退出")
        return result
    except Exception as exc:
        _raise_task_control_http(exc)


@router.delete("/{task_id}", dependencies=[Depends(require_admin)])
async def delete_task(
    task_id: int,
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    _ensure_task_visible(task)
    operator = _resolve_admin_operator(admin_payload)
    old_status = str(task.status or "draft")
    cancelled = False
    normalized_status = str(task.status or "").strip().lower()
    if normalized_status == "running":
        cancelled = await cancel_task_runner(task_id)
    now_value = datetime.utcnow()
    task.deleted_at = now_value
    task.last_claimed_row_number = 0
    task.last_claimed_recipient_id = 0
    task.updated_at = now_value
    if normalized_status in {"running", "paused"}:
        task.status = "stopped"
        notify_task_status_changed(task_id, "stopped")
    await db.commit()
    invalidate_dashboard_stats_cache()
    await cleanup_task_runtime_state(task_id)
    logger.info(
        "任务 %s 已软删除（原状态=%s → stopped），操作者=%s，SendLog 已保留，runner_cancelled=%s",
        task_id,
        old_status,
        operator,
        cancelled,
    )
    return {"success": True, "runner_cancelled": cancelled, "message": "任务已删除"}


@router.get(
    "/{task_id}/export/unset",
    dependencies=[Depends(require_admin), Depends(ensure_export_operation_no_audit)],
)
async def export_task_unset_recipients(
    request: Request,
    task_id: int = Path(..., ge=1),
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    """导出任务关联收件人中尚未出现在 send_log 的邮箱（TXT）。"""
    try:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    except Exception as exc:
        logger.exception("任务未发送导出时读取任务信息失败 task_id=%s", task_id)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="数据库繁忙，请稍后重试") from exc
    _ensure_task_visible(task)
    try:
        logger.info("开始导出任务未发送收件人 task_id=%s recipient_list_id=%s", task_id, task.recipient_list_id)
        sql = build_task_unset_sql(task_id, int(task.recipient_list_id))
        return await dispatch_smart_export(
            request,
            sql,
            f"task_{task_id}_unset.txt",
            user_id=int(admin_payload.get("user_id") or 0),
        )
    except ExportScopeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportTaskNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在") from None
    except ExportCopyError as exc:
        logger.exception("任务未发送导出 COPY 失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("任务未发送导出失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.get(
    "/{task_id}/export/success",
    dependencies=[Depends(require_admin), Depends(ensure_export_operation_no_audit)],
)
async def export_task_success_recipients(
    request: Request,
    task_id: int = Path(..., ge=1),
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    """导出 SDK 发送成功（send_log.status 为 sent/success，SQL DISTINCT 去重）的收件人邮箱（TXT）。"""
    try:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    except Exception as exc:
        logger.exception("任务成功导出时读取任务信息失败 task_id=%s", task_id)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="数据库繁忙，请稍后重试") from exc
    _ensure_task_visible(task)
    try:
        logger.info("开始导出任务成功收件人 task_id=%s", task_id)
        sql = build_task_success_sql(task_id)
        return await dispatch_smart_export(
            request,
            sql,
            f"task_{task_id}_success.txt",
            user_id=int(admin_payload.get("user_id") or 0),
        )
    except ExportScopeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportTaskNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在") from None
    except ExportCopyError as exc:
        logger.exception("任务成功导出 COPY 失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("任务成功导出失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.get(
    "/{task_id}/export/failed",
    dependencies=[Depends(require_admin), Depends(ensure_export_operation_no_audit)],
)
async def export_task_failed_recipients(
    request: Request,
    task_id: int = Path(..., ge=1),
    db: AsyncSession = Depends(get_db),
    admin_payload: dict = Depends(require_admin),
):
    """导出 SDK 发送失败（send_log.status='failed'）的收件人邮箱（TXT，去重）。"""
    try:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    except Exception as exc:
        logger.exception("任务失败导出时读取任务信息失败 task_id=%s", task_id)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="数据库繁忙，请稍后重试") from exc
    _ensure_task_visible(task)
    try:
        logger.info("开始导出任务失败收件人 task_id=%s（失败导出保留硬弹与退订地址，便于排查）", task_id)
        sql = build_task_failed_sql(task_id)
        return await dispatch_smart_export(
            request,
            sql,
            f"task_{task_id}_failed.txt",
            user_id=int(admin_payload.get("user_id") or 0),
        )
    except ExportScopeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportTaskNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在") from None
    except ExportCopyError as exc:
        logger.exception("任务失败导出 COPY 失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("任务失败导出失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.get("/{task_id}/stats", response_model=TaskStatsResponse, dependencies=[Depends(require_admin)])
async def get_task_stats(task_id: int, db: AsyncSession = Depends(get_db)):
    now_value = time.monotonic()
    cached = _task_stats_cache.get(int(task_id))
    if cached and now_value - cached[0] < _TASK_STATS_CACHE_TTL_SECONDS:
        return TaskStatsResponse(**cached[1])
    try:
        task = await asyncio.wait_for(db.scalar(select(Task).where(Task.id == task_id)), timeout=3.0)
        _ensure_task_visible(task)
        payload = await asyncio.wait_for(build_task_stats(db, task_id), timeout=5.0)
        _task_stats_cache[int(task_id)] = (time.monotonic(), dict(payload))
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在") from None
    except HTTPException:
        raise
    except Exception as exc:
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
            await db.rollback()
        logger.warning("任务 %s 统计数据构建失败，返回缓存或空数据 error_type=%s error=%s", task_id, type(exc).__name__, exc)
        cached = _task_stats_cache.get(int(task_id))
        if cached:
            payload = cached[1]
        else:
            runtime = get_task_runtime_metrics(task_id)
            payload = {
                "total_recipients": 0,
                "delivered": 0,
                "soft_bounced_or_retrying": 0,
                "hard_bounced": 0,
                "unsubscribed": 0,
                "pending": 0,
                "success_sdk": int(runtime.get("live_success_count", 0) or 0),
                "failed_sdk": int(runtime.get("live_fail_count", 0) or 0),
                "unsent_count": 0,
                "success": int(runtime.get("live_success_count", 0) or 0),
                "failed": int(runtime.get("live_fail_count", 0) or 0),
                "soft_bounced": 0,
                "retrying": 0,
                "active_workers": int(runtime.get("active_workers", 0) or 0),
                "avg_http_latency_ms": float(runtime.get("avg_http_latency_ms", runtime.get("avg_http_send_ms", 0)) or 0),
                "queue_prefetch_length": int(runtime.get("queue_prefetch_length", 0) or 0),
                "queue_write_length": int(runtime.get("queue_write_length", 0) or 0),
                "last_429_count_per_minute": int(runtime.get("last_429_count_per_minute", runtime.get("http_429_count", 0)) or 0),
            }
    return TaskStatsResponse(**payload)


@router.get("/{task_id}/perf", response_model=TaskPerfResponse, dependencies=[Depends(require_admin)])
async def get_task_perf(task_id: int, db: AsyncSession = Depends(get_db)):
    now_value = time.monotonic()
    cached = _task_perf_cache.get(int(task_id))
    if cached and now_value - cached[0] < _TASK_PERF_CACHE_TTL_SECONDS:
        return TaskPerfResponse(**cached[1])
    try:
        task = await asyncio.wait_for(
            db.scalar(select(Task).where(Task.id == task_id)),
            timeout=3.0,
        )
        _ensure_task_visible(task)
        payload = await asyncio.wait_for(build_task_perf(db, task_id), timeout=5.0)
        _task_perf_cache[int(task_id)] = (time.monotonic(), dict(payload))
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在") from None
    except Exception as exc:
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
            await db.rollback()
        logger.warning("任务 %s 性能数据构建失败，返回缓存或空数据 error_type=%s error=%s", task_id, type(exc).__name__, exc)
        cached = _task_perf_cache.get(int(task_id))
        if cached:
            payload = cached[1]
        else:
            payload = {
                "current_rate": 0.0,
                "success_rate": 0.0,
                "rate_limit_setting": 0.0,
                "max_theoretical_http_per_sec": 0.0,
                "http_rps_recent": [],
                "rate_window_seconds": 10,
                "rate_window_success_count": 0,
                "rate_window_processed_count": 0,
                "realtime_rate": 0.0,
                "sdk_realtime_rate": 0.0,
                "avg_http_latency_ms": 0,
                "active_workers": 0,
                "configured_concurrency": 0,
                "effective_concurrency": 0,
                "configured_user_concurrency": 0,
                "auto_concurrency_enabled": False,
                "queue_prefetch_length": 0,
                "queue_write_length": 0,
                "last_429_count": 0,
                "http_requests_per_second": 0.0,
                "send_http_batch_size": 50,
                "running": False,
            }
    try:
        return TaskPerfResponse(**payload)
    except Exception:
        logger.exception("任务 %s 性能数据序列化失败", task_id)
        cached = _task_perf_cache.get(int(task_id))
        if cached:
            return TaskPerfResponse(**cached[1])
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="性能数据暂不可用，请稍后刷新") from None


@router.get("/{task_id}/failures", response_model=TaskFailuresResponse, dependencies=[Depends(require_admin)])
async def get_task_failures(
    task_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    _ensure_task_visible(task)
    try:
        payload = await list_task_failures(db, task_id, page=page, page_size=page_size)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在") from None
    try:
        return TaskFailuresResponse(**payload)
    except Exception:
        logger.exception("任务 %s 失败明细序列化失败", task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="失败明细暂不可用，请稍后刷新",
        ) from None


@router.get("/{task_id}/alert", dependencies=[Depends(require_admin)])
async def get_task_alert(task_id: int, db: AsyncSession = Depends(get_db)):
    task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
    _ensure_task_visible(task)
    alert = get_task_performance_alert(task_id)
    if not alert:
        return {}
    return alert


def _empty_live_status(task_id: int) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=int(task_id), status="unknown", status_label="未知", success_count=0,
        fail_count=0, processed_count=0, total_recipients=0, progress_percent=0,
        current_rate=0, current_rate_10s=0, sdk_current_rate_10s=0,
        remaining_count=0, last_heartbeat=0, stalled=False,
    )


def _build_runtime_live_status(task_id: int) -> TaskStatusResponse:
    runtime = get_task_runtime_metrics(task_id) or {}
    success_count = max(0, int(runtime.get("sent_count", runtime.get("live_success_count", runtime.get("success", 0))) or 0))
    fail_count = max(0, int(runtime.get("failed_count", runtime.get("live_fail_count", runtime.get("failed", 0))) or 0))
    total_recipients = max(0, int(runtime.get("total_count", runtime.get("total_recipients", 0)) or 0))
    # processed 是本次运行增量，暂停/恢复后必须使用累计成功+失败，不能直接展示增量。
    processed_count = min(total_recipients, success_count + fail_count) if total_recipients > 0 else success_count + fail_count
    remaining_count = max(0, total_recipients - processed_count)
    current_rate_10s = max(0.0, float(runtime.get("current_rate_10s", runtime.get("sdk_current_rate_10s", runtime.get("current_rate", 0))) or 0))
    success_rate = max(0.0, float(runtime.get("sdk_success_rate_10s", runtime.get("success_rate", 0)) or 0))
    sdk_current_rate_1s = max(0.0, float(runtime.get("sdk_current_rate_1s", runtime.get("instant_rate_1s", 0)) or 0))
    sdk_success_rate_1s = max(0.0, float(runtime.get("sdk_success_rate_1s", 0) or 0))
    last_heartbeat = max(0.0, float(runtime.get("last_heartbeat", 0) or 0))
    stalled = bool(last_heartbeat > 0 and time.time() - last_heartbeat > 30.0)
    status_value = str(runtime.get("status", "running" if runtime else "unknown") or "unknown")
    return TaskStatusResponse(
        task_id=int(task_id), status=status_value, status_label=resolve_task_status_label(status_value),
        success_count=success_count, fail_count=fail_count,
        live_success_count=success_count, live_fail_count=fail_count,
        processed_count=processed_count, total_recipients=total_recipients,
        progress_percent=round((processed_count / total_recipients) * 100, 2) if total_recipients else 0,
        current_rate=sdk_current_rate_1s, current_rate_10s=current_rate_10s,
        sdk_current_rate_10s=current_rate_10s, success_rate=success_rate,
        sdk_current_rate_1s=sdk_current_rate_1s, sdk_success_rate_1s=sdk_success_rate_1s,
        sdk_processed_last_1s=max(0, int(round(sdk_current_rate_1s))),
        sdk_success_last_1s=max(0, int(round(sdk_success_rate_1s))),
        instant_rate_1s=sdk_current_rate_1s,
        remaining_count=remaining_count, last_heartbeat=last_heartbeat, stalled=stalled,
        result_queue_size=max(0, int(runtime.get("result_queue_size", 0) or 0)),
        effective_concurrency=max(0, int(runtime.get("effective_concurrency", 0) or 0)),
    )


@router.get("/{task_id}/live-status", response_model=TaskStatusResponse, dependencies=[Depends(require_admin_token_only)])
async def get_task_live_status(task_id: int):
    """只读进程内 runtime；不访问数据库、Redis，也不等待发送链路中的锁。"""
    started_at = time.perf_counter()
    try:
        response = await asyncio.wait_for(asyncio.sleep(0, result=_build_runtime_live_status(task_id)), timeout=2.0)
    except Exception:
        logger.warning("任务 %s 实时状态读取异常，已返回空数据", task_id, exc_info=True)
        response = _empty_live_status(task_id)
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    if elapsed_ms > 500:
        logger.warning("任务 %s 实时状态接口响应缓慢，耗时 %.2f 毫秒", task_id, elapsed_ms)
    return response


@router.get("/{task_id}/status", response_model=TaskStatusResponse, dependencies=[Depends(require_admin)])
async def get_task_status(task_id: int, db: AsyncSession = Depends(get_db)):
    try:
        async with asyncio.timeout(TASK_STATUS_QUERY_TIMEOUT_SECONDS):
            task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
            _ensure_task_visible(task)
            runtime = get_task_runtime_metrics(task_id)
            live_success_count = int(runtime.get("live_success_count", task.sent_count or 0))
            live_fail_count = int(runtime.get("live_fail_count", task.failed_count or 0))
            success_count = max(int(task.sent_count or 0), live_success_count)
            fail_count = max(int(task.failed_count or 0), live_fail_count)
            processed = success_count + fail_count
            total = int(task.total_count or 0)
            percent = round((processed / total) * 100, 2) if total else 0
            elapsed_seconds = float(runtime.get("elapsed_seconds", 0))
            if elapsed_seconds <= 0 and task.created_at:
                baseline = task.updated_at if task.status in {"finished", "failed", "stopped"} and task.updated_at else datetime.utcnow()
                elapsed_seconds = max(0.0, (baseline - task.created_at).total_seconds())
            success_rate = float(runtime.get("success_rate", 0))
            current_rate = float(runtime.get("current_rate", 0))
            if success_rate <= 0 and current_rate <= 0:
                success_total = int(task.sent_count or 0)
                processed_total = int((task.sent_count or 0) + (task.failed_count or 0))
                if success_total > 0 and elapsed_seconds > 0:
                    success_rate = round(success_total / elapsed_seconds, 2)
                if processed_total > 0 and elapsed_seconds > 0:
                    current_rate = round(processed_total / elapsed_seconds, 2)
            batch_size = max(1, int(task.batch_size or runtime.get("batch_size", 1) or 1))
            batch_count = int(runtime.get("batch_count", 0))
            if batch_count <= 0 and processed > 0:
                batch_count = (processed + batch_size - 1) // batch_size
            last_batch_size = int(runtime.get("last_batch_size", 0))
            last_claim_size = int(runtime.get("last_claim_size", 0))
            current_batch_size = last_claim_size
            if task.status in {"finished", "failed", "stopped"} and processed > 0:
                remainder = processed % batch_size
                last_batch_size = remainder if remainder else batch_size
            elif last_batch_size <= 0 and processed > 0:
                remainder = processed % batch_size
                last_batch_size = remainder if remainder else min(batch_size, processed)
            last_batch_duration_ms = int(runtime.get("last_batch_duration_ms", 0))
            last_claim_ms = float(runtime.get("last_claim_ms", 0))
            last_http_send_ms = float(runtime.get("last_http_send_ms", 0))
            last_build_batch_ms = float(runtime.get("last_build_batch_ms", 0))
            http_send_p99_ms = float(runtime.get("http_send_p99_ms", 0))
            result_queue_peak_after_batch = int(runtime.get("result_queue_peak_after_batch", 0))
            avg_http_send_ms = float(runtime.get("avg_http_send_ms", 0))
            batch_efficiency = float(runtime.get("batch_efficiency", 0))
            batch_size_optimization_hint = str(runtime.get("batch_size_optimization_hint", "") or "")
            suggested_send_http_batch_size = int(runtime.get("suggested_send_http_batch_size", 0))
            if task.status not in {"running", "paused"}:
                last_build_batch_ms = 0.0
                http_send_p99_ms = 0.0
                result_queue_peak_after_batch = 0
                avg_http_send_ms = 0.0
                batch_efficiency = 0.0
                batch_size_optimization_hint = ""
                suggested_send_http_batch_size = 0
                writer_flush_p99_ms = 0.0
                http_429_count = 0
            slow_http_count = int(runtime.get("slow_http_count", 0))
            http_timeout_count = int(runtime.get("http_timeout_count", 0))
            avg_batch_rate = round(last_batch_size / (last_batch_duration_ms / 1000), 2) if last_batch_duration_ms > 0 and last_batch_size > 0 else 0
            inflight_current = int(runtime.get("inflight", 0))
            async_send_active_requests = int(runtime.get("async_send_active_requests", inflight_current))
            result_queue_size = int(runtime.get("result_queue_size", 0))
            writer_last_flush_ms = float(runtime.get("writer_last_flush_ms", 0))
            writer_flush_p99_ms = float(runtime.get("writer_flush_p99_ms", 0))
            http_429_count = int(runtime.get("http_429_count", 0))
            result_queue_backpressure_count = int(runtime.get("result_queue_backpressure_count", 0))
            rate_limit_per_second = float(runtime.get("rate_limit_per_second", 0))
            global_concurrency_reserved = int(runtime.get("global_concurrency_reserved", 0))
            global_concurrency_occupied = int(runtime.get("global_concurrency_occupied", global_concurrency_reserved))
            global_concurrency_limit = int(runtime.get("global_concurrency_limit", 0))
            event_loop_latency_ms = float(runtime.get("event_loop_latency_ms", 0))
            main_loop_work_ms = float(runtime.get("main_loop_work_ms", 0))
            scheduling_delay_ms = float(runtime.get("scheduling_delay_ms", event_loop_latency_ms))
            event_loop_tasks_count = int(runtime.get("event_loop_tasks_count", 0))
            run_message = str(getattr(task, "run_message", "") or "").strip()
            error_message = ""
            if task.status == "failed":
                last_error = (await db.execute(select(SendLog.error_message).where(SendLog.task_id == task_id, SendLog.status == "failed").order_by(SendLog.created_at.desc(), SendLog.id.desc()).limit(1))).scalar_one_or_none()
                error_message = run_message or last_error or "任务执行失败"
            current_sender_name = str(runtime.get("current_sender_name") or "")
            current_sender_email = str(runtime.get("current_sender_email") or "")
            if not current_sender_email and task.channel_id:
                channel = (await db.execute(select(Channel).where(Channel.id == task.channel_id))).scalar_one_or_none()
                if channel is not None:
                    try:
                        async with asyncio.timeout(TASK_STATUS_SENDER_PEEK_TIMEOUT_SECONDS):
                            peeked_sender = await peek_last_sender(
                                channel,
                                rotation_enabled=bool(getattr(task, "sender_rotation_enabled", False)),
                            )
                    except TimeoutError:
                        logger.warning(
                            "task %s peek_last_sender timed out after %ss",
                            task_id,
                            TASK_STATUS_SENDER_PEEK_TIMEOUT_SECONDS,
                        )
                        peeked_sender = None
                    except Exception as exc:
                        logger.warning(
                            "task %s peek_last_sender failed error_type=%s error=%s",
                            task_id,
                            type(exc).__name__,
                            exc,
                        )
                        peeked_sender = None
                    if peeked_sender is not None:
                        current_sender_name = str(peeked_sender.get("name") or "")
                        current_sender_email = str(peeked_sender.get("email") or "")
            recommended_concurrency, concurrency_warning = get_task_concurrency_guidance_from_runtime(task_id, task)
            configured_http_batch_size = get_send_http_batch_size(task)
            configured_rate = float(task.rate_limit or 0)
            if task.status in {"running", "paused"}:
                send_http_batch_enabled = bool(runtime.get("send_http_batch_enabled", True))
                http_recipients_per_request = int(
                    runtime.get("http_recipients_per_request", configured_http_batch_size)
                )
                estimated_http_rps = float(
                    runtime.get(
                        "estimated_http_requests_per_second",
                        estimate_http_requests_per_second(configured_rate, http_recipients_per_request),
                    )
                )
            else:
                send_http_batch_enabled = True
                http_recipients_per_request = configured_http_batch_size
                estimated_http_rps = estimate_http_requests_per_second(
                    configured_rate,
                    configured_http_batch_size,
                )
            instant_rate_1s = float(runtime.get("instant_rate_1s", 0))
            response = TaskStatusResponse(
                task_id=task.id,
                status=task.status,
                status_label=resolve_task_status_label(task.status),
                success_count=success_count,
                fail_count=fail_count,
                live_success_count=live_success_count,
                live_fail_count=live_fail_count,
                processed_count=processed,
                total_recipients=total,
                progress_percent=percent,
                current_rate=current_rate,
                instant_rate_1s=instant_rate_1s,
                success_rate=success_rate,
                batch_count=batch_count,
                last_batch_size=last_batch_size,
                last_claim_size=last_claim_size,
                current_batch_size=current_batch_size,
                last_batch_duration_ms=last_batch_duration_ms,
                last_claim_ms=last_claim_ms,
                last_http_send_ms=last_http_send_ms,
                last_build_batch_ms=last_build_batch_ms,
                http_send_p99_ms=http_send_p99_ms,
                result_queue_peak_after_batch=result_queue_peak_after_batch,
                avg_http_send_ms=avg_http_send_ms,
                batch_efficiency=batch_efficiency,
                batch_size_optimization_hint=batch_size_optimization_hint,
                suggested_send_http_batch_size=suggested_send_http_batch_size,
                slow_http_count=slow_http_count,
                http_timeout_count=http_timeout_count,
                rate_limit=float(task.rate_limit or 0),
                elapsed_seconds=round(elapsed_seconds, 2),
                inflight_count=inflight_current,
                outstanding_writes=int(runtime.get("outstanding_writes", 0)),
                inflight_current=inflight_current,
                async_send_active_requests=async_send_active_requests,
                result_queue_size=result_queue_size,
                writer_last_flush_ms=writer_last_flush_ms,
                writer_flush_p99_ms=writer_flush_p99_ms,
                http_429_count=http_429_count,
                result_queue_backpressure_count=result_queue_backpressure_count,
                rate_limit_per_second=rate_limit_per_second,
                avg_batch_rate=avg_batch_rate,
                global_concurrency_reserved=global_concurrency_reserved,
                global_concurrency_occupied=global_concurrency_occupied,
                global_concurrency_limit=global_concurrency_limit,
                event_loop_latency_ms=event_loop_latency_ms,
                main_loop_work_ms=main_loop_work_ms,
                scheduling_delay_ms=scheduling_delay_ms,
                event_loop_tasks_count=event_loop_tasks_count,
                current_sender_name=current_sender_name,
                current_sender_email=current_sender_email,
                error_message=error_message,
                run_message=run_message,
                status_reason=run_message or error_message,
                recommended_concurrency=recommended_concurrency,
                concurrency_warning=concurrency_warning,
                http_recipients_per_request=http_recipients_per_request,
                estimated_http_requests_per_second=round(estimated_http_rps, 2),
                send_http_batch_size=configured_http_batch_size,
                send_http_batch_enabled=send_http_batch_enabled,
                effective_concurrency=int(runtime.get("effective_concurrency", runtime.get("concurrency", task.concurrency or 0))),
                configured_concurrency=int(runtime.get("configured_concurrency", task.concurrency or 0)),
                min_concurrency=int(runtime.get("min_concurrency", 0)),
                prefetch_limit=int(runtime.get("prefetch_limit", 0)),
                proxy_rtt_ms=round(float(runtime.get("proxy_rtt_seconds", 0)) * 1000.0, 1),
            )
            _cache_task_status_response(task_id, response)
            return response
    except TimeoutError:
        logger.warning("任务 %s 状态查询超过 %s 秒，返回缓存与运行时指标", task_id, TASK_STATUS_QUERY_TIMEOUT_SECONDS)
        return _build_timeout_fallback_status(
            task_id,
            f"任务状态查询超时（{TASK_STATUS_QUERY_TIMEOUT_SECONDS} 秒），已返回缓存与运行时指标",
        )
