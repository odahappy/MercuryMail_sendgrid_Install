import asyncio
import logging
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from time import monotonic
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.core.database import AsyncSessionLocal, commit_session_with_retry
from app.models.models import Channel, SendLog, WarmupContent, WarmupLog, WarmupPlan, WarmupRecipient, WarmupSystemLog
from app.services.channel_health import ensure_channel_ready_for_task_start
from app.services.channel_manager import mask_proxy_for_log
from app.services.connection_manager import mask_channel_proxy_info, validate_sendgrid_api_key_proxy_consistency
from app.services.locker import (
    acquire_warmup_lock,
    is_task_lock_held,
    is_warmup_lock_held,
    release_warmup_lock,
    try_acquire_warmup_lock,
)
from app.services.sendgrid_sdk import SyncSendGridClientManager
from app.services.task_runner import (
    WARMUP_DEFAULT_HTTP_BATCH_SIZE,
    acquire_rate_tokens,
    build_rate_limiter,
    get_scheduled_start_retry_interval_seconds,
    get_scheduled_start_retry_max_attempts,
    release_warmup_concurrency,
    send_warmup_sdk_batch,
    try_reserve_warmup_concurrency,
)
from app.utils.errors import sanitize_user_error_message
from app.utils.sendgrid_errors import is_sendgrid_messaging_limit_error, normalize_send_log_error

DEFAULT_TIMEZONE = "Asia/Shanghai"
SCHEDULED_CHANNEL_BUSY_REASON = "通道被占用"
SENDGRID_UNRECOVERABLE_WARMUP_STATUS_CODES = {401, 403}
logger = logging.getLogger(__name__)
RUNNING_WARMUPS: dict[int, asyncio.Task] = {}
WARMUP_STATES: dict[int, dict[str, object]] = {}
WARMUP_STOP_REQUESTED: set[int] = set()
WARMUP_STOP_GRACE_SECONDS = 10
WARMUP_DELETED_PLANS: set[int] = set()


class WarmupSendGridUnrecoverableError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = int(status_code or 0)
        self.message = normalize_send_log_error(message)
        super().__init__(self.message)


@dataclass
class WarmupScheduledStartRetryState:
    plan_id: int
    attempts: int = 0
    next_retry_at: float = 0.0
    reason: str = ""
    resume_unfinished: bool = False
    previous_status: str = "scheduled"
    due_local_date: date | None = None
    due_utc: datetime | None = None


_warmup_scheduled_start_retries: dict[int, WarmupScheduledStartRetryState] = {}
_warmup_scheduled_dispatcher_plans: set[int] = set()
_warmup_scheduler_claims: set[int] = set()
_warmup_scheduler_claims_lock = asyncio.Lock()


async def _try_claim_warmup_scheduler_plan(plan_id: int) -> bool:
    plan_key = int(plan_id)
    async with _warmup_scheduler_claims_lock:
        if plan_key in _warmup_scheduler_claims:
            return False
        current = RUNNING_WARMUPS.get(plan_key)
        if current is not None and not current.done():
            return False
        _warmup_scheduler_claims.add(plan_key)
        return True


async def _release_warmup_scheduler_plan_claim(plan_id: int) -> None:
    async with _warmup_scheduler_claims_lock:
        _warmup_scheduler_claims.discard(int(plan_id))


def cancel_warmup_scheduled_start_retry(plan_id: int) -> None:
    """养号计划暂停/停止时取消定时启动等待重试。"""
    plan_key = int(plan_id)
    _warmup_scheduled_start_retries.pop(plan_key, None)
    _warmup_scheduled_dispatcher_plans.discard(plan_key)
    _warmup_scheduler_claims.discard(plan_key)


def _build_warmup_retry_message(reason: str, attempts: int, max_attempts: int, interval_seconds: float) -> str:
    normalized_reason = str(reason or SCHEDULED_CHANNEL_BUSY_REASON).strip() or SCHEDULED_CHANNEL_BUSY_REASON
    return (
        f"{normalized_reason}，将于 {int(interval_seconds)} 秒后重试"
        f"（第 {attempts}/{max_attempts} 次）"
    )


def _ensure_warmup_retry_state(
    plan_id: int,
    *,
    resume_unfinished: bool,
    previous_status: str,
    due_local_date: date | None = None,
    due_utc: datetime | None = None,
) -> WarmupScheduledStartRetryState:
    plan_key = int(plan_id)
    entry = _warmup_scheduled_start_retries.get(plan_key)
    if entry is None:
        entry = WarmupScheduledStartRetryState(plan_id=plan_key)
        _warmup_scheduled_start_retries[plan_key] = entry
    entry.resume_unfinished = bool(resume_unfinished)
    if previous_status:
        entry.previous_status = str(previous_status)
    if due_local_date is not None:
        entry.due_local_date = due_local_date
    if due_utc is not None:
        entry.due_utc = _normalize_utc_datetime(due_utc)
    return entry


async def _is_warmup_scheduled_start_waiting(plan_id: int) -> bool:
    entry = _warmup_scheduled_start_retries.get(int(plan_id))
    if entry is None:
        return False
    return monotonic() < float(entry.next_retry_at or 0)


async def _revert_warmup_plan_for_retry(plan_id: int, message: str, *, target_status: str) -> None:
    safe_message = sanitize_user_error_message(message)
    async with AsyncSessionLocal() as db:
        plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == int(plan_id)))).scalar_one_or_none()
        if plan is None:
            return
        plan.status = str(target_status or "scheduled")
        plan.running = False
        plan.run_progress = 0
        plan.run_message = safe_message
        plan.run_finished_at = None
        plan.updated_at = datetime.utcnow()
        await db.commit()


async def _is_channel_busy_for_scheduled_warmup(channel_id: int, plan_id: int) -> bool:
    channel_key = int(channel_id)
    if await is_task_lock_held(channel_key):
        return True
    probe_lock = await try_acquire_warmup_lock(channel_key, int(plan_id))
    if probe_lock is None:
        return True
    await release_warmup_lock(probe_lock)
    return False


async def _enqueue_warmup_scheduled_start_retry(
    plan_id: int,
    reason: str,
    *,
    resume_unfinished: bool = False,
    previous_status: str = "scheduled",
    due_local_date: date | None = None,
    due_utc: datetime | None = None,
) -> None:
    plan_key = int(plan_id)
    interval_seconds = get_scheduled_start_retry_interval_seconds()
    max_attempts = get_scheduled_start_retry_max_attempts()
    entry = _ensure_warmup_retry_state(
        plan_key,
        resume_unfinished=resume_unfinished,
        previous_status=previous_status,
        due_local_date=due_local_date,
        due_utc=due_utc,
    )
    entry.attempts += 1
    entry.reason = str(reason or "").strip() or SCHEDULED_CHANNEL_BUSY_REASON
    if entry.attempts > max_attempts:
        _warmup_scheduled_start_retries.pop(plan_key, None)
        _warmup_scheduled_dispatcher_plans.discard(plan_key)
        await _release_warmup_scheduler_plan_claim(plan_key)
        exhausted_message = "任务启动失败：通道被占用，重试耗尽"
        await _revert_warmup_plan_for_retry(plan_key, exhausted_message, target_status="paused")
        logger.error("养号计划 %s 定时启动重试次数已用尽（%s 次）：通道被占用", plan_key, max_attempts)
        return
    entry.next_retry_at = monotonic() + interval_seconds
    _warmup_scheduled_start_retries[plan_key] = entry
    retry_message = _build_warmup_retry_message(entry.reason, entry.attempts, max_attempts, interval_seconds)
    await _revert_warmup_plan_for_retry(plan_key, retry_message, target_status=entry.previous_status or "scheduled")
    logger.warning(
        "养号计划 %s 定时启动等待重试（%s/%s）：%s",
        plan_key,
        entry.attempts,
        max_attempts,
        entry.reason,
    )


async def _defer_warmup_scheduled_start(
    plan_id: int,
    reason: str,
    *,
    resume_unfinished: bool = False,
    previous_status: str = "scheduled",
    warmup_lock: object | None = None,
) -> None:
    _warmup_scheduled_dispatcher_plans.discard(int(plan_id))
    await _release_warmup_scheduler_plan_claim(int(plan_id))
    if warmup_lock is not None:
        await release_warmup_lock(warmup_lock)
    await release_warmup_concurrency(int(plan_id))
    await _enqueue_warmup_scheduled_start_retry(
        int(plan_id),
        reason,
        resume_unfinished=resume_unfinished,
        previous_status=previous_status,
    )


def _is_warmup_scheduled_start_retryable(reason: str) -> bool:
    text = str(reason or "").strip()
    if not text:
        return False
    non_retry_markers = (
        "养号任务已在运行",
        "尚未到达计划定时启动时间",
        "今天已执行过",
        "养号计划不存在",
        "养号计划未启用",
        "养号计划未绑定通道",
        "绑定的通道已禁用",
        "养号计划未绑定代理",
        "养号文案未加载",
        "没有未发送收件人",
        "所有收件人均已发送",
        "通道 API 密钥已失效，无法启动任务",
        "通道状态异常，无法启动任务",
        "通道不可用，无法启动任务",
    )
    for marker in non_retry_markers:
        if marker in text:
            return False
    return True


def _safe_zoneinfo(timezone_name: str | None) -> ZoneInfo:
    name = (timezone_name or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_TIMEZONE)


def plan_local_now(plan: WarmupPlan, now_utc: datetime | None = None) -> datetime:
    current_utc = now_utc or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        current_utc = current_utc.replace(tzinfo=timezone.utc)
    else:
        current_utc = current_utc.astimezone(timezone.utc)
    return current_utc.astimezone(_safe_zoneinfo(plan.timezone or DEFAULT_TIMEZONE))


def local_date_for_plan(plan: WarmupPlan, now_utc: datetime | None = None) -> date:
    return plan_local_now(plan, now_utc).date()


def warmup_started_local_date(plan: WarmupPlan) -> date | None:
    started_at = getattr(plan, "started_at", None)
    if not isinstance(started_at, datetime):
        return None
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    else:
        started_at = started_at.astimezone(timezone.utc)
    return started_at.astimezone(_safe_zoneinfo(plan.timezone or DEFAULT_TIMEZONE)).date()


def calculate_warmup_elapsed_days(plan: WarmupPlan, local_today: date | None = None) -> int:
    started_date = warmup_started_local_date(plan)
    if started_date is None:
        return 0
    current_date = local_today or local_date_for_plan(plan)
    return max(1, (current_date - started_date).days + 1)


def _normalize_scheduled_time(raw_value: object, *, plan_id: int | None = None) -> time | None:
    if isinstance(raw_value, time):
        return raw_value.replace(tzinfo=None) if raw_value.tzinfo is not None else raw_value
    if isinstance(raw_value, datetime):
        return raw_value.timetz().replace(tzinfo=None) if raw_value.tzinfo is not None else raw_value.time()
    if isinstance(raw_value, timedelta):
        total_seconds = int(raw_value.total_seconds()) % 86400
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return time(hours, minutes, seconds)
    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(stripped, fmt).time()
            except ValueError:
                continue
    logger.error(
        "warmup plan %s scheduled_time has unsupported type=%s value=%r; auto-start is blocked until the value is fixed",
        plan_id,
        type(raw_value).__name__,
        raw_value,
    )
    return None


def scheduled_local_datetime(plan: WarmupPlan, target_date: date) -> datetime:
    scheduled = _normalize_scheduled_time(plan.scheduled_time, plan_id=int(getattr(plan, "id", 0) or 0) or None)
    if scheduled is None:
        raise ValueError(f"养号计划 {getattr(plan, 'id', '?')} 的定时时间无效，无法计算调度时刻")
    return datetime.combine(target_date, scheduled, tzinfo=_safe_zoneinfo(plan.timezone))


def scheduled_utc_datetime(plan: WarmupPlan, target_date: date) -> datetime:
    return scheduled_local_datetime(plan, target_date).astimezone(timezone.utc)


def _normalize_utc_datetime(value: datetime | None) -> datetime:
    current_utc = value or datetime.now(timezone.utc)
    if current_utc.tzinfo is None:
        return current_utc.replace(tzinfo=timezone.utc)
    return current_utc.astimezone(timezone.utc)


def next_warmup_run_utc(plan: WarmupPlan, now_utc: datetime | None = None) -> datetime | None:
    scheduled = _normalize_scheduled_time(plan.scheduled_time, plan_id=int(getattr(plan, "id", 0) or 0) or None)
    if scheduled is None:
        return None
    current_utc = _normalize_utc_datetime(now_utc)
    plan_timezone = plan.timezone or DEFAULT_TIMEZONE
    zone = _safe_zoneinfo(plan_timezone)
    local_now = current_utc.astimezone(zone)
    today_due_local = datetime.combine(local_now.date(), scheduled, tzinfo=zone)
    if local_now <= today_due_local:
        next_due_local = today_due_local
    else:
        next_due_local = datetime.combine(local_now.date() + timedelta(days=1), scheduled, tzinfo=zone)
    return next_due_local.astimezone(timezone.utc)


def is_warmup_due_now(plan: WarmupPlan, now_utc: datetime | None = None) -> bool:
    scheduled = _normalize_scheduled_time(plan.scheduled_time, plan_id=int(getattr(plan, "id", 0) or 0) or None)
    if scheduled is None:
        return False
    current_utc = _normalize_utc_datetime(now_utc)
    plan_timezone = plan.timezone or DEFAULT_TIMEZONE
    zone = _safe_zoneinfo(plan_timezone)
    local_now = current_utc.astimezone(zone)
    local_today = local_now.date()
    local_created_at = getattr(plan, "created_at", None)
    if isinstance(local_created_at, datetime):
        if local_created_at.tzinfo is None:
            local_created_at = local_created_at.replace(tzinfo=timezone.utc)
        local_created_at = local_created_at.astimezone(zone)
    else:
        local_created_at = None
    created_today_after_schedule = (
        local_created_at is not None
        and local_created_at.date() == local_today
        and local_created_at.time().replace(tzinfo=None) > scheduled
    )
    if created_today_after_schedule and getattr(plan, "last_run_date", None) is None:
        next_due_utc = next_warmup_run_utc(plan, current_utc)
        logger.debug(
            "warmup plan %s created after today's scheduled time; waiting next day scheduled_time=%s timezone=%s local_created_at=%s local_now=%s next_run_utc=%s",
            plan.id,
            scheduled,
            plan_timezone,
            local_created_at.isoformat(),
            local_now.isoformat(),
            next_due_utc.isoformat() if next_due_utc is not None else "None",
        )
        return False
    due_local = datetime.combine(local_today, scheduled, tzinfo=zone)
    due_utc = due_local.astimezone(timezone.utc)
    if current_utc < due_utc:
        logger.debug(
            "warmup plan %s not due yet scheduled_time=%s timezone=%s local_now=%s due_utc=%s next_run_utc=%s",
            plan.id,
            scheduled,
            plan_timezone,
            local_now.isoformat(),
            due_utc.isoformat(),
            due_utc.isoformat(),
        )
        return False
    return True


def calculate_daily_target(plan: WarmupPlan) -> int:
    start_count = max(1, int(plan.start_count or 1))
    max_daily = max(1, int(plan.max_daily_count or start_count))
    increase_rate = max(0.0, float(plan.daily_increase_rate or 0.0))
    local_today = local_date_for_plan(plan)
    if plan.last_run_date is not None and plan.last_run_date == local_today:
        stored_target = int(plan.last_daily_target or 0)
        if stored_target >= 1:
            return min(stored_target, max_daily)
        return min(start_count, max_daily)
    if plan.last_run_date is None:
        return min(start_count, max_daily)
    previous_target = int(plan.last_daily_target or 0)
    if previous_target < 1:
        previous_target = min(start_count, max_daily)
    raw_target = previous_target * (1.0 + increase_rate)
    return min(max(1, math.ceil(raw_target)), max_daily)


def calculate_remaining_today(plan: WarmupPlan, recipient_unsent: int) -> int:
    """本次恢复/启动时，在今日目标配额内最多可发送的封数。"""
    daily_target = calculate_daily_target(plan)
    unsent = max(0, int(recipient_unsent or 0))
    return min(unsent, daily_target)


def get_warmup_state(plan_id: int) -> dict[str, object]:
    return WARMUP_STATES.get(plan_id, {"running": False, "sent": 0, "failed": 0, "total": 0, "progress": 0.0, "last_message": "", "next_delay_seconds": 0.0})


async def _handle_runner_failure(plan_id: int, exception: BaseException) -> None:
    message = f"养号任务异常退出：{sanitize_user_error_message(str(exception) or type(exception).__name__)}"
    await _set_plan_runtime(plan_id, running=False, message=message)
    async with AsyncSessionLocal() as db:
        plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
        if plan is not None and str(plan.status or "").strip().lower() in {"scheduled", "active"}:
            plan.status = "paused"
            plan.updated_at = datetime.utcnow()
            await db.commit()
            logger.error("养号计划 %s 因 Runner 异常已自动暂停，修复后可手动重启", plan_id)


def _cleanup_runner(plan_id: int, task: asyncio.Task) -> None:
    current = RUNNING_WARMUPS.get(plan_id)
    if current is task:
        RUNNING_WARMUPS.pop(plan_id, None)
    release_warmup_concurrency(int(plan_id))
    if task.cancelled():
        logger.warning("养号计划 %s Runner 已取消", plan_id)
        return
    try:
        exception = task.exception()
    except asyncio.CancelledError:
        return
    if exception is not None:
        logger.error(
            "养号计划 %s Runner 未捕获异常，正在异步回收运行状态 error_type=%s error=%s",
            plan_id,
            type(exception).__name__,
            exception,
            exc_info=(type(exception), exception, exception.__traceback__),
        )
        asyncio.create_task(
            _handle_runner_failure(plan_id, exception),
            name=f"warmup-plan-{plan_id}-failure-cleanup",
        )


async def _append_warmup_system_log(
    plan_id: int,
    *,
    level: str = "info",
    message: str,
    progress: float | None = None,
    sent_count: int = 0,
    failed_count: int = 0,
) -> None:
    normalized_message = str(message or "").strip()
    if not normalized_message:
        return
    try:
        async with AsyncSessionLocal() as db:
            db.add(
                WarmupSystemLog(
                    plan_id=int(plan_id),
                    level=str(level or "info").strip() or "info",
                    message=normalized_message[:4000],
                    progress=float(progress or 0.0),
                    sent_count=max(0, int(sent_count or 0)),
                    failed_count=max(0, int(failed_count or 0)),
                    created_at=datetime.utcnow(),
                )
            )
            await db.commit()
    except Exception:
        logger.exception("写入养号系统日志失败 plan_id=%s level=%s message=%s", plan_id, level, normalized_message[:200])


async def _set_plan_runtime(plan_id: int, *, running: bool | None = None, progress: float | None = None, message: str | None = None) -> None:
    state = WARMUP_STATES.setdefault(plan_id, {"running": False, "sent": 0, "failed": 0, "total": 0, "progress": 0.0, "last_message": "", "next_delay_seconds": 0.0})
    if running is not None:
        state["running"] = running
    if progress is not None:
        state["progress"] = max(0.0, min(100.0, float(progress)))
    if message is not None:
        state["last_message"] = message
    plan_exists = False
    async with AsyncSessionLocal() as db:
        plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
        if plan is not None:
            plan_exists = True
            plan.running = bool(state.get("running", False))
            plan.run_progress = float(state.get("progress", 0.0) or 0.0)
            plan.run_message = str(state.get("last_message", "") or "")
            if running is True:
                plan.run_started_at = datetime.utcnow()
                plan.run_finished_at = None
            if running is False:
                plan.run_finished_at = datetime.utcnow()
            plan.updated_at = datetime.utcnow()
            await db.commit()
    if not plan_exists:
        return
    if message is not None:
        level = "info"
        text = str(message or "")
        if "失败" in text or "错误" in text:
            level = "error"
        elif "暂停" in text or "重试" in text or "等待" in text:
            level = "warning"
        await _append_warmup_system_log(
            plan_id,
            level=level,
            message=text,
            progress=float(state.get("progress", 0.0) or 0.0),
            sent_count=int(state.get("sent", 0) or 0),
            failed_count=int(state.get("failed", 0) or 0),
        )


async def _load_plan(plan_id: int) -> WarmupPlan | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(WarmupPlan)
            .options(selectinload(WarmupPlan.channel).selectinload(Channel.proxy), selectinload(WarmupPlan.content))
            .where(WarmupPlan.id == plan_id)
        )
        return result.scalar_one_or_none()


async def _load_warmup_send_content(plan_id: int) -> tuple[str, str, str] | None:
    """读取发送快照；内容来源属于计划配置，不依赖 WarmupContent 上不存在的 source 属性。"""
    async with AsyncSessionLocal() as db:
        row = (
            await db.execute(
                select(
                    WarmupContent.subject,
                    WarmupContent.text_body,
                    WarmupContent.html_body,
                    WarmupPlan.content_mode,
                )
                .join(WarmupPlan, WarmupPlan.id == WarmupContent.plan_id)
                .where(WarmupContent.plan_id == plan_id)
            )
        ).one_or_none()
        if row is None:
            return None
        subject = (row.subject or "").strip()
        text_body = (row.text_body or "").strip()
        html_body = (row.html_body or "").strip()
        if str(row.content_mode or "").strip().lower() == "ai" and html_body:
            text_body = ""
        if not subject or not (text_body or html_body):
            return None
        return subject, text_body, html_body


async def _count_unsent_recipients(plan_id: int, created_after: datetime | None = None) -> int:
    conditions = [WarmupRecipient.plan_id == plan_id, WarmupRecipient.sent.is_(False), WarmupRecipient.email != ""]
    if created_after is not None:
        conditions.append(WarmupRecipient.created_at > created_after)
    async with AsyncSessionLocal() as db:
        return int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(*conditions))).scalar_one())


async def _select_unsent_recipients(plan_id: int, target_count: int, created_after: datetime | None = None) -> list[WarmupRecipient]:
    conditions = [WarmupRecipient.plan_id == plan_id, WarmupRecipient.sent.is_(False), WarmupRecipient.email != ""]
    if created_after is not None:
        conditions.append(WarmupRecipient.created_at > created_after)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(WarmupRecipient)
            .where(*conditions)
            .order_by(func.random())
            .limit(max(1, int(target_count or 1)))
        )
        return list(result.scalars().all())


async def _mark_recipient_sending(plan_id: int, email: str) -> None:
    async with AsyncSessionLocal() as db:
        async with db.begin():
            db.add(WarmupLog(plan_id=plan_id, email=email, status="sending", error_message="", sent_at=datetime.utcnow()))


async def _reconcile_sent_from_logs(plan_id: int) -> int:
    """将 WarmupLog 中已成功但 recipient.sent 未更新的地址回填，防止暂停/取消竞态导致重复发送。"""
    reconciled = 0
    async with AsyncSessionLocal() as db:
        async with db.begin():
            plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
            success_emails_subq = (
                select(WarmupLog.email)
                .where(WarmupLog.plan_id == plan_id, WarmupLog.status == "success", WarmupLog.email != "")
                .distinct()
            )
            result = await db.execute(
                select(WarmupRecipient).where(
                    WarmupRecipient.plan_id == plan_id,
                    WarmupRecipient.sent.is_(False),
                    WarmupRecipient.email.in_(success_emails_subq),
                )
            )
            recipients = list(result.scalars().all())
            local_today = date.today()
            if plan is not None:
                try:
                    local_today = local_date_for_plan(plan)
                except Exception:
                    logger.debug("warmup plan %s reconcile sent_date fallback failed", plan_id, exc_info=True)
            for recipient in recipients:
                recipient.sent = True
                if recipient.sent_date is None:
                    recipient.sent_date = local_today
                reconciled += 1
            if reconciled > 0 and plan is not None:
                plan.total_sent = int(plan.total_sent or 0) + reconciled
                plan.updated_at = datetime.utcnow()
                logger.debug(
                    "warmup plan %s reconciled %s recipient(s) to sent=True from success logs",
                    plan_id,
                    reconciled,
                )
    return reconciled


async def _record_send_result(
    plan_id: int,
    recipient_id: int,
    email: str,
    success: bool,
    error_message: str,
    sent_date: date,
    *,
    sender_email: str = "",
    provider_message_id: str = "",
    channel: Channel | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        async with db.begin():
            plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
            recipient = (await db.execute(select(WarmupRecipient).where(WarmupRecipient.id == recipient_id, WarmupRecipient.plan_id == plan_id))).scalar_one_or_none()
            if recipient is not None and success and not bool(recipient.sent):
                recipient.sent = True
                recipient.sent_date = sent_date
                if plan is not None:
                    plan.total_sent = int(plan.total_sent or 0) + 1
                    plan.updated_at = datetime.utcnow()
            now_value = datetime.utcnow()
            normalized_status = "sent" if success else "failed"
            normalized_error = "" if success else normalize_send_log_error(error_message)[:2000]
            proxy_info = mask_channel_proxy_info(channel) if channel is not None else "直连"
            outbound_ip = str(getattr(channel, "proxy_outbound_ip", "") or "").strip() if channel is not None else ""
            if channel is not None and bool(getattr(channel, "use_proxy", False)) and not outbound_ip:
                outbound_ip = proxy_info
            normalized_sender = str(sender_email or getattr(channel, "sender_email", "") or "").strip()
            normalized_provider_message_id = str(provider_message_id or "").strip() or f"warmup:{plan_id}:{recipient_id}:{int(now_value.timestamp())}"
            response_text = str(error_message or "")[:4000]
            if success and not response_text.strip():
                response_text = "SendGrid 已接收请求，响应内容为空"
            if success and normalized_provider_message_id and "SendGrid消息ID=" not in response_text:
                response_text = f"{response_text}；SendGrid消息ID={normalized_provider_message_id}"
            db.add(WarmupLog(plan_id=plan_id, email=email, status="success" if success else "fail", error_message=normalized_error, sent_at=now_value))
            db.add(SendLog(
                task_id=None,
                recipient_id=None,
                email=email,
                sender_email=normalized_sender[:255] or None,
                status=normalized_status,
                final_status="processed" if success else "blocked",
                error=normalized_error,
                error_message=normalized_error,
                response_text=response_text[:4000],
                attempt_count=1,
                provider_message_id=normalized_provider_message_id[:255],
                log_type="warmup",
                egress_ip=outbound_ip[:64],
                proxy_info=proxy_info[:255],
                outbound_ip=outbound_ip[:45] or None,
                sent_at=now_value if success else None,
                created_at=now_value,
            ))


async def _finish_plan_day(
    plan_id: int,
    local_today: date,
    status: str,
    message: str,
    progress: float = 100.0,
    daily_target: int | None = None,
    *,
    mark_day_complete: bool = True,
    system_log_level: str | None = None,
) -> None:
    safe_message = sanitize_user_error_message(message)
    async with AsyncSessionLocal() as db:
        plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
        if plan is not None:
            plan.status = status
            plan.running = False
            if mark_day_complete:
                plan.last_run_date = local_today
                if daily_target is not None:
                    plan.last_daily_target = max(1, int(daily_target))
            plan.run_progress = progress
            plan.run_message = safe_message
            plan.run_finished_at = datetime.utcnow()
            plan.updated_at = datetime.utcnow()
            await db.commit()
    state = WARMUP_STATES.setdefault(plan_id, {"running": False, "sent": 0, "failed": 0, "total": 0, "progress": 0.0, "last_message": "", "next_delay_seconds": 0.0})
    state.update({"running": False, "progress": progress, "last_message": safe_message, "next_delay_seconds": 0.0})
    await _append_warmup_system_log(
        plan_id,
        level=str(system_log_level or ("error" if status in {"paused", "failed"} else "info")),
        message=safe_message,
        progress=progress,
        sent_count=int(state.get("sent", 0) or 0),
        failed_count=int(state.get("failed", 0) or 0),
    )


async def _mark_warmup_plan_failed_for_sendgrid_error(
    plan_id: int,
    local_today: date,
    error_message: str,
    *,
    progress: float,
    daily_target: int | None,
    channel_id: int | None = None,
) -> str:
    normalized_error = normalize_send_log_error(error_message)
    final_message = f"SendGrid 额度或限额耗尽：{normalized_error}"
    cancel_warmup_scheduled_start_retry(plan_id)
    WARMUP_STOP_REQUESTED.add(int(plan_id))
    await _finish_plan_day(
        plan_id,
        local_today,
        "failed",
        final_message,
        progress=progress,
        daily_target=daily_target,
        mark_day_complete=False,
        system_log_level="error",
    )
    if channel_id:
        async with AsyncSessionLocal() as db:
            channel = await db.get(Channel, int(channel_id))
            if channel is not None:
                channel.health_status = "error"
                channel.health_error = final_message[:255]
                channel.last_health_check = datetime.utcnow()
                channel.updated_at = datetime.utcnow()
                await commit_session_with_retry(db)
    logger.error(
        "养号计划 %s 因 SendGrid 额度或限额耗尽自动停止，原因：%s",
        plan_id,
        normalized_error,
    )
    return final_message


def _raise_if_unrecoverable_warmup_sendgrid_error(status_code: int, message: str) -> None:
    normalized_status = int(status_code or 0)
    if normalized_status not in SENDGRID_UNRECOVERABLE_WARMUP_STATUS_CODES:
        return
    normalized_message = normalize_send_log_error(message or "SendGrid 返回 401/403，不可恢复")
    raise WarmupSendGridUnrecoverableError(normalized_status, normalized_message)


async def _validate_plan_proxy_consistency(plan_id: int, channel: Channel, plan_proxy_id: int | None) -> tuple[int | None, str | None]:
    proxy_id = plan_proxy_id if plan_proxy_id is not None else channel.proxy_id
    if proxy_id is None:
        return None, None
    if not bool(channel.use_proxy):
        return None, "通道未启用代理，养号发送要求代理，请先在通道设置中启用代理"
    if channel.proxy_id is None:
        return None, "通道未绑定代理，养号发送要求代理"
    if int(channel.proxy_id) != int(proxy_id):
        return None, "计划代理与通道代理不一致，发送中止"
    async with AsyncSessionLocal() as db:
        db_plan = await db.get(WarmupPlan, plan_id)
        if db_plan is not None and int(db_plan.proxy_id or 0) != int(proxy_id):
            db_plan.proxy_id = int(proxy_id)
            db_plan.updated_at = datetime.utcnow()
            await db.commit()
    return int(proxy_id), None


def _validate_warmup_channel_proxy(channel: Channel, plan_proxy_id: int) -> str | None:
    proxy = channel.proxy
    if proxy is None:
        return "通道代理加载失败"
    if int(channel.proxy_id or 0) != int(plan_proxy_id):
        return "计划代理与通道代理不一致，发送中止"
    if int(proxy.id) != int(plan_proxy_id):
        return "通道代理与养号计划绑定代理不一致，发送中止"
    if not bool(proxy.is_active):
        return "通道代理不可用，发送中止"
    return None


async def run_warmup_day(plan_id: int, manual: bool = False, ignore_today_guard_for_new_recipients: bool = False, resume_unfinished: bool = False) -> dict[str, object]:
    current_task = asyncio.current_task()
    existing = RUNNING_WARMUPS.get(plan_id)
    if existing is not None and not existing.done() and existing is not current_task:
        return {"success": False, "message": "养号计划正在运行中", "sent": 0, "failed": 0}
    await _release_warmup_scheduler_plan_claim(plan_id)
    logger.warning(
        "养号计划 %s Runner 开始执行 manual=%s resume_unfinished=%s ignore_today_guard_for_new_recipients=%s",
        plan_id,
        manual,
        resume_unfinished,
        ignore_today_guard_for_new_recipients,
    )

    plan = await _load_plan(plan_id)
    if plan is None:
        return {"success": False, "message": "养号计划不存在", "sent": 0, "failed": 0}
    previous_status = str(plan.status or "scheduled")
    if plan.started_at is None:
        async with AsyncSessionLocal() as db:
            persisted_plan = await db.get(WarmupPlan, plan_id)
            if persisted_plan is not None and persisted_plan.started_at is None:
                persisted_plan.started_at = datetime.utcnow()
                persisted_plan.updated_at = datetime.utcnow()
                await db.commit()
                plan.started_at = persisted_plan.started_at
    auto_scheduled = plan_id in _warmup_scheduled_dispatcher_plans
    if auto_scheduled and not is_warmup_due_now(plan):
        not_due_message = "尚未到达计划定时启动时间，已取消本次自动启动"
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message=not_due_message)
        return {"success": False, "message": not_due_message, "sent": 0, "failed": 0}
    await _reconcile_sent_from_logs(plan_id)
    local_today = local_date_for_plan(plan)
    completed_today_at = plan.run_finished_at if plan.last_run_date == local_today else None
    if plan.last_run_date == local_today and not resume_unfinished and not ignore_today_guard_for_new_recipients:
        await _set_plan_runtime(plan_id, running=False, progress=100.0, message="今天已执行过养号发送")
        return {"success": False, "message": "今天已执行过该计划", "sent": 0, "failed": 0}
    if plan.last_run_date == local_today and ignore_today_guard_for_new_recipients and not resume_unfinished:
        newly_added_count = await _count_unsent_recipients(plan.id, completed_today_at)
        if newly_added_count <= 0:
            await _set_plan_runtime(plan_id, running=False, progress=100.0, message="今天已执行过养号发送，且没有新增未发送邮箱")
            return {"success": False, "message": "今天已执行过该计划，且没有新增未发送邮箱", "sent": 0, "failed": 0}
    if str(plan.status or "").strip().lower() == "failed":
        failed_message = str(plan.run_message or "养号计划已失败，请处理后再手动重启").strip()
        await _set_plan_runtime(plan_id, running=False, progress=float(plan.run_progress or 0), message=failed_message)
        return {"success": False, "message": failed_message, "sent": 0, "failed": 0}
    WARMUP_STOP_REQUESTED.discard(plan_id)
    if not manual and plan.status not in {"active", "scheduled"}:
        return {"success": False, "message": "养号计划未启用", "sent": 0, "failed": 0}
    if not manual and not is_warmup_due_now(plan):
        not_due_message = "尚未到达计划定时启动时间，系统将按计划时区的每日定时自动执行"
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message=not_due_message)
        return {"success": False, "message": not_due_message, "sent": 0, "failed": 0}
    if plan.channel is None:
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message="养号计划未绑定通道")
        return {"success": False, "message": "养号计划未绑定通道", "sent": 0, "failed": 0}
    if not bool(plan.channel.enabled) or not bool(plan.channel.is_active):
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message="绑定通道不可用")
        return {"success": False, "message": "绑定的通道已禁用", "sent": 0, "failed": 0}
    resolved_proxy_id, proxy_consistency_error = await _validate_plan_proxy_consistency(plan.id, plan.channel, plan.proxy_id)
    if resolved_proxy_id is None:
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message="养号计划未绑定代理")
        return {"success": False, "message": "养号计划未绑定代理，请选择已配置代理的通道", "sent": 0, "failed": 0}
    if proxy_consistency_error:
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message=proxy_consistency_error)
        return {"success": False, "message": proxy_consistency_error, "sent": 0, "failed": 0}
    plan.proxy_id = resolved_proxy_id
    proxy_error = _validate_warmup_channel_proxy(plan.channel, resolved_proxy_id)
    if proxy_error:
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message=proxy_error)
        return {"success": False, "message": proxy_error, "sent": 0, "failed": 0}
    logger.debug("warmup plan %s will send via channel proxy %s", plan.id, mask_proxy_for_log(plan.channel.proxy))
    consistency_error = await validate_sendgrid_api_key_proxy_consistency(plan.channel)
    if consistency_error:
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message=consistency_error)
        return {"success": False, "message": consistency_error, "sent": 0, "failed": 0}
    health_error = await ensure_channel_ready_for_task_start(int(plan.channel_id or 0))
    if health_error:
        logger.error("养号计划 %s 启动被拒绝：%s", plan.id, health_error)
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message=health_error)
        return {"success": False, "message": health_error, "sent": 0, "failed": 0}
    send_content_snapshot = await _load_warmup_send_content(plan.id)
    if send_content_snapshot is None:
        await _set_plan_runtime(plan_id, running=False, progress=0.0, message="养号文案未加载或内容为空，禁止发送")
        logger.error("养号计划 %s 发送前文案缺失或为空", plan.id)
        return {"success": False, "message": "养号文案未加载或内容为空，请重新生成或编辑文案", "sent": 0, "failed": 0}

    target_count = calculate_daily_target(plan)
    logger.warning(
        "养号计划 %s 今日目标计算完成：daily_target=%s start_count=%s max_daily_count=%s increase_rate=%s last_run_date=%s last_daily_target=%s",
        plan.id,
        target_count,
        int(plan.start_count or 0),
        int(plan.max_daily_count or 0),
        float(plan.daily_increase_rate or 0.0),
        plan.last_run_date,
        int(plan.last_daily_target or 0),
    )
    if resume_unfinished:
        recipient_created_after = None
        resume_message = "断点续发"
    else:
        recipient_created_after = completed_today_at if plan.last_run_date == local_today and ignore_today_guard_for_new_recipients else None
        resume_message = "养号启动"
    total_unsent = await _count_unsent_recipients(plan.id)
    filtered_unsent = await _count_unsent_recipients(plan.id, recipient_created_after)
    logger.warning(
        "养号计划 %s 收件人加载前统计：总未发送=%s 过滤后未发送=%s created_after=%s resume_unfinished=%s ignore_today_guard_for_new_recipients=%s",
        plan.id,
        total_unsent,
        filtered_unsent,
        recipient_created_after.isoformat() if recipient_created_after is not None else "None",
        resume_unfinished,
        ignore_today_guard_for_new_recipients,
    )
    recipients = await _select_unsent_recipients(plan.id, target_count, recipient_created_after)
    if not recipients and total_unsent > 0 and recipient_created_after is not None:
        logger.warning(
            "养号计划 %s 过滤后无可发收件人，但仍有未发送邮箱（%s）；自动回退为全量未发送队列，避免误判已完成",
            plan.id,
            total_unsent,
        )
        recipients = await _select_unsent_recipients(plan.id, target_count, None)
    if not recipients:
        if total_unsent > 0:
            await _set_plan_runtime(plan.id, running=False, progress=0.0, message="未找到可发送收件人，请检查收件人数据有效性")
            logger.warning(
                "养号计划 %s 仍有未发送邮箱（%s），但本次未选出可发送目标；跳过本轮完成判定",
                plan.id,
                total_unsent,
            )
            return {"success": False, "message": "未找到可发送收件人，请检查收件人数据有效性", "sent": 0, "failed": 0}
        await _finish_plan_day(plan.id, local_today, "completed", "没有未发送收件人，计划已完成", 100.0, daily_target=target_count)
        return {"success": True, "message": "所有收件人均已发送，计划完成", "sent": 0, "failed": 0}

    logger.warning(
        "养号计划 %s 已加载待发送收件人：本次发送=%s（目标=%s）",
        plan.id,
        len(recipients),
        target_count,
    )

    state = WARMUP_STATES.setdefault(plan.id, {"running": False, "sent": 0, "failed": 0, "total": 0, "progress": 0.0, "last_message": "", "next_delay_seconds": 0.0})
    state.update({"running": True, "sent": 0, "failed": 0, "total": len(recipients), "progress": 0.0, "next_delay_seconds": 0.0, "last_message": f"{resume_message}，今日目标 {target_count}，本次发送 {len(recipients)} 封"})
    await _set_plan_runtime(plan.id, running=True, progress=0.0, message=str(state["last_message"]))

    channel_id = int(plan.channel_id or 0)
    if channel_id <= 0:
        await _set_plan_runtime(plan.id, running=False, message="养号计划未绑定有效通道")
        return {"success": False, "message": "养号计划未绑定有效通道", "sent": 0, "failed": 0}
    if await is_task_lock_held(channel_id):
        conflict_message = "该通道已有发送任务在运行"
        if auto_scheduled:
            await _defer_warmup_scheduled_start(
                plan_id,
                SCHEDULED_CHANNEL_BUSY_REASON,
                resume_unfinished=resume_unfinished,
                previous_status=previous_status,
            )
            return {"success": False, "message": conflict_message, "sent": 0, "failed": 0}
        await _set_plan_runtime(plan.id, running=False, message=conflict_message)
        return {"success": False, "message": conflict_message, "sent": 0, "failed": 0}

    warmup_lock = None
    sync_sendgrid_manager = None
    subject, text_body, html_body = send_content_snapshot
    try:
        try:
            warmup_lock = await acquire_warmup_lock(channel_id, plan_id, ttl_seconds=120, timeout_seconds=5)
        except Exception as exc:
            lock_error = f"获取养号锁失败：{exc}"
            logger.error("养号计划 %s 获取养号锁失败 channel_id=%s error_type=%s error=%s", plan_id, channel_id, type(exc).__name__, exc)
            if auto_scheduled:
                await _defer_warmup_scheduled_start(
                    plan_id,
                    lock_error,
                    resume_unfinished=resume_unfinished,
                    previous_status=previous_status,
                )
                return {"success": False, "message": lock_error, "sent": 0, "failed": 0}
            await _set_plan_runtime(plan.id, running=False, message=lock_error)
            return {"success": False, "message": lock_error, "sent": 0, "failed": 0}
        if warmup_lock is None:
            lock_message = f"通道 {channel_id} 已有其他养号计划在运行"
            logger.error("养号计划 %s 养号锁不可用 channel_id=%s", plan_id, channel_id)
            if auto_scheduled:
                await _defer_warmup_scheduled_start(
                    plan_id,
                    SCHEDULED_CHANNEL_BUSY_REASON,
                    resume_unfinished=resume_unfinished,
                    previous_status=previous_status,
                )
                return {"success": False, "message": lock_message, "sent": 0, "failed": 0}
            await _set_plan_runtime(plan.id, running=False, message=lock_message)
            return {"success": False, "message": lock_message, "sent": 0, "failed": 0}
        _warmup_scheduled_dispatcher_plans.discard(plan_id)

        try:
            sync_sendgrid_manager = SyncSendGridClientManager(plan.channel)
        except Exception as exc:
            init_error = f"初始化 SendGrid SDK 客户端失败：{exc}"
            logger.error("养号计划 %s 初始化 SDK 客户端失败 channel_id=%s error_type=%s error=%s", plan_id, channel_id, type(exc).__name__, exc)
            await _finish_plan_day(
                plan.id,
                local_today,
                "paused",
                init_error,
                progress=0.0,
                mark_day_complete=False,
            )
            return {"success": False, "message": init_error, "sent": 0, "failed": 0}

        sent = 0
        failed = 0
        processed = 0
        batch_sender: dict[str, str] | None = None
        base_interval = max(1, int(plan.base_interval_seconds or 1))
        jitter = max(0, int(plan.interval_jitter_seconds or 0))
        # 养号需模拟真实节奏，强制使用小批次（单封）并结合令牌桶与间隔控制。
        http_batch_size = 1
        effective_rate = 1.0 / float(base_interval)
        warmup_limiter = build_rate_limiter(effective_rate)
        logger.warning(
            "养号计划 %s 发送节奏配置：base_interval_seconds=%s interval_jitter_seconds=%s batch_size=%s rate_limit=%.6f封/秒（默认批次上限=%s）",
            plan.id,
            base_interval,
            jitter,
            http_batch_size,
            effective_rate,
            int(WARMUP_DEFAULT_HTTP_BATCH_SIZE),
        )
        for batch_start in range(0, len(recipients), http_batch_size):
            if warmup_lock.lost_event.is_set():
                lock_lost_message = "养号通道锁已失效，为避免同通道并发发送，本次任务已安全停止"
                await _finish_plan_day(
                    plan.id,
                    local_today,
                    "paused",
                    lock_lost_message,
                    progress=float(state.get("progress", 0.0) or 0.0),
                    daily_target=target_count,
                    mark_day_complete=False,
                    system_log_level="error",
                )
                return {"success": False, "message": lock_lost_message, "sent": sent, "failed": failed}
            if plan.id in WARMUP_DELETED_PLANS:
                await _set_plan_runtime(plan.id, running=False, message="计划已删除，养号发送停止")
                return {"success": False, "message": "养号计划已删除", "sent": sent, "failed": failed}
            batch = recipients[batch_start:batch_start + http_batch_size]
            try:
                latest = await _load_plan(plan.id)
                if latest is None:
                    await _set_plan_runtime(plan.id, running=False, message="计划已删除，养号发送停止")
                    return {"success": False, "message": "养号计划已删除", "sent": sent, "failed": failed}
                latest_status = str(latest.status or "").strip().lower()
                if latest_status in {"paused", "failed"} or plan.id in WARMUP_STOP_REQUESTED:
                    stop_message = "计划已失败，当前发送批次停止" if latest_status == "failed" else "计划已暂停，当前发送批次停止"
                    await _set_plan_runtime(plan.id, running=False, message=stop_message)
                    return {"success": latest_status != "failed", "message": stop_message, "sent": sent, "failed": failed}
                if latest.channel is None:
                    raise RuntimeError("养号计划未绑定通道")
                if latest.proxy_id is None and latest.channel.proxy_id is not None:
                    latest.proxy_id = int(latest.channel.proxy_id)
                if latest.proxy_id is None:
                    raise RuntimeError("养号计划未绑定代理")
                latest.channel.use_proxy = True
                proxy_error = _validate_warmup_channel_proxy(latest.channel, int(latest.proxy_id))
                if proxy_error:
                    raise RuntimeError(proxy_error)
                logger.debug(
                    "warmup plan %s sdk batch send batch_size=%s recipients=%s channel_id=%s proxy=%s",
                    latest.id,
                    len(batch),
                    [item.email for item in batch],
                    latest.channel.id,
                    mask_proxy_for_log(latest.channel.proxy),
                )
                await acquire_rate_tokens(warmup_limiter, len(batch))
                for recipient in batch:
                    await _mark_recipient_sending(plan.id, recipient.email)
                batch_results = await send_warmup_sdk_batch(
                    latest.channel,
                    batch,
                    subject=subject,
                    text_body=text_body,
                    html_body=html_body,
                    sync_sendgrid_manager=sync_sendgrid_manager,
                    batch_sender=batch_sender,
                    rotation_enabled=bool(getattr(latest, "sender_rotation_enabled", False)),
                )
                result_by_email = {item.email.lower(): item for item in batch_results}
                batch_hit_sendgrid_limit = False
                limit_message = ""
                for recipient in batch:
                    item = result_by_email.get(str(recipient.email or "").strip().lower())
                    success = bool(item.success) if item is not None else False
                    result_message = (item.body if item is not None else "") or ""
                    if not success and not result_message:
                        result_message = "SendGrid 返回失败，但未提供错误详情"
                    if success:
                        sent += 1
                    else:
                        failed += 1
                        status_code = int(item.status_code) if item is not None else 0
                        if status_code in SENDGRID_UNRECOVERABLE_WARMUP_STATUS_CODES:
                            batch_hit_sendgrid_limit = True
                            result_message = normalize_send_log_error(result_message)
                            limit_message = result_message
                        if status_code and status_code != 202:
                            result_message = f"{result_message} | status_code={status_code}".strip(" |")
                    await _record_send_result(
                        plan.id,
                        recipient.id,
                        recipient.email,
                        success,
                        result_message,
                        local_today,
                        sender_email=str(getattr(item, "sender_email", "") or ""),
                        provider_message_id=str(getattr(item, "provider_message_id", "") or ""),
                        channel=latest.channel,
                    )
                    processed += 1
                    progress = round(processed * 100 / max(1, len(recipients)), 2)
                    message = f"{recipient.email} 发送{'成功' if success else '失败'}，进度 {processed}/{len(recipients)}"
                    state.update({"sent": sent, "failed": failed, "progress": progress, "last_message": message})
                    await _set_plan_runtime(plan.id, progress=progress, message=message)
                if batch_hit_sendgrid_limit:
                    final_limit_message = await _mark_warmup_plan_failed_for_sendgrid_error(
                        plan.id,
                        local_today,
                        limit_message or "SendGrid 返回 401/403，不可恢复",
                        progress=round(processed * 100 / max(1, len(recipients)), 2),
                        daily_target=target_count,
                        channel_id=int(latest.channel_id or 0) if latest.channel_id else None,
                    )
                    return {"success": False, "message": final_limit_message, "sent": sent, "failed": failed}
            except WarmupSendGridUnrecoverableError as exc:
                error_message = await _mark_warmup_plan_failed_for_sendgrid_error(
                    plan.id,
                    local_today,
                    exc.message,
                    progress=round(processed * 100 / max(1, len(recipients)), 2),
                    daily_target=target_count,
                    channel_id=int(getattr(plan, "channel_id", 0) or 0) or None,
                )
                for recipient in batch:
                    failed += 1
                    await _record_send_result(plan.id, recipient.id, recipient.email, False, error_message, local_today, channel=plan.channel)
                    processed += 1
                    progress = round(processed * 100 / max(1, len(recipients)), 2)
                    state.update({"sent": sent, "failed": failed, "progress": progress, "last_message": error_message})
                    await _set_plan_runtime(plan.id, running=False, progress=progress, message=error_message)
                return {"success": False, "message": error_message, "sent": sent, "failed": failed}
            except Exception as exc:
                error_message = normalize_send_log_error(str(exc) or "养号发送失败")
                logger.exception(
                    "warmup batch send failed plan_id=%s batch_size=%s error_type=%s error=%s",
                    plan.id,
                    len(batch),
                    type(exc).__name__,
                    exc,
                )
                for recipient in batch:
                    failed += 1
                    await _record_send_result(plan.id, recipient.id, recipient.email, False, error_message, local_today)
                    processed += 1
                    progress = round(processed * 100 / max(1, len(recipients)), 2)
                    message = f"{recipient.email} 发送失败：{error_message}，进度 {processed}/{len(recipients)}"
                    state.update({"sent": sent, "failed": failed, "progress": progress, "last_message": message})
                    await _set_plan_runtime(plan.id, progress=progress, message=message)
            if batch_start + len(batch) < len(recipients):
                delay = max(1.0, base_interval + random.uniform(-jitter, jitter))
                state["next_delay_seconds"] = round(delay, 2)
                logger.warning(
                    "养号计划 %s 批次间隔等待 %.2f 秒（base=%s jitter=%s）",
                    plan.id,
                    delay,
                    base_interval,
                    jitter,
                )
                delay_deadline = monotonic() + delay
                while True:
                    remaining_delay = delay_deadline - monotonic()
                    if remaining_delay <= 0:
                        break
                    if warmup_lock.lost_event.is_set():
                        lock_lost_message = "养号通道锁已失效，为避免同通道并发发送，本次任务已安全停止"
                        await _finish_plan_day(
                            plan.id,
                            local_today,
                            "paused",
                            lock_lost_message,
                            progress=float(state.get("progress", 0.0) or 0.0),
                            daily_target=target_count,
                            mark_day_complete=False,
                            system_log_level="error",
                        )
                        return {"success": False, "message": lock_lost_message, "sent": sent, "failed": failed}
                    if plan.id in WARMUP_STOP_REQUESTED:
                        stop_message = "计划已暂停，养号间隔等待已中断"
                        await _set_plan_runtime(plan.id, running=False, message=stop_message)
                        return {"success": True, "message": stop_message, "sent": sent, "failed": failed}
                    latest_during_delay = await _load_plan(plan.id)
                    if latest_during_delay is None:
                        await _set_plan_runtime(plan.id, running=False, message="计划已删除，养号发送停止")
                        return {"success": False, "message": "养号计划已删除", "sent": sent, "failed": failed}
                    latest_delay_status = str(latest_during_delay.status or "").strip().lower()
                    if latest_delay_status in {"paused", "failed"}:
                        stop_message = "计划已失败，养号间隔等待已中断" if latest_delay_status == "failed" else "计划已暂停，养号间隔等待已中断"
                        await _set_plan_runtime(plan.id, running=False, message=stop_message)
                        return {"success": latest_delay_status != "failed", "message": stop_message, "sent": sent, "failed": failed}
                    state["next_delay_seconds"] = round(max(0.0, remaining_delay), 2)
                    await asyncio.sleep(min(1.0, remaining_delay))
        async with AsyncSessionLocal() as db:
            remaining = int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(WarmupRecipient.plan_id == plan.id, WarmupRecipient.sent.is_(False)))).scalar_one())
        success_progress = round(sent * 100 / max(1, len(recipients)), 2)
        if sent == 0 and failed > 0:
            failure_message = f"养号本轮发送全部失败（{failed} 封），已记录失败并继续保留计划，避免单轮失败阻塞后续定时"
            logger.error(
                "养号计划 %s 本轮发送全部失败但不暂停计划：processed=%s sent=%s failed=%s remaining=%s",
                plan.id,
                processed,
                sent,
                failed,
                remaining,
            )
        elapsed_days_after_run = calculate_warmup_elapsed_days(plan, local_today)
        total_days = max(0, int(getattr(plan, "total_days", 0) or 0))
        reached_last_day = total_days > 0 and elapsed_days_after_run >= total_days
        if remaining == 0:
            final_status = "completed"
            final_progress = 100.0
            final_message = f"当天养号完成，成功 {sent}，失败 {failed}；全部收件人已发送，计划完成"
        elif reached_last_day:
            final_status = "finished"
            final_progress = 100.0
            final_message = f"第 {elapsed_days_after_run}/{total_days} 天养号发送完成，成功 {sent}，失败 {failed}；已达到设定天数，计划完成"
        else:
            final_status = "active"
            final_progress = success_progress
            day_progress = f"第 {elapsed_days_after_run}/{total_days} 天，" if total_days > 0 else ""
            final_message = f"{day_progress}当天养号结束，成功 {sent}，失败 {failed}，剩余未发送 {remaining} 封"
        await _finish_plan_day(
            plan.id,
            local_today,
            final_status,
            final_message,
            final_progress,
            daily_target=target_count,
            mark_day_complete=True,
        )
        return {
            "success": sent > 0,
            "message": final_message,
            "sent": sent,
            "failed": failed,
        }
    finally:
        _warmup_scheduled_dispatcher_plans.discard(plan_id)
        await _release_warmup_scheduler_plan_claim(plan_id)
        release_warmup_concurrency(plan_id)
        await release_warmup_lock(warmup_lock)
        logger.warning("养号计划 %s Runner 资源已释放 channel_id=%s", plan_id, channel_id)
        if sync_sendgrid_manager is not None:
            try:
                sync_sendgrid_manager.close()
            except Exception:
                logger.exception("养号计划 %s 关闭 SendGrid 同步客户端失败", plan.id)


async def start_warmup_runner(
    plan_id: int,
    manual: bool = False,
    ignore_today_guard_for_new_recipients: bool = False,
    resume_unfinished: bool = False,
    from_scheduled: bool = False,
) -> tuple[bool, str]:
    current = RUNNING_WARMUPS.get(plan_id)
    if current is not None and not current.done():
        return False, "养号任务已在运行"
    previous_status = "scheduled"
    if manual:
        cancel_warmup_scheduled_start_retry(plan_id)
        await _release_warmup_scheduler_plan_claim(plan_id)
        logger.info("养号计划 %s 手动启动前已清理定时重试和调度 claim", plan_id)
    plan = await _load_plan(plan_id)
    if plan is not None:
        previous_status = str(plan.status or "scheduled")
        if plan.channel_id is not None:
            health_error = await ensure_channel_ready_for_task_start(int(plan.channel_id))
            if health_error:
                logger.error("养号计划 %s 启动被拒绝：%s", plan_id, health_error)
                await _set_plan_runtime(plan_id, running=False, message=health_error)
                return False, health_error
    reserved, reserve_message = await try_reserve_warmup_concurrency(plan_id)
    if not reserved:
        logger.warning("养号计划 %s 全局并发预检未通过：%s", plan_id, reserve_message)
        if from_scheduled and _is_warmup_scheduled_start_retryable(reserve_message):
            await _enqueue_warmup_scheduled_start_retry(
                plan_id,
                reserve_message,
                resume_unfinished=resume_unfinished,
                previous_status=previous_status,
            )
            await _release_warmup_scheduler_plan_claim(plan_id)
            return False, reserve_message
        await _set_plan_runtime(plan_id, running=False, message=reserve_message)
        return False, reserve_message
    if from_scheduled:
        if plan is None or not is_warmup_due_now(plan):
            release_warmup_concurrency(plan_id)
            await _release_warmup_scheduler_plan_claim(plan_id)
            return False, "尚未到达计划定时启动时间"
        _warmup_scheduled_dispatcher_plans.add(plan_id)
    task = asyncio.create_task(
        run_warmup_day(
            plan_id,
            manual=manual,
            ignore_today_guard_for_new_recipients=ignore_today_guard_for_new_recipients,
            resume_unfinished=resume_unfinished,
        ),
        name=f"warmup-plan-{plan_id}",
    )
    RUNNING_WARMUPS[plan_id] = task
    task.add_done_callback(lambda done: _cleanup_runner(plan_id, done))
    logger.warning(
        "养号计划 %s Runner 已创建 manual=%s from_scheduled=%s resume_unfinished=%s task_name=%s",
        plan_id,
        manual,
        from_scheduled,
        resume_unfinished,
        task.get_name(),
    )
    return True, ""


async def stop_warmup_runner(plan_id: int, *, deleted: bool = False) -> bool:
    normalized_id = int(plan_id)
    cancel_warmup_scheduled_start_retry(normalized_id)
    if deleted:
        WARMUP_DELETED_PLANS.add(normalized_id)
    current = RUNNING_WARMUPS.get(normalized_id)
    if current is None or current.done():
        RUNNING_WARMUPS.pop(normalized_id, None)
        WARMUP_STOP_REQUESTED.discard(normalized_id)
        if deleted:
            WARMUP_STATES.pop(normalized_id, None)
            release_warmup_concurrency(normalized_id)
        else:
            await _reconcile_sent_from_logs(normalized_id)
            await _set_plan_runtime(normalized_id, running=False, message="计划已暂停")
        return False
    WARMUP_STOP_REQUESTED.add(normalized_id)
    try:
        try:
            await asyncio.wait_for(asyncio.shield(current), timeout=WARMUP_STOP_GRACE_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(
                "warmup plan %s stop grace period expired after %ss, cancelling runner",
                normalized_id,
                WARMUP_STOP_GRACE_SECONDS,
            )
            current.cancel()
            try:
                await current
            except asyncio.CancelledError:
                pass
    finally:
        WARMUP_STOP_REQUESTED.discard(normalized_id)
    RUNNING_WARMUPS.pop(normalized_id, None)
    release_warmup_concurrency(normalized_id)
    if deleted:
        WARMUP_STATES.pop(normalized_id, None)
    else:
        await _reconcile_sent_from_logs(normalized_id)
        await _set_plan_runtime(normalized_id, running=False, message="计划已暂停")
    return True


_WARMUP_UNAUTHORIZED_STOP_IN_PROGRESS: set[int] = set()


async def stop_warmup_for_channel_unauthorized(plan_id: int) -> bool:
    """通道 API 密钥失效时停止养号计划（含运行中 Runner）。"""
    normalized_id = int(plan_id or 0)
    if normalized_id <= 0:
        return False
    if normalized_id in _WARMUP_UNAUTHORIZED_STOP_IN_PROGRESS:
        return False
    _WARMUP_UNAUTHORIZED_STOP_IN_PROGRESS.add(normalized_id)
    try:
        plan = await _load_plan(normalized_id)
        runner = RUNNING_WARMUPS.get(normalized_id)
        runner_active = runner is not None and not runner.done()
        if plan is None and not runner_active:
            return False
        if plan is not None and not bool(plan.running) and not runner_active:
            return False

        logger.error("通道 API 密钥已失效，自动停止任务：%s", normalized_id)
        stop_message = "通道 API 密钥已失效，自动停止养号计划"
        cancel_warmup_scheduled_start_retry(normalized_id)
        if runner_active:
            await stop_warmup_runner(normalized_id)
        else:
            await _set_plan_runtime(normalized_id, running=False, message=stop_message)
        async with AsyncSessionLocal() as db:
            db_plan = await db.get(WarmupPlan, normalized_id)
            if db_plan is not None and str(db_plan.status or "").strip().lower() not in {"completed", "draft"}:
                db_plan.status = "paused"
                db_plan.running = False
                db_plan.run_message = stop_message
                db_plan.run_finished_at = datetime.utcnow()
                db_plan.updated_at = datetime.utcnow()
                await commit_session_with_retry(db)
        return True
    finally:
        _WARMUP_UNAUTHORIZED_STOP_IN_PROGRESS.discard(normalized_id)


async def _recover_running_flags() -> None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(WarmupPlan).where(WarmupPlan.running.is_(True)))
        plans = result.scalars().all()
        for plan in plans:
            plan.running = False
            if plan.last_run_date is None:
                local_today = local_date_for_plan(plan)
                plan.last_run_date = local_today - timedelta(days=1)
                logger.warning(
                    "warmup plan %s interrupted by service restart with empty last_run_date; "
                    "last_run_date set to %s to avoid duplicate auto batch today",
                    plan.id,
                    plan.last_run_date.isoformat(),
                )
            else:
                logger.warning(
                    "warmup plan %s interrupted by service restart; running flag cleared, last_run_date=%s",
                    plan.id,
                    plan.last_run_date.isoformat() if plan.last_run_date else "none",
                )
            plan.run_message = "服务重启后已清理上次运行态，将继续按计划调度"
            plan.run_finished_at = datetime.utcnow()
            plan.updated_at = datetime.utcnow()
        if plans:
            await db.commit()


async def _count_plan_unsent(plan_id: int) -> int:
    return await _count_unsent_recipients(plan_id)


async def _finish_plan_if_total_days_reached(plan: WarmupPlan, db) -> bool:
    total_days = max(0, int(getattr(plan, "total_days", 0) or 0))
    if total_days <= 0 or not getattr(plan, "started_at", None):
        return False
    local_today = plan_local_now(plan, datetime.now(timezone.utc)).date()
    elapsed_days = calculate_warmup_elapsed_days(plan, local_today)
    # 调度扫描发生在当日发送之前；达到总天数时仍应允许最后一天执行，
    # 仅当已经越过配置的最后一天才做兜底完成。
    if elapsed_days <= total_days:
        return False
    message = "养号计划已完成，累计发送天数已达到预设值"
    plan.status = "finished"
    plan.running = False
    plan.run_progress = 100
    plan.run_message = message
    plan.run_finished_at = datetime.utcnow()
    plan.updated_at = datetime.utcnow()
    db.add(WarmupSystemLog(plan_id=plan.id, level="info", message=message, progress=100))
    await db.commit()
    cancel_warmup_scheduled_start_retry(plan.id)
    WARMUP_STOP_REQUESTED.add(int(plan.id))
    logger.info("养号计划 %s 已完成，累计发送天数已达到预设值", plan.id)
    return True


async def _activate_scheduled_plan_for_auto_start(plan_id: int) -> None:
    async with AsyncSessionLocal() as db:
        plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
        if plan is None:
            return
        if await _finish_plan_if_total_days_reached(plan, db):
            return
        plan.status = "active"
        plan.running = False
        if plan.started_at is None:
            plan.started_at = datetime.utcnow()
        plan.run_progress = 0
        plan.run_message = "定时到达，计划已自动启动"
        plan.run_started_at = datetime.utcnow()
        plan.run_finished_at = None
        plan.updated_at = datetime.utcnow()
        await db.commit()


async def _activate_paused_plan_for_auto_resume(plan_id: int) -> None:
    async with AsyncSessionLocal() as db:
        plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
        if plan is None:
            return
        plan.status = "active"
        plan.running = True
        plan.run_progress = 0
        plan.run_message = "定时到达，已自动从暂停状态恢复，正在从未发送邮箱继续执行"
        plan.run_started_at = datetime.utcnow()
        plan.run_finished_at = None
        plan.updated_at = datetime.utcnow()
        await db.commit()


async def _process_warmup_scheduled_start_retries() -> int:
    started = 0
    now_value = monotonic()
    interval_seconds = get_scheduled_start_retry_interval_seconds()
    max_attempts = get_scheduled_start_retry_max_attempts()
    pending_ids = [
        plan_id
        for plan_id, entry in list(_warmup_scheduled_start_retries.items())
        if now_value >= float(entry.next_retry_at or 0)
    ]
    for plan_id in pending_ids:
        entry = _warmup_scheduled_start_retries.get(int(plan_id))
        if entry is None:
            continue
        if not await _try_claim_warmup_scheduler_plan(int(plan_id)):
            continue
        plan = await _load_plan(int(plan_id))
        if plan is None:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            continue
        if str(plan.status or "") not in {"scheduled", "active", "paused"}:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            continue
        retry_now_utc = datetime.now(timezone.utc)
        retry_local_date = local_date_for_plan(plan, retry_now_utc)
        current_due_utc = scheduled_utc_datetime(plan, retry_local_date)
        expected_due_date = entry.due_local_date or retry_local_date
        expected_due_utc = _normalize_utc_datetime(entry.due_utc) if entry.due_utc is not None else current_due_utc
        if retry_local_date != expected_due_date or retry_now_utc < current_due_utc or current_due_utc != expected_due_utc:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            await _revert_warmup_plan_for_retry(
                int(plan_id),
                "计划日期或定时时间已变化，本次通道占用重试已取消，将等待下一次设定时间",
                target_status=entry.previous_status or "scheduled",
            )
            logger.warning(
                "养号计划 %s 取消过期定时重试 expected_date=%s current_date=%s expected_due_utc=%s current_due_utc=%s now_utc=%s",
                plan_id,
                expected_due_date,
                retry_local_date,
                expected_due_utc.isoformat(),
                current_due_utc.isoformat(),
                retry_now_utc.isoformat(),
            )
            continue
        if plan.last_run_date == retry_local_date:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            continue
        if plan.channel_id is None:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            continue
        channel_key = int(plan.channel_id)
        if await _is_channel_busy_for_scheduled_warmup(channel_key, int(plan_id)):
            await _enqueue_warmup_scheduled_start_retry(
                int(plan_id),
                SCHEDULED_CHANNEL_BUSY_REASON,
                resume_unfinished=entry.resume_unfinished,
                previous_status=entry.previous_status,
            )
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            logger.info(
                "养号计划 %s 定时启动重试：通道 %s 仍被占用，%s 秒后再次尝试（第 %s/%s 次）",
                plan_id,
                channel_key,
                int(interval_seconds),
                min(entry.attempts + 1, max_attempts),
                max_attempts,
            )
            continue
        health_error = await ensure_channel_ready_for_task_start(channel_key)
        if health_error:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            await _release_warmup_scheduler_plan_claim(int(plan_id))
            await _revert_warmup_plan_for_retry(int(plan_id), health_error, target_status="paused")
            logger.error("养号计划 %s 定时启动重试被拒绝：%s", plan_id, health_error)
            continue
        if entry.resume_unfinished:
            await _activate_paused_plan_for_auto_resume(int(plan_id))
            runner_started, _ = await start_warmup_runner(int(plan_id), manual=True, resume_unfinished=True, from_scheduled=True)
        else:
            if str(plan.status or "").strip().lower() == "scheduled":
                await _activate_scheduled_plan_for_auto_start(int(plan_id))
            runner_started, _ = await start_warmup_runner(int(plan_id), manual=False, from_scheduled=True)
        if runner_started:
            _warmup_scheduled_start_retries.pop(int(plan_id), None)
            started += 1
        else:
            await _release_warmup_scheduler_plan_claim(int(plan_id))
    return started


async def _try_schedule_plan(plan: WarmupPlan, now_utc: datetime, *, resume_unfinished: bool) -> None:
    if not await _try_claim_warmup_scheduler_plan(int(plan.id)):
        return
    if not is_warmup_due_now(plan, now_utc):
        await _release_warmup_scheduler_plan_claim(int(plan.id))
        return
    if await _is_warmup_scheduled_start_waiting(plan.id):
        await _release_warmup_scheduler_plan_claim(int(plan.id))
        return
    local_now = plan_local_now(plan, now_utc)
    logger.debug(
        "养号调度命中计划 plan_id=%s status=%s resume_unfinished=%s scheduled_time=%s timezone=%s local_now=%s now_utc=%s",
        plan.id,
        plan.status,
        resume_unfinished,
        plan.scheduled_time,
        plan.timezone or DEFAULT_TIMEZONE,
        local_now.isoformat(),
        now_utc.isoformat(),
    )
    previous_status = str(plan.status or "scheduled")
    if plan.channel_id is not None:
        channel_key = int(plan.channel_id)
        health_error = await ensure_channel_ready_for_task_start(channel_key)
        if health_error:
            await _release_warmup_scheduler_plan_claim(int(plan.id))
            await _revert_warmup_plan_for_retry(int(plan.id), health_error, target_status="paused")
            logger.error("养号调度拒绝启动计划 %s：%s", plan.id, health_error)
            return
        if await _is_channel_busy_for_scheduled_warmup(channel_key, int(plan.id)):
            due_local_date = local_now.date()
            due_utc = scheduled_utc_datetime(plan, due_local_date)
            await _enqueue_warmup_scheduled_start_retry(
                int(plan.id),
                SCHEDULED_CHANNEL_BUSY_REASON,
                resume_unfinished=resume_unfinished,
                previous_status=previous_status,
                due_local_date=due_local_date,
                due_utc=due_utc,
            )
            await _release_warmup_scheduler_plan_claim(int(plan.id))
            logger.warning(
                "养号计划 %s 因通道 %s 被占用进入定时重试，已释放调度 claim",
                plan.id,
                channel_key,
            )
            return
    if resume_unfinished:
        await _activate_paused_plan_for_auto_resume(plan.id)
        started, reason = await start_warmup_runner(plan.id, manual=True, resume_unfinished=True, from_scheduled=True)
    else:
        if (plan.status or "").strip().lower() == "scheduled":
            await _activate_scheduled_plan_for_auto_start(plan.id)
        started, reason = await start_warmup_runner(plan.id, manual=False, from_scheduled=True)
    if not started:
        await _release_warmup_scheduler_plan_claim(int(plan.id))
        if _is_warmup_scheduled_start_retryable(reason):
            await _enqueue_warmup_scheduled_start_retry(
                int(plan.id),
                reason,
                resume_unfinished=resume_unfinished,
                previous_status=previous_status,
            )
            return
        logger.warning("养号调度跳过 plan_id=%s 原因=%s", plan.id, reason)
        await _set_plan_runtime(plan.id, running=False, message=reason or "养号计划启动失败")


async def warmup_scheduler_loop() -> None:
    from app.core.database import assert_database_tables_ready

    await assert_database_tables_ready()
    await _recover_running_flags()
    await asyncio.sleep(2)
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            retried = await _process_warmup_scheduled_start_retries()
            if retried:
                logger.debug("养号定时调度本轮已通过重试启动 %s 个计划", retried)
            async with AsyncSessionLocal() as db:
                auto_result = await db.execute(
                    select(WarmupPlan).where(
                        WarmupPlan.status.in_(("scheduled", "active")),
                        WarmupPlan.running.is_(False),
                    )
                )
                auto_plans = list(auto_result.scalars().all())
            auto_due_count = 0
            auto_started = 0
            for plan in auto_plans:
                async with AsyncSessionLocal() as check_db:
                    live_plan = (await check_db.execute(select(WarmupPlan).where(WarmupPlan.id == plan.id))).scalar_one_or_none()
                    if live_plan is None or await _finish_plan_if_total_days_reached(live_plan, check_db):
                        continue
                local_today = plan_local_now(plan, now_utc).date()
                if plan.last_run_date == local_today:
                    continue
                if await _is_warmup_scheduled_start_waiting(plan.id):
                    continue
                if not is_warmup_due_now(plan, now_utc):
                    continue
                auto_due_count += 1
                before_running = bool(get_warmup_state(plan.id).get("running", False))
                await _try_schedule_plan(plan, now_utc, resume_unfinished=False)
                after_running = bool(get_warmup_state(plan.id).get("running", False))
                if not before_running and after_running:
                    auto_started += 1
            logger.warning(
                "养号调度扫描完成：utc=%s active_or_scheduled=%s due_active_or_scheduled=%s started_active_or_scheduled=%s retry_started=%s",
                now_utc.isoformat(),
                len(auto_plans),
                auto_due_count,
                auto_started,
                retried,
            )
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("养号调度扫描待执行计划时失败")
            await asyncio.sleep(30)
