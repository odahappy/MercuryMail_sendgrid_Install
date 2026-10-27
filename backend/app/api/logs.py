import asyncio
import logging
import re
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, field_validator
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import require_admin
from app.core.database import get_db
from app.models.models import Recipient, SendLog, Task, WarmupPlan

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/logs", tags=["logs"])
LOG_DEEP_COUNT_PAGE_THRESHOLD = 50
LOG_DEEP_COUNT_TOTAL_SENTINEL = -1
LOG_DELETE_BATCH_SIZE = 5000
LOG_DELETE_BATCH_PAUSE_SECONDS = 0.02
LOG_TYPE_LABELS = {
    "all": "全部",
    "regular": "普通发送",
    "monitor": "监控邮箱",
    "warmup": "养号",
}


def _active_task_log_clause():
    """日志查看展示未软删除任务记录；养号等无任务日志允许展示。"""
    return (Task.id.is_(None)) | (Task.deleted_at.is_(None))


def _warmup_plan_filter(model, plan_id: int | None):
    return None


def _extract_warmup_plan_id(log) -> int:
    return 0


def _build_task_name_map(tasks: list[Task]) -> dict[int, str]:
    return {int(task.id): str(task.name or "") for task in tasks if getattr(task, "id", None)}


def _build_warmup_plan_name_map(plans: list[WarmupPlan]) -> dict[int, str]:
    return {int(plan.id): str(plan.name or "") for plan in plans if getattr(plan, "id", None)}


def _normalize_recorded_egress(value: str | None) -> str:
    raw_value = str(value or "").strip()
    if not raw_value or raw_value == "直连":
        return ""
    without_protocol = re.sub(r"^[a-z][a-z0-9+.-]*://", "", raw_value, flags=re.IGNORECASE)
    without_auth = without_protocol.rsplit("@", 1)[-1]
    without_path = without_auth.split("/", 1)[0].strip()
    match = re.match(r"^(\d{1,3}(?:\.\d{1,3}){3})(?::\d{1,5})?$", without_path)
    if match:
        return match.group(1)
    return without_path


class LogOut(BaseModel):
    id: int
    task_id: int
    task_name: str
    email: str
    sender_email: str | None = None
    status: str
    log_type: str = "regular"
    error_message: str
    response_text: str
    attempt_count: int
    egress_ip: str = ""
    proxy_info: str = ""
    outbound_ip: str | None = None
    sent_at: datetime | None
    created_at: datetime
    monitor_trigger_count: int = 0


class LogListResponse(BaseModel):
    items: list[LogOut]
    total: int
    page: int
    page_size: int


class MonitorLogStatsResponse(BaseModel):
    total: int
    success_count: int
    failed_count: int
    success_rate: float
    latest_trigger_count: int
    unique_monitor_emails: int
    avg_attempt_count: float


class ClearLogsResponse(BaseModel):
    success: bool
    log_type: str
    deleted_count: int
    message: str


class ClearLogsQuery(BaseModel):
    type: str = "all"

    @field_validator("type")
    @classmethod
    def validate_log_type(cls, value):
        normalized = str(value or "all").strip().lower()
        if normalized not in LOG_TYPE_LABELS:
            raise ValueError("日志类型必须是 all、regular、monitor 或 warmup")
        return normalized


def _clear_log_type_clause(log_type: str):
    if log_type == "all":
        return None
    if log_type == "regular":
        return func.coalesce(SendLog.log_type, "regular") == "regular"
    return SendLog.log_type == log_type


async def _delete_send_logs_in_batches(db: AsyncSession, log_type: str) -> int:
    """按主键小批量删除并逐批提交，避免超大事务长期锁定 send_logs。"""
    deleted_total = 0
    type_clause = _clear_log_type_clause(log_type)
    while True:
        id_query = select(SendLog.id).order_by(SendLog.id.asc()).limit(LOG_DELETE_BATCH_SIZE)
        if type_clause is not None:
            id_query = id_query.where(type_clause)
        ids = list((await db.execute(id_query)).scalars().all())
        if not ids:
            return deleted_total
        result = await db.execute(delete(SendLog).where(SendLog.id.in_(ids)))
        deleted_total += max(0, int(result.rowcount or 0))
        await db.commit()
        if len(ids) < LOG_DELETE_BATCH_SIZE:
            return deleted_total
        await asyncio.sleep(LOG_DELETE_BATCH_PAUSE_SECONDS)


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1]
    return datetime.fromisoformat(normalized)


def _normalize_sdk_status_for_display(log: SendLog) -> str:
    raw_status = str(log.status or "").strip().lower()
    if raw_status in {"monitor_sent", "success"}:
        return "sent"
    if raw_status in {"monitor_failed", "fail", "blocked", "dropped"}:
        return "failed"
    if raw_status == "unknown":
        return "unknown"
    if raw_status:
        return raw_status
    return "pending"


def _resolve_log_sender_email_for_display(log: SendLog) -> str | None:
    direct = str(getattr(log, "sender_email", "") or "").strip()
    if direct:
        return direct[:255]
    return None


def _resolve_log_proxy_info_for_display(log: SendLog) -> str:
    direct = str(getattr(log, "proxy_info", "") or "").strip()
    if direct:
        return direct[:255]
    return ""


def _resolve_log_response_text_for_display(log: SendLog) -> str:
    response = str(getattr(log, "response_text", "") or "").strip()
    if response:
        return response
    return ""



def log_to_out(
    log: SendLog,
    *,
    task_names: dict[int, str] | None = None,
    warmup_plan_names: dict[int, str] | None = None,
) -> LogOut:
    trigger_count = int(getattr(log, "monitor_trigger_count", 0) or 0)
    task_id = int(getattr(log, "task_id", 0) or 0)
    task_name = str(getattr(getattr(log, "task", None), "name", "") or "")
    if not task_name and task_names and task_id:
        task_name = task_names.get(task_id, "")
    if not task_name and str(getattr(log, "log_type", "") or "") == "warmup":
        plan_id = _extract_warmup_plan_id(log)
        if warmup_plan_names and plan_id:
            task_name = warmup_plan_names.get(plan_id, "")
        if not task_name and plan_id:
            task_name = f"养号计划 #{plan_id}"
    return LogOut(
        id=log.id,
        task_id=task_id,
        task_name=task_name,
        email=(getattr(getattr(log, "recipient", None), "email", "") or "") or (log.email or ""),
        sender_email=_resolve_log_sender_email_for_display(log),
        status=_normalize_sdk_status_for_display(log),
        log_type=str(getattr(log, "log_type", "") or "regular"),
        error_message=log.error_message or log.error or "",
        response_text=_resolve_log_response_text_for_display(log),
        attempt_count=log.attempt_count or 0,
        egress_ip=_normalize_recorded_egress(getattr(log, "egress_ip", "") or ""),
        proxy_info=_resolve_log_proxy_info_for_display(log),
        outbound_ip=_normalize_recorded_egress(getattr(log, "outbound_ip", "") or "") or None,
        sent_at=log.sent_at,
        created_at=log.created_at,
        monitor_trigger_count=trigger_count,
    )


@router.get("/monitors/stats", response_model=MonitorLogStatsResponse, dependencies=[Depends(require_admin)])
async def monitor_log_stats(
    task_id: int | None = Query(None, ge=1),
    email: str | None = Query(None, max_length=255),
    db: AsyncSession = Depends(get_db),
):
    try:
        def build_monitor_filters(model):
            model_filters = [model.status.in_(["monitor_sent", "monitor_failed"]), model.log_type == "monitor"]
            if task_id:
                model_filters.append(model.task_id == task_id)
            if email and email.strip():
                model_filters.append(model.email.ilike(f"%{email.strip()}%"))
            return model_filters

        hot_filters = build_monitor_filters(SendLog)

        def _hot_monitor_count_query(*extra_filters):
            return (
                select(func.count())
                .select_from(SendLog)
                .join(Task, SendLog.task_id == Task.id)
                .where(*hot_filters, _active_task_log_clause(), *extra_filters)
            )

        total = int((await db.execute(_hot_monitor_count_query())).scalar_one())
        success_count = int((await db.execute(_hot_monitor_count_query(SendLog.status == "monitor_sent"))).scalar_one())
        failed_count = int((await db.execute(_hot_monitor_count_query(SendLog.status == "monitor_failed"))).scalar_one())
        unique_monitor_emails = int((await db.execute(
            select(func.count(func.distinct(SendLog.email)))
            .select_from(SendLog)
            .join(Task, SendLog.task_id == Task.id)
            .where(*hot_filters, _active_task_log_clause())
        )).scalar_one())
        avg_attempt = (
            await db.execute(
                select(func.avg(SendLog.attempt_count))
                .select_from(SendLog)
                .join(Task, SendLog.task_id == Task.id)
                .where(*hot_filters, _active_task_log_clause())
            )
        ).scalar_one_or_none() or 0
        latest_trigger_count = int(
            (
                await db.execute(
                    select(func.coalesce(SendLog.monitor_trigger_count, 0))
                    .select_from(SendLog)
                    .join(Task, SendLog.task_id == Task.id)
                    .where(*hot_filters, _active_task_log_clause())
                    .order_by(SendLog.created_at.desc(), SendLog.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            or 0
        )
        return MonitorLogStatsResponse(
            total=total,
            success_count=success_count,
            failed_count=failed_count,
            success_rate=round((success_count / total * 100) if total else 0, 2),
            latest_trigger_count=latest_trigger_count,
            unique_monitor_emails=unique_monitor_emails,
            avg_attempt_count=round(float(avg_attempt), 2),
        )
    except Exception as exc:
        logger.exception("监控日志统计失败 task_id=%s email=%s", task_id, email)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务器繁忙，请稍后重试") from exc


@router.get("/monitors", response_model=LogListResponse, dependencies=[Depends(require_admin)])
async def list_monitor_logs(
    task_id: int | None = Query(None, ge=1),
    email: str | None = Query(None, max_length=255),
    status: str | None = Query(None, max_length=32),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    query = (
        select(SendLog)
        .join(Task, SendLog.task_id == Task.id)
        .options(selectinload(SendLog.task))
        .where(SendLog.log_type == "monitor", _active_task_log_clause())
    )
    count_query = (
        select(func.count())
        .select_from(SendLog)
        .join(Task, SendLog.task_id == Task.id)
        .where(SendLog.log_type == "monitor", _active_task_log_clause())
    )
    filters = []
    if task_id:
        filters.append(SendLog.task_id == task_id)
    if email and email.strip():
        filters.append(SendLog.email.ilike(f"%{email.strip()}%"))
    if status:
        status_value = status.strip()
        if status_value == "sent":
            filters.append(SendLog.status.in_(["sent", "success", "monitor_sent"]))
        elif status_value == "failed":
            filters.append(SendLog.status.in_(["failed", "fail", "monitor_failed"]))
        elif status_value == "unknown":
            filters.append(SendLog.status == "unknown")
        elif status_value in {"monitor_sent", "monitor_failed"}:
            filters.append(SendLog.status == status_value)
    parsed_start = parse_time(start_time)
    parsed_end = parse_time(end_time)
    if parsed_start:
        filters.append(SendLog.created_at >= parsed_start)
    if parsed_end:
        filters.append(SendLog.created_at <= parsed_end)
    if filters:
        query = query.where(*filters)
        count_query = count_query.where(*filters)
    total = int((await db.execute(count_query)).scalar_one())
    result = await db.execute(query.order_by(SendLog.created_at.desc(), SendLog.id.desc()).offset((page - 1) * page_size).limit(page_size))
    return LogListResponse(items=[log_to_out(log) for log in result.scalars().all()], total=total, page=page, page_size=page_size)


@router.delete("", response_model=ClearLogsResponse)
async def clear_logs(
    query: ClearLogsQuery = Depends(),
    operator: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    normalized_type = query.type
    type_label = LOG_TYPE_LABELS[normalized_type]
    operator_id = int(operator.get("user_id") or 0)
    operator_name = str(operator.get("username") or "未知管理员")
    try:
        deleted_count = await _delete_send_logs_in_batches(db, normalized_type)
    except Exception as exc:
        await db.rollback()
        logger.exception(
            "管理员清空日志失败：操作者=%s(user_id=%s)，类型=%s",
            operator_name,
            operator_id,
            type_label,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清空{type_label}日志失败，请稍后重试",
        ) from exc
    logger.warning(
        "管理员执行不可逆日志清空：操作者=%s(user_id=%s)，类型=%s，删除行数=%s",
        operator_name,
        operator_id,
        type_label,
        deleted_count,
    )
    return ClearLogsResponse(
        success=True,
        log_type=normalized_type,
        deleted_count=deleted_count,
        message=f"{type_label}日志已清空，共删除 {deleted_count} 条",
    )


@router.get("", response_model=LogListResponse, dependencies=[Depends(require_admin)])
async def list_logs(
    response: Response,
    task_id: int | None = Query(None, ge=1),
    email: str | None = Query(None, max_length=255),
    status: str | None = Query(None, max_length=32),
    log_type: str | None = Query(None, max_length=32),
    plan_id: int | None = Query(None, ge=1),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    normalized_log_type = (log_type or "all").strip().lower()
    if normalized_log_type not in {"", "all", "regular", "monitor", "warmup"}:
        normalized_log_type = "regular"
    if normalized_log_type == "":
        normalized_log_type = "all"

    def build_filters(model, *, include_recipient: bool = False):
        model_filters = []
        if normalized_log_type == "all":
            model_filters.append(func.coalesce(model.log_type, "regular").in_(["regular", "monitor", "warmup"]))
        elif normalized_log_type == "regular":
            model_filters.append(func.coalesce(model.log_type, "regular") == "regular")
        else:
            model_filters.append(model.log_type == normalized_log_type)
        if task_id:
            model_filters.append(model.task_id == task_id)
        warmup_filter = _warmup_plan_filter(model, plan_id)
        if warmup_filter is not None:
            model_filters.append(warmup_filter)
        if email and email.strip():
            email_value = f"%{email.strip()}%"
            if include_recipient:
                model_filters.append((Recipient.email.ilike(email_value)) | (model.email.ilike(email_value)))
            else:
                model_filters.append(model.email.ilike(email_value))
        if status:
            status_value = status.strip().lower()
            if status_value in {"success", "sent"}:
                if normalized_log_type == "monitor":
                    model_filters.append(model.status.in_(["sent", "success", "monitor_sent"]))
                elif normalized_log_type in {"regular", "warmup"}:
                    model_filters.append(model.status.in_(["sent", "success", "delivered"]))
                else:
                    model_filters.append(model.status.in_(["sent", "success", "delivered", "monitor_sent"]))
            elif status_value in {"failed", "fail"}:
                if normalized_log_type == "monitor":
                    model_filters.append(model.status.in_(["failed", "fail", "monitor_failed", "blocked", "dropped", "bounced"]))
                elif normalized_log_type in {"regular", "warmup"}:
                    model_filters.append(model.status.in_(["failed", "fail", "bounced", "blocked", "dropped"]))
                else:
                    model_filters.append(model.status.in_(["failed", "fail", "bounced", "blocked", "dropped", "monitor_failed"]))
            else:
                model_filters.append(model.status == status_value)
        parsed_start = parse_time(start_time)
        parsed_end = parse_time(end_time)
        if parsed_start:
            model_filters.append(model.created_at >= parsed_start)
        if parsed_end:
            model_filters.append(model.created_at <= parsed_end)
        return model_filters

    hot_filters = build_filters(SendLog, include_recipient=True)
    hot_query = (
        select(SendLog)
        .outerjoin(Task, SendLog.task_id == Task.id)
        .outerjoin(SendLog.recipient)
        .options(selectinload(SendLog.task), selectinload(SendLog.recipient))
        .where(_active_task_log_clause(), *hot_filters)
    )
    hot_count_query = (
        select(func.count())
        .select_from(SendLog)
        .outerjoin(Task, SendLog.task_id == Task.id)
        .outerjoin(Recipient, SendLog.recipient_id == Recipient.id)
        .where(_active_task_log_clause(), *hot_filters)
    )

    deep_page_without_precise_filter = (
        page > LOG_DEEP_COUNT_PAGE_THRESHOLD
        and not task_id
        and not plan_id
        and not (email and email.strip())
        and not start_time
        and not end_time
    )
    if deep_page_without_precise_filter:
        total = LOG_DEEP_COUNT_TOTAL_SENTINEL
        response.headers["X-Mercury-Logs-Total-Skipped"] = "1"
    else:
        hot_total = int((await db.execute(hot_count_query)).scalar_one())
        total = int((await db.execute(hot_count_query)).scalar_one())
    candidate_limit = min(page * page_size, (LOG_DEEP_COUNT_PAGE_THRESHOLD + 1) * page_size)
    hot_result = await db.execute(hot_query.order_by(SendLog.created_at.desc(), SendLog.id.desc()).limit(candidate_limit))
    combined = list(hot_result.scalars().all())
    combined.sort(key=lambda item: (item.created_at or datetime.min, int(item.id or 0)), reverse=True)
    start_index = (page - 1) * page_size
    page_items = combined[start_index : start_index + page_size]
    task_names: dict[int, str] = {}
    warmup_plan_ids = sorted({_extract_warmup_plan_id(log) for log in page_items if str(getattr(log, "log_type", "") or "") == "warmup" and _extract_warmup_plan_id(log) > 0})
    warmup_plan_names: dict[int, str] = {}
    if warmup_plan_ids:
        plan_rows = (await db.execute(select(WarmupPlan).where(WarmupPlan.id.in_(warmup_plan_ids)))).scalars().all()
        warmup_plan_names = _build_warmup_plan_name_map(list(plan_rows))
    return LogListResponse(
        items=[log_to_out(log, task_names=task_names, warmup_plan_names=warmup_plan_names) for log in page_items],
        total=total,
        page=page,
        page_size=page_size,
    )
