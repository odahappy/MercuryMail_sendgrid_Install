import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.api.deps import ensure_export_operation_no_audit, require_admin
from app.core.database import AsyncSessionLocal
from app.models.models import RecipientList
from app.services.async_export_service import (
    AsyncExportNotFoundError,
    build_async_export_download_response,
    get_async_export_meta,
    get_async_export_status,
)
from app.services.export_service import (
    ExportScopeError,
    build_group_export_sql,
    build_task_delivery_export_sql,
    build_valid_recipients_sql,
    dispatch_smart_export,
    export_timestamp_filename,
    list_export_tasks,
    resolve_group_export_filename,
)

from app.utils.export import ExportCopyError, format_export_error_message

router = APIRouter(prefix="/api/export", tags=["export"])
logger = logging.getLogger(__name__)


class ValidRecipientsDeliveryExportIn(BaseModel):
    task_ids: list[int] = Field(..., min_length=1, max_length=500)
    type: Literal["success", "soft", "all"] = Field(...)


class ExportTaskOut(BaseModel):
    id: int
    name: str
    status: str
    status_label: str = "未知"
    started_at: object | None = None
    timezone: str = "Asia/Shanghai"


class AsyncExportStatusOut(BaseModel):
    export_id: str
    status: str
    download_url: str | None = None
    filename: str | None = None
    file_size: int | None = None
    progress_percent: int = 0
    error: str | None = None


_EXPORT_AUDIT_DEPS = [Depends(ensure_export_operation_no_audit)]


def _ensure_async_export_owner(meta: dict[str, object], admin_payload: dict) -> None:
    request_user_id = int(admin_payload.get("user_id") or 0)
    owner_user_id = int(meta.get("user_id") or 0)
    if request_user_id <= 0 or owner_user_id <= 0 or request_user_id != owner_user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="导出任务不存在或已过期")


async def _ensure_recipient_list_exists(group_id: int) -> None:
    normalized_group_id = int(group_id or 0)
    if normalized_group_id <= 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="分组不存在")
    async with AsyncSessionLocal() as db:
        exists = (await db.execute(select(RecipientList.id).where(RecipientList.id == normalized_group_id))).scalar_one_or_none()
    if exists is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="分组不存在")


@router.get("/tasks", response_model=list[ExportTaskOut], dependencies=_EXPORT_AUDIT_DEPS)
async def list_tasks_for_export(
    window: str = Query(..., description="hot=近3天, archive=3~30天前"),
    admin_payload: dict = Depends(require_admin),
):
    """列出可用于有效收件人导出的任务（热数据或归档窗口）。"""
    try:
        logger.info("列出导出任务 window=%s user_id=%s", window, admin_payload.get("user_id"))
        return await list_export_tasks(window)
    except ExportScopeError as exc:
        logger.exception("列出导出任务参数异常 window=%s", window)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("列出导出任务失败 window=%s", window)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="获取任务列表失败") from exc


@router.post("/valid-recipients/hot", dependencies=_EXPORT_AUDIT_DEPS)
async def export_valid_recipients_hot(
    request: Request,
    payload: ValidRecipientsDeliveryExportIn,
    admin_payload: dict = Depends(require_admin),
):
    """按所选热数据任务与投递结果类型导出有效收件人。"""
    filename = export_timestamp_filename("valid_recipients_hot")
    try:
        logger.info("开始导出热数据有效收件人 task_ids=%s export_type=%s user_id=%s", payload.task_ids, payload.type, admin_payload.get("user_id"))
        sql = build_task_delivery_export_sql(
            log_table="send_logs",
            task_ids=payload.task_ids,
            export_type=payload.type,
        )
        return await dispatch_smart_export(request, sql, filename, user_id=int(admin_payload.get("user_id") or 0))
    except ExportScopeError as exc:
        logger.exception("导出热数据有效收件人参数异常 task_ids=%s export_type=%s", payload.task_ids, payload.type)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportCopyError as exc:
        logger.exception("导出热数据有效收件人 COPY 失败 task_ids=%s export_type=%s", payload.task_ids, payload.type)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("导出热数据有效收件人失败 task_ids=%s export_type=%s", payload.task_ids, payload.type)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.post("/valid-recipients/archive", dependencies=_EXPORT_AUDIT_DEPS)
async def export_valid_recipients_archive(
    request: Request,
    payload: ValidRecipientsDeliveryExportIn,
    admin_payload: dict = Depends(require_admin),
):
    """按所选归档任务与投递结果类型导出有效收件人。"""
    filename = export_timestamp_filename("valid_recipients_archive")
    try:
        logger.info("开始导出归档有效收件人 task_ids=%s export_type=%s user_id=%s", payload.task_ids, payload.type, admin_payload.get("user_id"))
        sql = build_task_delivery_export_sql(
            log_table="send_log_archive",
            task_ids=payload.task_ids,
            export_type=payload.type,
        )
        return await dispatch_smart_export(request, sql, filename, user_id=int(admin_payload.get("user_id") or 0))
    except ExportScopeError as exc:
        logger.exception("导出归档有效收件人参数异常 task_ids=%s export_type=%s", payload.task_ids, payload.type)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportCopyError as exc:
        logger.exception("导出归档有效收件人 COPY 失败 task_ids=%s export_type=%s", payload.task_ids, payload.type)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("导出归档有效收件人失败 task_ids=%s export_type=%s", payload.task_ids, payload.type)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.get("/valid-recipients", dependencies=_EXPORT_AUDIT_DEPS)
async def export_valid_recipients(
    request: Request,
    source: str = Query(..., description="all=全局有效收件人池"),
    admin_payload: dict = Depends(require_admin),
):
    """导出全局有效收件人 TXT（UTF-8 无 BOM，每行一个邮箱）。"""
    normalized = str(source or "").strip().lower()
    if normalized != "all":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="source 参数必须为 all；热/归档数据请使用 POST 接口并选择任务",
        )
    filename = export_timestamp_filename("valid_recipients_all")
    try:
        logger.info("开始导出全局有效收件人 source=%s user_id=%s", normalized, admin_payload.get("user_id"))
        sql = build_valid_recipients_sql(normalized)
        return await dispatch_smart_export(request, sql, filename, user_id=int(admin_payload.get("user_id") or 0))
    except ExportScopeError as exc:
        logger.exception("导出有效收件人参数异常 source=%s", normalized)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportCopyError as exc:
        logger.exception("导出有效收件人 COPY 失败 source=%s", normalized)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("导出有效收件人失败 source=%s", normalized)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.get("/group/{group_id}", dependencies=_EXPORT_AUDIT_DEPS)
async def export_group_recipients(
    request: Request,
    group_id: int,
    admin_payload: dict = Depends(require_admin),
):
    """导出分组内全部原始邮箱（不含任何投递/退订筛选）。"""
    try:
        logger.info("开始导出分组邮箱 group_id=%s user_id=%s", group_id, admin_payload.get("user_id"))
        await _ensure_recipient_list_exists(group_id)
        filename = await resolve_group_export_filename(group_id)
        sql = build_group_export_sql(group_id)
        return await dispatch_smart_export(request, sql, filename, user_id=int(admin_payload.get("user_id") or 0))
    except HTTPException:
        raise
    except ExportScopeError as exc:
        logger.exception("导出分组邮箱参数异常 group_id=%s", group_id)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ExportCopyError as exc:
        logger.exception("导出分组邮箱 COPY 失败 group_id=%s", group_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("导出分组邮箱失败 group_id=%s", group_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=format_export_error_message(exc)) from exc


@router.get("/async/{export_id}", response_model=AsyncExportStatusOut, dependencies=_EXPORT_AUDIT_DEPS)
async def get_async_export_task_status(export_id: str, admin_payload: dict = Depends(require_admin)):
    """查询异步导出任务状态。"""
    try:
        logger.info("查询异步导出状态 export_id=%s user_id=%s", export_id, admin_payload.get("user_id"))
        meta = await get_async_export_meta(export_id)
        _ensure_async_export_owner(meta, admin_payload)
        return await get_async_export_status(export_id)
    except AsyncExportNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("查询异步导出状态失败 export_id=%s error=%s", export_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=format_export_error_message(exc),
        ) from exc


@router.get("/async/{export_id}/download", dependencies=_EXPORT_AUDIT_DEPS)
async def download_async_export_file(export_id: str, admin_payload: dict = Depends(require_admin)):
    """下载已完成的异步导出文件。"""
    try:
        logger.info("下载异步导出文件 export_id=%s user_id=%s", export_id, admin_payload.get("user_id"))
        meta = await get_async_export_meta(export_id)
        _ensure_async_export_owner(meta, admin_payload)
        return await build_async_export_download_response(export_id)
    except HTTPException:
        raise
    except AsyncExportNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("下载异步导出文件失败 export_id=%s error=%s", export_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=format_export_error_message(exc),
        ) from exc
