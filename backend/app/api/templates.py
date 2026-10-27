import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_admin
from app.core.database import get_db
from app.models.models import EmailTemplate, Task, URLPool
from app.services import template_service

router = APIRouter(prefix="/api/templates", tags=["templates"])
logger = logging.getLogger(__name__)
DEFAULT_RANDOM_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
ACTIVE_TASK_STATUSES = ("running", "pending", "paused", "scheduled")


class TemplateBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=160)
    subject: str = Field(default="", max_length=500)
    content_type: str = Field(default="html")
    html_content: str = ""
    text_content: str = ""
    random_length: int = Field(default=16, ge=1, le=512)
    random_chars: str = Field(default=DEFAULT_RANDOM_CHARS, min_length=1, max_length=255)
    random_rules: dict[str, Any] = Field(default_factory=dict)
    campaign_id: str = Field(default="", max_length=120)
    unsubscribe_enabled: bool = False
    unsubscribe_url: str = ""
    unsubscribe_description: str = Field(default="一键退订", max_length=160)

    @field_validator("content_type")
    @classmethod
    def validate_content_type(cls, value: str) -> str:
        normalized = (value or "html").strip().lower()
        if normalized not in {"html", "text"}:
            raise ValueError("正文类型只能是 html 或 text")
        return normalized

    @model_validator(mode="after")
    def validate_body(self) -> "TemplateBase":
        if not str(self.html_content or "").strip() and not str(self.text_content or "").strip():
            raise ValueError("HTML 正文和纯文本正文不能同时为空")
        return self


class TemplateCreate(TemplateBase):
    model_config = ConfigDict(title="TemplateCreate")


class TemplateUpdate(TemplateBase):
    model_config = ConfigDict(title="TemplateUpdate")


class TemplateOut(BaseModel):
    id: int
    name: str
    subject: str
    content_type: str
    html_content: str
    text_content: str
    html_body: str
    text_body: str
    random_length: int
    random_chars: str
    random_rules: dict[str, Any]
    campaign_id: str
    unsubscribe_enabled: bool
    unsubscribe_url: str
    unsubscribe_description: str
    sendgrid_substitution_enabled: bool
    substitution_warnings: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class TemplateListResponse(BaseModel):
    items: list[TemplateOut]
    total: int
    page: int
    page_size: int


class TemplatePreviewRequest(BaseModel):
    variables: dict[str, Any] = Field(default_factory=dict)
    url_pool_id: int | None = None
    task_id: int | None = None


class TemplatePreviewResponse(BaseModel):
    subject: str
    html: str
    text: str


def _parse_random_rules(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dump_random_rules(value: dict[str, Any] | None) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


async def _ensure_url_pool_exists(db: AsyncSession, url_pool_id: int | None) -> None:
    if url_pool_id is None:
        return
    pool = (await db.execute(select(URLPool.id).where(URLPool.id == url_pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")


async def _ensure_template_name_available(db: AsyncSession, name: str, exclude_id: int | None = None) -> None:
    query = select(EmailTemplate.id).where(EmailTemplate.name == name)
    if exclude_id is not None:
        query = query.where(EmailTemplate.id != exclude_id)
    existing = (await db.execute(query)).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="模板名称已存在")


async def _count_template_tasks(db: AsyncSession, template_id: int) -> int:
    result = await db.execute(
        select(func.count()).select_from(Task).where(
            Task.template_id == template_id,
            Task.deleted_at.is_(None),
            Task.status.in_(ACTIVE_TASK_STATUSES),
        )
    )
    return max(0, int(result.scalar_one() or 0))


async def template_to_out(db: AsyncSession, template: EmailTemplate, *, substitution_warnings: list[str] | None = None) -> TemplateOut:
    html_body = template.html_body or ""
    text_body = template.text_body or ""
    warnings = list(substitution_warnings or [])
    if not warnings:
        warnings = template_service.validate_substitution_template(template)
    return TemplateOut(
        id=template.id,
        name=template.name or "",
        subject=template.subject or "",
        content_type=template.content_type or "html",
        html_content=html_body,
        text_content=text_body,
        html_body=html_body,
        text_body=text_body,
        random_length=int(template.random_length or 16),
        random_chars=template.random_chars or DEFAULT_RANDOM_CHARS,
        random_rules=_parse_random_rules(template.random_rules_json),
        campaign_id=template.campaign_id or "",
        unsubscribe_enabled=bool(template.unsubscribe_enabled),
        unsubscribe_url=template.unsubscribe_url or "",
        unsubscribe_description=template.unsubscribe_description or "一键退订",
        sendgrid_substitution_enabled=True,
        substitution_warnings=warnings,
        created_at=template.created_at,
        updated_at=template.updated_at,
    )


def apply_payload(template: EmailTemplate, payload: TemplateBase) -> None:
    template.name = payload.name.strip()
    template.subject = payload.subject.strip()
    template.content_type = payload.content_type
    template.html_body = payload.html_content if payload.content_type == "html" else ""
    template.text_body = payload.text_content if payload.content_type == "text" else ""
    template.random_length = payload.random_length
    template.random_chars = payload.random_chars
    template.random_rules_json = _dump_random_rules(payload.random_rules)
    template.campaign_id = payload.campaign_id.strip()
    template.unsubscribe_enabled = bool(payload.unsubscribe_enabled)
    template.unsubscribe_url = payload.unsubscribe_url.strip()
    template.unsubscribe_description = payload.unsubscribe_description.strip() or "一键退订"
    template.sendgrid_substitution_enabled = True


def _collect_substitution_warnings(template: EmailTemplate) -> list[str]:
    return template_service.validate_substitution_template(template)


async def list_templates_handler(page: int, page_size: int, db: AsyncSession) -> TemplateListResponse:
    total = int((await db.execute(select(func.count()).select_from(EmailTemplate))).scalar_one())
    result = await db.execute(select(EmailTemplate).order_by(EmailTemplate.updated_at.desc(), EmailTemplate.id.desc()).offset((page - 1) * page_size).limit(page_size))
    templates = result.scalars().all()
    return TemplateListResponse(items=[await template_to_out(db, item) for item in templates], total=total, page=page, page_size=page_size)


async def create_template_handler(payload: TemplateCreate, db: AsyncSession) -> TemplateOut:
    await _ensure_template_name_available(db, payload.name.strip())
    template = EmailTemplate(created_at=datetime.utcnow(), updated_at=datetime.utcnow())
    apply_payload(template, payload)
    db.add(template)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="模板名称已存在")
    await db.refresh(template)
    template_service.compile_template(template)
    warnings = _collect_substitution_warnings(template)
    return await template_to_out(db, template, substitution_warnings=warnings)


@router.get("", response_model=TemplateListResponse, dependencies=[Depends(require_admin)])
async def list_templates(page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    try:
        return await list_templates_handler(page, page_size, db)
    except Exception as exc:
        logger.exception("获取模板列表失败 page=%s page_size=%s", page, page_size)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.get("/", response_model=TemplateListResponse, dependencies=[Depends(require_admin)], include_in_schema=False)
async def list_templates_with_slash(page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    return await list_templates_handler(page, page_size, db)


@router.post("", response_model=TemplateOut, dependencies=[Depends(require_admin)])
async def create_template(payload: TemplateCreate, db: AsyncSession = Depends(get_db)):
    return await create_template_handler(payload, db)


@router.post("/", response_model=TemplateOut, dependencies=[Depends(require_admin)], include_in_schema=False)
async def create_template_with_slash(payload: TemplateCreate, db: AsyncSession = Depends(get_db)):
    return await create_template_handler(payload, db)


@router.put("/{template_id}", response_model=TemplateOut, dependencies=[Depends(require_admin)])
async def update_template(template_id: int, payload: TemplateUpdate, db: AsyncSession = Depends(get_db)):
    template = (await db.execute(select(EmailTemplate).where(EmailTemplate.id == template_id))).scalar_one_or_none()
    if template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="模板不存在")
    await _ensure_template_name_available(db, payload.name.strip(), exclude_id=template_id)
    template_service.invalidate_template_cache(template_id)
    apply_payload(template, payload)
    template.updated_at = datetime.utcnow()
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="模板名称已存在")
    await db.refresh(template)
    template_service.compile_template(template)
    warnings = _collect_substitution_warnings(template)
    return await template_to_out(db, template, substitution_warnings=warnings)


@router.delete("/{template_id}", dependencies=[Depends(require_admin)])
async def delete_template(template_id: int, db: AsyncSession = Depends(get_db)):
    template = (await db.execute(select(EmailTemplate).where(EmailTemplate.id == template_id))).scalar_one_or_none()
    if template is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="模板不存在")
    task_count = await _count_template_tasks(db, template_id)
    if task_count > 0:
        blocking_task = (
            await db.execute(
                select(Task.id, Task.name).where(
                    Task.template_id == template_id,
                    Task.deleted_at.is_(None),
                    Task.status.in_(ACTIVE_TASK_STATUSES),
                ).order_by(Task.id.desc()).limit(1)
            )
        ).first()
        task_label = blocking_task.name if blocking_task is not None and blocking_task.name else f"任务#{blocking_task.id}" if blocking_task is not None else "运行中任务"
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"模板正被运行中的任务「{task_label}」使用，无法删除")
    now_value = datetime.utcnow()
    await db.execute(
        update(Task)
        .where(
            Task.template_id == template_id,
            or_(
                Task.deleted_at.is_not(None),
                Task.status.is_(None),
                ~Task.status.in_(ACTIVE_TASK_STATUSES),
            ),
        )
        .values(template_id=None, updated_at=now_value)
    )
    template_service.invalidate_template_cache(template_id)
    await db.delete(template)
    await db.commit()
    return {"success": True, "message": "模板已删除"}



@router.post("/{template_id}/preview", response_model=TemplatePreviewResponse, dependencies=[Depends(require_admin)])
async def preview_template(template_id: int, payload: TemplatePreviewRequest, db: AsyncSession = Depends(get_db)):
    try:
        template = (await db.execute(select(EmailTemplate).where(EmailTemplate.id == template_id))).scalar_one_or_none()
        if template is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="模板不存在")
        effective_url_pool_id = payload.url_pool_id
        if effective_url_pool_id is None and payload.task_id is not None:
            task = (await db.execute(select(Task).where(Task.id == payload.task_id))).scalar_one_or_none()
            if task is None or task.deleted_at is not None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在")
            binding_error = template_service.validate_task_url_pool_binding(task)
            if binding_error:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=binding_error)
            effective_url_pool_id = task.url_pool_id
        requires_url_pool = template_service.template_requires_url_pool(template)
        if requires_url_pool and effective_url_pool_id is None:
            first_pool_id = (
                await db.execute(select(URLPool.id).order_by(URLPool.id.asc()).limit(1))
            ).scalar_one_or_none()
            effective_url_pool_id = first_pool_id
        if requires_url_pool and effective_url_pool_id is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="模板需要追踪链接，请指定 URL 池或关联已绑定 URL 池的任务",
            )
        await _ensure_url_pool_exists(db, effective_url_pool_id)
        rendered = await template_service.preview_substitution_template(
            template,
            url_pool_id=effective_url_pool_id,
            variables=payload.variables,
        )
        return TemplatePreviewResponse(subject=rendered.subject, html=rendered.html, text=rendered.text)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("预览模板失败 template_id=%s payload_task_id=%s url_pool_id=%s", template_id, payload.task_id, payload.url_pool_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc
