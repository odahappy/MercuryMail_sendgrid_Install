import logging
import re
from datetime import date, datetime, time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import delete as sql_delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import require_admin
from app.core.database import get_db
from app.models.models import Channel, Proxy, WarmupContent, WarmupLog, WarmupPlan, WarmupRecipient, WarmupSystemLog
from app.services.ai_writer import generate_email_content, generate_warmup_content
from app.services.locker import is_task_lock_held, is_warmup_lock_held
from app.services.warmup_sender import calculate_daily_target, calculate_remaining_today, get_warmup_state, start_warmup_runner, stop_warmup_runner

router = APIRouter(prefix="/api/warmup", tags=["warmup"])
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DEFAULT_TIMEZONE = "Asia/Shanghai"
WARMUP_RECIPIENT_INSERT_BATCH_SIZE = 500
logger = logging.getLogger(__name__)


def normalize_timezone(value: str | None) -> str:
    timezone_name = (value or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("服务器不支持该时区") from exc
    return timezone_name


def parse_emails(text: str) -> list[str]:
    emails = []
    seen = set()
    for item in re.split(r"[\n,;\s]+", text or ""):
        email = item.strip().lower()
        if EMAIL_PATTERN.match(email) and email not in seen:
            emails.append(email)
            seen.add(email)
    return emails


async def batch_insert_warmup_recipients(
    db: AsyncSession,
    plan_id: int,
    emails: list[str],
    *,
    created_at: datetime | None = None,
) -> int:
    if not emails:
        return 0
    insert_at = created_at or datetime.utcnow()
    added = 0
    for offset in range(0, len(emails), WARMUP_RECIPIENT_INSERT_BATCH_SIZE):
        chunk = emails[offset : offset + WARMUP_RECIPIENT_INSERT_BATCH_SIZE]
        statement = (
            pg_insert(WarmupRecipient)
            .values([{"plan_id": plan_id, "email": email, "sent": False, "created_at": insert_at} for email in chunk])
            .on_conflict_do_nothing(index_elements=["plan_id", "email"])
        )
        result = await db.execute(statement)
        added += max(0, int(result.rowcount or 0))
    return added


class ContentIn(BaseModel):
    subject: str = Field(..., min_length=1, max_length=500)
    text_body: str = Field(default="", max_length=200000)
    html_body: str = Field(default="", max_length=200000)

    @field_validator("subject")
    @classmethod
    def strip_subject(cls, value: str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("邮件主题不能为空")
        return stripped

    @field_validator("text_body", "html_body", mode="before")
    @classmethod
    def strip_body(cls, value: str):
        return (value or "").strip()

    @model_validator(mode="after")
    def validate_body(self) -> "ContentIn":
        if not self.text_body and not self.html_body:
            raise ValueError("HTML 正文和纯文本正文至少填写一个")
        return self


class WarmupPlanCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=160)
    channel_id: int
    start_count: int = Field(default=5, ge=1, le=100000)
    total_days: int = Field(default=0, ge=0, le=3650)
    daily_increase_rate: float = Field(default=0.2, ge=0, le=10)
    max_daily_count: int = Field(default=100, ge=1, le=1000000)
    base_interval_seconds: int = Field(default=60, ge=1, le=86400)
    interval_jitter_seconds: int = Field(default=10, ge=0, le=86400)
    scheduled_time: time
    timezone: str = Field(default=DEFAULT_TIMEZONE, min_length=1, max_length=64)
    prompt: str = Field(default="", max_length=20000)
    recipients_text: str = Field(..., min_length=3, max_length=2000000)
    content_language: str = Field(default="zh", pattern="^(zh|ja|en|de|fr|es|it|pt|ko|random)$")
    tone: str = Field(default="自然日常", min_length=1, max_length=80)
    must_include: str = Field(default="", max_length=5000)
    avoid: str = Field(default="", max_length=5000)
    fidelity_mode: str = Field(default="strict", pattern="^(strict|balanced|creative)$")
    ai_generation_mode: str = Field(default="manual_prompt", pattern="^(manual_prompt|conditions)$")
    email_type: str = Field(default="business", pattern="^(business|personal|notice|greeting|thanks|random)$")
    include_url: bool = Field(default=False)
    sender_rotation_enabled: bool = Field(default=False, description="是否启用养号发件人轮询；需通道配置多个启用发件人时生效。")
    content_mode: str = Field(default="ai", pattern="^(ai|manual)$")
    content: ContentIn | None = None

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("字段不能为空")
        return stripped

    @field_validator("prompt", mode="before")
    @classmethod
    def strip_prompt(cls, value: str):
        return (value or "").strip()

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str):
        return normalize_timezone(value)

    @model_validator(mode="after")
    def validate_content_mode(self) -> "WarmupPlanCreate":
        content_mode = self.content_mode or "ai"
        prompt = self.prompt or ""
        content = self.content
        if content_mode == "manual" and content is None:
            raise ValueError("手动输入模式必须填写邮件主题和正文")
        if content_mode == "ai" and self.ai_generation_mode == "manual_prompt" and not prompt:
            raise ValueError("手动描述生成必须填写描述")
        return self


class WarmupPlanUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=160)
    start_count: int | None = Field(default=None, ge=1, le=100000)
    total_days: int | None = Field(default=None, ge=0, le=3650)
    daily_increase_rate: float | None = Field(default=None, ge=0, le=10)
    max_daily_count: int | None = Field(default=None, ge=1, le=1000000)
    base_interval_seconds: int | None = Field(default=None, ge=1, le=86400)
    interval_jitter_seconds: int | None = Field(default=None, ge=0, le=86400)
    scheduled_time: time | None = None
    timezone: str | None = Field(default=None, min_length=1, max_length=64)
    prompt: str | None = Field(default=None, max_length=20000)
    content_language: str | None = Field(default=None, pattern="^(zh|ja|en|de)$")
    tone: str | None = Field(default=None, min_length=1, max_length=80)
    must_include: str | None = Field(default=None, max_length=5000)
    avoid: str | None = Field(default=None, max_length=5000)
    fidelity_mode: str | None = Field(default=None, pattern="^(strict|balanced|creative)$")
    sender_rotation_enabled: bool | None = None

    @field_validator("name")
    @classmethod
    def strip_name_optional(cls, value: str | None):
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("字段不能为空")
        return stripped

    @field_validator("prompt", "must_include", "avoid", mode="before")
    @classmethod
    def strip_optional_text(cls, value):
        if value is None:
            return None
        return str(value).strip()

    @field_validator("tone", mode="before")
    @classmethod
    def strip_tone(cls, value):
        if value is None:
            return None
        stripped = str(value).strip()
        return stripped or None

    @field_validator("timezone")
    @classmethod
    def validate_timezone_optional(cls, value: str | None):
        if value is None:
            return None
        return normalize_timezone(value)

    @model_validator(mode="after")
    def validate_has_updates(self) -> "WarmupPlanUpdate":
        if not any(
            getattr(self, field_name) is not None
            for field_name in self.model_fields
        ):
            raise ValueError("请至少提供一个需要更新的字段")
        return self


class WarmupContentPreviewRequest(BaseModel):
    ai_generation_mode: str = Field(default="manual_prompt", pattern="^(manual_prompt|conditions)$")
    prompt: str = Field(default="", max_length=20000)
    content_language: str = Field(default="zh", pattern="^(zh|ja|en|de|fr|es|it|pt|ko|random)$")
    tone: str = Field(default="自然日常", min_length=1, max_length=80)
    must_include: str = Field(default="", max_length=5000)
    avoid: str = Field(default="", max_length=5000)
    fidelity_mode: str = Field(default="strict", pattern="^(strict|balanced|creative)$")
    email_type: str = Field(default="business", pattern="^(business|personal|notice|greeting|thanks|random)$")
    include_url: bool = False

    @field_validator("prompt", mode="before")
    @classmethod
    def strip_prompt_preview(cls, value: str):
        return (value or "").strip()

    @model_validator(mode="after")
    def validate_generation_mode(self) -> "WarmupContentPreviewRequest":
        if self.ai_generation_mode == "manual_prompt" and not self.prompt:
            raise ValueError("手动描述生成必须填写描述")
        return self


class WarmupRegenerateRequest(BaseModel):
    prompt: str | None = Field(default=None, max_length=20000)
    content_language: str | None = Field(default=None, pattern="^(zh|ja|en|de)$")
    tone: str | None = Field(default=None, min_length=1, max_length=80)
    must_include: str | None = Field(default=None, max_length=5000)
    avoid: str | None = Field(default=None, max_length=5000)
    fidelity_mode: str | None = Field(default=None, pattern="^(strict|balanced|creative)$")

    @field_validator("prompt", mode="before")
    @classmethod
    def strip_prompt(cls, value):
        return (value or "").strip()

    @field_validator("tone", mode="before")
    @classmethod
    def strip_tone_regenerate(cls, value):
        if value is None:
            return None
        stripped = str(value).strip()
        return stripped or None

    @field_validator("must_include", "avoid", mode="before")
    @classmethod
    def strip_optional_text_regenerate(cls, value):
        return (value or "").strip()


class WarmupRecipientAddRequest(BaseModel):
    emails: list[str] = Field(..., min_length=1, max_length=10000)

    @field_validator("emails")
    @classmethod
    def normalize_emails(cls, value: list[str]):
        normalized = []
        seen = set()
        for item in value:
            email = str(item or "").strip().lower()
            if EMAIL_PATTERN.match(email) and email not in seen:
                normalized.append(email)
                seen.add(email)
        if not normalized:
            raise ValueError("没有有效的收件人邮箱")
        return normalized


class ContentOut(BaseModel):
    subject: str
    html_body: str = ""
    text_body: str = ""
    source: str = "ai"
    error: str = ""


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


class WarmupPlanOut(BaseModel):
    id: int
    name: str
    channel_id: int
    channel_name: str
    channel: ChannelOut | None = None
    proxy_name: str = ""
    proxy_address: str = "直连"
    start_count: int
    total_days: int = 0
    started_at: datetime | None = None
    elapsed_days: int = 0
    remaining_days: int | None = None
    daily_increase_rate: float
    max_daily_count: int
    base_interval_seconds: int
    interval_jitter_seconds: int
    scheduled_time: time
    timezone: str
    status: str
    total_sent: int
    last_run_date: date | None
    prompt: str
    content_mode: str | None = None
    content_language: str = "zh"
    tone: str = "自然日常"
    must_include: str = ""
    avoid: str = ""
    fidelity_mode: str = "strict"
    recipient_total: int
    recipient_sent: int
    recipient_unsent: int
    target_today: int
    daily_target: int
    remaining_today: int
    running: bool
    progress: float
    last_message: str
    next_delay_seconds: float = 0
    success_count: int = 0
    fail_count: int = 0
    content: ContentOut | None = None
    content_stale: bool = False
    sender_rotation_enabled: bool = False
    created_at: datetime
    updated_at: datetime


class PlanListResponse(BaseModel):
    items: list[WarmupPlanOut]
    total: int
    page: int
    page_size: int


class WarmupPlanStatusOut(BaseModel):
    running: bool
    progress: float
    last_message: str
    daily_target: int
    remaining_today: int
    recipient_unsent: int
    next_delay_seconds: float = 0


class WarmupRecipientAddResponse(BaseModel):
    success: bool
    added_count: int
    skipped_count: int
    total_input_count: int
    duplicate_input_count: int
    plan: WarmupPlanOut
    message: str


async def get_plan_or_404(plan_id: int, db: AsyncSession) -> WarmupPlan:
    result = await db.execute(
        select(WarmupPlan)
        .options(selectinload(WarmupPlan.channel).selectinload(Channel.proxy), selectinload(WarmupPlan.proxy), selectinload(WarmupPlan.content))
        .where(WarmupPlan.id == plan_id)
    )
    plan = result.scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="养号计划不存在")
    return plan


def ensure_plan_editable(plan: WarmupPlan) -> None:
    if bool(plan.running):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="养号计划正在运行，请先暂停后再编辑")


AI_GENERATION_PARAM_FIELDS = ("prompt", "content_language", "tone", "must_include", "avoid", "fidelity_mode")


def _normalize_ai_generation_field(field_name: str, value: str | None) -> str:
    if field_name == "content_language":
        return (value or "zh").strip() or "zh"
    if field_name == "fidelity_mode":
        return (value or "strict").strip() or "strict"
    if field_name == "tone":
        return (value or "自然日常").strip() or "自然日常"
    return (value or "").strip()


def snapshot_ai_generation_params(plan: WarmupPlan) -> dict[str, str]:
    return {
        field_name: _normalize_ai_generation_field(field_name, getattr(plan, field_name, None))
        for field_name in AI_GENERATION_PARAM_FIELDS
    }


def ai_generation_params_changed(before: dict[str, str], after: dict[str, str]) -> bool:
    return before != after


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


def plan_generation_params(
    plan: WarmupPlan,
    *,
    prompt: str | None = None,
    content_language: str | None = None,
    tone: str | None = None,
    must_include: str | None = None,
    avoid: str | None = None,
    fidelity_mode: str | None = None,
) -> dict[str, str]:
    return {
        "prompt": (prompt if prompt is not None else plan.prompt or "").strip(),
        "language": (content_language or plan.content_language or "zh").strip() or "zh",
        "tone": (tone or plan.tone or "自然日常").strip() or "自然日常",
        "must_include": (must_include if must_include is not None else plan.must_include or "").strip(),
        "avoid": (avoid if avoid is not None else plan.avoid or "").strip(),
        "fidelity_mode": (fidelity_mode or plan.fidelity_mode or "strict").strip() or "strict",
    }


async def generate_plan_content(
    plan: WarmupPlan,
    *,
    prompt: str | None = None,
    content_language: str | None = None,
    tone: str | None = None,
    must_include: str | None = None,
    avoid: str | None = None,
    fidelity_mode: str | None = None,
    allow_fallback: bool = False,
) -> dict:
    params = plan_generation_params(
        plan,
        prompt=prompt,
        content_language=content_language,
        tone=tone,
        must_include=must_include,
        avoid=avoid,
        fidelity_mode=fidelity_mode,
    )
    if not params["prompt"]:
        return {"subject": "", "text": "", "source": "error", "error": "AI 生成模式必须填写提示词"}
    return await generate_warmup_content(
        params["prompt"],
        params["language"],
        allow_fallback=allow_fallback,
        tone=params["tone"],
        must_include=params["must_include"],
        avoid=params["avoid"],
        fidelity_mode=params["fidelity_mode"],
    )


def raise_if_generated_content_invalid(content: dict) -> None:
    source = str(content.get("source") or "")
    if source == "error":
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=content.get("error") or "AI 文案生成失败")
    if source == "fallback":
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=content.get("error") or "AI 文案生成失败，已拒绝使用兜底文案")
    subject = str(content.get("subject") or "").strip()
    text = str(content.get("text") or "").strip()
    if not subject or not text:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="AI 文案生成结果不完整，缺少主题或正文")


async def plan_to_out(db: AsyncSession, plan: WarmupPlan, include_content: bool = True, content_stale: bool = False) -> WarmupPlanOut:
    proxy = plan.channel.proxy if plan.channel else None
    proxy_name = proxy.name if proxy else ""
    proxy_address = mask_proxy_address(proxy)
    channel_out = None
    if plan.channel is not None:
        channel_out = ChannelOut(
            id=plan.channel.id,
            name=plan.channel.name or "-",
            use_proxy=bool(plan.channel.use_proxy or plan.channel.proxy_id),
            proxy_id=plan.channel.proxy_id,
            proxy_name=proxy_name,
            proxy_address=proxy_address,
            proxy=proxy_to_out(proxy),
        )
    recipient_total = int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(WarmupRecipient.plan_id == plan.id))).scalar_one())
    recipient_sent = int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(WarmupRecipient.plan_id == plan.id, WarmupRecipient.sent.is_(True)))).scalar_one())
    recipient_unsent = max(0, recipient_total - recipient_sent)
    daily_target = calculate_daily_target(plan)
    remaining_today = calculate_remaining_today(plan, recipient_unsent)
    local_today = datetime.now(ZoneInfo(plan.timezone or DEFAULT_TIMEZONE)).date()
    started_date = None
    if plan.started_at:
        started_value = plan.started_at
        if started_value.tzinfo is None:
            started_value = started_value.replace(tzinfo=ZoneInfo("UTC"))
        started_date = started_value.astimezone(ZoneInfo(plan.timezone or DEFAULT_TIMEZONE)).date()
    elapsed_days = max(0, (local_today - started_date).days + 1) if started_date else 0
    remaining_days = max(0, int(plan.total_days or 0) - elapsed_days) if int(plan.total_days or 0) > 0 else None
    success_count = int((await db.execute(select(func.count()).select_from(WarmupLog).where(WarmupLog.plan_id == plan.id, WarmupLog.status == "success"))).scalar_one())
    fail_count = int((await db.execute(select(func.count()).select_from(WarmupLog).where(WarmupLog.plan_id == plan.id, WarmupLog.status == "fail"))).scalar_one())
    state = get_warmup_state(plan.id)
    content = plan.content if include_content else None
    return WarmupPlanOut(
        id=plan.id,
        name=plan.name,
        channel_id=plan.channel_id,
        channel_name=plan.channel.name if plan.channel else "-",
        channel=channel_out,
        proxy_name=proxy_name,
        proxy_address=proxy_address,
        start_count=plan.start_count,
        total_days=int(plan.total_days or 0),
        started_at=plan.started_at,
        elapsed_days=elapsed_days,
        remaining_days=remaining_days,
        daily_increase_rate=float(plan.daily_increase_rate or 0),
        max_daily_count=plan.max_daily_count,
        base_interval_seconds=plan.base_interval_seconds,
        interval_jitter_seconds=plan.interval_jitter_seconds,
        scheduled_time=plan.scheduled_time,
        timezone=plan.timezone or DEFAULT_TIMEZONE,
        status=plan.status or "draft",
        total_sent=int(plan.total_sent or 0),
        last_run_date=plan.last_run_date,
        prompt=plan.prompt or "",
        content_mode=plan.content_mode,
        content_language=plan.content_language or "zh",
        tone=plan.tone or "自然日常",
        must_include=plan.must_include or "",
        avoid=plan.avoid or "",
        fidelity_mode=plan.fidelity_mode or "strict",
        recipient_total=recipient_total,
        recipient_sent=recipient_sent,
        recipient_unsent=recipient_unsent,
        target_today=daily_target,
        daily_target=daily_target,
        remaining_today=remaining_today,
        running=bool(plan.running or state.get("running", False)),
        progress=float(plan.run_progress or state.get("progress", 0.0) or 0.0),
        last_message=plan.run_message or str(state.get("last_message", "") or ""),
        next_delay_seconds=float(state.get("next_delay_seconds", 0.0) or 0.0),
        success_count=success_count,
        fail_count=fail_count,
        content=ContentOut(subject=content.subject, html_body=content.html_body or "", text_body=content.text_body or "") if content else None,
        content_stale=bool(content_stale),
        sender_rotation_enabled=bool(getattr(plan, "sender_rotation_enabled", False)),
        created_at=plan.created_at,
        updated_at=plan.updated_at,
    )


@router.post("/content-preview", response_model=ContentOut, dependencies=[Depends(require_admin)])
async def preview_content(payload: WarmupContentPreviewRequest):
    try:
        if payload.ai_generation_mode == "manual_prompt":
            content = await generate_email_content(mode="manual", prompt=payload.prompt, content_format="html")
        else:
            content = await generate_email_content(
                mode="conditional",
                email_type=payload.email_type,
                language=payload.content_language,
                tone=payload.tone,
                include_url=payload.include_url,
                content_format="html",
            )
        if content.get("source") == "error":
            raise HTTPException(status_code=502, detail=content.get("error") or "AI 文案生成失败")
        return ContentOut(subject=content["subject"], text_body=content.get("text", ""), html_body=content.get("html", ""), source=content.get("source", "ai"), error=content.get("error", ""))
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("预览养号文案失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.post("/plans", response_model=WarmupPlanOut, dependencies=[Depends(require_admin)])
async def create_plan(payload: WarmupPlanCreate, db: AsyncSession = Depends(get_db)):
    try:
        channel = (await db.execute(select(Channel).options(selectinload(Channel.proxy)).where(Channel.id == payload.channel_id))).scalar_one_or_none()
        if channel is None:
            raise HTTPException(status_code=404, detail="通道不存在")
        if not bool(channel.enabled) or not bool(channel.is_active):
            raise HTTPException(status_code=400, detail="绑定通道已禁用，请选择可用通道")
        if not channel.proxy_id or channel.proxy is None:
            raise HTTPException(status_code=400, detail="AI 养号计划必须选择已绑定代理的通道，禁止直连发送")
        if not bool(channel.proxy.is_active):
            raise HTTPException(status_code=400, detail="通道绑定代理不可用，请先启用或修改代理")
        emails = parse_emails(payload.recipients_text)
        if not emails:
            raise HTTPException(status_code=400, detail="没有有效的收件人邮箱")
        if payload.content_mode == "manual":
            selected_content = payload.content
        elif payload.content is not None:
            selected_content = payload.content
        else:
            if payload.ai_generation_mode == "manual_prompt":
                generated = await generate_email_content(mode="manual", prompt=payload.prompt, content_format="html")
            else:
                generated = await generate_email_content(
                    mode="conditional",
                    email_type=payload.email_type,
                    language=payload.content_language,
                    tone=payload.tone,
                    include_url=payload.include_url,
                    content_format="html",
                )
            if generated.get("source") == "error":
                raise HTTPException(status_code=502, detail=generated.get("error") or "AI 文案生成失败")
            selected_content = ContentIn(subject=generated["subject"], text_body=generated.get("text", ""), html_body=generated.get("html", ""))
        if selected_content is None:
            raise HTTPException(status_code=400, detail="养号文案不能为空")
        proxy_id = int(channel.proxy_id)
        if not bool(channel.use_proxy):
            channel.use_proxy = True
            channel.updated_at = datetime.utcnow()
        if channel.proxy is None or int(channel.proxy.id) != proxy_id:
            raise HTTPException(status_code=400, detail="通道代理加载失败，请重新保存通道代理后再创建养号计划")
        plan = WarmupPlan(
            name=payload.name,
            channel_id=payload.channel_id,
            proxy_id=proxy_id,
            start_count=payload.start_count,
            total_days=payload.total_days,
            daily_increase_rate=payload.daily_increase_rate,
            max_daily_count=payload.max_daily_count,
            base_interval_seconds=payload.base_interval_seconds,
            interval_jitter_seconds=payload.interval_jitter_seconds,
            scheduled_time=payload.scheduled_time,
            timezone=payload.timezone,
            status="scheduled",
            total_sent=0,
            prompt=payload.prompt,
            content_mode=payload.content_mode,
            content_language=payload.content_language,
            tone=payload.tone,
            must_include=payload.must_include,
            avoid=payload.avoid,
            fidelity_mode=payload.fidelity_mode,
            sender_rotation_enabled=bool(payload.sender_rotation_enabled),
            running=False,
            run_progress=0,
            run_message="计划已创建，等待每日定时自动启动；也可手动点击启动立即执行",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(plan)
        await db.flush()
        db.add(WarmupContent(plan_id=plan.id, subject=selected_content.subject, html_body=selected_content.html_body, text_body=selected_content.text_body, created_at=datetime.utcnow(), updated_at=datetime.utcnow()))
        await batch_insert_warmup_recipients(db, plan.id, emails)
        await db.commit()
        return await plan_to_out(db, await get_plan_or_404(plan.id, db))
    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        logger.exception("保存养号计划失败 name=%s channel_id=%s recipients_count=%s", payload.name, payload.channel_id, len(emails) if 'emails' in locals() else 0)
        raise HTTPException(status_code=400, detail=f"保存养号计划失败：{exc}")


@router.get("/plans", response_model=PlanListResponse, dependencies=[Depends(require_admin)])
async def list_plans(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    try:
        total = int((await db.execute(select(func.count()).select_from(WarmupPlan))).scalar_one())
        result = await db.execute(select(WarmupPlan).options(selectinload(WarmupPlan.channel).selectinload(Channel.proxy), selectinload(WarmupPlan.proxy), selectinload(WarmupPlan.content)).order_by(WarmupPlan.created_at.desc(), WarmupPlan.id.desc()).offset((page - 1) * page_size).limit(page_size))
        items = [await plan_to_out(db, plan, include_content=False) for plan in result.scalars().all()]
        await db.commit()
        return PlanListResponse(items=items, total=total, page=page, page_size=page_size)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("获取养号计划列表失败 page=%s page_size=%s", page, page_size)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.get("/plans/{plan_id}", response_model=WarmupPlanOut, dependencies=[Depends(require_admin)])
async def get_plan(plan_id: int, db: AsyncSession = Depends(get_db)):
    try:
        output = await plan_to_out(db, await get_plan_or_404(plan_id, db), include_content=True)
        await db.commit()
        return output
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("获取养号计划失败 plan_id=%s", plan_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.get("/plans/{plan_id}/status", response_model=WarmupPlanStatusOut, dependencies=[Depends(require_admin)])
async def get_plan_status(plan_id: int, db: AsyncSession = Depends(get_db)):
    try:
        plan = await get_plan_or_404(plan_id, db)
        recipient_unsent = int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(WarmupRecipient.plan_id == plan_id, WarmupRecipient.sent.is_(False)))).scalar_one())
        daily_target = calculate_daily_target(plan)
        remaining_today = calculate_remaining_today(plan, recipient_unsent)
        state = get_warmup_state(plan_id)
        await db.commit()
        return WarmupPlanStatusOut(
            running=bool(plan.running or state.get("running", False)),
            progress=float(plan.run_progress or state.get("progress", 0.0) or 0.0),
            last_message=plan.run_message or str(state.get("last_message", "") or ""),
            daily_target=daily_target,
            remaining_today=remaining_today,
            recipient_unsent=recipient_unsent,
            next_delay_seconds=float(state.get("next_delay_seconds", 0.0) or 0.0),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("获取养号计划状态失败 plan_id=%s", plan_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.put("/plans/{plan_id}", response_model=WarmupPlanOut, dependencies=[Depends(require_admin)])
async def update_plan(plan_id: int, payload: WarmupPlanUpdate, db: AsyncSession = Depends(get_db)):
    try:
        plan = await get_plan_or_404(plan_id, db)
        ensure_plan_editable(plan)
        previous_status = plan.status or "draft"
        ai_params_before = snapshot_ai_generation_params(plan)
        schedule_changed = payload.scheduled_time is not None or payload.timezone is not None
        if payload.name is not None:
            plan.name = payload.name
        if payload.start_count is not None:
            plan.start_count = payload.start_count
        if payload.total_days is not None:
            plan.total_days = payload.total_days
        if payload.daily_increase_rate is not None:
            plan.daily_increase_rate = payload.daily_increase_rate
        if payload.max_daily_count is not None:
            plan.max_daily_count = payload.max_daily_count
        if payload.base_interval_seconds is not None:
            plan.base_interval_seconds = payload.base_interval_seconds
        if payload.interval_jitter_seconds is not None:
            plan.interval_jitter_seconds = payload.interval_jitter_seconds
        if payload.scheduled_time is not None:
            plan.scheduled_time = payload.scheduled_time
        if payload.timezone is not None:
            plan.timezone = payload.timezone
        if payload.prompt is not None:
            plan.prompt = payload.prompt
        if payload.content_language is not None:
            plan.content_language = payload.content_language
        if payload.tone is not None:
            plan.tone = payload.tone
        if payload.must_include is not None:
            plan.must_include = payload.must_include
        if payload.avoid is not None:
            plan.avoid = payload.avoid
        if payload.fidelity_mode is not None:
            plan.fidelity_mode = payload.fidelity_mode
        if payload.sender_rotation_enabled is not None:
            plan.sender_rotation_enabled = bool(payload.sender_rotation_enabled)
        plan.updated_at = datetime.utcnow()
        if previous_status == "paused":
            plan.status = "scheduled" if schedule_changed else previous_status
            if schedule_changed:
                plan.run_message = "计划配置已更新并等待新的每日定时时间，系统不会立即执行；如需立刻发送请手动点击启动"
            else:
                plan.run_message = "计划配置已更新，仍处于暂停状态；再次启动将从剩余未发送邮箱继续"
        else:
            plan.status = previous_status
            if schedule_changed and previous_status in {"active", "scheduled"}:
                plan.run_message = "计划配置已更新，将严格等待新的每日定时时间自动执行"
        await db.commit()
        content_stale = (plan.content_mode or "ai") != "manual" and ai_generation_params_changed(ai_params_before, snapshot_ai_generation_params(plan))
        return await plan_to_out(db, await get_plan_or_404(plan_id, db), include_content=True, content_stale=content_stale)
    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        logger.exception("更新养号计划失败 plan_id=%s", plan_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.put("/plans/{plan_id}/content", response_model=ContentOut, dependencies=[Depends(require_admin)])
async def update_plan_content(plan_id: int, payload: ContentIn, db: AsyncSession = Depends(get_db)):
    try:
        plan = await get_plan_or_404(plan_id, db)
        ensure_plan_editable(plan)
        previous_status = plan.status or "draft"
        now = datetime.utcnow()
        if plan.content is None:
            db.add(WarmupContent(plan_id=plan.id, subject=payload.subject, html_body=payload.html_body, text_body=payload.text_body, created_at=now, updated_at=now))
        else:
            plan.content.subject = payload.subject
            plan.content.html_body = payload.html_body
            plan.content.text_body = payload.text_body
            plan.content.updated_at = now
        plan.status = previous_status
        plan.updated_at = now
        await db.commit()
        refreshed = await get_plan_or_404(plan_id, db)
        content = refreshed.content
        if content is None:
            raise HTTPException(status_code=500, detail="养号文案保存后读取失败")
        return ContentOut(subject=content.subject, html_body=content.html_body or "", text_body=content.text_body or "", source="manual")
    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        logger.exception("更新养号文案失败 plan_id=%s", plan_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.post("/plans/{plan_id}/recipients", response_model=WarmupRecipientAddResponse, dependencies=[Depends(require_admin)])
async def add_plan_recipients(plan_id: int, payload: WarmupRecipientAddRequest, db: AsyncSession = Depends(get_db)):
    plan = await get_plan_or_404(plan_id, db)
    ensure_plan_editable(plan)
    previous_status = plan.status or "draft"
    emails = payload.emails
    existing_result = await db.execute(select(WarmupRecipient.email).where(WarmupRecipient.plan_id == plan_id, WarmupRecipient.email.in_(emails)))
    existing_emails = {str(email).lower() for email in existing_result.scalars().all()}
    new_emails = [email for email in emails if email not in existing_emails]
    now = datetime.utcnow()
    added_count = 0
    try:
        if new_emails:
            added_count = await batch_insert_warmup_recipients(db, plan_id, new_emails, created_at=now)
        if added_count > 0:
            if plan.status == "completed":
                plan.status = "active"
                plan.running = False
                plan.run_message = "已追加新邮箱，计划已重新激活；历史发送进度已保留，新邮箱将作为未发送地址继续处理"
            elif previous_status == "paused":
                plan.status = "paused"
                plan.run_message = "已追加新邮箱，计划仍处于暂停状态；再次启动将从全部未发送邮箱继续"
            elif plan.status in {"active", "paused", "scheduled"}:
                plan.run_message = "已追加新邮箱，未发送邮箱会自动纳入后续养号发送"
            plan.updated_at = now
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=400, detail=f"添加养号收件人失败：{exc}")
    refreshed_plan = await get_plan_or_404(plan_id, db)
    skipped_count = max(0, len(emails) - added_count)
    return WarmupRecipientAddResponse(
        success=True,
        added_count=added_count,
        skipped_count=skipped_count,
        total_input_count=len(emails),
        duplicate_input_count=0,
        plan=await plan_to_out(db, refreshed_plan, include_content=True),
        message=f"已新增 {added_count} 个邮箱，跳过 {skipped_count} 个已存在邮箱",
    )


@router.post("/plans/{plan_id}/start", dependencies=[Depends(require_admin)])
async def start_plan(plan_id: int, db: AsyncSession = Depends(get_db)):
    plan = await get_plan_or_404(plan_id, db)
    was_paused = plan.status == "paused"
    local_today = datetime.now(ZoneInfo(normalize_timezone(plan.timezone))).date()
    ignore_today_guard_for_new_recipients = False
    resume_unfinished = False
    unsent = int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(WarmupRecipient.plan_id == plan_id, WarmupRecipient.sent.is_(False)))).scalar_one())
    if was_paused:
        if unsent <= 0:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="没有未发送邮箱可恢复，请先添加收件人")
        resume_unfinished = True
    elif plan.last_run_date == local_today:
        completed_today_at = plan.run_finished_at
        new_unsent = 0
        if completed_today_at is not None:
            new_unsent = int((await db.execute(select(func.count()).select_from(WarmupRecipient).where(WarmupRecipient.plan_id == plan_id, WarmupRecipient.sent.is_(False), WarmupRecipient.created_at > completed_today_at))).scalar_one())
        if new_unsent <= 0:
            if unsent > 0:
                resume_unfinished = True
            else:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="今天已执行过养号发送")
        elif unsent > new_unsent:
            resume_unfinished = True
        else:
            ignore_today_guard_for_new_recipients = True
    if plan.running:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="养号任务已在运行")
    if unsent <= 0:
        plan.status = "completed"
        plan.updated_at = datetime.utcnow()
        await db.commit()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="收件人已全部发送完成")
    if plan.channel_id and await is_warmup_lock_held(int(plan.channel_id), plan_id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该养号计划正在运行")
    if plan.channel_id and await is_task_lock_held(int(plan.channel_id)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该通道已有发送任务在运行")
    plan.status = "active"
    if plan.started_at is None:
        plan.started_at = datetime.utcnow()
    plan.running = False
    plan.run_progress = 0
    plan.run_message = "断点续发已接收，正在从未发送邮箱继续执行" if resume_unfinished else "手动启动已接收，正在立即执行当天养号任务"
    plan.run_started_at = datetime.utcnow()
    plan.run_finished_at = None
    plan.updated_at = datetime.utcnow()
    await db.commit()
    started, start_reason = await start_warmup_runner(
        plan_id,
        manual=True,
        ignore_today_guard_for_new_recipients=ignore_today_guard_for_new_recipients,
        resume_unfinished=resume_unfinished,
    )
    if not started:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=start_reason or "养号任务已在运行")
    plan.running = True
    await db.commit()
    return {
        "success": True,
        "message": "养号任务已从剩余未发送邮箱继续" if resume_unfinished else "养号任务已立即启动",
        "running": True,
        "resume_unfinished": resume_unfinished,
    }


@router.post("/plans/{plan_id}/pause", dependencies=[Depends(require_admin)])
async def pause_plan(plan_id: int, db: AsyncSession = Depends(get_db)):
    plan = await get_plan_or_404(plan_id, db)
    plan.status = "paused"
    plan.running = False
    plan.run_message = "任务已暂停，可编辑配置、文案和邮箱；再次启动将从剩余未发送邮箱继续"
    plan.run_finished_at = datetime.utcnow()
    plan.updated_at = datetime.utcnow()
    await db.commit()
    await stop_warmup_runner(plan_id)
    return {"success": True, "message": "养号计划已暂停"}


@router.post("/plans/{plan_id}/regenerate-content", response_model=WarmupPlanOut, dependencies=[Depends(require_admin)])
async def regenerate_content(plan_id: int, payload: WarmupRegenerateRequest | None = Body(default=None), db: AsyncSession = Depends(get_db)):
    plan = await get_plan_or_404(plan_id, db)
    ensure_plan_editable(plan)
    previous_status = plan.status or "draft"
    if plan.content_mode == "manual":
        raise HTTPException(status_code=400, detail="手动输入文案的养号计划不能重新生成 AI 文案，请使用编辑文案功能修改")
    request = payload or WarmupRegenerateRequest()
    params = plan_generation_params(
        plan,
        prompt=request.prompt or None,
        content_language=request.content_language,
        tone=request.tone,
        must_include=request.must_include,
        avoid=request.avoid,
        fidelity_mode=request.fidelity_mode,
    )
    logger.info("regenerate_content plan_id=%s params=%s", plan_id, params)
    content = await generate_plan_content(
        plan,
        prompt=request.prompt or None,
        content_language=request.content_language,
        tone=request.tone,
        must_include=request.must_include,
        avoid=request.avoid,
        fidelity_mode=request.fidelity_mode,
        allow_fallback=False,
    )
    raise_if_generated_content_invalid(content)
    if request.prompt and request.prompt != (plan.prompt or ""):
        plan.prompt = request.prompt
    if request.content_language:
        plan.content_language = request.content_language
    if request.tone:
        plan.tone = request.tone
    if request.must_include is not None:
        plan.must_include = request.must_include
    if request.avoid is not None:
        plan.avoid = request.avoid
    if request.fidelity_mode:
        plan.fidelity_mode = request.fidelity_mode
    if plan.content is None:
        db.add(WarmupContent(plan_id=plan.id, subject=content["subject"], html_body="", text_body=content["text"], created_at=datetime.utcnow(), updated_at=datetime.utcnow()))
    else:
        plan.content.subject = content["subject"]
        plan.content.html_body = ""
        plan.content.text_body = content["text"]
        plan.content.updated_at = datetime.utcnow()
    plan.status = previous_status
    plan.updated_at = datetime.utcnow()
    await db.commit()
    return await plan_to_out(db, await get_plan_or_404(plan_id, db), include_content=True)


@router.delete("/plans/{plan_id}", dependencies=[Depends(require_admin)])
async def delete_plan(plan_id: int, db: AsyncSession = Depends(get_db)):
    plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
    if plan is None:
        raise HTTPException(status_code=404, detail="养号计划不存在")
    plan.status = "paused"
    plan.running = False
    plan.run_message = "养号计划正在删除，发送引擎已收到停止信号"
    plan.run_finished_at = datetime.utcnow()
    plan.updated_at = datetime.utcnow()
    await db.commit()
    runner_stopped = await stop_warmup_runner(plan_id, deleted=True)
    await db.execute(sql_delete(WarmupSystemLog).where(WarmupSystemLog.plan_id == plan_id))
    await db.execute(sql_delete(WarmupLog).where(WarmupLog.plan_id == plan_id))
    await db.execute(sql_delete(WarmupRecipient).where(WarmupRecipient.plan_id == plan_id))
    await db.execute(sql_delete(WarmupContent).where(WarmupContent.plan_id == plan_id))
    plan = (await db.execute(select(WarmupPlan).where(WarmupPlan.id == plan_id))).scalar_one_or_none()
    if plan is not None:
        await db.delete(plan)
    await db.commit()
    return {"success": True, "runner_stopped": runner_stopped, "message": "养号计划已删除，发送引擎已停止"}
