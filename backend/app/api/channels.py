import json
import logging
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import require_admin
from app.core.database import get_db
from app.models.models import AIConfig, Channel, Proxy, Task, WarmupPlan
from app.services.dashboard_stats_service import invalidate_dashboard_stats_cache
from app.services.health import clear_channel_health_redis_keys
from app.services.channel_manager import force_close_httpx_client, reload_httpx_client
from app.services.connection_manager import (
    build_channel_proxy_url,
    mask_api_key,
    measure_channel_proxy_latency,
    probe_proxy_outbound_ip,
    proxy_fingerprint,
    probe_sendgrid_scopes,
    is_masked_api_key,
)
from app.utils.concurrency import calculate_recommended_concurrency

router = APIRouter(prefix="/api/channels", tags=["channels"])
logger = logging.getLogger(__name__)
ACTIVE_TASK_STATUSES = ("running", "pending", "scheduled")
ACTIVE_WARMUP_STATUSES = ("scheduled", "active")
CHANNEL_DELETED_MARKER = "通道已删除"


class SenderIdentity(BaseModel):
    name: str = Field(default="", max_length=255)
    email: str = Field(..., min_length=3, max_length=255)
    enabled: bool = True

    @field_validator("email")
    @classmethod
    def validate_email(cls, value):
        email = value.strip()
        if "@" not in email:
            raise ValueError("发件人邮箱格式不正确")
        return email

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value):
        return "" if value is None else str(value).strip()


class ChannelProxyIn(BaseModel):
    name: str | None = Field(default="", max_length=120)
    host: str = Field(..., min_length=1, max_length=255)
    port: int = Field(..., ge=1, le=65535)
    type: Literal["HTTP", "SOCKS5"] = "HTTP"
    username: str | None = Field(default="", max_length=255)
    password: str | None = Field(default=None, max_length=255)
    keep_existing_password: bool = False
    is_active: bool = True
    max_connections: int = Field(default=5, ge=1, le=1000)

    @field_validator("host")
    @classmethod
    def normalize_host(cls, value):
        stripped = value.strip()
        if not stripped:
            raise ValueError("代理主机不能为空")
        return stripped

    @field_validator("name", "username", "password", mode="before")
    @classmethod
    def normalize_optional_text(cls, value):
        return "" if value is None else str(value).strip()


class ChannelBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=160)
    api_key: str = Field(..., min_length=1)
    sender_name: str | None = Field(default="", max_length=255)
    sender_email: str | None = Field(default="", max_length=255)
    sender_emails: list[SenderIdentity] = Field(default_factory=list)
    daily_limit: int | None = Field(default=None, ge=0)
    hourly_limit: int | None = Field(default=None, ge=0)
    per_minute_limit: int | None = Field(default=None, ge=0)
    second_limit: int | None = Field(default=None, ge=0)
    use_proxy: bool = False
    proxy: ChannelProxyIn | None = None
    is_active: bool = True
    enabled: bool | None = None

    @field_validator("name", "api_key")
    @classmethod
    def normalize_required_text(cls, value):
        if value is None:
            return value
        stripped = value.strip()
        if not stripped:
            raise ValueError("字段不能为空")
        return stripped

    @field_validator("sender_email", mode="before")
    @classmethod
    def normalize_sender_email(cls, value):
        return "" if value is None else str(value).strip()

    @field_validator("daily_limit", "hourly_limit", "per_minute_limit", "second_limit", mode="before")
    @classmethod
    def normalize_limit(cls, value):
        if value in (None, "", 0, "0"):
            return None
        return value

    @field_validator("sender_emails")
    @classmethod
    def require_sender(cls, value):
        if not value:
            raise ValueError("至少配置一个发件人邮箱")
        if not any(item.enabled is not False for item in value):
            raise ValueError("至少需要启用一个发件人邮箱")
        return value


class ChannelCreate(ChannelBase):
    model_config = ConfigDict(title="ChannelCreate")


class ChannelUpdate(ChannelBase):
    model_config = ConfigDict(title="ChannelUpdate")
    api_key: str | None = Field(default=None)

    @field_validator("api_key", mode="before")
    @classmethod
    def normalize_optional_api_key(cls, value):
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class ProxyBrief(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    host: str
    port: int
    type: str
    username: str = ""
    has_password: bool = False
    is_active: bool


class ChannelOut(BaseModel):
    id: int
    name: str
    api_key: str
    sender_name: str
    sender_email: str
    sender_emails: list[SenderIdentity]
    daily_limit: int | None
    hourly_limit: int | None
    per_minute_limit: int | None
    second_limit: int | None
    use_proxy: bool
    proxy_id: int | None
    proxy: ProxyBrief | None
    is_active: bool
    enabled: bool
    last_error: str
    proxy_outbound_ip: str | None = None
    health_status: str = "healthy"
    health_status_label: str = "健康"
    last_health_check: datetime | None = None
    health_error: str | None = None
    created_at: datetime
    updated_at: datetime


class ChannelListResponse(BaseModel):
    items: list[ChannelOut]
    total: int
    page: int
    page_size: int


class ChannelTestResponse(BaseModel):
    success: bool
    message: str


class ChannelProxyLatencyResponse(BaseModel):
    success: bool
    latency_ms: int | None = None
    proxy_used: bool = False
    recommended_concurrency: int | None = None
    message: str = ""


def decode_sender_emails(channel: Channel) -> list[SenderIdentity]:
    try:
        raw_items = json.loads(channel.sender_emails_json or "[]")
    except json.JSONDecodeError:
        raw_items = []
    items = []
    for item in raw_items:
        if isinstance(item, dict) and item.get("email"):
            items.append(SenderIdentity(
                name=item.get("name") or "",
                email=item.get("email"),
                enabled=item.get("enabled", True) is not False,
            ))
    if not items and channel.sender_email:
        items.append(SenderIdentity(name=channel.sender_name or "", email=channel.sender_email))
    return items


def resolve_health_status_label(health_status: str | None) -> str:
    normalized = str(health_status or "healthy").strip().lower()
    mapping = {
        "healthy": "健康",
        "unauthorized": "未授权",
        "error": "异常",
    }
    return mapping.get(normalized, "未知")


def proxy_brief_to_out(proxy: Proxy | None) -> ProxyBrief | None:
    if proxy is None:
        return None
    return ProxyBrief(
        id=int(proxy.id),
        name=str(proxy.name or ""),
        host=str(proxy.host or ""),
        port=int(proxy.port or 0),
        type=str(getattr(proxy, "type", "") or getattr(proxy, "protocol", "") or "HTTP"),
        username=str(proxy.username or ""),
        has_password=bool(str(getattr(proxy, "password", "") or "").strip()),
        is_active=bool(proxy.is_active),
    )


async def serialize_channel(channel: Channel) -> ChannelOut:
    health_status = str(getattr(channel, "health_status", "") or "healthy")
    return ChannelOut(
        id=channel.id,
        name=channel.name,
        api_key=mask_api_key(channel.api_key or ""),
        sender_name=channel.sender_name or "",
        sender_email=channel.sender_email or "",
        sender_emails=decode_sender_emails(channel),
        daily_limit=channel.daily_limit,
        hourly_limit=channel.hourly_limit,
        per_minute_limit=channel.per_minute_limit,
        second_limit=channel.second_limit,
        use_proxy=bool(channel.use_proxy),
        proxy_id=channel.proxy_id,
        proxy=proxy_brief_to_out(channel.proxy),
        is_active=bool(channel.is_active),
        enabled=bool(channel.enabled),
        last_error=channel.last_error or "",
        proxy_outbound_ip=str(channel.proxy_outbound_ip or "").strip() or None,
        health_status=health_status,
        health_status_label=resolve_health_status_label(health_status),
        last_health_check=getattr(channel, "last_health_check", None),
        health_error=str(getattr(channel, "health_error", "") or "").strip() or None,
        created_at=channel.created_at,
        updated_at=channel.updated_at,
    )


def proxy_name_for_channel(channel: Channel, proxy_data: ChannelProxyIn) -> str:
    return proxy_data.name or f"{channel.name} 专属代理"


def proxy_username_from_data(data: ChannelProxyIn, existing_proxy: Proxy | None = None) -> str:
    if "username" not in data.model_fields_set and existing_proxy is not None:
        return str(getattr(existing_proxy, "username", "") or "").strip()
    return str(data.username or "").strip()


def proxy_password_from_data(data: ChannelProxyIn, existing_proxy: Proxy | None = None) -> str:
    if existing_proxy is not None and bool(data.keep_existing_password):
        return str(getattr(existing_proxy, "password", "") or "")
    if "password" not in data.model_fields_set and existing_proxy is not None:
        return str(getattr(existing_proxy, "password", "") or "")
    return "" if data.password is None else str(data.password)


def proxy_from_data(proxy: Proxy, channel: Channel, data: ChannelProxyIn) -> None:
    existing_proxy = proxy if getattr(proxy, "id", None) else None
    proxy.name = proxy_name_for_channel(channel, data)
    proxy.host = data.host.strip()
    proxy.port = int(data.port)
    proxy.protocol = data.type
    proxy.type = data.type
    proxy.username = proxy_username_from_data(data, existing_proxy)
    proxy.password = proxy_password_from_data(data, existing_proxy)
    proxy.is_active = bool(data.is_active)
    proxy.max_connections = int(data.max_connections or getattr(proxy, "max_connections", None) or 5)
    proxy.updated_at = datetime.utcnow()


def data_proxy_fingerprint(data: ChannelBase, existing_proxy: Proxy | None = None) -> tuple | None:
    if not data.use_proxy:
        return None
    if data.proxy is None:
        if existing_proxy is not None:
            return proxy_fingerprint(existing_proxy)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="开启代理连接时必须填写代理配置")
    return (
        "socks5" if data.proxy.type == "SOCKS5" else "http",
        data.proxy.host.strip(),
        int(data.proxy.port),
        proxy_username_from_data(data.proxy, existing_proxy),
        proxy_password_from_data(data.proxy, existing_proxy).strip(),
    )


def channel_proxy_fingerprint_value(channel: Channel) -> tuple | None:
    if not bool(channel.use_proxy) or channel.proxy is None:
        return None
    return proxy_fingerprint(channel.proxy)


async def ensure_channel_proxy_mutation_allowed(db: AsyncSession, channel_id: int) -> None:
    blocking_tasks = await _find_blocking_tasks_for_channel(db, channel_id)
    if blocking_tasks:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="通道正被任务使用，无法修改代理")
    blocking_warmups = await _find_blocking_warmups_for_channel(db, channel_id)
    if blocking_warmups:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="通道正被养号使用，无法修改代理")


async def validate_proxy_binding(
    data: ChannelBase,
    db: AsyncSession,
    current_channel_id: int | None = None,
    existing_proxy: Proxy | None = None,
):
    expected = data_proxy_fingerprint(data, existing_proxy)
    normalized_api_key = str(data.api_key or "").strip()
    if not normalized_api_key or is_masked_api_key(normalized_api_key):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="API Key 无效，请重新填写后再保存")
    conditions = [Channel.api_key == normalized_api_key]
    if current_channel_id is not None:
        conditions.append(Channel.id != current_channel_id)
    result = await db.execute(select(Channel).options(selectinload(Channel.proxy)).where(*conditions))
    for existing in result.scalars().all():
        actual = proxy_fingerprint(existing.proxy) if bool(existing.use_proxy) and existing.proxy is not None else None
        if actual != expected:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="同一 SendGrid API Key 的所有通道必须使用完全相同的代理设置，请统一为直连或相同代理后再保存")


async def apply_channel_proxy(channel: Channel, data: ChannelBase, db: AsyncSession):
    if not data.use_proxy:
        old_proxy = channel.proxy
        channel.use_proxy = False
        channel.proxy_id = None
        channel.proxy_outbound_ip = None
        if old_proxy is not None:
            other_count = int((await db.execute(select(func.count()).select_from(Channel).where(Channel.proxy_id == old_proxy.id, Channel.id != channel.id))).scalar_one())
            if other_count == 0:
                await db.delete(old_proxy)
        return
    if data.proxy is None:
        if channel.proxy is not None:
            channel.use_proxy = True
            channel.proxy_id = channel.proxy.id
            return
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="开启代理连接时必须填写代理配置")
    proxy = channel.proxy
    if proxy is None:
        proxy = Proxy(created_at=datetime.utcnow())
        db.add(proxy)
    proxy_from_data(proxy, channel, data.proxy)
    await db.flush()
    channel.use_proxy = True
    channel.proxy_id = proxy.id
    channel.proxy_outbound_ip = None


def apply_channel_data(channel: Channel, data: ChannelBase):
    first_sender = next((item for item in data.sender_emails if item.enabled is not False), data.sender_emails[0])
    channel.name = data.name
    incoming_key = (data.api_key or "").strip()
    if incoming_key and not is_masked_api_key(incoming_key):
        channel.api_key = incoming_key
        channel.smtp_password = incoming_key
    channel.sender_name = data.sender_name or first_sender.name
    channel.sender_email = data.sender_email or first_sender.email
    channel.sender_emails_json = json.dumps([item.model_dump() for item in data.sender_emails], ensure_ascii=False)
    channel.daily_limit = data.daily_limit
    channel.hourly_limit = data.hourly_limit
    channel.per_minute_limit = data.per_minute_limit
    channel.second_limit = data.second_limit
    channel.is_active = bool(data.is_active)
    channel.enabled = bool(data.enabled) if data.enabled is not None else bool(data.is_active)
    channel.updated_at = datetime.utcnow()


async def get_channel_or_404(channel_id: int, db: AsyncSession) -> Channel:
    channel = (await db.execute(
        select(Channel).options(selectinload(Channel.proxy)).where(Channel.id == channel_id)
    )).scalar_one_or_none()
    if channel is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="通道不存在")
    return channel


@router.get("", response_model=ChannelListResponse, dependencies=[Depends(require_admin)])
async def list_channels(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    visible_clause = Channel.name != CHANNEL_DELETED_MARKER
    total = int((await db.execute(select(func.count()).select_from(Channel).where(visible_clause))).scalar_one())
    result = await db.execute(
        select(Channel)
        .where(visible_clause)
        .options(selectinload(Channel.proxy))
        .order_by(Channel.created_at.desc(), Channel.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return ChannelListResponse(
        items=[await serialize_channel(channel) for channel in result.scalars().all()],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("", response_model=ChannelOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_admin)])
async def create_channel(data: ChannelCreate, db: AsyncSession = Depends(get_db)):
    try:
        await validate_proxy_binding(data, db)
        channel = Channel(type="sendgrid", host="api.sendgrid.com", port=443, use_tls=True)
        apply_channel_data(channel, data)
        channel.created_at = datetime.utcnow()
        db.add(channel)
        await db.flush()
        await apply_channel_proxy(channel, data, db)
        await db.commit()
        channel = await get_channel_or_404(channel.id, db)
        return await serialize_channel(channel)
    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as exc:
        await db.rollback()
        logger.exception("创建 SendGrid 通道失败：数据库写入异常 use_proxy=%s proxy_present=%s", data.use_proxy, data.proxy is not None)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道保存失败，请检查代理主机、端口和通道配置后重试") from exc
    except Exception as exc:
        await db.rollback()
        logger.exception("创建 SendGrid 通道失败：未知异常 use_proxy=%s proxy_present=%s", data.use_proxy, data.proxy is not None)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道保存失败，请检查代理信息是否完整后重试") from exc


@router.put("/{channel_id}", response_model=ChannelOut, dependencies=[Depends(require_admin)])
async def update_channel(channel_id: int, data: ChannelUpdate, db: AsyncSession = Depends(get_db)):
    try:
        channel = await get_channel_or_404(channel_id, db)
        old_api_key = str(channel.api_key or "").strip()
        old_proxy_id = channel.proxy_id
        old_use_proxy = bool(channel.use_proxy)
        old_proxy_fingerprint = channel_proxy_fingerprint_value(channel)
        incoming_api_key = str(data.api_key or "").strip()
        api_key_changed = False
        if data.api_key is None or not incoming_api_key or is_masked_api_key(incoming_api_key):
            if not old_api_key or is_masked_api_key(old_api_key):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="当前通道缺少有效 API Key，请重新填写后再保存")
            data.api_key = old_api_key
        else:
            data.api_key = incoming_api_key
            api_key_changed = incoming_api_key != old_api_key
        new_proxy_fingerprint = data_proxy_fingerprint(data, channel.proxy)
        proxy_changed = old_proxy_fingerprint != new_proxy_fingerprint
        if proxy_changed:
            await ensure_channel_proxy_mutation_allowed(db, channel_id)
        await validate_proxy_binding(data, db, current_channel_id=channel_id, existing_proxy=channel.proxy)
        provided_fields = sorted(data.model_fields_set - {"api_key"})
        logger.info(
            "准备更新 SendGrid 通道：通道ID=%s，实际入参字段=%s，API Key是否提供=%s，API Key是否变更=%s，是否使用代理=%s→%s，代理ID=%s，代理是否变更=%s，代理密码操作=%s",
            channel_id,
            provided_fields,
            "api_key" in data.model_fields_set and bool(incoming_api_key) and not is_masked_api_key(incoming_api_key),
            api_key_changed,
            old_use_proxy,
            bool(data.use_proxy),
            old_proxy_id,
            proxy_changed,
            (
                "未提交"
                if data.proxy is None or "password" not in data.proxy.model_fields_set
                else "保留原值"
                if data.proxy.keep_existing_password or not str(data.proxy.password or "").strip()
                else "已更新"
            ),
        )
        apply_channel_data(channel, data)
        await apply_channel_proxy(channel, data, db)
        if api_key_changed:
            channel.health_status = "healthy"
            channel.health_error = None
            channel.last_health_check = None
        await reload_httpx_client(channel)
        await db.commit()
        logger.info(
            "SendGrid 通道更新完成：通道ID=%s，API Key是否变更=%s，是否使用代理=%s，代理ID=%s，健康状态=%s",
            channel_id,
            api_key_changed,
            bool(channel.use_proxy),
            channel.proxy_id,
            channel.health_status,
        )
        channel = await get_channel_or_404(channel_id, db)
        return await serialize_channel(channel)
    except HTTPException:
        await db.rollback()
        raise
    except SQLAlchemyError as exc:
        await db.rollback()
        logger.exception("更新 SendGrid 通道失败：数据库写入异常，通道ID=%s，是否使用代理=%s，是否提交代理配置=%s", channel_id, data.use_proxy, data.proxy is not None)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道更新失败，请检查代理主机、端口和通道配置后重试") from exc
    except Exception as exc:
        await db.rollback()
        logger.exception("更新 SendGrid 通道失败：未知异常，通道ID=%s，是否使用代理=%s，是否提交代理配置=%s", channel_id, data.use_proxy, data.proxy is not None)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="通道更新失败，请检查代理信息是否完整后重试") from exc


async def _find_blocking_tasks_for_channel(db: AsyncSession, channel_id: int) -> list[tuple[int, str, str]]:
    rows = (
        await db.execute(
            select(Task.id, Task.name, Task.status).where(
                Task.channel_id == channel_id,
                Task.deleted_at.is_(None),
                Task.status.in_(ACTIVE_TASK_STATUSES),
            ).order_by(Task.id.desc()).limit(5)
        )
    ).all()
    return [(int(row.id), str(row.name or ""), str(row.status or "")) for row in rows]


async def _find_blocking_warmups_for_channel(db: AsyncSession, channel_id: int) -> list[tuple[int, str, str]]:
    rows = (
        await db.execute(
            select(WarmupPlan.id, WarmupPlan.name, WarmupPlan.status).where(
                WarmupPlan.channel_id == channel_id,
                WarmupPlan.running.is_(True),
            ).order_by(WarmupPlan.id.desc()).limit(5)
        )
    ).all()
    if rows:
        return [(int(row.id), str(row.name or ""), str(row.status or "")) for row in rows]
    rows = (
        await db.execute(
            select(WarmupPlan.id, WarmupPlan.name, WarmupPlan.status).where(
                WarmupPlan.channel_id == channel_id,
                WarmupPlan.status.in_(ACTIVE_WARMUP_STATUSES),
            ).order_by(WarmupPlan.id.desc()).limit(5)
        )
    ).all()
    return [(int(row.id), str(row.name or ""), str(row.status or "")) for row in rows]


def _format_blocking_tasks_detail(blocking_tasks: list[tuple[int, str, str]]) -> str:
    labels = [
        f"「{name or f'任务#{task_id}'}」(状态={status_label})"
        for task_id, name, status_label in blocking_tasks[:3]
    ]
    if not labels:
        return ""
    suffix = f" 等 {len(blocking_tasks)} 个任务" if len(blocking_tasks) > 3 else ""
    return f"通道正被运行中任务使用：{', '.join(labels)}{suffix}，无法删除"


def _format_blocking_warmups_detail(blocking_warmups: list[tuple[int, str, str]]) -> str:
    labels = [
        f"「{name or f'计划#{plan_id}'}」(状态={status_label})"
        for plan_id, name, status_label in blocking_warmups[:3]
    ]
    if not labels:
        return ""
    suffix = f" 等 {len(blocking_warmups)} 个养号计划" if len(blocking_warmups) > 3 else ""
    return f"通道正被养号计划使用：{', '.join(labels)}{suffix}，无法删除"


async def _proxy_still_referenced(db: AsyncSession, proxy_id: int, *, excluding_channel_id: int) -> str:
    other_channels = int(
        (
            await db.execute(
                select(func.count()).select_from(Channel).where(
                    Channel.proxy_id == proxy_id,
                    Channel.id != excluding_channel_id,
                )
            )
        ).scalar_one()
        or 0
    )
    if other_channels > 0:
        return f"仍有 {other_channels} 个通道绑定该代理"
    ai_config = (
        await db.execute(select(AIConfig.id).where(AIConfig.outbound_proxy_id == proxy_id).limit(1))
    ).scalar_one_or_none()
    if ai_config is not None:
        return "AI 配置仍引用该代理"
    warmup_proxy = (
        await db.execute(select(WarmupPlan.id).where(WarmupPlan.proxy_id == proxy_id).limit(1))
    ).scalar_one_or_none()
    if warmup_proxy is not None:
        return "养号计划仍引用该代理"
    return ""


@router.delete("/{channel_id}", dependencies=[Depends(require_admin)])
async def delete_channel(channel_id: int, db: AsyncSession = Depends(get_db)):
    channel = await get_channel_or_404(channel_id, db)
    blocking_tasks = await _find_blocking_tasks_for_channel(db, channel_id)
    if blocking_tasks:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_format_blocking_tasks_detail(blocking_tasks),
        )
    blocking_warmups = await _find_blocking_warmups_for_channel(db, channel_id)
    if blocking_warmups:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=_format_blocking_warmups_detail(blocking_warmups),
        )
    now_value = datetime.utcnow()
    proxy = channel.proxy
    proxy_id = int(proxy.id) if proxy is not None else None
    api_key = str(channel.api_key or "").strip()
    await clear_channel_health_redis_keys(channel_id, proxy_id)
    await force_close_httpx_client(channel_id)
    channel.name = CHANNEL_DELETED_MARKER
    channel.enabled = False
    channel.is_active = False
    channel.last_error = "通道已删除，仅保留历史任务关联"
    channel.updated_at = now_value
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        logger.warning("删除通道失败，数据库约束冲突 channel_id=%s error=%s", channel_id, exc)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="删除失败：该通道仍被其他数据引用，请刷新页面后重试；若问题持续请联系管理员",
        ) from exc
    invalidate_dashboard_stats_cache()
    logger.info("通道 %s 已软删除，历史任务关联已保留，事件拉取已同步清理", channel_id)
    return {"success": True, "message": "通道已删除，历史任务关联已保留"}


@router.post("/{channel_id}/test-connection", response_model=ChannelTestResponse, dependencies=[Depends(require_admin)])
async def test_channel_connection(channel_id: int, db: AsyncSession = Depends(get_db)):
    try:
        channel = await get_channel_or_404(channel_id, db)
        probe = await probe_sendgrid_scopes(channel)
        success = bool(probe.success)
        status_code = int(probe.status_code or 0)
        body_text = str(probe.body or probe.error_message or "")
        latency_ok = success
        latency_ms = probe.latency_ms if probe.latency_ms > 0 else None
        latency_error = probe.error_message
        if success:
            message_text = "连接成功，API Key 有效，SendGrid API 与通道代理链路正常"
            if bool(channel.use_proxy):
                try:
                    proxy_url = build_channel_proxy_url(channel, strict=True)
                except ValueError as exc:
                    logger.warning("通道 %s 代理出口 IP 探测跳过：%s", channel_id, exc)
                    message_text = f"{message_text}，代理配置可用，但出口 IP 未能探测"
                else:
                    proxy_outbound_ip = await probe_proxy_outbound_ip(proxy_url) if proxy_url else None
                    if proxy_outbound_ip:
                        message_text = f"{message_text}，代理出口 IP：{proxy_outbound_ip}"
                    else:
                        message_text = f"{message_text}，代理出口 IP 未能获取"
                    if latency_ok and latency_ms is not None:
                        message_text = f"{message_text}，代理延迟 {latency_ms} 毫秒"
                    elif latency_error:
                        message_text = f"{message_text}，代理延迟检测失败：{latency_error}"
            return ChannelTestResponse(success=True, message=message_text)

        error_message = "连接失败，请检查 API Key、网络或代理配置"
        if status_code in (401, 403):
            error_message = "API Key 无效或已删除"
        elif "proxy" in body_text.lower():
            error_message = "代理连接失败，请检查代理地址、端口、账号或密码"
        elif "timeout" in body_text.lower() or "超时" in body_text:
            error_message = "连接测试超时，请检查代理或网络"
        elif body_text.strip():
            error_message = body_text.strip()[:120]
        logger.warning(
            "通道测试连接失败 channel_id=%s status_code=%s error=%s body=%s",
            channel_id,
            status_code,
            error_message,
            body_text[:200],
        )
        return ChannelTestResponse(success=False, message=error_message)
    except HTTPException:
        raise
    except TimeoutError:
        logger.warning("通道测试连接超时 channel_id=%s", channel_id)
        return ChannelTestResponse(success=False, message="连接测试超时，请检查代理或网络")
    except Exception as exc:
        logger.exception("通道测试连接异常 channel_id=%s", channel_id)
        return ChannelTestResponse(success=False, message=f"连接测试失败：{str(exc)[:120] or '未知错误'}")


@router.post("/{channel_id}/proxy-latency", response_model=ChannelProxyLatencyResponse, dependencies=[Depends(require_admin)])
async def measure_proxy_latency(
    channel_id: int,
    rate_limit: float | None = Query(default=None, ge=0, le=100000),
    send_http_batch_size: int | None = Query(default=None, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    channel = await get_channel_or_404(channel_id, db)
    success, latency_ms, error_message, proxy_used = await measure_channel_proxy_latency(channel)
    recommended_concurrency = None
    if success and rate_limit is not None and float(rate_limit) > 0:
        from app.core.config import settings

        batch_size = max(1, int(send_http_batch_size or settings.default_http_batch_size))
        recommended_concurrency = calculate_recommended_concurrency(float(rate_limit), latency_ms, batch_size)
    if not success:
        return ChannelProxyLatencyResponse(
            success=False,
            proxy_used=proxy_used,
            message=error_message or "代理延迟检测失败",
        )
    route_label = "代理" if proxy_used else "直连"
    detail_message = f"{route_label}延迟 {latency_ms} 毫秒"
    if recommended_concurrency is not None:
        detail_message = f"{detail_message}，推荐并发数 {recommended_concurrency}"
    return ChannelProxyLatencyResponse(
        success=True,
        latency_ms=latency_ms,
        proxy_used=proxy_used,
        recommended_concurrency=recommended_concurrency,
        message=detail_message,
    )


@router.put("/{channel_id}/proxy", response_model=ChannelOut, dependencies=[Depends(require_admin)])
async def upsert_channel_proxy(channel_id: int, data: ChannelProxyIn, db: AsyncSession = Depends(get_db)):
    channel = await get_channel_or_404(channel_id, db)
    await ensure_channel_proxy_mutation_allowed(db, channel_id)
    payload = ChannelUpdate(
        name=channel.name,
        api_key=channel.api_key,
        sender_name=channel.sender_name or "",
        sender_email=channel.sender_email or "",
        sender_emails=decode_sender_emails(channel),
        daily_limit=channel.daily_limit,
        hourly_limit=channel.hourly_limit,
        per_minute_limit=channel.per_minute_limit,
        second_limit=channel.second_limit,
        use_proxy=True,
        proxy=data,
        is_active=bool(channel.is_active),
        enabled=bool(channel.enabled),
    )
    await validate_proxy_binding(payload, db, current_channel_id=channel_id)
    await apply_channel_proxy(channel, payload, db)
    await reload_httpx_client(channel)
    await db.commit()
    return await serialize_channel(await get_channel_or_404(channel_id, db))


@router.delete("/{channel_id}/proxy", response_model=ChannelOut, dependencies=[Depends(require_admin)])
async def delete_channel_proxy(channel_id: int, db: AsyncSession = Depends(get_db)):
    channel = await get_channel_or_404(channel_id, db)
    await ensure_channel_proxy_mutation_allowed(db, channel_id)
    payload = ChannelUpdate(
        name=channel.name,
        api_key=channel.api_key,
        sender_name=channel.sender_name or "",
        sender_email=channel.sender_email or "",
        sender_emails=decode_sender_emails(channel),
        daily_limit=channel.daily_limit,
        hourly_limit=channel.hourly_limit,
        per_minute_limit=channel.per_minute_limit,
        second_limit=channel.second_limit,
        use_proxy=False,
        proxy=None,
        is_active=bool(channel.is_active),
        enabled=bool(channel.enabled),
    )
    await validate_proxy_binding(payload, db, current_channel_id=channel_id)
    await apply_channel_proxy(channel, payload, db)
    await reload_httpx_client(channel)
    await db.commit()
    return await serialize_channel(await get_channel_or_404(channel_id, db))


