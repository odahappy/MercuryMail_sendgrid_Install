"""SendGrid 通道账号健康自动检测（异步后台，不影响发送速率）。"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from time import monotonic

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.database import AsyncSessionLocal, commit_session_with_retry
from app.models.models import Channel, Task
from app.services.connection_manager import probe_sendgrid_scopes
from app.utils.crypto import decrypt_value


logger = logging.getLogger(__name__)

CHANNEL_HEALTH_CHECK_INTERVAL_SECONDS = 600

HEALTH_STATUS_HEALTHY = "healthy"
HEALTH_STATUS_UNAUTHORIZED = "unauthorized"
HEALTH_STATUS_ERROR = "error"


@dataclass
class ChannelHealthProbeResult:
    health_status: str
    health_error: str
    http_status: int = 0
    skipped: bool = False
    skip_reason: str = ""


def resolve_channel_api_key(channel: Channel) -> str:
    raw_key = str(getattr(channel, "api_key", "") or "").strip()
    if not raw_key:
        return ""
    decrypted = decrypt_value(raw_key).strip()
    return decrypted or raw_key


async def probe_channel_health(channel: Channel) -> ChannelHealthProbeResult:
    result = await probe_sendgrid_scopes(channel)
    status_code = int(result.status_code or 0)
    if result.success:
        return ChannelHealthProbeResult(
            health_status=HEALTH_STATUS_HEALTHY,
            health_error="",
            http_status=status_code,
        )
    if status_code in (401, 403):
        label = "401 未授权" if status_code == 401 else "403 禁止访问"
        return ChannelHealthProbeResult(
            health_status=HEALTH_STATUS_UNAUTHORIZED,
            health_error=f"{label}：API Key 无效或已删除",
            http_status=status_code,
        )
    body_preview = str(result.body or result.error_message or "").strip()[:120]
    detail = f"HTTP 状态 {status_code}" if status_code else (body_preview or "连接测试失败，请检查 API Key、网络或代理配置")
    if status_code and body_preview:
        detail = f"{detail}：{body_preview}"
    return ChannelHealthProbeResult(
        health_status=HEALTH_STATUS_ERROR,
        health_error=detail,
        http_status=status_code,
    )


def _resolve_persisted_status(
    previous_status: str,
    probe: ChannelHealthProbeResult,
) -> str:
    if probe.health_status == HEALTH_STATUS_UNAUTHORIZED:
        return HEALTH_STATUS_UNAUTHORIZED
    if probe.health_status == HEALTH_STATUS_HEALTHY:
        return HEALTH_STATUS_HEALTHY
    previous = str(previous_status or HEALTH_STATUS_HEALTHY).strip().lower()
    if previous == HEALTH_STATUS_HEALTHY:
        return HEALTH_STATUS_HEALTHY
    return HEALTH_STATUS_ERROR


def apply_probe_to_channel(
    channel: Channel,
    probe: ChannelHealthProbeResult,
    *,
    previous_status: str | None = None,
) -> str:
    previous = str(previous_status or getattr(channel, "health_status", "") or HEALTH_STATUS_HEALTHY)
    final_status = _resolve_persisted_status(previous, probe)
    channel.health_status = final_status
    channel.health_error = str(probe.health_error or "")[:255]
    channel.last_health_check = datetime.utcnow()
    return final_status


async def persist_channel_health(
    channel_id: int,
    probe: ChannelHealthProbeResult,
    *,
    previous_status: str | None = None,
) -> None:
    now_value = datetime.utcnow()
    resolved_previous = previous_status
    channel_name = ""
    final_status = ""
    async with AsyncSessionLocal() as db:
        channel = await db.get(Channel, int(channel_id))
        if channel is None:
            logger.info("通道健康状态落库跳过：通道 %s 已删除", channel_id)
            return
        if resolved_previous is None:
            resolved_previous = str(getattr(channel, "health_status", "") or HEALTH_STATUS_HEALTHY)
        channel_name = str(getattr(channel, "name", "") or "").strip()
        apply_probe_to_channel(channel, probe, previous_status=resolved_previous)
        final_status = str(getattr(channel, "health_status", "") or HEALTH_STATUS_HEALTHY)
        channel.updated_at = now_value
        await commit_session_with_retry(db)
    if normalize_channel_health_status(final_status) == HEALTH_STATUS_UNAUTHORIZED:
        logger.warning(
            "健康检查探测到未授权，仅更新观测状态，不影响发送任务 channel_id=%s channel_name=%s；发送任务仅根据 /v3/mail/send 的真实返回处理",
            channel_id,
            channel_name or "-",
        )
    elif normalize_channel_health_status(probe.health_status) == HEALTH_STATUS_ERROR:
        logger.warning(
            "通道健康探测异常，仅标记错误状态，不停止发送任务 channel_id=%s channel_name=%s error=%s",
            channel_id,
            channel_name or "-",
            str(probe.health_error or "未知错误"),
        )


def build_health_probe_from_scopes_result(
    *,
    success: bool,
    status_code: int,
    body: str,
) -> ChannelHealthProbeResult:
    if success:
        return ChannelHealthProbeResult(
            health_status=HEALTH_STATUS_HEALTHY,
            health_error="",
            http_status=int(status_code or 200),
        )
    if int(status_code or 0) in (401, 403):
        label = "401 未授权" if status_code == 401 else "403 禁止访问"
        return ChannelHealthProbeResult(
            health_status=HEALTH_STATUS_UNAUTHORIZED,
            health_error=f"{label}：API Key 无效或已删除",
            http_status=int(status_code or 0),
        )
    detail = str(body or "").strip()[:120]
    return ChannelHealthProbeResult(
        health_status=HEALTH_STATUS_ERROR,
        health_error=detail or "连接测试失败，请检查 API Key、网络或代理配置",
        http_status=int(status_code or 0),
    )


async def apply_health_from_scopes_probe(
    channel: Channel,
    *,
    success: bool,
    status_code: int,
    body: str,
) -> ChannelHealthProbeResult:
    probe = build_health_probe_from_scopes_result(
        success=success,
        status_code=int(status_code or 0),
        body=str(body or ""),
    )
    apply_probe_to_channel(channel, probe)
    return probe


async def _load_running_channel_ids() -> set[int]:
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(Task.channel_id).where(Task.status == "running", Task.channel_id.is_not(None)).distinct()
            )
        ).scalars().all()
    return {int(item) for item in rows if item is not None}


async def check_all_channels() -> dict[str, int]:
    started_at = monotonic()
    stats = {
        "checked": 0,
        "checked_running": 0,
        "skipped_unauthorized": 0,
        "healthy": 0,
        "unauthorized": 0,
        "error": 0,
    }
    running_channel_ids = await _load_running_channel_ids()

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Channel)
            .options(selectinload(Channel.proxy))
            .where(Channel.enabled.is_(True), Channel.is_active.is_(True))
            .order_by(Channel.id.asc())
        )
        channels = list(result.scalars().all())

    for channel in channels:
        channel_id = int(channel.id)
        previous_status = str(getattr(channel, "health_status", "") or HEALTH_STATUS_HEALTHY)
        if normalize_channel_health_status(previous_status) == HEALTH_STATUS_UNAUTHORIZED:
            stats["skipped_unauthorized"] += 1
            logger.debug("通道健康检测跳过（401/403 待手动恢复） channel_id=%s", channel_id)
            continue

        if channel_id in running_channel_ids:
            stats["checked_running"] += 1
            logger.debug("通道健康检测跳过运行中通道，避免 /v3/scopes 探测与发送链路互相干扰 channel_id=%s", channel_id)
            continue

        probe = await probe_channel_health(channel)
        await persist_channel_health(channel_id, probe, previous_status=previous_status)
        stats["checked"] += 1
        final_status = _resolve_persisted_status(previous_status, probe)
        if final_status == HEALTH_STATUS_HEALTHY:
            stats["healthy"] += 1
        elif final_status == HEALTH_STATUS_UNAUTHORIZED:
            stats["unauthorized"] += 1
        else:
            stats["error"] += 1

    elapsed_ms = (monotonic() - started_at) * 1000.0
    logger.info(
        "通道账号健康检测完成 checked=%s checked_running=%s skipped_unauthorized=%s healthy=%s unauthorized=%s error=%s elapsed_ms=%.1f",
        stats["checked"],
        stats["checked_running"],
        stats["skipped_unauthorized"],
        stats["healthy"],
        stats["unauthorized"],
        stats["error"],
        elapsed_ms,
    )
    return stats


async def channel_health_check_loop() -> None:
    from app.core.database import assert_database_tables_ready

    await assert_database_tables_ready()
    logger.info(
        "通道账号健康检测已启动，间隔 %s 秒（运行中任务的通道跳过后台探测，失效由发送链路自检处理）",
        CHANNEL_HEALTH_CHECK_INTERVAL_SECONDS,
    )
    while True:
        try:
            await check_all_channels()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("通道健康检测异常")
        await asyncio.sleep(CHANNEL_HEALTH_CHECK_INTERVAL_SECONDS)


CHANNEL_HEALTH_STALE_SECONDS = 600


def channel_configured_for_proxy(channel: Channel) -> bool:
    return bool(getattr(channel, "use_proxy", False)) or bool(getattr(channel, "proxy_id", None))


def normalize_channel_health_status(status: str | None) -> str:
    return str(status or HEALTH_STATUS_HEALTHY).strip().lower()


def is_channel_health_blocking(status: str | None) -> bool:
    normalized = normalize_channel_health_status(status)
    return normalized == HEALTH_STATUS_UNAUTHORIZED


def is_channel_health_check_stale(channel: Channel) -> bool:
    last_check = getattr(channel, "last_health_check", None)
    if last_check is None:
        return True
    age_seconds = (datetime.utcnow() - last_check).total_seconds()
    return age_seconds >= float(CHANNEL_HEALTH_STALE_SECONDS)


async def load_channel_for_health_check(channel_id: int) -> Channel | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Channel)
            .options(selectinload(Channel.proxy))
            .where(Channel.id == int(channel_id))
        )
        return result.scalar_one_or_none()


async def refresh_channel_health_if_stale(channel_id: int) -> Channel | None:
    """兼容入口：发送/启动调用只读取快照，不同步发起网络探测。"""
    return await load_channel_for_health_check(channel_id)


def task_start_blocked_message_for_status(status: str | None) -> str | None:
    normalized = normalize_channel_health_status(status)
    if normalized == HEALTH_STATUS_UNAUTHORIZED:
        return "通道未授权，无法启动任务"
    return None


def task_start_blocked_message_from_probe(probe: ChannelHealthProbeResult) -> str | None:
    if probe.health_status == HEALTH_STATUS_UNAUTHORIZED:
        return "通道未授权，无法启动任务"
    return None


def build_channel_start_blocked_message(channel: Channel) -> str | None:
    if not bool(getattr(channel, "enabled", True)) or not bool(getattr(channel, "is_active", True)):
        return "通道不可用，无法启动任务"
    return task_start_blocked_message_for_status(getattr(channel, "health_status", None))


async def ensure_channel_ready_for_task_start(channel_id: int) -> str | None:
    """任务启动只校验配置可用性；健康探测结果不得参与发送准入。"""
    normalized_id = int(channel_id or 0)
    if normalized_id <= 0:
        logger.warning("任务启动被拒绝：无效的通道 ID")
        return "通道不可用，无法启动任务"

    channel = await load_channel_for_health_check(normalized_id)
    if channel is None:
        logger.warning("任务启动被拒绝：通道 %s 不存在", normalized_id)
        return "通道不可用，无法启动任务"

    if not bool(getattr(channel, "enabled", True)) or not bool(getattr(channel, "is_active", True)):
        logger.warning("任务启动被拒绝：通道 %s 已禁用", normalized_id)
        return "通道不可用，无法启动任务"

    persisted_health = normalize_channel_health_status(getattr(channel, "health_status", None))
    if persisted_health != HEALTH_STATUS_HEALTHY:
        logger.warning(
            "任务启动忽略通道探测快照 channel_id=%s health_status=%s detail=%s；仅真实 Mail Send 响应可影响发送任务",
            normalized_id,
            persisted_health,
            str(getattr(channel, "health_error", "") or "")[:255],
        )
    return None


# 运行时保护：通道 health_status 首次变为 unauthorized 时，由 channel_runtime_guard 自动停止关联任务。
