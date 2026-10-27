"""通道 API 密钥失效时的运行时任务保护（发送任务 + 养号计划）。"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.models import Channel, Task, WarmupPlan

logger = logging.getLogger(__name__)

HEALTH_STATUS_UNAUTHORIZED = "unauthorized"
CHANNEL_UNAUTHORIZED_STOP_MESSAGE = "通道 API 密钥已失效，自动停止任务"
CHANNEL_UNAUTHORIZED_WARMUP_STOP_MESSAGE = "通道 API 密钥已失效，自动停止养号计划"

_TERMINAL_TASK_STATUSES = frozenset(
    {"completed", "failed", "aborted", "finished", "draft", "cancelled", "stopped"}
)

_channel_stop_locks: dict[int, asyncio.Lock] = {}
_channel_stop_tasks: dict[int, asyncio.Task] = {}


def _normalize_channel_health_status(status: str | None) -> str:
    return str(status or "healthy").strip().lower()


def is_channel_unauthorized_transition(previous_status: str | None, new_status: str | None) -> bool:
    previous = _normalize_channel_health_status(previous_status)
    current = _normalize_channel_health_status(new_status)
    return current == HEALTH_STATUS_UNAUTHORIZED and previous != HEALTH_STATUS_UNAUTHORIZED


def _channel_stop_lock(channel_id: int) -> asyncio.Lock:
    lock = _channel_stop_locks.get(int(channel_id))
    if lock is None:
        lock = asyncio.Lock()
        _channel_stop_locks[int(channel_id)] = lock
    return lock


def schedule_channel_unauthorized_runtime_stop(
    channel_id: int,
    *,
    channel_name: str | None = None,
    previous_status: str | None = None,
    new_status: str | None = HEALTH_STATUS_UNAUTHORIZED,
) -> None:
    """异步调度通道失效保护，不阻塞健康检查主流程。"""
    if not is_channel_unauthorized_transition(previous_status, new_status):
        return
    normalized_id = int(channel_id or 0)
    if normalized_id <= 0:
        return

    existing = _channel_stop_tasks.get(normalized_id)
    if existing is not None and not existing.done():
        logger.debug("通道 %s 失效保护已在执行中，跳过重复调度", normalized_id)
        return

    task = asyncio.create_task(
        execute_channel_unauthorized_runtime_stop(normalized_id, channel_name=channel_name),
        name=f"channel-unauthorized-stop-{normalized_id}",
    )
    _channel_stop_tasks[normalized_id] = task

    def _cleanup(done: asyncio.Task) -> None:
        current = _channel_stop_tasks.get(normalized_id)
        if current is done:
            _channel_stop_tasks.pop(normalized_id, None)
        if done.cancelled():
            return
        try:
            exc = done.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            logger.exception("通道 %s 失效保护执行异常", normalized_id, exc_info=exc)

    task.add_done_callback(_cleanup)


async def execute_channel_unauthorized_runtime_stop(
    channel_id: int,
    *,
    channel_name: str | None = None,
) -> dict[str, object]:
    normalized_id = int(channel_id or 0)
    if normalized_id <= 0:
        return {"channel_id": normalized_id, "stopped_task_ids": [], "stopped_warmup_ids": []}

    lock = _channel_stop_lock(normalized_id)
    if lock.locked():
        logger.debug("通道 %s 失效保护锁已被占用，跳过并发重复执行", normalized_id)
        return {"channel_id": normalized_id, "skipped": True}

    async with lock:
        resolved_name = str(channel_name or "").strip()
        if not resolved_name:
            async with AsyncSessionLocal() as db:
                resolved_name = str(
                    (await db.execute(select(Channel.name).where(Channel.id == normalized_id))).scalar_one_or_none()
                    or ""
                ).strip()
        if not resolved_name:
            resolved_name = f"通道 {normalized_id}"

        task_ids = await _load_send_task_ids_for_channel(normalized_id)
        warmup_ids = await _load_running_warmup_ids_for_channel(normalized_id)

        stopped_task_ids: list[int] = []
        stopped_warmup_ids: list[int] = []

        from app.services.task_runner import abort_task_for_channel_unauthorized
        from app.services.warmup_sender import stop_warmup_for_channel_unauthorized

        for task_id in task_ids:
            try:
                if await abort_task_for_channel_unauthorized(task_id):
                    stopped_task_ids.append(int(task_id))
            except Exception:
                logger.exception("通道 %s 自动停止发送任务 %s 失败", normalized_id, task_id)

        for plan_id in warmup_ids:
            try:
                if await stop_warmup_for_channel_unauthorized(plan_id):
                    stopped_warmup_ids.append(int(plan_id))
            except Exception:
                logger.exception("通道 %s 自动停止养号计划 %s 失败", normalized_id, plan_id)

        if stopped_task_ids or stopped_warmup_ids:
            logger.error(
                "通道 API 密钥已失效，已自动停止关联任务：channel_id=%s channel_name=%s 发送任务=%s 养号计划=%s",
                normalized_id,
                resolved_name,
                stopped_task_ids or "无",
                stopped_warmup_ids or "无",
            )
            await _record_channel_unauthorized_stop_event(
                normalized_id,
                resolved_name,
                stopped_task_ids,
                stopped_warmup_ids,
            )
        else:
            logger.info(
                "通道 %s（%s）API 密钥已失效，当前无运行中的发送任务或养号计划需要停止",
                normalized_id,
                resolved_name,
            )

        return {
            "channel_id": normalized_id,
            "channel_name": resolved_name,
            "stopped_task_ids": stopped_task_ids,
            "stopped_warmup_ids": stopped_warmup_ids,
        }


async def _load_send_task_ids_for_channel(channel_id: int) -> list[int]:
    async with AsyncSessionLocal() as db:
        rows = (
            await db.execute(
                select(Task.id, Task.status).where(
                    Task.channel_id == int(channel_id),
                    Task.status.in_(("running", "paused")),
                )
            )
        ).all()
    task_ids: list[int] = []
    for task_id, task_status in rows:
        status = str(task_status or "").strip().lower()
        if status in _TERMINAL_TASK_STATUSES:
            continue
        if status in {"running", "paused"}:
            task_ids.append(int(task_id))
    return task_ids


async def _load_running_warmup_ids_for_channel(channel_id: int) -> list[int]:
    async with AsyncSessionLocal() as db:
        db_rows = (
            await db.execute(
                select(WarmupPlan.id).where(
                    WarmupPlan.channel_id == int(channel_id),
                    WarmupPlan.running.is_(True),
                )
            )
        ).scalars().all()
    plan_ids = {int(item) for item in db_rows if item is not None}

    from app.services.warmup_sender import RUNNING_WARMUPS

    for plan_id, runner in list(RUNNING_WARMUPS.items()):
        if runner is None or runner.done():
            continue
        plan = await _load_warmup_plan_channel(plan_id)
        if plan is not None and int(plan) == int(channel_id):
            plan_ids.add(int(plan_id))
    return sorted(plan_ids)


async def _load_warmup_plan_channel(plan_id: int) -> int | None:
    async with AsyncSessionLocal() as db:
        channel_id = (
            await db.execute(select(WarmupPlan.channel_id).where(WarmupPlan.id == int(plan_id)))
        ).scalar_one_or_none()
    if channel_id is None:
        return None
    return int(channel_id)


async def _record_channel_unauthorized_stop_event(
    channel_id: int,
    channel_name: str,
    task_ids: list[int],
    warmup_ids: list[int],
) -> None:
    try:
        from app.core.redis import redis_client

        payload = json.dumps(
            {
                "channel_id": int(channel_id),
                "channel_name": channel_name,
                "task_ids": [int(item) for item in task_ids],
                "warmup_ids": [int(item) for item in warmup_ids],
                "message": CHANNEL_UNAUTHORIZED_STOP_MESSAGE,
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
            ensure_ascii=False,
        )
        key = "dashboard:channel_unauthorized_stops"
        pipe = redis_client.pipeline()
        pipe.lpush(key, payload)
        pipe.ltrim(key, 0, 99)
        pipe.expire(key, 86400)
        await pipe.execute()
    except Exception:
        logger.exception("记录通道失效自动停止事件失败 channel_id=%s", channel_id)
