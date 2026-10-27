import asyncio
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis import get_redis, redis_client
from app.models.models import Channel, Task
from app.utils.redis_helpers import safe_redis_op

logger = logging.getLogger(__name__)

WINDOW_SECONDS = 60
MIN_ERRORS = 5
ERROR_RATE_THRESHOLD = 0.5
PROXY_HEALTH_MONITOR_INTERVAL_SECONDS = 30
PROXY_HEALTH_MONITOR_INITIAL_DELAY_SECONDS = 5
PROXY_HEALTH_MONITOR_PROBE_TIMEOUT_SECONDS = 20
PROXY_HEALTH_RETRY_DELAY_SECONDS = 10
PROXY_HEALTH_ATTEMPTS = 2
CHANNEL_AUTH_PAUSE_REASON = "通道认证失败，已自动暂停"
PROXY_HEALTH_PAUSE_REASON = "代理健康检测连续失败，已自动暂停"


def channel_paused_key(channel_id: int) -> str:
    return f"channel_paused:{int(channel_id)}"


def channel_attempts_key(channel_id: int) -> str:
    return f"channel:{int(channel_id)}:health:attempts"


def channel_errors_key(channel_id: int) -> str:
    return f"channel:{int(channel_id)}:health:errors"


def channel_health_disabled_key(channel_id: int) -> str:
    return f"channel:{int(channel_id)}:health:disabled"


def _coerce_retry_after_seconds(value: str | int | float | None) -> float:
    if value is None:
        return 2.0
    raw = str(value).strip()
    if not raw:
        return 2.0
    try:
        return min(max(float(raw), 1.0), 300.0)
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime

        target = parsedate_to_datetime(raw)
        if target.tzinfo is not None:
            return min(max((target - datetime.now(timezone.utc)).total_seconds(), 1.0), 300.0)
        return min(max((target.replace(tzinfo=None) - datetime.utcnow()).total_seconds(), 1.0), 300.0)
    except (TypeError, ValueError, OverflowError):
        return 2.0


def _channel_health_redis_keys(channel_id: int, proxy_id: int | None = None) -> list[str]:
    normalized_channel_id = int(channel_id)
    keys = [
        channel_attempts_key(normalized_channel_id),
        channel_errors_key(normalized_channel_id),
        channel_health_disabled_key(normalized_channel_id),
        f"channel_health:{normalized_channel_id}",
        f"channel_last_check:{normalized_channel_id}",
        f"channel_paused:{normalized_channel_id}",
        f"channel_health_status:{normalized_channel_id}",
        f"channel_health_error:{normalized_channel_id}",
        f"channel_last_health_check:{normalized_channel_id}",
        f"channel:{normalized_channel_id}:health",
        f"channel:{normalized_channel_id}:health:status",
        f"channel:{normalized_channel_id}:health:last_check",
        f"channel:{normalized_channel_id}:health:paused",
        f"channel:{normalized_channel_id}:health:error",
        f"channel:{normalized_channel_id}:health:warning",
    ]
    if proxy_id is not None:
        normalized_proxy_id = int(proxy_id)
        keys.extend(
            [
                f"proxy:{normalized_proxy_id}:channel:{normalized_channel_id}:health",
                f"proxy:{normalized_proxy_id}:channel:{normalized_channel_id}:last_check",
                f"channel:{normalized_channel_id}:proxy:{normalized_proxy_id}:health",
            ]
        )
    return keys


async def _delete_matching_redis_keys(redis, patterns: list[str]) -> int:
    deleted_total = 0
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=200)
            if keys:
                deleted_total += int(await redis.delete(*keys) or 0)
            if int(cursor or 0) == 0:
                break
    return deleted_total


async def clear_channel_health_redis_keys(channel_id: int, proxy_id: int | None = None) -> None:
    """清理通道健康检测相关 Redis 键。"""
    normalized_channel_id = int(channel_id)
    redis = redis_client
    generator = get_redis()
    try:
        redis = await generator.__anext__()
        keys = _channel_health_redis_keys(normalized_channel_id, proxy_id)
        deleted = int(await redis.delete(*keys) or 0) if keys else 0
        deleted += await _delete_matching_redis_keys(
            redis,
            [
                f"channel:{normalized_channel_id}:health:*",
                f"channel_health:*:{normalized_channel_id}",
                f"channel:*:{normalized_channel_id}:health*",
            ],
        )
        logger.info("已清理通道健康检测 Redis 键 channel_id=%s proxy_id=%s deleted=%s", normalized_channel_id, proxy_id, deleted)
    except StopAsyncIteration:
        logger.warning("清理通道健康检测 Redis 键失败：无法获取 Redis 连接 channel_id=%s", normalized_channel_id)
    except Exception as exc:
        logger.warning("清理通道健康检测 Redis 键失败 channel_id=%s proxy_id=%s error=%s", normalized_channel_id, proxy_id, exc)
    finally:
        try:
            await generator.aclose()
        except Exception:
            pass


async def handle_channel_auth_failure(channel_id: int) -> list[int]:
    """标记通道鉴权故障；仅暂停实际运行任务，禁止误暂停尚未启动的计划任务。"""
    normalized_id = int(channel_id)
    ttl = max(60, int(settings.channel_auth_pause_ttl_seconds))
    await safe_redis_op(
        lambda: redis_client.set(channel_paused_key(normalized_id), "1", ex=ttl),
        fallback_value=False,
        op_name=f"标记通道认证暂停:{normalized_id}",
        timeout_seconds=1.0,
    )
    now_value = datetime.utcnow()
    pausable_statuses = ("running", "starting")
    async with AsyncSessionLocal() as db:
        task_ids = list((await db.execute(
            select(Task.id).where(
                Task.channel_id == normalized_id,
                Task.deleted_at.is_(None),
                or_(
                    Task.status.in_(pausable_statuses),
                    Task.run_message == CHANNEL_AUTH_PAUSE_REASON,
                ),
            )
        )).scalars().all())
        if task_ids:
            await db.execute(
                update(Task)
                .where(Task.id.in_(task_ids))
                .values(status="paused", run_message=CHANNEL_AUTH_PAUSE_REASON, updated_at=now_value)
            )
        await db.execute(
            update(Channel)
            .where(Channel.id == normalized_id)
            .values(
                health_status="unauthorized",
                health_error=CHANNEL_AUTH_PAUSE_REASON,
                last_error=CHANNEL_AUTH_PAUSE_REASON,
                last_health_check=now_value,
                updated_at=now_value,
            )
        )
        await db.commit()

    from app.services.task_runner import cancel_scheduled_start_retry, cancel_task_runner, is_task_runner_active

    normalized_task_ids = [int(task_id) for task_id in task_ids]
    for task_id in normalized_task_ids:
        cancel_scheduled_start_retry(task_id)
        if is_task_runner_active(task_id):
            asyncio.create_task(
                cancel_task_runner(task_id, graceful_timeout=5.0),
                name=f"cancel-task-{task_id}-channel-auth-failure",
            )
    logger.error("通道 %s 认证失败，已自动暂停相关任务 task_ids=%s", normalized_id, normalized_task_ids)
    return normalized_task_ids


async def _recover_channel_auth_tasks(channel_id: int) -> list[int]:
    normalized_id = int(channel_id)
    async with AsyncSessionLocal() as db:
        task_ids = list((await db.execute(
            select(Task.id).where(
                Task.channel_id == normalized_id,
                Task.deleted_at.is_(None),
                Task.status == "paused",
                Task.run_message == CHANNEL_AUTH_PAUSE_REASON,
            )
        )).scalars().all())
        await db.execute(
            update(Channel)
            .where(Channel.id == normalized_id)
            .values(health_status="healthy", health_error=None, last_error="", last_health_check=datetime.utcnow(), updated_at=datetime.utcnow())
        )
        await db.commit()
    if not task_ids:
        await safe_redis_op(
            lambda: redis_client.delete(channel_paused_key(normalized_id)),
            fallback_value=0,
            op_name=f"清除通道认证暂停:{normalized_id}",
            timeout_seconds=1.0,
        )
        return []
    from app.services.task_runner import is_task_runner_active, start_task_runner

    resumed: list[int] = []
    pending: list[int] = []
    for task_id in task_ids:
        normalized_task_id = int(task_id)
        if is_task_runner_active(normalized_task_id):
            pending.append(normalized_task_id)
            logger.warning("通道认证已恢复，但旧 Runner 尚未退出，本轮暂不重启 channel_id=%s task_id=%s", normalized_id, normalized_task_id)
            continue
        try:
            if await start_task_runner(normalized_task_id, rollback_status="paused"):
                resumed.append(normalized_task_id)
            else:
                pending.append(normalized_task_id)
        except Exception:
            pending.append(normalized_task_id)
            logger.exception(
                "通道认证恢复后自动继续任务失败 channel_id=%s task_id=%s",
                normalized_id,
                normalized_task_id,
            )
    if pending:
        await safe_redis_op(
            lambda: redis_client.set(channel_paused_key(normalized_id), "1", ex=max(60, int(settings.channel_auth_pause_ttl_seconds))),
            fallback_value=False,
            op_name=f"保留通道认证恢复重试标记:{normalized_id}",
            timeout_seconds=1.0,
        )
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(Channel)
                .where(Channel.id == normalized_id)
                .values(health_status="unauthorized", health_error=CHANNEL_AUTH_PAUSE_REASON, updated_at=datetime.utcnow())
            )
            await db.commit()
        logger.warning("通道认证恢复仍有任务等待安全重启 channel_id=%s pending_task_ids=%s", normalized_id, pending)
    else:
        await safe_redis_op(
            lambda: redis_client.delete(channel_paused_key(normalized_id)),
            fallback_value=0,
            op_name=f"清除通道认证暂停:{normalized_id}",
            timeout_seconds=1.0,
        )
    for task_id in resumed:
        logger.info("通道 %s 认证恢复，任务 %s 已从持久化游标自动继续发送", normalized_id, task_id)
    return resumed


def _describe_proxy_probe_error(exc: BaseException | None, reported_error: str = "") -> str:
    detail = str(reported_error or "").strip()
    if detail:
        return detail[:255]
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "代理连接测试超时"
    if isinstance(exc, ConnectionRefusedError):
        return "代理连接被拒绝"
    if isinstance(exc, OSError):
        error_text = str(exc).lower()
        if "name or service not known" in error_text or "nodename nor servname" in error_text or "getaddrinfo" in error_text:
            return f"代理 DNS 解析失败：{exc}"[:255]
        return f"代理网络错误：{exc}"[:255]
    return f"代理检测异常：{type(exc).__name__}: {exc}"[:255] if exc is not None else "代理健康检测失败"


async def _probe_proxy_with_retry(channel, probe_factory) -> tuple[bool, int, str, bool]:
    last_result: tuple[bool, int, str, bool] = (False, 0, "代理健康检测失败", False)
    for attempt in range(1, PROXY_HEALTH_ATTEMPTS + 1):
        try:
            raw_result = await asyncio.wait_for(
                probe_factory(channel),
                timeout=PROXY_HEALTH_MONITOR_PROBE_TIMEOUT_SECONDS,
            )
            if hasattr(raw_result, "success"):
                result = (
                    bool(raw_result.success),
                    int(raw_result.latency_ms or 0),
                    str(raw_result.error_message or raw_result.body or ""),
                    bool(raw_result.proxy_used),
                )
            else:
                success, latency_ms, error_message, proxy_used = raw_result
                result = (bool(success), int(latency_ms or 0), str(error_message or ""), bool(proxy_used))
            if result[0]:
                return result
            last_result = (False, result[1], _describe_proxy_probe_error(None, result[2]), result[3])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_result = (False, 0, _describe_proxy_probe_error(exc), bool(getattr(channel, "use_proxy", False)))
        if attempt < PROXY_HEALTH_ATTEMPTS:
            logger.warning(
                "代理健康检测首次失败，%s 秒后重试 channel_id=%s error=%s",
                PROXY_HEALTH_RETRY_DELAY_SECONDS,
                int(getattr(channel, "id", 0) or 0),
                last_result[2],
            )
            await asyncio.sleep(PROXY_HEALTH_RETRY_DELAY_SECONDS)
    return last_result


async def run_proxy_health_monitor() -> None:
    """后台代理健康监控入口，供 main.py 启动时导入。"""
    from app.core.database import assert_database_tables_ready, commit_session_with_retry
    from app.services.connection_manager import measure_channel_proxy_latency, probe_sendgrid_scopes

    await assert_database_tables_ready()
    await asyncio.sleep(PROXY_HEALTH_MONITOR_INITIAL_DELAY_SECONDS)
    fast_interval = max(10, int(settings.channel_paused_check_interval_seconds))
    logger.warning(
        "通道健康监控已启动，常规间隔 %s 秒，认证暂停快速探测间隔 %s 秒",
        PROXY_HEALTH_MONITOR_INTERVAL_SECONDS,
        fast_interval,
    )
    last_regular_probe_at = 0.0
    while True:
        try:
            run_regular_probe = time.monotonic() - last_regular_probe_at >= PROXY_HEALTH_MONITOR_INTERVAL_SECONDS
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Channel)
                    .options(selectinload(Channel.proxy))
                    .where(Channel.enabled.is_(True), Channel.is_active.is_(True))
                    .order_by(Channel.id.asc())
                )
                channels = list(result.scalars().all())

            checked = 0
            failed = 0
            for channel in channels:
                channel_id = int(getattr(channel, "id", 0) or 0)
                redis_paused = bool(await safe_redis_op(
                    lambda: redis_client.exists(channel_paused_key(channel_id)),
                    fallback_value=False,
                    op_name=f"读取通道认证暂停:{channel_id}",
                    timeout_seconds=1.0,
                    log_warning=False,
                ))
                paused = redis_paused or str(getattr(channel, "health_status", "") or "").lower() == "unauthorized"
                if not paused and (not run_regular_probe or not bool(getattr(channel, "use_proxy", False))):
                    continue
                try:
                    if paused:
                        success, latency_ms, error_message, proxy_used = await _probe_proxy_with_retry(
                            channel,
                            probe_sendgrid_scopes,
                        )
                        if success:
                            await _recover_channel_auth_tasks(channel_id)
                            checked += 1
                            continue
                    else:
                        success, latency_ms, error_message, proxy_used = await _probe_proxy_with_retry(
                            channel,
                            measure_channel_proxy_latency,
                        )
                    if not proxy_used and not paused:
                        continue
                    checked += 1
                    now_value = datetime.utcnow()
                    async with AsyncSessionLocal() as db:
                        db_channel = await db.get(Channel, channel_id)
                        if db_channel is None:
                            continue
                        db_channel.last_health_check = now_value
                        db_channel.updated_at = now_value
                        if success:
                            db_channel.health_status = "healthy"
                            db_channel.health_error = None
                            db_channel.last_error = ""
                        else:
                            failed += 1
                            safe_error = str(error_message or "代理健康检测失败")[:255]
                            db_channel.health_status = "error"
                            db_channel.health_error = safe_error
                            db_channel.last_error = safe_error
                        await commit_session_with_retry(db)
                    if not success:
                        logger.warning(
                            "代理健康检测失败 channel_id=%s latency_ms=%s error=%s",
                            channel_id,
                            latency_ms,
                            error_message or "未知错误",
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "代理健康检测异常 channel_id=%s error_type=%s error=%s",
                        channel_id,
                        type(exc).__name__,
                        repr(exc),
                        exc_info=True,
                    )
            if run_regular_probe:
                last_regular_probe_at = time.monotonic()
            logger.info("通道健康监控完成 checked=%s failed=%s", checked, failed)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("通道健康监控循环异常")
        await asyncio.sleep(fast_interval)


async def trim_sliding_window(key: str, now: float) -> None:
    await redis_client.zremrangebyscore(key, 0, now - WINDOW_SECONDS)


async def record_send_outcome(channel_id: int, success: bool) -> None:
    if success:
        await record_send_outcomes(channel_id, success_count=1, failure_count=0)
    else:
        await record_send_outcomes(channel_id, success_count=0, failure_count=1)


async def record_send_outcomes(channel_id: int, *, success_count: int = 0, failure_count: int = 0) -> None:
    """批量记录通道健康结果，避免发送成功路径按收件人逐条 Redis 往返。"""
    normalized_success = max(0, int(success_count or 0))
    normalized_failure = max(0, int(failure_count or 0))
    total_attempts = normalized_success + normalized_failure
    if total_attempts <= 0:
        return
    now = time.time()
    attempt_key = channel_attempts_key(channel_id)
    pipe = redis_client.pipeline()
    attempt_members = {f"{now}:{uuid4()}": now for _ in range(total_attempts)}
    pipe.zadd(attempt_key, attempt_members)
    pipe.zremrangebyscore(attempt_key, 0, now - WINDOW_SECONDS)
    pipe.expire(attempt_key, WINDOW_SECONDS * 2)
    if normalized_failure > 0:
        error_key = channel_errors_key(channel_id)
        error_members = {f"{now}:{uuid4()}": now for _ in range(normalized_failure)}
        pipe.zadd(error_key, error_members)
        pipe.zremrangebyscore(error_key, 0, now - WINDOW_SECONDS)
        pipe.expire(error_key, WINDOW_SECONDS * 2)
    await pipe.execute()
    if normalized_failure > 0:
        await evaluate_circuit_breaker(channel_id, now)


async def evaluate_circuit_breaker(channel_id: int, now: float | None = None) -> bool:
    current = now if now is not None else time.time()
    error_key = channel_errors_key(channel_id)
    attempt_key = channel_attempts_key(channel_id)
    errors = int(await redis_client.zcount(error_key, current - WINDOW_SECONDS, current))
    attempts = int(await redis_client.zcount(attempt_key, current - WINDOW_SECONDS, current))
    if errors < MIN_ERRORS or attempts == 0:
        return False
    error_rate = errors / attempts
    if error_rate <= ERROR_RATE_THRESHOLD:
        return False
    await mark_channel_health_warning(channel_id, f"健康监控告警：60 秒内 {errors}/{attempts} 次失败（{round(error_rate * 100, 1)}%），仅记录不熔断")
    return True


async def mark_channel_health_warning(channel_id: int, reason: str) -> None:
    await redis_client.set(channel_health_disabled_key(channel_id), reason, ex=WINDOW_SECONDS * 2)
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(Channel)
            .where(Channel.id == channel_id)
            .values(last_error=reason, updated_at=datetime.utcnow())
        )
        await db.commit()


async def disable_channel(channel_id: int, reason: str) -> None:
    await mark_channel_health_warning(channel_id, reason)
