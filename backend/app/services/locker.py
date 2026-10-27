import asyncio
import logging
from dataclasses import dataclass, field
from uuid import uuid4

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.core.redis import redis_lock_client as redis_client
from app.utils.redis_helpers import safe_redis_op

logger = logging.getLogger(__name__)

CHANNEL_TASK_BUSY_MESSAGE = "该通道已有任务运行，请使用不同通道并行发送"
REDIS_LOCK_RETRY_ATTEMPTS = 3
REDIS_LOCK_RETRY_BACKOFF_SECONDS = 0.05
LOCAL_LOCK_ACQUIRE_TIMEOUT_SECONDS = 5.0

HEARTBEAT_MAX_CONSECUTIVE_FAILURES = 3
_local_channel_locks: dict[str, asyncio.Lock] = {}
_local_channel_lock_guard = asyncio.Lock()


class RedisLockUnavailableError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


@dataclass
class ChannelLock:
    channel_id: int
    token: str
    scope: str = ""
    owner: str = ""
    ttl_seconds: int = 60
    heartbeat_interval: int = 20
    heartbeat_task: asyncio.Task | None = None
    local_lock: asyncio.Lock | None = None
    degraded_local: bool = False
    lost_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def key(self) -> str:
        if self.scope:
            return f"channel_lock:{self.channel_id}:{self.scope}"
        return f"channel_lock:{self.channel_id}"

    @property
    def value(self) -> str:
        return f"{self.token}:{self.owner}" if self.owner else self.token


def _lock_matches(current: str | None, lock: ChannelLock) -> bool:
    if not current:
        return False
    return current == lock.value or current == lock.token


def _scoped_lock_key(channel_id: int, scope: str) -> str:
    if scope:
        return f"channel_lock:{channel_id}:{scope}"
    return f"channel_lock:{channel_id}"


def _is_transient_redis_error(exc: Exception) -> bool:
    return isinstance(exc, (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError, ConnectionError, OSError))


async def _acquire_local_degraded_lock(lock: ChannelLock, *, key: str, owner: str) -> ChannelLock | None:
    async with _local_channel_lock_guard:
        local_lock = _local_channel_locks.setdefault(key, asyncio.Lock())
    try:
        await asyncio.wait_for(local_lock.acquire(), timeout=LOCAL_LOCK_ACQUIRE_TIMEOUT_SECONDS)
    except TimeoutError:
        logger.warning(
            "本地降级通道锁获取超时，拒绝长期等待 key=%s owner=%s timeout=%ss",
            key,
            owner,
            LOCAL_LOCK_ACQUIRE_TIMEOUT_SECONDS,
        )
        return None
    lock.local_lock = local_lock
    lock.degraded_local = True
    logger.warning("通道锁使用本地降级，Redis 不可用 key=%s owner=%s", key, owner)
    lock.heartbeat_task = asyncio.create_task(heartbeat_channel_lock(lock))
    return lock


async def _redis_call_with_retry(operation, *, label: str):
    for attempt in range(1, REDIS_LOCK_RETRY_ATTEMPTS + 1):
        result = await safe_redis_op(
            operation(),
            fallback_value=None,
            op_name=f"Redis通道锁:{label}:attempt={attempt}",
        )
        if result is not None:
            return result
        if attempt < REDIS_LOCK_RETRY_ATTEMPTS:
            await asyncio.sleep(REDIS_LOCK_RETRY_BACKOFF_SECONDS * attempt)
    message = f"Redis 锁服务暂时不可用（{label}），将使用本地互斥锁降级"
    logger.warning("%s attempts=%s", message, REDIS_LOCK_RETRY_ATTEMPTS)
    raise RedisLockUnavailableError(message)


async def _acquire_scoped_lock(
    channel_id: int,
    scope: str,
    owner: str,
    ttl_seconds: int = 120,
    timeout_seconds: int = 5,
) -> ChannelLock | None:
    token = str(uuid4())
    key = _scoped_lock_key(channel_id, scope)
    lock = ChannelLock(
        channel_id=channel_id,
        token=token,
        scope=scope,
        owner=owner,
        ttl_seconds=ttl_seconds,
    )
    async def _acquire_once():
        acquired = await redis_client.set(key, lock.value, nx=True, ex=ttl_seconds)
        if not acquired:
            current = await redis_client.get(key)
            ttl = await redis_client.ttl(key)
            logger.warning("Redis 通道锁 key=%s 已被占用 current=%s ttl=%s", key, current, ttl)
            return None
        return lock

    try:
        if timeout_seconds <= 0:
            acquired_lock = await _redis_call_with_retry(_acquire_once, label=f"acquire:{key}")
        else:
            async with asyncio.timeout(timeout_seconds):
                logger.warning("正在获取 Redis 通道锁 key=%s owner=%s ttl=%s", key, owner, ttl_seconds)
                acquired_lock = await _redis_call_with_retry(_acquire_once, label=f"acquire:{key}")
        if acquired_lock is None:
            return None
        logger.warning("已获取 Redis 通道锁 key=%s owner=%s", key, owner)
        acquired_lock.heartbeat_task = asyncio.create_task(heartbeat_channel_lock(acquired_lock))
        return acquired_lock
    except TimeoutError:
        logger.warning("获取 Redis 通道锁超时 key=%s owner=%s 等待=%ss，启用本地降级锁", key, owner, timeout_seconds)
        return await _acquire_local_degraded_lock(lock, key=key, owner=owner)
    except RedisLockUnavailableError:
        return await _acquire_local_degraded_lock(lock, key=key, owner=owner)
    except Exception as exc:
        logger.warning("获取 Redis 通道锁失败 key=%s owner=%s error_type=%s error=%s，启用本地降级锁", key, owner, type(exc).__name__, exc)
        if _is_transient_redis_error(exc):
            return await _acquire_local_degraded_lock(lock, key=key, owner=owner)
        raise


async def acquire_task_lock(channel_id: int, task_id: int, ttl_seconds: int = 120, timeout_seconds: int = 5) -> ChannelLock | None:
    return await _acquire_scoped_lock(channel_id, "task", f"task:{task_id}", ttl_seconds, timeout_seconds)


async def try_acquire_task_lock(channel_id: int, task_id: int, ttl_seconds: int = 120) -> ChannelLock | None:
    """非阻塞尝试获取发送任务锁，失败立即返回 None。"""
    return await _acquire_scoped_lock(channel_id, "task", f"task:{task_id}", ttl_seconds, timeout_seconds=0)


def _warmup_lock_scope(plan_id: int) -> str:
    return f"warmup:{int(plan_id)}"


async def acquire_warmup_lock(channel_id: int, plan_id: int, ttl_seconds: int = 120, timeout_seconds: int = 5) -> ChannelLock | None:
    return await _acquire_scoped_lock(channel_id, _warmup_lock_scope(plan_id), f"warmup:{plan_id}", ttl_seconds, timeout_seconds)


async def try_acquire_warmup_lock(channel_id: int, plan_id: int, ttl_seconds: int = 120) -> ChannelLock | None:
    """非阻塞尝试获取养号计划级锁，失败立即返回 None。"""
    return await _acquire_scoped_lock(channel_id, _warmup_lock_scope(plan_id), f"warmup:{plan_id}", ttl_seconds, timeout_seconds=0)


async def acquire_channel_lock(channel_id: int, ttl_seconds: int = 60, owner: str = "", timeout_seconds: int = 5) -> ChannelLock | None:
    return await _acquire_scoped_lock(channel_id, "", owner, ttl_seconds, timeout_seconds)


async def heartbeat_channel_lock(lock: ChannelLock) -> None:
    consecutive_failures = 0
    while True:
        try:
            await asyncio.sleep(lock.heartbeat_interval)
            if lock.degraded_local:
                redis_available = await safe_redis_op(
                    redis_client.ping(),
                    fallback_value=False,
                    op_name=f"探测通道锁Redis恢复:{lock.key}",
                    timeout_seconds=1.0,
                    log_warning=False,
                )
                if redis_available:
                    logger.info("Redis 通道锁服务已恢复，本地降级锁将在当前批量周期结束后释放 key=%s owner=%s", lock.key, lock.owner)
                    return
                continue
            current = await redis_client.get(lock.key)
            if not _lock_matches(current, lock):
                logger.warning(
                    "channel lock %s lost because holder no longer matches redis value=%s owner=%s",
                    lock.channel_id,
                    current,
                    lock.owner,
                )
                lock.lost_event.set()
                return
            renewed = await redis_client.expire(lock.key, lock.ttl_seconds)
            if renewed:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.warning(
                    "通道锁 %s 心跳续期返回 false consecutive_failures=%s",
                    lock.channel_id,
                    consecutive_failures,
                )
            if consecutive_failures >= HEARTBEAT_MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "通道锁 %s 心跳连续失败 %s 次；通知 runner 退出",
                    lock.channel_id,
                    consecutive_failures,
                )
                lock.lost_event.set()
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            consecutive_failures += 1
            logger.exception(
                "通道锁 %s 心跳异常 consecutive_failures=%s",
                lock.channel_id,
                consecutive_failures,
            )
            if consecutive_failures >= HEARTBEAT_MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    "通道锁 %s 异常后心跳连续失败 %s 次；通知 runner",
                    lock.channel_id,
                    consecutive_failures,
                )
                lock.lost_event.set()
                return


async def _release_scoped_lock(lock: ChannelLock | None) -> None:
    if lock is None:
        return
    if lock.heartbeat_task is not None:
        lock.heartbeat_task.cancel()
        try:
            await lock.heartbeat_task
        except asyncio.CancelledError:
            pass
    if lock.degraded_local:
        if lock.local_lock is not None and lock.local_lock.locked():
            lock.local_lock.release()
        logger.warning("已释放本地降级通道锁 key=%s owner=%s", lock.key, lock.owner)
        return
    await safe_redis_op(
        redis_client.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] or redis.call('get', KEYS[1]) == ARGV[2] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            1,
            lock.key,
            lock.value,
            lock.token,
        ),
        fallback_value=0,
        op_name=f"释放通道锁:{lock.key}",
    )


async def release_task_lock(lock: ChannelLock | None) -> None:
    await _release_scoped_lock(lock)


async def release_warmup_lock(lock: ChannelLock | None) -> None:
    await _release_scoped_lock(lock)


async def release_channel_lock(lock: ChannelLock | None) -> None:
    await _release_scoped_lock(lock)


async def _is_scoped_lock_held(channel_id: int, scope: str) -> bool:
    key = _scoped_lock_key(channel_id, scope)
    try:
        async def _check_lock():
            async with asyncio.timeout(5):
                return bool(await redis_client.get(key))

        return bool(await _redis_call_with_retry(_check_lock, label=f"held:{key}"))
    except RedisLockUnavailableError as exc:
        logger.warning(
            "通道锁检查不可用 channel_id=%s scope=%s error=%s；视为仍持有锁",
            channel_id,
            scope,
            exc.message,
        )
        return True
    except Exception as exc:
        logger.warning(
            "通道锁检查失败 channel_id=%s scope=%s error_type=%s error=%s；视为仍持有锁",
            channel_id,
            scope,
            type(exc).__name__,
            exc,
        )
        return True


async def is_task_lock_held(channel_id: int) -> bool:
    return await _is_scoped_lock_held(channel_id, "task")


async def is_warmup_lock_held(channel_id: int, plan_id: int | None = None) -> bool:
    if plan_id is not None:
        return await _is_scoped_lock_held(channel_id, _warmup_lock_scope(plan_id))
    pattern = f"channel_lock:{int(channel_id)}:warmup:*"
    try:
        async def _scan_locks():
            async with asyncio.timeout(5):
                async for _ in redis_client.scan_iter(match=pattern, count=100):
                    return True
                return False

        return bool(await _redis_call_with_retry(_scan_locks, label=f"held:{pattern}"))
    except RedisLockUnavailableError as exc:
        logger.warning(
            "养号锁扫描不可用 channel_id=%s error=%s；视为仍持有锁",
            channel_id,
            exc.message,
        )
        return True
    except Exception as exc:
        logger.warning(
            "养号锁扫描失败 channel_id=%s error_type=%s error=%s；视为仍持有锁",
            channel_id,
            type(exc).__name__,
            exc,
        )
        return True


async def is_channel_locked(channel_id: int) -> bool:
    return await is_task_lock_held(channel_id) or await is_warmup_lock_held(channel_id)
