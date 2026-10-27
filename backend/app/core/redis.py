import asyncio
import logging
from functools import wraps
from urllib.parse import quote, urlsplit, urlunsplit

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from app.core.config import settings

logger = logging.getLogger(__name__)

REDIS_RETRY_ATTEMPTS = 3
REDIS_RETRY_BASE_DELAY_SECONDS = 0.5
REDIS_OPERATION_TIMEOUT_SECONDS = max(5.0, float(getattr(settings, "redis_socket_timeout_seconds", 5.0) or 5.0))
REDIS_CONNECT_TIMEOUT_SECONDS = max(5.0, float(getattr(settings, "redis_socket_connect_timeout_seconds", 5.0) or 5.0))
REDIS_HEALTH_CHECK_INTERVAL_SECONDS = max(30, int(getattr(settings, "redis_health_check_interval_seconds", 30) or 30))
REDIS_MAX_CONNECTIONS = max(128, int(getattr(settings, "redis_max_connections", 128) or 128))
REDIS_LOCK_MAX_CONNECTIONS = max(128, int(getattr(settings, "redis_lock_max_connections", 128) or 128))
REDIS_BACKGROUND_MAX_CONNECTIONS = max(128, int(getattr(settings, "redis_background_max_connections", 128) or 128))
REDIS_RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0)
REDIS_TRANSIENT_ERRORS = (RedisConnectionError, RedisTimeoutError, ConnectionError, TimeoutError, asyncio.TimeoutError)


def _build_redis_url_with_password() -> str:
    raw_url = str(settings.redis_url or "").strip()
    password = str(settings.redis_password or "").strip()
    if not raw_url or not password:
        return raw_url
    parsed = urlsplit(raw_url)
    if parsed.password:
        return raw_url
    username = parsed.username or ""
    userinfo = f"{quote(username)}:{quote(password)}@" if username else f":{quote(password)}@"
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{userinfo}{host}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def redis_retry(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        last_error = None
        for attempt in range(1, REDIS_RETRY_ATTEMPTS + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                last_error = exc
                message = str(exc).lower()
                if attempt >= REDIS_RETRY_ATTEMPTS or not any(marker in message for marker in (
                    'connection',
                    'timeout',
                    'closed',
                    'refused',
                    'reset',
                    'busy',
                )):
                    logger.warning("Redis 操作失败 func=%s attempt=%s error=%s", func.__name__, attempt, exc)
                    raise
                delay = REDIS_RETRY_DELAYS_SECONDS[min(attempt - 1, len(REDIS_RETRY_DELAYS_SECONDS) - 1)]
                logger.warning("Redis 操作重试 func=%s attempt=%s/%s delay=%.2fs error=%s", func.__name__, attempt, REDIS_RETRY_ATTEMPTS, delay, exc)
                await asyncio.sleep(delay)
        raise last_error

    return wrapper


def _create_redis_pool(max_connections: int) -> ConnectionPool:
    return ConnectionPool.from_url(
        _build_redis_url_with_password(),
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_OPERATION_TIMEOUT_SECONDS,
        health_check_interval=REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
        retry_on_timeout=True,
        socket_keepalive=True,
        max_connections=max(16, int(max_connections or REDIS_MAX_CONNECTIONS)),
    )


redis_pool = _create_redis_pool(REDIS_MAX_CONNECTIONS)
redis_lock_pool = _create_redis_pool(REDIS_LOCK_MAX_CONNECTIONS)
redis_background_pool = _create_redis_pool(REDIS_BACKGROUND_MAX_CONNECTIONS)
redis_client = Redis(connection_pool=redis_pool)
redis_lock_client = Redis(connection_pool=redis_lock_pool)
redis_background_client = Redis(connection_pool=redis_background_pool)


@redis_retry
async def ping_redis():
    await redis_client.ping()


async def ping_redis_once() -> None:
    """健康检查用：单次 ping，不做指数重试，避免门禁被 3×5s 超时拖死。"""
    await redis_client.ping()


async def get_redis():
    yield redis_client


async def close_redis():
    await redis_client.aclose()
    await redis_lock_client.aclose()
    await redis_background_client.aclose()
