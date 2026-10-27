import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

T = TypeVar("T")

REDIS_SAFE_OPERATION_TIMEOUT_SECONDS = 5.0
REDIS_SAFE_OPERATION_RETRY_ATTEMPTS = 3
REDIS_SAFE_OPERATION_RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0)


async def safe_redis_op(
    coro: Awaitable[T] | Callable[[], Awaitable[T]],
    fallback_value: T | Callable[[], T] | Callable[[], Awaitable[T]] | None = None,
    op_name: str = "",
    *,
    timeout_seconds: float = REDIS_SAFE_OPERATION_TIMEOUT_SECONDS,
    log_warning: bool = True,
) -> T | None:
    """执行 Redis 操作；传入工厂时指数退避重试三次，失败后才返回安全兜底值。"""
    timeout = max(0.05, float(timeout_seconds))
    attempts = REDIS_SAFE_OPERATION_RETRY_ATTEMPTS if callable(coro) else 1
    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            operation = coro() if callable(coro) else coro
            return await asyncio.wait_for(operation, timeout=timeout)
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, RedisError, ConnectionError, TimeoutError, OSError) as exc:
            last_error = exc
        except Exception as exc:
            last_error = exc
        if attempt < attempts:
            delay = REDIS_SAFE_OPERATION_RETRY_DELAYS_SECONDS[min(attempt - 1, len(REDIS_SAFE_OPERATION_RETRY_DELAYS_SECONDS) - 1)]
            await asyncio.sleep(delay)
    if last_error is not None:
        log = logger.warning if log_warning else logger.debug
        log(
            "Redis 操作连续失败，使用安全兜底值；不得据此新增或缩减并发 op=%s attempts=%s error_type=%s error=%s",
            op_name or "未命名操作",
            attempts,
            type(last_error).__name__,
            last_error,
        )
    try:
        if callable(fallback_value):
            value = fallback_value()
            if inspect.isawaitable(value):
                return await value
            return value
        return fallback_value
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "Redis 降级值计算失败，返回 None 继续运行 op=%s error_type=%s error=%s",
            op_name or "未命名操作",
            type(exc).__name__,
            exc,
        )
        return None
