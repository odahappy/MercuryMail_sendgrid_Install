"""仪表盘事件计数（Redis），避免与 dashboard_stats_service 循环依赖。"""

from __future__ import annotations

import time
from uuid import uuid4

from app.core.redis import redis_client

CONCURRENCY_REJECTION_REDIS_KEY = "dashboard:concurrency_rejections"
CONCURRENCY_REJECTION_WINDOW_SECONDS = 900


async def record_global_concurrency_rejection() -> None:
    now_value = time.time()
    member = str(uuid4())
    pipe = redis_client.pipeline()
    pipe.zadd(CONCURRENCY_REJECTION_REDIS_KEY, {member: now_value})
    pipe.zremrangebyscore(CONCURRENCY_REJECTION_REDIS_KEY, 0, now_value - CONCURRENCY_REJECTION_WINDOW_SECONDS)
    pipe.expire(CONCURRENCY_REJECTION_REDIS_KEY, CONCURRENCY_REJECTION_WINDOW_SECONDS)
    await pipe.execute()


async def count_recent_concurrency_rejections(window_seconds: int = 300) -> int:
    now_value = time.time()
    cutoff = now_value - max(1, int(window_seconds))
    return int(await redis_client.zcount(CONCURRENCY_REJECTION_REDIS_KEY, cutoff, now_value))
