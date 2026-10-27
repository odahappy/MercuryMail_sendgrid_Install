"""httpx 客户端定期回收：每 30 分钟重建空闲连接，防止代理侧半死连接堆积。"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

HTTPX_CLIENT_RECYCLE_INTERVAL_SECONDS = 30 * 60


async def httpx_client_recycle_loop() -> None:
    while True:
        await asyncio.sleep(HTTPX_CLIENT_RECYCLE_INTERVAL_SECONDS)
        try:
            from app.services import channel_manager

            recycled = await channel_manager.recycle_idle_httpx_clients()
            if recycled > 0:
                logger.info("httpx 客户端定期回收完成 recycled=%s", recycled)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("httpx 客户端定期回收失败")
