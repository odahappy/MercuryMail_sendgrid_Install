import logging

from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.models import Channel, Proxy

logger = logging.getLogger(__name__)


def _proxy_endpoint_key(host: str | None, port: int | None) -> str:
    normalized_host = str(host or "").strip().lower()
    normalized_port = max(0, int(port or 0))
    if not normalized_host:
        return ""
    return f"{normalized_host}:{normalized_port}"


async def _load_proxy_enabled_channels() -> list[tuple[int, str, int, str, int, str]]:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(
                Channel.id,
                Channel.name,
                Proxy.id,
                Proxy.host,
                Proxy.port,
                Proxy.name,
            )
            .join(Proxy, Channel.proxy_id == Proxy.id)
            .where(
                Channel.use_proxy.is_(True),
                Channel.enabled.is_(True),
                Channel.is_active.is_(True),
                Channel.proxy_id.is_not(None),
                Proxy.is_active.is_(True),
            )
            .order_by(Channel.id.asc())
        )
        return list(result.all())


def _group_channels_by_proxy_endpoint(
    rows: list[tuple[int, str, int, str, int, str]],
) -> dict[str, list[dict[str, int | str]]]:
    grouped: dict[str, list[dict[str, int | str]]] = {}
    for channel_id, channel_name, proxy_id, proxy_host, proxy_port, proxy_name in rows:
        endpoint = _proxy_endpoint_key(proxy_host, proxy_port)
        if not endpoint:
            continue
        grouped.setdefault(endpoint, []).append(
            {
                "channel_id": int(channel_id),
                "channel_name": str(channel_name or ""),
                "proxy_id": int(proxy_id),
                "proxy_name": str(proxy_name or ""),
            }
        )
    return grouped


def _format_shared_proxy_warning(endpoint: str, channels: list[dict[str, int | str]]) -> str:
    channel_parts = [
        f"{item['channel_id']}:{item['channel_name']}(proxy_id={item['proxy_id']})"
        for item in channels
    ]
    return (
        f"代理共享风险：多个通道共用同一代理出口 {endpoint}，多任务并发时可能出现带宽或连接数瓶颈。"
        f" 涉及通道：{', '.join(channel_parts)}。"
        "建议为高并发任务分配独立代理出口，或降低并发/分散任务启动时间。"
    )


async def audit_all_shared_proxy_endpoints() -> int:
    rows = await _load_proxy_enabled_channels()
    grouped = _group_channels_by_proxy_endpoint(rows)
    warning_count = 0
    for endpoint, channels in grouped.items():
        if len(channels) < 2:
            continue
        logger.warning(_format_shared_proxy_warning(endpoint, channels))
        warning_count += 1
    if warning_count == 0:
        logger.warning("代理共享风险检查完成：未发现多个通道共用同一代理出口")
    else:
        logger.warning("代理共享风险检查完成：发现 %s 组共用代理出口", warning_count)
    return warning_count


async def warn_if_channel_shares_proxy_endpoint(channel_id: int | None) -> bool:
    if channel_id is None:
        return False
    rows = await _load_proxy_enabled_channels()
    grouped = _group_channels_by_proxy_endpoint(rows)
    for endpoint, channels in grouped.items():
        if len(channels) < 2:
            continue
        if any(int(item["channel_id"]) == int(channel_id) for item in channels):
            logger.warning(_format_shared_proxy_warning(endpoint, channels))
            return True
    return False
