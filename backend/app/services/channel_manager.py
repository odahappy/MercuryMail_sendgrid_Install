import asyncio
import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

import httpx

from app.core.config import (
    SEND_EMAIL_HTTP_BUDGET_SECONDS,
    SEND_EMAIL_QUEUE_WAIT_BUDGET_SECONDS,
    SEND_EMAIL_TIMEOUT_SECONDS,
    settings,
)
from app.core.redis import redis_client
from app.models.models import Channel, Proxy
from app.services import connection_manager

logger = logging.getLogger(__name__)
_transport_error_samples: dict[str, tuple[int, float]] = {}
_TRANSPORT_ERROR_SAMPLE_INTERVAL_SECONDS = 60.0

SEND_THREADPOOL_PRESSURE_WARN_INTERVAL_SECONDS = 60.0
SENDER_INDEX_COUNTER_TTL_SECONDS = 60 * 60 * 24 * 365


_httpx_clients_lock = asyncio.Lock()
_httpx_clients: dict[tuple, httpx.AsyncClient] = {}
_httpx_channel_keys: dict[int, tuple] = {}
_httpx_client_fingerprints: dict[tuple, tuple] = {}
_httpx_client_pool_sizes: dict[tuple, int] = {}
_httpx_client_created_at: dict[tuple, float] = {}
_httpx_refs: dict[tuple, int] = {}
_httpx_ref_holders: dict[tuple, set[tuple[str, int]]] = {}
_httpx_client_channels: dict[tuple, set[int]] = {}
_httpx_retired_clients: dict[tuple, list[httpx.AsyncClient]] = {}
_httpx_pool_pressure_last_warn_at: dict[tuple, float] = {}
_send_threadpool: ThreadPoolExecutor | None = None

SENDGRID_HTTPX_POOL_MIN_SIZE = 50
SENDGRID_HTTPX_POOL_MAX_SIZE = 1000
SENDGRID_HTTPX_KEEPALIVE_EXPIRY_SECONDS = connection_manager.SENDGRID_HTTPX_KEEPALIVE_EXPIRY_DIRECT_SECONDS
SENDGRID_HTTPX_POOL_RTT_HEADROOM = 1.2
PROXY_TUNNEL_KEEPALIVE_INTERVAL_SECONDS = connection_manager.PROXY_TUNNEL_KEEPALIVE_INTERVAL_SECONDS
SEND_THREADPOOL_MIN_WORKERS = 128


_send_threadpool_workers = 0


def resolve_send_threadpool_workers(*, min_required: int = 0) -> int:
    """兼容旧 executor 的容量查询；正式 SDK 发送使用任务独立 SenderPool。"""
    return max(SEND_THREADPOOL_MIN_WORKERS, int(min_required or 0))


def _create_send_threadpool(workers: int) -> ThreadPoolExecutor:
    normalized_workers = max(SEND_THREADPOOL_MIN_WORKERS, int(workers or SEND_THREADPOOL_MIN_WORKERS))
    return ThreadPoolExecutor(
        max_workers=normalized_workers,
        thread_name_prefix="mercury-send",
    )


def _retire_sync_channel_client(channel_id: int) -> None:
    """释放通道级同步 SDK 客户端引用（任务结束时调用）。"""
    normalized_id = int(channel_id or 0)
    if normalized_id <= 0:
        return
    logger.debug("sync sendgrid client retired channel_id=%s", normalized_id)


def configure_send_threadpool() -> int:
    global _send_threadpool, _send_threadpool_workers
    workers = resolve_send_threadpool_workers()
    if _send_threadpool is None:
        _send_threadpool = _create_send_threadpool(workers)
        _send_threadpool_workers = workers
        logger.debug("MercuryMail SendGrid 发送线程池已创建 workers=%s", workers)
    return _send_threadpool_workers


def ensure_send_threadpool_capacity(min_required: int) -> int:
    """确保 mercury-send 线程池不小于运行中任务并发总和，避免 HTTP 在 executor 内排队。"""
    global _send_threadpool, _send_threadpool_workers
    target_workers = resolve_send_threadpool_workers(min_required=min_required)
    if _send_threadpool is None:
        _send_threadpool = _create_send_threadpool(target_workers)
        _send_threadpool_workers = target_workers
        logger.debug("MercuryMail SendGrid 发送线程池已创建 workers=%s", target_workers)
        return target_workers
    if _send_threadpool_workers >= target_workers:
        return _send_threadpool_workers
    previous_workers = _send_threadpool_workers
    retiring_pool = _send_threadpool
    _send_threadpool = _create_send_threadpool(target_workers)
    _send_threadpool_workers = target_workers
    retiring_pool.shutdown(wait=False, cancel_futures=False)
    logger.info(
        "MercuryMail SendGrid 发送线程池已扩容 previous_workers=%s new_workers=%s min_required=%s",
        previous_workers,
        target_workers,
        int(min_required or 0),
    )
    return target_workers


def get_send_threadpool_workers() -> int:
    return int(_send_threadpool_workers or 0)


def estimate_send_threadpool_active_workers() -> int:
    pool = _send_threadpool
    if pool is None:
        return 0
    threads = getattr(pool, "_threads", None)
    if not threads:
        return 0
    active_count = 0
    for thread in threads:
        if not getattr(thread, "is_alive", lambda: False)():
            continue
        thread_name = str(getattr(thread, "name", "") or "")
        if thread_name.startswith("mercury-send"):
            active_count += 1
    return active_count


_send_threadpool_pressure_samples: dict[str, tuple[int, float]] = {}


def maybe_warn_send_threadpool_pressure(worker_concurrency: int | None) -> None:
    threshold = max(1, int(worker_concurrency or 0)) + 10
    active_threads = estimate_send_threadpool_active_workers()
    if active_threads <= threshold:
        return
    sample_key = "send_threadpool_pressure"
    now_value = time.monotonic()
    suppressed_count, last_logged_at = _send_threadpool_pressure_samples.get(sample_key, (0, 0.0))
    suppressed_count += 1
    if now_value - last_logged_at < SEND_THREADPOOL_PRESSURE_WARN_INTERVAL_SECONDS:
        _send_threadpool_pressure_samples[sample_key] = (suppressed_count, last_logged_at)
        return
    if suppressed_count > 1:
        logger.warning(
            "mercury-send 线程池压力偏高 active_threads=%s threshold=%s worker_concurrency=%s pool_workers=%s | suppressed_similar=%s",
            active_threads,
            threshold,
            int(worker_concurrency or 0),
            get_send_threadpool_workers(),
            suppressed_count,
        )
    else:
        logger.warning(
            "mercury-send 线程池压力偏高 active_threads=%s threshold=%s worker_concurrency=%s pool_workers=%s",
            active_threads,
            threshold,
            int(worker_concurrency or 0),
            get_send_threadpool_workers(),
        )
    _send_threadpool_pressure_samples[sample_key] = (0, now_value)


def shutdown_send_threadpool() -> None:
    global _send_threadpool, _send_threadpool_workers
    if _send_threadpool is not None:
        _send_threadpool.shutdown(wait=False, cancel_futures=True)
        _send_threadpool = None
        _send_threadpool_workers = 0
        logger.debug("MercuryMail SendGrid 发送线程池已关闭")


def _get_send_threadpool() -> ThreadPoolExecutor:
    pool = _send_threadpool
    if pool is None:
        configure_send_threadpool()
        pool = _send_threadpool
        if pool is None:
            workers = resolve_send_threadpool_workers()
            pool = _create_send_threadpool(workers)
    return pool


def get_estimated_proxy_rtt_seconds() -> float:
    return max(0.1, float(settings.estimated_proxy_rtt_seconds))


def recommend_task_concurrency(
    rate_limit: float,
    estimated_rtt: float | None = None,
    send_http_batch_size: int | None = None,
) -> int:
    from app.utils.concurrency import calculate_recommended_concurrency

    rate = max(0.0, float(rate_limit or 0))
    if rate <= 0:
        return SENDGRID_HTTPX_POOL_MIN_SIZE
    rtt = float(estimated_rtt if estimated_rtt is not None else get_estimated_proxy_rtt_seconds())
    batch = max(1, int(send_http_batch_size or settings.default_http_batch_size))
    latency_ms = int(round(max(0.001, rtt) * 1000))
    return calculate_recommended_concurrency(rate, latency_ms, batch)


def build_concurrency_warning(
    concurrency: int | None,
    rate_limit: float,
    estimated_rtt: float | None = None,
    send_http_batch_size: int | None = None,
) -> str | None:
    from app.utils.concurrency import calculate_recommended_concurrency

    rate = max(0.0, float(rate_limit or 0))
    if rate <= 0:
        return None
    rtt = float(estimated_rtt if estimated_rtt is not None else get_estimated_proxy_rtt_seconds())
    batch = max(1, int(send_http_batch_size or settings.default_http_batch_size))
    latency_ms = int(round(max(0.001, rtt) * 1000))
    recommended = calculate_recommended_concurrency(rate, latency_ms, batch)
    normalized = max(1, int(concurrency or 1))
    threshold = max(1, int(recommended * 0.8))
    if normalized >= threshold:
        return None
    http_rps = rate / float(batch)
    return (
        f"当前并发数 {normalized} 低于推荐值 {recommended}（80% 阈值为 {threshold}；"
        f"HTTP 频率 {http_rps:g} 次/秒 × RTT {rtt:g} 秒 × {SENDGRID_HTTPX_POOL_RTT_HEADROOM:g}），"
        f"可能无法达到目标发送速率 {rate:g} 封/秒"
    )


def _resolve_sendgrid_httpx_pool_limits(
    worker_concurrency: int | None = None,
    *,
    rate_limit: float = 0.0,
    estimated_rtt: float | None = None,
    use_proxy: bool = False,
) -> httpx.Limits:
    concurrency = max(1, int(worker_concurrency or 0))
    rate = max(0.0, float(rate_limit or 0))
    rate_based = 0
    if rate > 0:
        rtt = float(estimated_rtt if estimated_rtt is not None else get_estimated_proxy_rtt_seconds())
        rate_based = int(rate * rtt * SENDGRID_HTTPX_POOL_RTT_HEADROOM)
    pool_size = max(SENDGRID_HTTPX_POOL_MIN_SIZE, concurrency * 2, rate_based)
    pool_size = min(pool_size, SENDGRID_HTTPX_POOL_MAX_SIZE)
    keepalive_expiry = connection_manager.resolve_sendgrid_keepalive_expiry(use_proxy=use_proxy)
    return httpx.Limits(
        max_connections=pool_size,
        max_keepalive_connections=pool_size,
        keepalive_expiry=keepalive_expiry,
    )


def _required_pool_size(worker_concurrency: int | None = None, rate_limit: float = 0.0) -> int:
    return int(
        _resolve_sendgrid_httpx_pool_limits(worker_concurrency, rate_limit=rate_limit).max_connections
        or SENDGRID_HTTPX_POOL_MIN_SIZE
    )


def _log_sampled_transport_warning(sample_key: str, message: str, *args) -> None:
    now_value = time.monotonic()
    suppressed_count, last_logged_at = _transport_error_samples.get(sample_key, (0, 0.0))
    suppressed_count += 1
    if now_value - last_logged_at >= _TRANSPORT_ERROR_SAMPLE_INTERVAL_SECONDS:
        if suppressed_count > 1:
            logger.warning(message + " | suppressed_similar=%s", *args, suppressed_count)
        else:
            logger.warning(message, *args)
        _transport_error_samples[sample_key] = (0, now_value)
    else:
        _transport_error_samples[sample_key] = (suppressed_count, last_logged_at)


def _inspect_httpx_connection_count(client: httpx.AsyncClient) -> int | None:
    transport = getattr(client, "_transport", None)
    pool = getattr(transport, "_pool", None)
    connections = getattr(pool, "_connections", None)
    if connections is None:
        return None
    try:
        return len(connections)
    except TypeError:
        return None


async def _maybe_warn_httpx_pool_pressure(client_key: tuple, client: httpx.AsyncClient, *, channel_id: int, pool_size: int) -> None:
    if pool_size <= 0:
        return
    connection_count = _inspect_httpx_connection_count(client)
    if connection_count is None:
        return
    usage_ratio = float(connection_count) / float(pool_size)
    if usage_ratio < 0.8:
        return
    now_value = time.monotonic()
    last_warn_at = float(_httpx_pool_pressure_last_warn_at.get(client_key, 0.0) or 0.0)
    if now_value - last_warn_at < 60.0:
        return
    _httpx_pool_pressure_last_warn_at[client_key] = now_value
    logger.warning(
        "通道 %s httpx 连接池使用率过高 usage=%.1f%% connections=%s pool_size=%s，正在尝试回收空闲连接；若同时出现 Errno 24，请检查容器与系统 nofile 限制",
        channel_id,
        usage_ratio * 100.0,
        connection_count,
        pool_size,
    )
    refs = int(_httpx_refs.get(client_key, 0) or 0)
    if refs > 0:
        logger.warning("通道 %s httpx 连接池仍被 %s 个任务持有，跳过主动关闭以避免中断发送", channel_id, refs)
        return
    await force_close_httpx_client(channel_id)


def _sender_index_key(channel_id: int) -> str:
    return f"sender_index:{channel_id}"


def parse_sender_emails(channel: Channel, *, active_only: bool = False) -> list[dict[str, str]]:
    try:
        parsed = json.loads(channel.sender_emails_json or "[]")
    except json.JSONDecodeError:
        parsed = []
    senders: list[dict[str, str]] = []
    for item in parsed:
        if isinstance(item, dict):
            if active_only and item.get("enabled") is False:
                continue
            name = str(item.get("name") or "").strip()
            email = str(item.get("email") or "").strip()
            if email:
                senders.append({"name": name, "email": email})
    if senders:
        return senders
    if active_only:
        return []
    fallback_email = (channel.sender_email or "").strip()
    if fallback_email:
        return [{"name": channel.sender_name or "MercuryMail", "email": fallback_email}]
    return []


def count_active_senders(channel: Channel) -> int:
    return len(parse_sender_emails(channel, active_only=True))


def should_rotate_senders(channel: Channel, *, rotation_enabled: bool) -> bool:
    """仅在任务/养号启用轮询且通道有多个启用发件人时走 Redis 轮询。"""
    return bool(rotation_enabled) and count_active_senders(channel) > 1


def resolve_default_sender(channel: Channel) -> dict[str, str]:
    """返回通道默认发件人：第一个启用项，或通道主发件人。"""
    active_senders = parse_sender_emails(channel, active_only=True)
    if active_senders:
        return active_senders[0]
    channel_id = int(getattr(channel, "id", 0) or 0)
    fallback_email = str(getattr(channel, "sender_email", "") or "").strip()
    fallback_name = str(getattr(channel, "sender_name", "") or "MercuryMail").strip() or "MercuryMail"
    if fallback_email:
        logger.warning(
            "通道 %s 无启用发件人列表，使用主发件人 %s",
            channel_id or "未知",
            fallback_email,
        )
        return {"name": fallback_name, "email": fallback_email}
    logger.warning(
        "通道 %s 未配置发件人，将依赖 SendGrid 账号默认发件人",
        channel_id or "未知",
    )
    return {"name": fallback_name, "email": ""}


async def pick_next_sender(channel: Channel, *, rotation_enabled: bool = False) -> dict[str, str]:
    active_senders = parse_sender_emails(channel, active_only=True)
    if not active_senders:
        return resolve_default_sender(channel)
    channel_id = int(getattr(channel, "id", 0) or 0)
    if not should_rotate_senders(channel, rotation_enabled=rotation_enabled):
        return active_senders[0]
    counter_key = _sender_index_key(channel_id)
    # 生产环境将 Redis 视为硬依赖：多实例部署下进程内计数无法保证发件人轮询一致。
    try:
        counter = int(await redis_client.incr(counter_key))
        if counter == 1 or counter % 128 == 0:
            await redis_client.expire(counter_key, SENDER_INDEX_COUNTER_TTL_SECONDS)
        index = (counter - 1) % len(active_senders)
    except Exception as exc:
        logger.error(
            "pick_next_sender redis unavailable channel_id=%s error_type=%s error=%s",
            channel_id,
            type(exc).__name__,
            exc,
        )
        raise RuntimeError(f"发件人轮询依赖 Redis，当前不可用：{exc}") from exc
    sender = active_senders[index]
    logger.debug(
        "pick_next_sender channel_id=%s index=%s total=%s from_email=%s rotation_enabled=%s",
        channel_id,
        index,
        len(active_senders),
        sender.get("email", ""),
        rotation_enabled,
    )
    return sender


async def batch_pick_next_sender(channel: Channel, *, rotation_enabled: bool = False) -> dict[str, str]:
    """按 claim/监控批次选取发件人，一次 Redis INCR 供本批次所有邮件复用。"""
    return await pick_next_sender(channel, rotation_enabled=rotation_enabled)


async def peek_last_sender(channel: Channel, *, rotation_enabled: bool = False) -> dict[str, str] | None:
    active_senders = parse_sender_emails(channel, active_only=True)
    if not active_senders:
        default_sender = resolve_default_sender(channel)
        return default_sender if str(default_sender.get("email") or "").strip() else None
    channel_id = int(getattr(channel, "id", 0) or 0)
    if not should_rotate_senders(channel, rotation_enabled=rotation_enabled):
        return active_senders[0]
    counter_key = _sender_index_key(channel_id)
    counter = 0
    try:
        raw_counter = await redis_client.get(counter_key)
        counter = int(raw_counter or 0)
    except Exception as exc:
        logger.error(
            "peek_last_sender redis unavailable channel_id=%s error_type=%s error=%s",
            channel_id,
            type(exc).__name__,
            exc,
        )
        raise RuntimeError(f"发件人轮询依赖 Redis，当前不可用：{exc}") from exc
    if counter <= 0:
        return active_senders[0]
    return active_senders[(counter - 1) % len(active_senders)]


def _build_proxies(channel: Channel) -> dict[str, str] | None:
    try:
        proxies = connection_manager.get_channel_proxies(channel)
    except Exception as exc:
        logger.exception("构建代理失败 channel_id=%s error_type=%s error=%s", getattr(channel, "id", ""), type(exc).__name__, exc)
        raise ValueError(f"通道代理不可用，发送中止：{exc}") from exc
    if bool(getattr(channel, "use_proxy", False)) and not proxies:
        logger.debug("build proxies empty channel_id=%s use_proxy=True proxy_id=%s", getattr(channel, "id", ""), getattr(channel, "proxy_id", None))
        raise ValueError("通道代理不可用，发送中止：代理配置为空")
    if bool(getattr(channel, "use_proxy", False)):
        proxy = getattr(channel, "proxy", None)
        if proxy is None:
            logger.debug("build proxies failed channel_id=%s reason=proxy_not_loaded proxy_id=%s", getattr(channel, "id", ""), getattr(channel, "proxy_id", None))
            raise ValueError("通道代理不可用，发送中止：代理对象加载失败")
        if not bool(getattr(proxy, "is_active", False)):
            logger.debug("build proxies failed channel_id=%s reason=proxy_inactive proxy_id=%s", getattr(channel, "id", ""), getattr(proxy, "id", None))
            raise ValueError("通道代理不可用，发送中止：代理未启用")
        for scheme in ("http", "https"):
            if not str(proxies.get(scheme) or "").strip():
                logger.debug("build proxies failed channel_id=%s reason=missing_scheme scheme=%s proxy_id=%s", getattr(channel, "id", ""), scheme, getattr(proxy, "id", None))
                raise ValueError(f"通道代理不可用，发送中止：{scheme} 代理地址缺失")
    masked_proxies = {scheme: mask_proxy_url_for_log(url) for scheme, url in (proxies or {}).items()}
    logger.debug("build proxies channel_id=%s use_proxy=%s final_proxy=%s", getattr(channel, "id", ""), bool(getattr(channel, "use_proxy", False)), masked_proxies or "直连")
    return proxies


def mask_proxy_url_for_log(proxy_url: str | None) -> str:
    value = str(proxy_url or "").strip()
    if not value:
        return "直连"
    if "://" not in value:
        return value
    scheme, rest = value.split("://", 1)
    if "@" in rest:
        rest = rest.split("@", 1)[1]
        return f"{scheme}://***@{rest}"
    return value


def mask_proxy_for_log(proxy: Proxy | None) -> str:
    if proxy is None:
        return "direct"
    scheme = connection_manager.proxy_scheme(proxy)
    host = str(proxy.host or "").strip()
    port = str(proxy.port or "").strip()
    endpoint = f"{host}:{port}" if port else host
    if not endpoint:
        return "proxy"
    if proxy.username or proxy.password:
        return f"{scheme}://***@{endpoint}"
    return endpoint


def _channel_proxy_fingerprint(channel: Channel) -> tuple:
    if not bool(getattr(channel, "use_proxy", False)):
        return ("direct", int(getattr(channel, "proxy_id", 0) or 0))
    return ("proxy", int(getattr(channel, "proxy_id", 0) or 0), connection_manager.channel_proxy_fingerprint(channel))


def _shared_httpx_client_key(channel: Channel) -> tuple:
    api_key_hash = connection_manager.api_key_hash(str(getattr(channel, "api_key", "") or ""))
    return (api_key_hash, _channel_proxy_fingerprint(channel))


def _proxy_url_from_channel(channel: Channel) -> str | None:
    proxies = _build_proxies(channel)
    if not proxies:
        return None
    return str(proxies.get("https") or proxies.get("http") or "").strip() or None


async def _create_httpx_client(
    channel: Channel,
    worker_concurrency: int | None = None,
    rate_limit: float = 0.0,
) -> httpx.AsyncClient:
    api_key = (channel.api_key or "").strip()
    if not api_key:
        raise ValueError("SendGrid API Key 不能为空")
    proxy_url = _proxy_url_from_channel(channel) if bool(getattr(channel, "use_proxy", False)) else None
    if bool(getattr(channel, "use_proxy", False)) and not proxy_url:
        raise ValueError("代理未配置，发送中止")
    use_proxy = bool(getattr(channel, "use_proxy", False))
    # DNS：httpx==0.27 无公开 AsyncResolver；同步发送走线程池，异步传输由 httpcore anyio 将
    # getaddrinfo 卸载到线程。升级 httpx>=0.28 后可评估 resolver=httpx.AsyncResolver()。
    # keepalive_expiry 经 resolve_sendgrid_keepalive_expiry 配置，减少重连触发的 DNS 查询。
    pool_limits = _resolve_sendgrid_httpx_pool_limits(
        worker_concurrency,
        rate_limit=rate_limit,
        use_proxy=use_proxy,
    )
    transport = connection_manager.build_sendgrid_async_transport(
        pool_limits=pool_limits,
        proxy_url=proxy_url,
    )
    client = httpx.AsyncClient(
        transport=transport,
        timeout=connection_manager.sendgrid_httpx_timeout(),
        trust_env=False,
        http2=bool(getattr(settings, "enable_sendgrid_http2", True)),
        headers=connection_manager.sendgrid_transport_headers(api_key, for_post=False),
    )
    channel_id = int(getattr(channel, "id", 0) or 0)
    logger.debug(
        "httpx client prepared channel_id=%s use_proxy=%s proxy=%s trust_env=%s pool_max_connections=%s pool_max_keepalive=%s keepalive_expiry=%s",
        getattr(channel, "id", ""),
        bool(getattr(channel, "use_proxy", False)),
        mask_proxy_url_for_log(proxy_url) if proxy_url else "直连",
        False,
        pool_limits.max_connections,
        pool_limits.max_keepalive_connections,
        pool_limits.keepalive_expiry,
    )
    return client


async def _aclose_client_safe(client: httpx.AsyncClient) -> None:
    try:
        await client.aclose()
    except Exception:
        logger.exception("httpx 客户端关闭失败")


def _sync_httpx_ref_count_locked(client_key: tuple) -> int:
    holders = _httpx_ref_holders.get(client_key)
    count = len(holders or set())
    if count > 0:
        _httpx_refs[client_key] = count
    else:
        _httpx_refs.pop(client_key, None)
        _httpx_ref_holders.pop(client_key, None)
    return count


def _add_httpx_ref_holder_locked(client_key: tuple, holder: tuple[str, int]) -> int:
    holders = _httpx_ref_holders.setdefault(client_key, set())
    holders.add(holder)
    return _sync_httpx_ref_count_locked(client_key)


def _remove_httpx_ref_holder_locked(client_key: tuple, holder: tuple[str, int]) -> int:
    holders = _httpx_ref_holders.get(client_key)
    if holders is not None:
        holders.discard(holder)
    return _sync_httpx_ref_count_locked(client_key)


def _add_httpx_channel_locked(client_key: tuple, channel_id: int) -> None:
    normalized_id = int(channel_id or 0)
    if normalized_id > 0:
        _httpx_client_channels.setdefault(client_key, set()).add(normalized_id)
        _httpx_channel_keys[normalized_id] = client_key


def _remove_httpx_channel_locked(client_key: tuple, channel_id: int) -> int:
    normalized_id = int(channel_id or 0)
    channels = _httpx_client_channels.get(client_key)
    if channels is not None and normalized_id > 0:
        channels.discard(normalized_id)
        if not channels:
            _httpx_client_channels.pop(client_key, None)
    if normalized_id > 0 and _httpx_channel_keys.get(normalized_id) == client_key:
        _httpx_channel_keys.pop(normalized_id, None)
    return len(_httpx_client_channels.get(client_key) or set())


def _client_key_has_live_refs_locked(client_key: tuple) -> bool:
    return bool(_httpx_ref_holders.get(client_key)) or bool(_httpx_client_channels.get(client_key))


def _close_client_key_if_unused_locked(client_key: tuple, pending_close: list[httpx.AsyncClient]) -> bool:
    if _client_key_has_live_refs_locked(client_key):
        return False
    client = _httpx_clients.pop(client_key, None)
    _httpx_client_fingerprints.pop(client_key, None)
    _httpx_client_pool_sizes.pop(client_key, None)
    _httpx_client_created_at.pop(client_key, None)
    _httpx_pool_pressure_last_warn_at.pop(client_key, None)
    _httpx_refs.pop(client_key, None)
    _httpx_ref_holders.pop(client_key, None)
    _httpx_client_channels.pop(client_key, None)
    if client is not None:
        pending_close.append(client)
    pending_close.extend(_httpx_retired_clients.pop(client_key, []))
    return client is not None


def _retire_shared_client_locked(client_key: tuple, pending_close: list[httpx.AsyncClient]) -> int:
    client = _httpx_clients.pop(client_key, None)
    inherited_refs = _sync_httpx_ref_count_locked(client_key)
    _httpx_client_fingerprints.pop(client_key, None)
    _httpx_client_pool_sizes.pop(client_key, None)
    _httpx_client_created_at.pop(client_key, None)
    _httpx_pool_pressure_last_warn_at.pop(client_key, None)
    _httpx_client_channels.pop(client_key, None)
    for channel_id, mapped_key in list(_httpx_channel_keys.items()):
        if mapped_key == client_key:
            _httpx_channel_keys.pop(channel_id, None)
    if client is None:
        return 0
    if inherited_refs > 0:
        _httpx_retired_clients.setdefault(client_key, []).append(client)
    else:
        pending_close.append(client)
    return inherited_refs


async def get_httpx_client(
    channel: Channel,
    worker_concurrency: int | None = None,
    rate_limit: float = 0.0,
    *,
    owner_task_id: int | None = None,
) -> httpx.AsyncClient:
    channel_id = int(getattr(channel, "id", 0) or 0)
    fingerprint = _channel_proxy_fingerprint(channel)
    client_key = _shared_httpx_client_key(channel)
    holder_id = int(owner_task_id if owner_task_id is not None else channel_id or id(channel))
    holder = ("task", holder_id) if owner_task_id is not None else ("channel", holder_id)
    from app.services.task_runner import resolve_channel_httpx_pool_concurrency

    effective_workers = resolve_channel_httpx_pool_concurrency(channel_id, worker_concurrency)
    required_pool = _required_pool_size(effective_workers, rate_limit=rate_limit)
    single_task_workers = max(1, int(worker_concurrency or 0))
    if effective_workers > single_task_workers:
        logger.warning(
            "通道 %s httpx 连接池按同 API Key/代理聚合并发扩容 task_concurrency=%s channel_aggregated=%s required_pool=%s",
            channel_id,
            single_task_workers,
            effective_workers,
            required_pool,
        )
    if channel_id <= 0:
        return await _create_httpx_client(channel, worker_concurrency=effective_workers, rate_limit=rate_limit)

    pending_close: list[httpx.AsyncClient] = []
    cached_client: httpx.AsyncClient | None = None
    async with _httpx_clients_lock:
        mapped_key = _httpx_channel_keys.get(channel_id)
        if mapped_key is not None and mapped_key != client_key:
            _remove_httpx_ref_holder_locked(mapped_key, holder)
            _remove_httpx_channel_locked(mapped_key, channel_id)
            _close_client_key_if_unused_locked(mapped_key, pending_close)
        if client_key in _httpx_clients and _httpx_client_fingerprints.get(client_key) == fingerprint:
            existing_pool = int(_httpx_client_pool_sizes.get(client_key, 0))
            if existing_pool >= required_pool:
                _add_httpx_ref_holder_locked(client_key, holder)
                _add_httpx_channel_locked(client_key, channel_id)
                cached_client = _httpx_clients[client_key]
            else:
                logger.warning(
                    "通道 %s httpx 连接池容量不足，准备平滑扩容 existing_pool=%s required_pool=%s effective_workers=%s",
                    channel_id,
                    existing_pool,
                    required_pool,
                    effective_workers,
                )
    if cached_client is not None:
        for stale_client in pending_close:
            await _aclose_client_safe(stale_client)
        await _maybe_warn_httpx_pool_pressure(
            client_key,
            cached_client,
            channel_id=channel_id,
            pool_size=int(_httpx_client_pool_sizes.get(client_key, required_pool) or required_pool),
        )
        return cached_client

    new_client = await _create_httpx_client(
        channel,
        worker_concurrency=max(required_pool, effective_workers),
        rate_limit=rate_limit,
    )
    async with _httpx_clients_lock:
        if client_key in _httpx_clients and _httpx_client_fingerprints.get(client_key) == fingerprint:
            _add_httpx_ref_holder_locked(client_key, holder)
            _httpx_channel_keys[channel_id] = client_key
            pending_close.append(new_client)
            client_to_return = _httpx_clients[client_key]
        else:
            if client_key in _httpx_clients:
                _retire_shared_client_locked(client_key, pending_close)
            _httpx_clients[client_key] = new_client
            _httpx_client_fingerprints[client_key] = fingerprint
            _httpx_client_pool_sizes[client_key] = int(max(required_pool, effective_workers, SENDGRID_HTTPX_POOL_MIN_SIZE))
            _httpx_client_created_at[client_key] = time.monotonic()
            _add_httpx_ref_holder_locked(client_key, holder)
            _add_httpx_channel_locked(client_key, channel_id)
            client_to_return = new_client
    for stale_client in pending_close:
        await _aclose_client_safe(stale_client)
    await _maybe_warn_httpx_pool_pressure(
        client_key,
        client_to_return,
        channel_id=channel_id,
        pool_size=int(_httpx_client_pool_sizes.get(client_key, required_pool) or required_pool),
    )
    return client_to_return


async def _borrow_httpx_client(channel: Channel) -> httpx.AsyncClient:
    channel_id = int(getattr(channel, "id", 0) or 0)
    fingerprint = _channel_proxy_fingerprint(channel)
    client_key = _httpx_channel_keys.get(channel_id) or _shared_httpx_client_key(channel)
    async with _httpx_clients_lock:
        client = _httpx_clients.get(client_key)
        if client is not None:
            if _httpx_client_fingerprints.get(client_key) != fingerprint:
                logger.warning(
                    "borrow httpx client fingerprint mismatch channel_id=%s cached=%s current=%s",
                    channel_id,
                    _httpx_client_fingerprints.get(client_key),
                    fingerprint,
                )
            return client
    raise RuntimeError(f"通道 {channel_id} 的 httpx 客户端尚未初始化，请先获取客户端引用")


async def reload_httpx_client(
    channel: Channel,
    worker_concurrency: int | None = None,
    rate_limit: float = 0.0,
    *,
    owner_task_id: int | None = None,
) -> None:
    channel_id = int(getattr(channel, "id", 0) or 0)
    if channel_id <= 0:
        return
    await force_close_httpx_client(channel_id, owner_task_id=owner_task_id)
    await get_httpx_client(channel, worker_concurrency=worker_concurrency, rate_limit=rate_limit, owner_task_id=owner_task_id)
    logger.debug("httpx client reloaded channel_id=%s shared_key=%s", channel_id, _shared_httpx_client_key(channel))


async def release_httpx_client(channel_id: int, *, owner_task_id: int | None = None) -> None:
    normalized_id = int(channel_id or 0)
    if normalized_id <= 0:
        return
    holder_id = int(owner_task_id if owner_task_id is not None else normalized_id)
    holder = ("task", holder_id) if owner_task_id is not None else ("channel", holder_id)
    clients_to_close: list[httpx.AsyncClient] = []
    async with _httpx_clients_lock:
        client_key = _httpx_channel_keys.get(normalized_id)
        if client_key is None:
            return
        _remove_httpx_ref_holder_locked(client_key, holder)
        _remove_httpx_channel_locked(client_key, normalized_id)
        _close_client_key_if_unused_locked(client_key, clients_to_close)
    _retire_sync_channel_client(normalized_id)
    for client in clients_to_close:
        await _aclose_client_safe(client)


async def close_httpx_client(channel_id: int) -> None:
    await release_httpx_client(channel_id)


async def force_close_httpx_client(channel_id: int, *, owner_task_id: int | None = None, force_all: bool = False) -> None:
    normalized_id = int(channel_id or 0)
    if normalized_id <= 0:
        return
    holder_id = int(owner_task_id if owner_task_id is not None else normalized_id)
    holder = ("task", holder_id) if owner_task_id is not None else ("channel", holder_id)
    clients_to_close: list[httpx.AsyncClient] = []
    async with _httpx_clients_lock:
        client_key = _httpx_channel_keys.get(normalized_id)
        if client_key is None:
            return
        if force_all:
            _retire_shared_client_locked(client_key, clients_to_close)
        else:
            _remove_httpx_ref_holder_locked(client_key, holder)
            _remove_httpx_channel_locked(client_key, normalized_id)
            _close_client_key_if_unused_locked(client_key, clients_to_close)
    _retire_sync_channel_client(normalized_id)
    for client in clients_to_close:
        await _aclose_client_safe(client)


async def close_all_httpx_clients() -> None:
    clients_to_close: list[httpx.AsyncClient] = []
    async with _httpx_clients_lock:
        clients_to_close.extend(_httpx_clients.values())
        _httpx_clients.clear()
        _httpx_channel_keys.clear()
        _httpx_client_fingerprints.clear()
        _httpx_client_pool_sizes.clear()
        _httpx_client_created_at.clear()
        _httpx_pool_pressure_last_warn_at.clear()
        _httpx_refs.clear()
        _httpx_ref_holders.clear()
        _httpx_client_channels.clear()
        for retired_clients in _httpx_retired_clients.values():
            clients_to_close.extend(retired_clients)
        _httpx_retired_clients.clear()
    for client in clients_to_close:
        await _aclose_client_safe(client)

@asynccontextmanager
async def _ephemeral_httpx_client(channel: Channel, worker_concurrency: int | None = None):
    client = await _create_httpx_client(channel, worker_concurrency=worker_concurrency)
    try:
        yield client
    finally:
        await _aclose_client_safe(client)


def _send_batch_mail_sync(manager, mail, *, invalidate_reason: str = "batch_send_error"):
    from app.services.sendgrid_sdk import send_batch_mail_sync

    client, connection_reused = manager.acquire_client_for_send()
    try:
        result = send_batch_mail_sync(client, mail)
        result.connection_reused = connection_reused
        # 单个批次的代理/网络失败只影响本批，不立即关闭通道级 SDK 客户端。
        # 多任务共用同一通道时，立即失效会让其他健康任务跟着重建/失败，形成级联故障。
        return result
    except Exception:
        manager.invalidate_client_if_current(client, reason=invalidate_reason)
        raise
    finally:
        manager.release_client_after_send(client)


async def recycle_idle_httpx_clients(*, max_age_seconds: float = 1800.0) -> int:
    """定期回收超过 max_age_seconds 的 httpx 客户端，重建连接池以防半死连接。"""
    from app.core.database import AsyncSessionLocal
    from app.models.models import Channel
    from sqlalchemy.orm import selectinload

    now_value = time.monotonic()
    candidates: list[tuple] = []
    async with _httpx_clients_lock:
        for client_key, created_at in list(_httpx_client_created_at.items()):
            refs = int(_httpx_refs.get(client_key, 0))
            if refs > 0:
                continue
            if (now_value - float(created_at or 0)) >= float(max_age_seconds):
                candidates.append(client_key)

    recycled = 0
    if not candidates:
        return 0

    clients_to_close: list[httpx.AsyncClient] = []
    async with _httpx_clients_lock:
        for client_key in candidates:
            client = _httpx_clients.pop(client_key, None)
            _httpx_client_fingerprints.pop(client_key, None)
            _httpx_client_pool_sizes.pop(client_key, None)
            _httpx_client_created_at.pop(client_key, None)
            _httpx_pool_pressure_last_warn_at.pop(client_key, None)
            _httpx_refs.pop(client_key, None)
            for channel_id, mapped_key in list(_httpx_channel_keys.items()):
                if mapped_key == client_key:
                    _httpx_channel_keys.pop(channel_id, None)
            if client is not None:
                clients_to_close.append(client)
                recycled += 1
    for client in clients_to_close:
        await _aclose_client_safe(client)
    if recycled:
        logger.info("httpx 共享客户端已定期回收 count=%s", recycled)
    return recycled


async def get_sender_pool_stats() -> dict[str, int | float]:
    from app.services.sender_pool import aggregate_sender_pool_stats

    try:
        return aggregate_sender_pool_stats()
    except Exception:
        return {"workers": 0, "alive_workers": 0, "queue_depth": 0, "inflight": 0, "dropped": 0}


async def send_batch_mail_async(manager, mail):
    """经任务独立 SenderPool 执行 SendGrid SDK 同步批量发送。"""
    from app.services.sendgrid_sdk import SendBatchResult
    from app.services.sender_pool import DeliveryUnknownError, SenderQueueTimeoutError, SENDER_POOL_MAX_WORKERS, ensure_sender_pool_capacity, get_sender_pool
    from app.services.task_runner import update_sender_pool_runtime_metrics

    effective = max(1, min(int(getattr(manager, "effective_concurrency", 1) or 1), SENDER_POOL_MAX_WORKERS))
    owner_task_id = int(getattr(manager, "owner_task_id", 0) or 0)
    queue_budget = float(SEND_EMAIL_QUEUE_WAIT_BUDGET_SECONDS)
    http_budget = float(SEND_EMAIL_HTTP_BUDGET_SECONDS)
    total_budget = float(SEND_EMAIL_TIMEOUT_SECONDS)
    pool = get_sender_pool(owner_task_id, workers=effective)
    queued_started_at = time.monotonic()
    ensure_sender_pool_capacity(effective, task_id=owner_task_id)
    update_sender_pool_runtime_metrics(owner_task_id, pool.aggregate_stats(), queue_wait_ms=0.0)
    observed_queue_wait_ms = 0.0

    def observe_queue_wait(value: float) -> None:
        nonlocal observed_queue_wait_ms
        observed_queue_wait_ms = max(0.0, float(value or 0.0))

    try:
        result = await pool.run_sync(
            _send_batch_mail_sync,
            manager,
            mail,
            timeout=http_budget,
            queue_timeout=queue_budget,
            queue_wait_observer=observe_queue_wait,
        )
        queue_wait_ms = round(observed_queue_wait_ms, 2)
        update_sender_pool_runtime_metrics(owner_task_id, pool.aggregate_stats(), queue_wait_ms=queue_wait_ms)
        if not result.success:
            logger.warning(
                "SendGrid SDK 批量发送返回失败 status_code=%s body=%s",
                result.status_code,
                str(result.body or "")[:500],
            )
        return result
    except SenderQueueTimeoutError as exc:
        queue_wait_ms = round((time.monotonic() - queued_started_at) * 1000, 2)
        pool_stats = pool.aggregate_stats()
        update_sender_pool_runtime_metrics(owner_task_id, pool_stats, queue_wait_ms=queue_wait_ms, timed_out=True)
        logger.warning(
            "SendGrid SDK 队列等待超时 task_id=%s total_budget=%ss queue_budget=%ss http_budget=%ss queue_wait_ms=%s pool=%s",
            owner_task_id,
            round(total_budget, 2),
            round(queue_budget, 2),
            round(http_budget, 2),
            queue_wait_ms,
            pool_stats,
        )
        raise RuntimeError(f"SendGrid SDK 队列等待超时（总预算 {int(total_budget)} 秒，其中队列等待 {int(queue_budget)} 秒、HTTP 请求 {int(http_budget)} 秒）") from exc
    except DeliveryUnknownError as exc:
        queue_wait_ms = round((time.monotonic() - queued_started_at) * 1000, 2)
        pool_stats = pool.aggregate_stats()
        update_sender_pool_runtime_metrics(owner_task_id, pool_stats, queue_wait_ms=queue_wait_ms, timed_out=True)
        manager.invalidate_client(reason="batch_send_timeout")
        logger.warning(
            "SendGrid SDK HTTP 请求超时 task_id=%s total_budget=%ss queue_budget=%ss http_budget=%ss queue_wait_ms=%s pool=%s delivery_unknown=%s，投递状态未知，禁止自动重试",
            owner_task_id,
            round(total_budget, 2),
            round(queue_budget, 2),
            round(http_budget, 2),
            queue_wait_ms,
            pool_stats,
            pool_stats.get("delivery_unknown_active", 0),
        )
        raise


async def proxy_tunnel_keepalive_loop() -> None:
    while True:
        await asyncio.sleep(PROXY_TUNNEL_KEEPALIVE_INTERVAL_SECONDS)
        async with _httpx_clients_lock:
            channel_items = [
                (client_key, client, _httpx_client_fingerprints.get(client_key))
                for client_key, client in _httpx_clients.items()
            ]
        for channel_id, client, fingerprint in channel_items:
            if fingerprint is None or fingerprint[0] != "proxy":
                continue
            try:
                response = await client.get(connection_manager.SENDGRID_SCOPES_URL)
                if 200 <= int(response.status_code or 0) < 300:
                    logger.debug("proxy tunnel keepalive ok channel_id=%s", channel_id)
                else:
                    logger.debug(
                        "proxy tunnel keepalive non-2xx channel_id=%s status_code=%s",
                        channel_id,
                        response.status_code,
                    )
            except Exception as exc:
                logger.debug(
                    "proxy tunnel keepalive failed channel_id=%s error_type=%s error=%s",
                    channel_id,
                    type(exc).__name__,
                    exc,
                )


async def close_http_clients() -> None:
    await close_all_httpx_clients()


async def _warmup_channel_proxy_tunnel(channel: Channel, client: httpx.AsyncClient) -> bool:
    api_key = (channel.api_key or "").strip()
    if not api_key:
        return False
    try:
        response = await client.get(
            connection_manager.SENDGRID_SCOPES_URL,
            headers=connection_manager.sendgrid_transport_headers(api_key, for_post=False),
        )
        status_code = int(response.status_code or 0)
        if 200 <= status_code < 300:
            logger.debug("channel proxy tunnel warmup ok channel_id=%s", getattr(channel, "id", ""))
            return True
        logger.debug(
            "channel proxy tunnel warmup non-2xx channel_id=%s status_code=%s",
            getattr(channel, "id", ""),
            status_code,
        )
        return False
    except Exception as exc:
        logger.debug(
            "channel proxy tunnel warmup failed channel_id=%s error_type=%s error=%s",
            getattr(channel, "id", ""),
            type(exc).__name__,
            exc,
        )
        return False


async def warmup_active_channel_httpx_clients() -> int:
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from app.core.database import AsyncSessionLocal
    from app.models.models import Channel

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Channel)
            .options(selectinload(Channel.proxy))
            .where(Channel.is_active.is_(True), Channel.enabled.is_(True))
        )
        channels = list(result.scalars().all())
    warmed = 0
    for channel in channels:
        channel_id = int(getattr(channel, "id", 0) or 0)
        if channel_id <= 0:
            continue
        try:
            client = await get_httpx_client(channel, worker_concurrency=SENDGRID_HTTPX_POOL_MIN_SIZE)
            if bool(getattr(channel, "use_proxy", False)):
                await _warmup_channel_proxy_tunnel(channel, client)
            warmed += 1
            logger.debug("warmup httpx client cached channel_id=%s", channel_id)
        except Exception:
            logger.warning("启动预热 httpx 客户端失败 channel_id=%s", channel_id, exc_info=True)
    logger.warning(
        "MercuryMail httpx 连接池预热完成 active_channels=%s warmed=%s keepalive_expiry=%s",
        len(channels),
        warmed,
        SENDGRID_HTTPX_KEEPALIVE_EXPIRY_SECONDS,
    )
    return warmed
