import hashlib
import importlib.util
import logging
import time
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.models import Channel

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SendGridScopesProbeResult:
    success: bool
    status_code: int
    body: str
    latency_ms: int
    proxy_used: bool
    error_message: str = ""


SENDGRID_USER_AGENT = "sendgrid/6.11.0;python"

SENDGRID_TEST_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
SENDGRID_LATENCY_PROBE_TIMEOUT = httpx.Timeout(connect=5.0, read=5.0, write=5.0, pool=5.0)
SENDGRID_SCOPES_URL = "https://api.sendgrid.com/v3/scopes"
SENDGRID_HEALTH_PROBE_CLIENT_TTL_SECONDS = 1800.0
SENDGRID_HTTPX_CONNECT_TIMEOUT = 10.0
SENDGRID_HTTPX_READ_TIMEOUT = 25.0
SENDGRID_HTTPX_WRITE_TIMEOUT = 25.0
SENDGRID_HTTPX_POOL_TIMEOUT = 15.0
SENDGRID_HTTPX_KEEPALIVE_EXPIRY_DIRECT_SECONDS = 120.0
SENDGRID_HTTPX_KEEPALIVE_EXPIRY_PROXY_SECONDS = 120.0
SENDGRID_SDK_CLIENT_TIMEOUT_SECONDS = 45.0
SENDGRID_SDK_POOL_MIN_CONNECTIONS = 100
SENDGRID_SDK_POOL_MIN_KEEPALIVE = 50
SENDGRID_TRANSPORT_RETRIES = 0
PROXY_TUNNEL_KEEPALIVE_INTERVAL_SECONDS = 60.0


def api_key_hash(api_key: str) -> str:
    normalized = str(api_key or "").strip()
    if not normalized:
        return "empty"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]


def sendgrid_transport_headers(api_key: str, *, for_post: bool = False) -> dict[str, str]:
    headers = {
        "User-Agent": SENDGRID_USER_AGENT,
        "Accept": "application/json",
        "Accept-Encoding": "identity",
    }
    normalized_key = (api_key or "").strip()
    if normalized_key:
        headers["Authorization"] = f"Bearer {normalized_key}"
    if for_post:
        headers["Content-Type"] = "application/json"
    return headers


def sendgrid_httpx_limits() -> httpx.Limits:
    return httpx.Limits(max_connections=1, max_keepalive_connections=0)


def sendgrid_httpx_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=SENDGRID_HTTPX_CONNECT_TIMEOUT,
        read=SENDGRID_HTTPX_READ_TIMEOUT,
        write=SENDGRID_HTTPX_WRITE_TIMEOUT,
        pool=SENDGRID_HTTPX_POOL_TIMEOUT,
    )


def resolve_sendgrid_keepalive_expiry(*, use_proxy: bool) -> float:
    if use_proxy:
        return SENDGRID_HTTPX_KEEPALIVE_EXPIRY_PROXY_SECONDS
    return SENDGRID_HTTPX_KEEPALIVE_EXPIRY_DIRECT_SECONDS


def normalize_sendgrid_sdk_effective_concurrency(effective_concurrency: int | None) -> int:
    return max(1, int(effective_concurrency or 1))


def resolve_sendgrid_sdk_pool_limits(
    effective_concurrency: int | None,
    *,
    keepalive_expiry: float | None = None,
) -> httpx.Limits:
    """根据有效并发数计算 SendGrid SDK httpx 连接池上限。"""
    normalized = normalize_sendgrid_sdk_effective_concurrency(effective_concurrency)
    limits_kwargs: dict[str, int | float] = {
        "max_connections": max(SENDGRID_SDK_POOL_MIN_CONNECTIONS, normalized * 2),
        "max_keepalive_connections": max(SENDGRID_SDK_POOL_MIN_KEEPALIVE, normalized),
    }
    if keepalive_expiry is not None:
        limits_kwargs["keepalive_expiry"] = float(keepalive_expiry)
    return httpx.Limits(**limits_kwargs)


def sendgrid_sdk_client_timeout() -> httpx.Timeout:
    return httpx.Timeout(SENDGRID_SDK_CLIENT_TIMEOUT_SECONDS, connect=SENDGRID_HTTPX_CONNECT_TIMEOUT)


def resolve_sendgrid_sdk_http2_enabled() -> bool:
    """检测 h2 依赖是否可用；不可用时静默回退 HTTP/1.1。"""
    if not bool(getattr(settings, "enable_sendgrid_http2", True)):
        return False
    return importlib.util.find_spec("h2") is not None


def log_sendgrid_sdk_pool_state(
    *,
    effective_concurrency: int,
    pool_limits: httpx.Limits,
    http2_enabled: bool,
    use_proxy: bool,
    channel_id: int | str | None = None,
    reused: bool | None = None,
    client_id: int | None = None,
) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    logger.debug(
        "SendGrid SDK 连接池 channel_id=%s effective_concurrency=%s max_connections=%s "
        "max_keepalive=%s keepalive_expiry=%s http2=%s proxy=%s reused=%s client_id=%s",
        channel_id if channel_id is not None else "",
        effective_concurrency,
        pool_limits.max_connections,
        pool_limits.max_keepalive_connections,
        getattr(pool_limits, "keepalive_expiry", None),
        http2_enabled,
        "是" if use_proxy else "否",
        reused,
        client_id,
    )


def build_sendgrid_async_transport(
    *,
    pool_limits: httpx.Limits,
    proxy_url: str | None,
) -> httpx.AsyncHTTPTransport:
    # httpx==0.27（requirements.txt 锁定）无公开 AsyncResolver；httpcore anyio 后端将
    # getaddrinfo 卸载到线程。若升级 httpx>=0.28，可评估 AsyncResolver 进一步降低 DNS 延迟。
    return httpx.AsyncHTTPTransport(
        limits=pool_limits,
        trust_env=False,
        proxy=proxy_url,
        http2=False,
        retries=SENDGRID_TRANSPORT_RETRIES,
    )


def build_sendgrid_sync_transport(
    *,
    pool_limits: httpx.Limits,
    proxy_url: str | None,
    http2: bool = False,
) -> httpx.HTTPTransport:
    return httpx.HTTPTransport(
        limits=pool_limits,
        trust_env=False,
        proxy=proxy_url,
        http2=bool(http2),
        retries=SENDGRID_TRANSPORT_RETRIES,
    )


def sendgrid_test_timeout() -> httpx.Timeout:
    return SENDGRID_TEST_TIMEOUT


def mask_api_key(api_key: str) -> str:
    normalized = (api_key or "").strip()
    if len(normalized) <= 10:
        return "***"
    return f"{normalized[:6]}...{normalized[-4:]}"


def is_masked_api_key(api_key: str) -> bool:
    normalized = (api_key or "").strip()
    if not normalized or normalized == "***":
        return True
    return "..." in normalized and len(normalized) >= 14


def proxy_scheme(proxy) -> str:
    return "socks5" if str(proxy.type or proxy.protocol or "HTTP").upper() == "SOCKS5" else "http"


def build_proxy_url(proxy) -> str:
    """构建 HTTP/SOCKS5 代理 URL，供 httpx 与 SendGrid SDK 模块复用。"""
    scheme = proxy_scheme(proxy)
    host = str(getattr(proxy, "host", "") or "").strip()
    port = int(getattr(proxy, "port", 0) or 0)
    if not host:
        raise ValueError("代理地址不能为空")
    if port <= 0:
        raise ValueError("代理端口不能为空")
    auth = ""
    username = str(getattr(proxy, "username", "") or "").strip()
    password = str(getattr(proxy, "password", "") or "")
    if username:
        auth = quote(username, safe="")
        if password:
            auth = f"{auth}:{quote(password, safe='')}"
        auth = f"{auth}@"
    return f"{scheme}://{auth}{host}:{port}"


def validate_ai_proxy_url(proxy_url: str | None) -> str:
    """校验 AI 出站代理地址，支持 http/https/socks5。"""
    normalized = str(proxy_url or "").strip()
    if not normalized:
        raise ValueError("AI 出站代理地址不能为空")
    lowered = normalized.lower()
    if lowered.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return normalized
    raise ValueError("AI 出站代理地址必须以 http://、https:// 或 socks5:// 开头")


def proxy_fingerprint(proxy) -> tuple[str, str, int, str, str]:
    return (
        proxy_scheme(proxy),
        str(getattr(proxy, "host", "") or "").strip(),
        int(getattr(proxy, "port", 0) or 0),
        str(getattr(proxy, "username", "") or "").strip(),
        str(getattr(proxy, "password", "") or "").strip(),
    )


def channel_proxy_fingerprint(channel) -> tuple | None:
    if not bool(getattr(channel, "use_proxy", False)):
        return None
    proxy = getattr(channel, "proxy", None)
    if proxy is None or not bool(getattr(proxy, "is_active", False)):
        return ("unavailable", int(getattr(channel, "proxy_id", 0) or 0))
    return proxy_fingerprint(proxy)


def build_channel_proxy_url(channel, *, strict: bool = False) -> str | None:
    if not bool(getattr(channel, "use_proxy", False)):
        return None
    proxy = getattr(channel, "proxy", None)
    if proxy is None or not bool(proxy.is_active):
        if strict:
            raise ValueError("代理未配置，发送中止")
        return None
    return build_proxy_url(proxy)


def build_httpx_proxy_url(channel, *, strict: bool = False) -> str | None:
    return build_channel_proxy_url(channel, strict=strict)


def mask_channel_proxy_info(channel) -> str:
    if not bool(getattr(channel, "use_proxy", False)):
        return "直连"
    proxy = getattr(channel, "proxy", None)
    if proxy is None:
        return "直连"
    proxy_url = build_proxy_url(proxy)
    if "@" in proxy_url:
        scheme, rest = proxy_url.split("://", 1)
        return f"{scheme}://***@{rest.split('@', 1)[1]}"
    return proxy_url


async def probe_proxy_outbound_ip(proxy_url: str) -> str | None:
    """仅用于通道连接测试，发送链路禁止调用。

    与 test_sendgrid_scopes 相同：经通道代理 URL 创建 httpx.AsyncClient（trust_env=False）。
    若代理无法访问 ipify，出口 IP 将留空，不影响发送。
    """
    normalized_url = str(proxy_url or "").strip()
    if not normalized_url:
        return None
    try:
        async with httpx.AsyncClient(
            proxy=normalized_url,
            timeout=httpx.Timeout(5.0),
            trust_env=False,
        ) as client:
            response = await client.get("https://api.ipify.org?format=json")
            response.raise_for_status()
            payload = response.json()
        ip_value = str(payload.get("ip") or "").strip()
        if ip_value and len(ip_value) <= 45:
            return ip_value
    except Exception as exc:
        logger.warning("代理出口 IP 探测失败：%s", exc)
    return None


async def resolve_outbound_proxy_url(proxy_id: int | None) -> str | None:
    """加载 AI 出站代理 URL；proxy_id 为空时返回 None（直连）。"""
    if proxy_id is None:
        return None
    from app.core.database import AsyncSessionLocal
    from app.models.models import Proxy

    normalized_id = int(proxy_id or 0)
    if normalized_id <= 0:
        return None
    async with AsyncSessionLocal() as db:
        proxy = await db.get(Proxy, normalized_id)
    if proxy is None:
        raise ValueError("AI 出站代理不存在，请重新选择")
    if not bool(getattr(proxy, "is_active", False)):
        raise ValueError("AI 出站代理未启用，请更换代理或启用后再试")
    return build_proxy_url(proxy)


def get_channel_proxies(channel) -> dict[str, str] | None:
    if not bool(getattr(channel, "use_proxy", False)):
        return None
    proxy_url = build_channel_proxy_url(channel, strict=True)
    if not str(proxy_url or "").strip():
        raise ValueError("代理未配置，发送中止")
    return {"http": proxy_url, "https": proxy_url}


def warn_if_socks_dependencies_missing() -> None:
    missing = []
    if importlib.util.find_spec("socksio") is None:
        missing.append("httpx[socks] / socksio")
    if missing:
        logger.warning("SOCKS5 代理依赖不可用：%s；如需使用 SOCKS5 代理，请安装 httpx[socks]", "，".join(missing))


async def validate_sendgrid_api_key_proxy_consistency(channel) -> str | None:
    api_key = (getattr(channel, "api_key", "") or "").strip()
    if not api_key:
        return "SendGrid API Key 不能为空"
    result_use_proxy = bool(getattr(channel, "use_proxy", False))
    result_proxy_id = int(getattr(channel, "proxy_id", 0) or 0) if result_use_proxy else 0
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Channel)
            .options(selectinload(Channel.proxy))
            .where(Channel.api_key == api_key, Channel.enabled.is_(True), Channel.is_active.is_(True))
        )
        rows = list(result.scalars().all())
    expected = channel_proxy_fingerprint(channel)
    for item in rows:
        actual = channel_proxy_fingerprint(item)
        if actual != expected:
            masked_key = mask_api_key(api_key)
            logger.warning(
                "sendgrid api key proxy consistency failed key=%s base_channel_id=%s base_use_proxy=%s base_proxy_id=%s conflict_channel_id=%s conflict_use_proxy=%s conflict_proxy_id=%s",
                masked_key,
                getattr(channel, "id", ""),
                bool(expected),
                expected or "direct",
                item.id,
                bool(actual),
                actual or "direct",
            )
            return f"同一 SendGrid API Key（{masked_key}）存在启用通道代理配置不一致，请统一代理设置后再操作"
    return None


def sendgrid_latency_probe_timeout() -> httpx.Timeout:
    return SENDGRID_LATENCY_PROBE_TIMEOUT


def map_sendgrid_probe_error(exc: Exception, *, latency_context: bool = False) -> str:
    if isinstance(exc, httpx.ProxyError):
        return "代理连接失败，请检查代理地址、端口、账号或密码"
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout)):
        return "连接测试超时，请检查代理或网络"
    if isinstance(exc, httpx.ConnectError):
        return "无法连接 SendGrid，请检查网络或代理配置"
    if isinstance(exc, httpx.HTTPError):
        return "延迟检测失败，请检查 API Key、网络或代理配置" if latency_context else "连接测试失败，请检查 API Key、网络或代理配置"
    return f"检测异常：{exc}"


def _health_probe_client_key(channel, api_key: str, proxy_url: str | None) -> tuple:
    return (
        api_key_hash(api_key),
        channel_proxy_fingerprint(channel),
        str(proxy_url or ""),
    )


_health_probe_clients: dict[tuple, tuple[httpx.AsyncClient, float]] = {}


async def _get_sendgrid_health_probe_client(
    channel,
    *,
    api_key: str,
    proxy_url: str | None,
) -> httpx.AsyncClient:
    now_value = time.monotonic()
    client_key = _health_probe_client_key(channel, api_key, proxy_url)
    cached = _health_probe_clients.get(client_key)
    if cached is not None:
        client, created_at = cached
        if not client.is_closed and now_value - created_at < SENDGRID_HEALTH_PROBE_CLIENT_TTL_SECONDS:
            return client
        await client.aclose()
        _health_probe_clients.pop(client_key, None)
    transport = build_sendgrid_async_transport(
        pool_limits=resolve_sendgrid_sdk_pool_limits(
            1,
            keepalive_expiry=resolve_sendgrid_keepalive_expiry(use_proxy=bool(proxy_url)),
        ),
        proxy_url=proxy_url,
    )
    client = httpx.AsyncClient(
        transport=transport,
        timeout=sendgrid_test_timeout(),
        trust_env=False,
        http2=False,
        headers=sendgrid_transport_headers(api_key, for_post=False),
    )
    _health_probe_clients[client_key] = (client, now_value)
    return client


async def probe_sendgrid_scopes(channel) -> SendGridScopesProbeResult:
    api_key = (getattr(channel, "api_key", "") or "").strip()
    proxy_used = bool(getattr(channel, "use_proxy", False))
    if not api_key:
        return SendGridScopesProbeResult(False, 0, "SendGrid API Key 不能为空", 0, proxy_used, "SendGrid API Key 不能为空")
    consistency_error = await validate_sendgrid_api_key_proxy_consistency(channel)
    if consistency_error:
        return SendGridScopesProbeResult(False, 0, consistency_error, 0, proxy_used, consistency_error)
    try:
        proxy_url = build_httpx_proxy_url(channel, strict=proxy_used)
    except ValueError as exc:
        error = str(exc)
        return SendGridScopesProbeResult(False, 0, error, 0, proxy_used, error)
    if proxy_used and not proxy_url:
        error = "通道启用了代理但代理配置无效，无法检测连接"
        return SendGridScopesProbeResult(False, 0, error, 0, proxy_used, error)
    client = await _get_sendgrid_health_probe_client(channel, api_key=api_key, proxy_url=proxy_url)
    for attempt in range(2):
        try:
            started_at = time.perf_counter()
            response = await client.get(SENDGRID_SCOPES_URL, timeout=sendgrid_test_timeout())
            latency_ms = max(1, int(round((time.perf_counter() - started_at) * 1000)))
            status_code = int(response.status_code or 0)
            success = 200 <= status_code < 300
            body = str(response.text or "")
            if status_code in (401, 403) and attempt == 0:
                # 代理/WAF 可能在陈旧隧道上生成伪 401/403。健康展示必须经全新连接复核，
                # 但复核结果仍只用于观测，不能参与 Mail Send 准入。
                await client.aclose()
                _health_probe_clients.pop(_health_probe_client_key(channel, api_key, proxy_url), None)
                client = await _get_sendgrid_health_probe_client(channel, api_key=api_key, proxy_url=proxy_url)
                continue
            error_message = "" if success else "SendGrid 返回异常响应，请检查 API Key 或代理配置"
            return SendGridScopesProbeResult(success, status_code, body, latency_ms, proxy_used, error_message)
        except Exception as exc:
            # 健康客户端的长连接可能已被代理单侧回收。首次网络异常只淘汰探测客户端，
            # 使用全新连接复核一次；该客户端与发送 SDK 完全独立，不得失效发送客户端。
            if attempt == 0 and isinstance(exc, httpx.TransportError):
                await client.aclose()
                _health_probe_clients.pop(_health_probe_client_key(channel, api_key, proxy_url), None)
                client = await _get_sendgrid_health_probe_client(channel, api_key=api_key, proxy_url=proxy_url)
                continue
            error = map_sendgrid_probe_error(exc)
            return SendGridScopesProbeResult(False, 0, error, 0, proxy_used, error)
    return SendGridScopesProbeResult(False, 0, "连接测试失败", 0, proxy_used, "连接测试失败")


async def measure_channel_proxy_latency(channel) -> tuple[bool, int, str, bool]:
    result = await probe_sendgrid_scopes(channel)
    if result.success:
        return True, result.latency_ms, "", result.proxy_used
    return False, result.latency_ms, result.error_message or result.body or "延迟检测失败，请检查 API Key、网络或代理配置", result.proxy_used


async def test_sendgrid_scopes(channel) -> tuple[bool, int, str]:
    result = await probe_sendgrid_scopes(channel)
    return result.success, result.status_code, result.body or result.error_message


async def close_all_clients():
    from app.services.channel_manager import close_all_httpx_clients

    await close_all_httpx_clients()
    for client, _created_at in list(_health_probe_clients.values()):
        await client.aclose()
    _health_probe_clients.clear()
