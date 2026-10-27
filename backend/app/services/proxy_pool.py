import time
from dataclasses import dataclass

import httpx

from app.services.connection_manager import (
    build_proxy_url,
    sendgrid_httpx_limits,
    sendgrid_test_timeout,
    sendgrid_transport_headers,
)

SENDGRID_SCOPES_URL = "https://api.sendgrid.com/v3/scopes"


@dataclass
class ProxyTestResult:
    success: bool
    message: str
    delay_ms: float


async def test_proxy(proxy, api_key: str | None = None) -> ProxyTestResult:
    started = time.perf_counter()
    normalized_key = (api_key or "").strip()
    request_headers = sendgrid_transport_headers(normalized_key, for_post=False)
    try:
        async with httpx.AsyncClient(
            proxy=build_proxy_url(proxy),
            timeout=sendgrid_test_timeout(),
            trust_env=False,
            limits=sendgrid_httpx_limits(),
            http2=False,
            headers=request_headers,
        ) as client:
            response = await client.get(SENDGRID_SCOPES_URL, headers=request_headers)
        delay_ms = (time.perf_counter() - started) * 1000
        if normalized_key:
            success = 200 <= response.status_code < 300
            message = (
                f"代理可连通 SendGrid API，API Key 有效，状态码 {response.status_code}"
                if success
                else f"代理可连通 SendGrid API，但 API Key 或权限校验失败，状态码 {response.status_code}"
            )
        else:
            success = response.status_code in {401, 403} or 200 <= response.status_code < 500
            message = f"代理可连通 SendGrid API，状态码 {response.status_code}"
        return ProxyTestResult(success=success, message=message, delay_ms=round(delay_ms, 2))
    except httpx.HTTPError as error:
        delay_ms = (time.perf_counter() - started) * 1000
        return ProxyTestResult(success=False, message=str(error), delay_ms=round(delay_ms, 2))
