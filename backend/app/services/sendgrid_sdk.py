"""SendGrid 官方 Python SDK 集成：代理注入、客户端生命周期与批量发送同步入口。

所有邮件发送均经官方 sendgrid 包的 SendGridAPIClient.send(Mail) 完成；
httpx 仅作为 SDK 底层传输层注入代理（trust_env=False），禁止绕过 SDK 直接请求 /v3/mail/send。
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from email.message import Message
from typing import Any

import httpx
import python_http_client
from python_http_client.client import Client as PythonHttpClient
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content, CustomArg, Email, Header, Mail, Personalization, Substitution, To

from app.core.config import SEND_EMAIL_HTTP_BUDGET_SECONDS
from app.models.models import Channel
from app.services.template_service import StaticMailContent
from app.services.sender_pool import DeliveryUnknownError
from app.services import connection_manager
from app.utils.errors import httpx_transport_user_message, sanitize_user_error_message
from app.utils.sendgrid_errors import is_sendgrid_messaging_limit_error, normalize_send_log_error, sanitize_send_log_secrets

logger = logging.getLogger(__name__)

SENDGRID_SDK_HOST = "https://api.sendgrid.com"
SENDGRID_SDK_TIMEOUT_SECONDS = float(SEND_EMAIL_HTTP_BUDGET_SECONDS)
SYNC_SENDGRID_CLIENT_REBUILD_SECONDS = 900.0
SYNC_SENDGRID_CLIENT_KEEPALIVE_INTERVAL_SECONDS = 60.0
SYNC_SENDGRID_IDLE_PROBE_SECONDS = 60.0
SYNC_SENDGRID_CLIENT_LOCK_TIMEOUT_SECONDS = 10.0
SYNC_SENDGRID_CLIENT_BUILD_WARN_MS = 5000.0
SYNC_SENDGRID_GET_CLIENT_SLOW_MS = 5000.0

_PERSONALIZATION_EMAIL_PATTERN = re.compile(
    r"personalizations\[(\d+)\](?:\.to\[(\d+)\])?(?:\.email)?",
    re.IGNORECASE,
)


@dataclass
class SendBatchResult:
    success: bool
    status_code: int
    body: str
    retry_after: str | None = None
    provider_message_id: str = ""
    response_headers: dict[str, str] = field(default_factory=dict)
    per_recipient_errors: dict[str, str] = field(default_factory=dict)
    per_recipient_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    delay_ms: float = 0.0
    connection_reused: bool | None = None
    response_origin: str = "unknown"
    sendgrid_error_type: str = ""


def classify_sendgrid_response_origin(headers: dict[str, str] | None, body: str = "") -> str:
    normalized_headers = {str(k or "").lower(): str(v or "") for k, v in (headers or {}).items()}
    server = normalized_headers.get("server", "").lower()
    via = normalized_headers.get("via", "").lower()
    request_id = normalized_headers.get("x-message-id") or normalized_headers.get("x-request-id") or normalized_headers.get("x-envoy-upstream-service-time")
    body_text = str(body or "").lower()
    if "sendgrid" in server or request_id:
        return "sendgrid"
    if "cloudflare" in server or "cloudflare" in body_text:
        return "cloudflare"
    # SendGrid's canonical error body is authoritative even when a proxy adds
    # Via/Server headers. Otherwise a proxy can turn an account response into
    # a false proxy-auth classification.
    if "authenticated user is not authorized to send mail" in body_text or "sendgrid" in body_text or '"errors"' in body_text:
        return "sendgrid"
    if "nginx" in server or "squid" in server or "proxy" in via or "proxy" in body_text:
        return "proxy"
    return "unknown"


def classify_sendgrid_auth_error(status_code: int, body: str, headers: dict[str, str] | None = None) -> str:
    status = int(status_code or 0)
    text = str(body or "").strip().lower()
    origin = classify_sendgrid_response_origin(headers, body)
    if status not in (401, 403):
        return ""
    if origin not in {"sendgrid", "unknown"}:
        return f"{origin}_auth_response"
    if "mail.send" in text or "mail send" in text or "access forbidden" in text or "forbidden" in text and "scope" in text:
        return "missing_mail_send_scope"
    if "verified sender" in text or "sender identity" in text or "from address" in text or "does not match a verified" in text:
        return "sender_identity_forbidden"
    if "api key" in text and ("invalid" in text or "expired" in text or "revoked" in text):
        return "api_key_invalid"
    if "authorization required" in text or "unauthorized" in text or "authorization" in text and "denied" in text:
        return "api_key_unauthorized"
    if status == 401:
        return "auth_401_unknown"
    return "send_forbidden_unknown"


class _UrllibResponseAdapter:
    """将 httpx.Response 适配为 python_http_client.Response 所需的 urllib 接口。"""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self._body = response.content

    def getcode(self) -> int:
        return int(self._response.status_code)

    def read(self) -> bytes:
        return self._body

    def info(self) -> Message:
        message = Message()
        for key, value in self._response.headers.items():
            message[key] = value
        return message


class HttpxBackedPythonHttpClient(PythonHttpClient):
    """通过 httpx 执行 HTTP 请求，支持 HTTP/SOCKS5 代理且 trust_env=False。"""

    def __init__(
        self,
        host: str,
        request_headers: dict[str, str] | None = None,
        *,
        version: int | None = 3,
        url_path: list[str] | None = None,
        append_slash: bool = False,
        proxy_url: str | None = None,
        timeout: float = SENDGRID_SDK_TIMEOUT_SECONDS,
        httpx_client: httpx.Client | None = None,
    ) -> None:
        super().__init__(
            host,
            request_headers=request_headers,
            version=version,
            url_path=url_path,
            append_slash=append_slash,
            timeout=timeout,
        )
        self._proxy_url = (proxy_url or "").strip() or None
        self._owns_httpx_client = httpx_client is None
        if httpx_client is not None:
            self._httpx_client = httpx_client
        else:
            self._httpx_client = _build_sdk_httpx_client(
                proxy_url=self._proxy_url,
                effective_concurrency=1,
            )

    def _build_client(self, name=None):
        url_path = self._url_path + [name] if name else self._url_path
        return HttpxBackedPythonHttpClient(
            host=self.host,
            request_headers=self.request_headers,
            version=self._version,
            url_path=url_path,
            append_slash=self.append_slash,
            proxy_url=self._proxy_url,
            timeout=self.timeout or SENDGRID_SDK_TIMEOUT_SECONDS,
            httpx_client=self._httpx_client,
        )

    @property
    def httpx_client(self) -> httpx.Client:
        return self._httpx_client

    def close(self) -> None:
        if self._owns_httpx_client:
            try:
                self._httpx_client.close()
            except Exception:
                logger.exception("SendGrid SDK httpx 客户端关闭失败")

    def _make_request(self, opener, request, timeout=None):  # noqa: ARG002
        timeout_value = float(timeout or self.timeout or SENDGRID_SDK_TIMEOUT_SECONDS)
        method = request.get_method()
        url = request.get_full_url()
        headers = {key: value for key, value in request.header_items()}
        content = request.data
        try:
            response = self._httpx_client.request(
                method,
                url,
                headers=headers,
                content=content,
                timeout=timeout_value,
            )
        except httpx.TimeoutException as exc:
            raise DeliveryUnknownError(
                f"SendGrid HTTP 请求超时（超过 {timeout_value:.1f} 秒），未收到响应，投递状态未知",
                future_id=0,
            ) from exc
        except httpx.HTTPError as exc:
            if _httpx_error_has_ambiguous_delivery(exc):
                raise DeliveryUnknownError(
                    "SendGrid HTTP 请求发送后连接异常，未收到完整响应，投递状态未知，需人工确认，禁止自动重试",
                    future_id=0,
                ) from exc
            raise _translate_httpx_error(exc) from exc
        return _UrllibResponseAdapter(response)


class ProxySendGridAPIClient(SendGridAPIClient):
    """SendGrid API 客户端：底层 python_http_client 走 httpx 以支持代理。"""

    def __init__(
        self,
        api_key: str,
        *,
        proxy_url: str | None = None,
        timeout: float = SENDGRID_SDK_TIMEOUT_SECONDS,
        effective_concurrency: int = 1,
        httpx_client: httpx.Client | None = None,
    ) -> None:
        normalized_key = (api_key or "").strip()
        if not normalized_key:
            raise ValueError("SendGrid API Key 不能为空")
        super().__init__(api_key=normalized_key, host=SENDGRID_SDK_HOST)
        self._proxy_url = (proxy_url or "").strip() or None
        self._timeout = float(timeout)
        self._effective_concurrency = connection_manager.normalize_sendgrid_sdk_effective_concurrency(
            effective_concurrency
        )
        if httpx_client is None:
            httpx_client = _build_sdk_httpx_client(
                proxy_url=self._proxy_url,
                effective_concurrency=self._effective_concurrency,
                channel_id=None,
            )
        self._http_client = HttpxBackedPythonHttpClient(
            host=self.host,
            request_headers=self._default_headers,
            version=3,
            proxy_url=self._proxy_url,
            timeout=self._timeout,
            httpx_client=httpx_client,
        )
        self.client = self._http_client

    @property
    def httpx_transport(self) -> httpx.Client:
        return self._http_client.httpx_client


def _build_sdk_httpx_client(
    *,
    proxy_url: str | None,
    effective_concurrency: int = 1,
    channel_id: int | str | None = None,
) -> httpx.Client:
    use_proxy = bool((proxy_url or "").strip())
    normalized_concurrency = connection_manager.normalize_sendgrid_sdk_effective_concurrency(effective_concurrency)
    keepalive_expiry = connection_manager.resolve_sendgrid_keepalive_expiry(use_proxy=use_proxy)
    pool_limits = connection_manager.resolve_sendgrid_sdk_pool_limits(
        normalized_concurrency,
        keepalive_expiry=keepalive_expiry,
    )
    http2_enabled = connection_manager.resolve_sendgrid_sdk_http2_enabled()
    normalized_timeout = connection_manager.sendgrid_sdk_client_timeout()
    transport = connection_manager.build_sendgrid_sync_transport(
        pool_limits=pool_limits,
        proxy_url=(proxy_url or "").strip() or None,
        http2=http2_enabled,
    )
    client_kwargs: dict[str, object] = {
        "timeout": normalized_timeout,
        "trust_env": False,
        "limits": pool_limits,
        "transport": transport,
    }
    if http2_enabled:
        client_kwargs["http2"] = True
    try:
        client = httpx.Client(**client_kwargs)
    except TypeError:
        client_kwargs.pop("http2", None)
        client = httpx.Client(**client_kwargs)
        http2_enabled = False
    connection_manager.log_sendgrid_sdk_pool_state(
        effective_concurrency=normalized_concurrency,
        pool_limits=pool_limits,
        http2_enabled=http2_enabled,
        use_proxy=use_proxy,
        channel_id=channel_id,
        reused=False,
        client_id=id(client),
    )
    return client


def _httpx_error_has_ambiguous_delivery(exc: httpx.HTTPError) -> bool:
    """POST 写出后未收到完整响应时，不能断言请求未到达 SendGrid。"""
    return isinstance(
        exc,
        (
            httpx.ReadError,
            httpx.WriteError,
            httpx.RemoteProtocolError,
        ),
    )


def _translate_httpx_error(exc: httpx.HTTPError) -> python_http_client.exceptions.HTTPError:
    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        body = (response.text or "").encode("utf-8")
        headers = dict(response.headers.items())
        status_code = int(response.status_code or 0)
        error_cls = python_http_client.exceptions.err_dict.get(status_code, python_http_client.exceptions.HTTPError)
        return error_cls(status_code, response.reason_phrase or "HTTP 错误", body, headers)
    message = httpx_transport_user_message(exc)
    body = message.encode("utf-8")
    return python_http_client.exceptions.HTTPError(0, "传输错误", body, {})


def _resolve_channel_proxy_url(channel: Channel) -> str | None:
    if not bool(getattr(channel, "use_proxy", False)):
        return None
    return connection_manager.build_channel_proxy_url(channel, strict=True)


def _channel_proxy_fingerprint(channel: Channel) -> tuple:
    if not bool(getattr(channel, "use_proxy", False)):
        return ("direct", int(getattr(channel, "proxy_id", 0) or 0))
    return (
        "proxy",
        int(getattr(channel, "proxy_id", 0) or 0),
        connection_manager.channel_proxy_fingerprint(channel),
    )


def create_client(
    channel: Channel,
    *,
    timeout: float = SENDGRID_SDK_TIMEOUT_SECONDS,
    effective_concurrency: int = 1,
) -> ProxySendGridAPIClient:
    """根据通道配置创建 SendGrid SDK 客户端；启用代理时强制经 httpx 注入代理 URL。"""
    api_key = (getattr(channel, "api_key", "") or "").strip()
    if not api_key:
        raise ValueError("SendGrid API Key 不能为空")
    proxy_url = _resolve_channel_proxy_url(channel) if bool(getattr(channel, "use_proxy", False)) else None
    if bool(getattr(channel, "use_proxy", False)) and not proxy_url:
        raise ValueError("代理未配置，发送中止")
    normalized_concurrency = connection_manager.normalize_sendgrid_sdk_effective_concurrency(effective_concurrency)
    client = ProxySendGridAPIClient(
        api_key=api_key,
        proxy_url=proxy_url,
        timeout=timeout,
        effective_concurrency=normalized_concurrency,
    )
    if proxy_url and getattr(channel, "proxy", None) is not None:
        masked = connection_manager.build_proxy_url(channel.proxy)
        if "@" in masked:
            scheme, rest = masked.split("://", 1)
            masked_proxy = f"{scheme}://***@{rest.split('@', 1)[1]}"
        else:
            masked_proxy = masked
    else:
        masked_proxy = "直连"
    logger.debug(
        "SendGrid SDK client created channel_id=%s use_proxy=%s proxy=%s effective_concurrency=%s client_id=%s",
        getattr(channel, "id", ""),
        bool(getattr(channel, "use_proxy", False)),
        masked_proxy if proxy_url else "直连",
        normalized_concurrency,
        id(client.httpx_transport),
    )
    return client


def close_client(client: SendGridAPIClient | None) -> None:
    if client is None:
        return
    http_client = getattr(client, "_http_client", None)
    if isinstance(http_client, HttpxBackedPythonHttpClient):
        http_client.close()
        return
    if isinstance(client, ProxySendGridAPIClient):
        client._http_client.close()
        return
    transport = getattr(client, "httpx_transport", None)
    if isinstance(transport, httpx.Client):
        try:
            transport.close()
        except Exception:
            logger.exception("SendGrid SDK httpx 传输层关闭失败")


def _decode_response_body(raw_body: Any) -> str:
    if raw_body is None:
        return ""
    if isinstance(raw_body, bytes):
        return raw_body.decode("utf-8", errors="replace").strip()
    return str(raw_body).strip()


def _headers_to_dict(headers: Any) -> dict[str, str]:
    if headers is None:
        return {}
    if isinstance(headers, dict):
        return {str(key): str(value) for key, value in headers.items()}
    items = getattr(headers, "items", None)
    if callable(items):
        try:
            return {str(key): str(value) for key, value in items()}
        except Exception:
            return {}
    return {}


def _header_value(headers: Any, name: str) -> str:
    normalized_name = str(name or "").strip().lower()
    if not normalized_name:
        return ""
    header_dict = _headers_to_dict(headers)
    for key, value in header_dict.items():
        if str(key or "").strip().lower() == normalized_name and str(value or "").strip():
            return str(value).strip()
    getter = getattr(headers, "get", None)
    if callable(getter):
        for candidate in (name, normalized_name):
            value = getter(candidate)
            if value:
                return str(value).strip()
    return ""


def _extract_retry_after(headers: Any) -> str | None:
    value = _header_value(headers, "Retry-After")
    return value or None


def _extract_provider_message_id(headers: Any) -> str:
    for key in ("X-Message-Id", "x-message-id", "X-Message-ID", "x-message-id"):
        value = _header_value(headers, key)
        if value:
            return value[:255]
    return ""


def _extract_mail_recipient_emails(mail: Mail | dict[str, Any] | None) -> list[str]:
    if mail is None:
        return []
    mail_dict = mail if isinstance(mail, dict) else mail.get()
    personalizations = mail_dict.get("personalizations") if isinstance(mail_dict, dict) else None
    if not isinstance(personalizations, list):
        return []
    emails: list[str] = []
    for personalization in personalizations:
        if not isinstance(personalization, dict):
            continue
        to_items = personalization.get("to") or []
        if not isinstance(to_items, list) or not to_items:
            continue
        first = to_items[0]
        if isinstance(first, dict):
            email = str(first.get("email") or "").strip()
        else:
            email = str(getattr(first, "email", "") or "").strip()
        if email:
            emails.append(email)
    return emails


def build_per_recipient_results(
    mail: Mail | dict[str, Any] | None,
    *,
    success: bool,
    status_code: int,
    body: str,
    per_recipient_errors: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    emails = _extract_mail_recipient_emails(mail)
    normalized_errors = {
        str(email or "").strip().lower(): str(message or "").strip()
        for email, message in (per_recipient_errors or {}).items()
        if str(email or "").strip()
    }
    global_error = normalize_send_log_error(str(body or "").strip() or "邮件发送失败，请稍后重试")
    results: dict[str, dict[str, Any]] = {}
    if success:
        for email in emails:
            results[email] = {"success": True, "status_code": int(status_code), "error": ""}
        return results
    if normalized_errors:
        for email in emails:
            error_message = normalized_errors.get(email.lower(), "")
            if error_message:
                raw_error = sanitize_send_log_secrets(error_message)
                results[email] = {
                    "success": False,
                    "status_code": int(status_code),
                    "error": raw_error,
                }
            else:
                results[email] = {"success": True, "status_code": int(status_code), "error": ""}
        return results
    for email in emails:
        results[email] = {
            "success": False,
            "status_code": int(status_code),
            "error": global_error,
        }
    return results


def build_unsubscribe_headers(unsubscribe_url: str) -> dict[str, str]:
    """构建 List-Unsubscribe 邮件头，与群发发送路径一致。"""
    normalized_url = str(unsubscribe_url or "").strip()
    if not normalized_url:
        return {}
    return {
        "List-Unsubscribe": f"<{normalized_url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def build_batch_mail(
    *,
    from_email: str,
    from_name: str,
    static_content: StaticMailContent,
    recipients_data: list[dict[str, Any]],
    subject_override: str | None = None,
) -> Mail:
    normalized_from_email = str(from_email or "").strip()
    if not normalized_from_email:
        raise ValueError("发件人邮箱不能为空")
    if not recipients_data:
        raise ValueError("批量发送收件人列表不能为空")
    subject = str(subject_override or static_content.subject or "").strip() or "MercuryMail"
    mail = Mail(from_email=Email(normalized_from_email, from_name or ""), subject=subject)
    if static_content.is_html:
        mail.add_content(Content("text/html", static_content.content))
    else:
        mail.add_content(Content("text/plain", static_content.content))
    for item in recipients_data:
        recipient_email = str(item.get("email") or "").strip()
        if not recipient_email:
            continue
        personalization = Personalization()
        personalization.add_to(To(recipient_email))
        custom_args = dict(item.get("custom_args") or {}) if isinstance(item.get("custom_args"), dict) else {}
        substitutions = item.get("substitutions") or {}
        if isinstance(substitutions, dict):
            for key, value in substitutions.items():
                substitution_key = str(key or "").strip()
                if substitution_key:
                    personalization.add_substitution(Substitution(substitution_key, "" if value is None else str(value)))
        headers = item.get("headers") or {}
        if isinstance(headers, dict):
            for header_key, header_value in headers.items():
                normalized_header_key = str(header_key or "").strip()
                if normalized_header_key:
                    personalization.add_header(Header(normalized_header_key, "" if header_value is None else str(header_value)))
        custom_args.setdefault("task_id", str(item.get("task_id") or ""))
        custom_args.setdefault("recipient_id", str(item.get("recipient_id") or ""))
        custom_args.setdefault("send_log_id", str(item.get("send_log_id") or ""))
        custom_args.setdefault("log_type", str(item.get("log_type") or "regular"))
        for arg_key, arg_value in custom_args.items():
            normalized_arg_key = str(arg_key or "").strip()
            if normalized_arg_key:
                personalization.add_custom_arg(
                    CustomArg(normalized_arg_key, "" if arg_value is None else str(arg_value))
                )
        mail.add_personalization(personalization)
    if not mail.personalizations:
        raise ValueError("批量发送未包含有效收件人")
    return mail


def _parse_per_recipient_errors(body: str, mail: Mail | dict[str, Any] | None) -> dict[str, str]:
    errors: dict[str, str] = {}
    if not body:
        return errors
    try:
        payload = json.loads(body)
    except (TypeError, json.JSONDecodeError):
        return errors
    raw_errors = payload.get("errors") if isinstance(payload, dict) else None
    if not isinstance(raw_errors, list):
        return errors
    email_lookup: list[str] = []
    if mail is not None:
        mail_dict = mail if isinstance(mail, dict) else mail.get()
        personalizations = mail_dict.get("personalizations") if isinstance(mail_dict, dict) else None
        if isinstance(personalizations, list):
            for personalization in personalizations:
                if not isinstance(personalization, dict):
                    continue
                to_items = personalization.get("to") or []
                if isinstance(to_items, list) and to_items:
                    first = to_items[0]
                    if isinstance(first, dict):
                        email_lookup.append(str(first.get("email") or "").strip().lower())
                    else:
                        email_lookup.append(str(getattr(first, "email", "") or "").strip().lower())
    for item in raw_errors:
        if not isinstance(item, dict):
            continue
        message = str(item.get("message") or item.get("error") or "SendGrid 返回错误").strip()
        field_name = str(item.get("field") or "").strip()
        email = ""
        match = _PERSONALIZATION_EMAIL_PATTERN.search(field_name)
        if match:
            index = int(match.group(1))
            if 0 <= index < len(email_lookup):
                email = email_lookup[index]
        if email:
            errors[email] = message
    return errors


def _sdk_error_to_message(exc: Exception, *, status_code: int = 0, body: str = "") -> str:
    if isinstance(exc, httpx.HTTPError):
        return httpx_transport_user_message(exc, status_code=status_code, body=body)
    if isinstance(exc, python_http_client.exceptions.HTTPError):
        normalized_status = int(getattr(exc, "status_code", 0) or status_code or 0)
        normalized_body = _decode_response_body(getattr(exc, "body", body))
        if normalized_body:
            body_message = normalize_send_log_error(normalized_body)
            if is_sendgrid_messaging_limit_error(normalized_body):
                return body_message
            if isinstance(exc, (python_http_client.exceptions.UnauthorizedError, python_http_client.exceptions.ForbiddenError)):
                # 401/403 的响应体通常明确说明是 Mail Send scope、Sender Identity 还是账号策略问题；
                # 不能用通用文案覆盖，否则发送层无法给出可操作的诊断。
                return sanitize_send_log_secrets(normalized_body)
            return body_message
        if normalized_status == 0 and normalized_body:
            return normalize_send_log_error(normalized_body)
        if isinstance(exc, python_http_client.exceptions.UnauthorizedError):
            return "SendGrid 认证失败，请检查 API 密钥"
        if isinstance(exc, python_http_client.exceptions.ForbiddenError):
            return "SendGrid 权限不足，请检查 API 密钥权限"
        if isinstance(exc, python_http_client.exceptions.TooManyRequestsError):
            return "SendGrid 请求过于频繁，触发 429 限流，请稍后重试"
        return httpx_transport_user_message(exc, status_code=normalized_status, body=normalized_body)
    return normalize_send_log_error(str(exc) or "邮件发送失败，请稍后重试")


def send_batch_mail_sync(client: SendGridAPIClient, mail: Mail) -> SendBatchResult:
    """同步发送 Mail 对象；设计为在 mercury-send 线程池中调用。"""
    started_at = time.perf_counter()
    mail_dict = mail.get() if hasattr(mail, "get") else mail
    try:
        response = client.send(mail)
        status_code = int(getattr(response, "status_code", 0) or 0)
        body = _decode_response_body(getattr(response, "body", ""))
        response_headers = _headers_to_dict(getattr(response, "headers", None))
        retry_after = _extract_retry_after(response_headers)
        provider_message_id = _extract_provider_message_id(response_headers)
        if not body and 200 <= status_code < 300:
            body = "SendGrid 已接收请求，响应内容为空"
        if provider_message_id and 200 <= status_code < 300:
            body = f"{body}；SendGrid消息ID={provider_message_id}"
        success = 200 <= status_code < 300
        per_recipient_errors = _parse_per_recipient_errors(body, mail_dict) if not success else {}
        per_recipient_results = {} if success else build_per_recipient_results(
            mail_dict,
            success=False,
            status_code=status_code,
            body=body,
            per_recipient_errors=per_recipient_errors,
        )
        delay_ms = round((time.perf_counter() - started_at) * 1000, 2)
        return SendBatchResult(
            success=success,
            status_code=status_code,
            body=body,
            retry_after=retry_after,
            provider_message_id=provider_message_id,
            response_headers=response_headers,
            per_recipient_errors=per_recipient_errors,
            per_recipient_results=per_recipient_results,
            delay_ms=delay_ms,
            response_origin=classify_sendgrid_response_origin(response_headers, body),
            sendgrid_error_type=classify_sendgrid_auth_error(status_code, body, response_headers),
        )
    except python_http_client.exceptions.HTTPError as exc:
        status_code = int(getattr(exc, "status_code", 0) or 0)
        body = _decode_response_body(getattr(exc, "body", ""))
        response_headers = _headers_to_dict(getattr(exc, "headers", None))
        retry_after = _extract_retry_after(response_headers)
        delay_ms = round((time.perf_counter() - started_at) * 1000, 2)
        per_recipient_errors = _parse_per_recipient_errors(body, mail_dict)
        error_body = _sdk_error_to_message(exc, status_code=status_code, body=body)
        response_origin = classify_sendgrid_response_origin(response_headers, body)
        sendgrid_error_type = classify_sendgrid_auth_error(status_code, body or error_body, response_headers)
        if body:
            logger.warning(
                "SendGrid SDK HTTP 错误：status=%s origin=%s error_type=%s 原始响应=%s",
                status_code,
                response_origin,
                sendgrid_error_type or "-",
                body[:500],
            )
        return SendBatchResult(
            success=False,
            status_code=status_code,
            body=error_body,
            retry_after=retry_after,
            response_headers=response_headers,
            per_recipient_errors=per_recipient_errors,
            per_recipient_results=build_per_recipient_results(
                mail_dict,
                success=False,
                status_code=status_code,
                body=error_body,
                per_recipient_errors=per_recipient_errors,
            ),
            delay_ms=delay_ms,
            response_origin=response_origin,
            sendgrid_error_type=sendgrid_error_type,
        )
    except httpx.HTTPError as exc:
        delay_ms = round((time.perf_counter() - started_at) * 1000, 2)
        error_body = _sdk_error_to_message(exc)
        return SendBatchResult(
            success=False,
            status_code=0,
            body=error_body,
            retry_after=None,
            per_recipient_errors={},
            per_recipient_results=build_per_recipient_results(
                mail_dict,
                success=False,
                status_code=0,
                body=error_body,
                per_recipient_errors={},
            ),
            delay_ms=delay_ms,
        )
    except (TimeoutError, httpx.TimeoutException) as exc:
        delay_ms = round((time.perf_counter() - started_at) * 1000, 2)
        raise DeliveryUnknownError(
            "SendGrid HTTP 请求未收到响应，投递状态未知，需人工确认，禁止自动重试",
            future_id=0,
        ) from exc
    except Exception as exc:
        delay_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.exception("SendGrid SDK 发送失败 error_type=%s", type(exc).__name__)
        error_body = _sdk_error_to_message(exc)
        return SendBatchResult(
            success=False,
            status_code=0,
            body=error_body,
            retry_after=None,
            per_recipient_errors={},
            per_recipient_results=build_per_recipient_results(
                mail_dict,
                success=False,
                status_code=0,
                body=error_body,
                per_recipient_errors={},
            ),
            delay_ms=delay_ms,
        )


class SyncSendGridClientManager:
    """SendGrid SDK 同步客户端管理器：15 分钟定期重建 + 连接错误即时剔除 + 后台保活探测。"""

    def __init__(
        self,
        channel: Channel,
        *,
        timeout: float = SENDGRID_SDK_TIMEOUT_SECONDS,
        effective_concurrency: int = 1,
    ) -> None:
        self.channel = channel
        self.timeout = float(timeout)
        self.effective_concurrency = connection_manager.normalize_sendgrid_sdk_effective_concurrency(
            effective_concurrency
        )
        self._client: ProxySendGridAPIClient | None = None
        self._client_fingerprint: tuple | None = None
        self._created_at: float = 0.0
        self._last_used_at: float = 0.0
        self._last_request_at: float = 0.0
        self._last_acquire_reused: bool = False
        self._invalidated: bool = False
        self._inflight_by_client: dict[int, int] = {}
        self._retired_clients: dict[int, ProxySendGridAPIClient] = {}
        self._lock = threading.Lock()
        self._closed = False
        # 禁止在发送客户端上执行 /v3/scopes 保活。探测与 POST /mail/send 共用连接池会
        # 争抢代理容量，并可能把探测侧协议错误带入正常发送连接的生命周期。
        self._keepalive_stop = threading.Event()
        self._keepalive_thread: threading.Thread | None = None

    @contextmanager
    def _client_lock(self, *, label: str):
        acquired = self._lock.acquire(timeout=SYNC_SENDGRID_CLIENT_LOCK_TIMEOUT_SECONDS)
        if not acquired:
            channel_id = getattr(self.channel, "id", "")
            logger.error("SendGrid SDK 客户端锁超时 channel_id=%s label=%s", channel_id, label)
            raise RuntimeError(f"SendGrid SDK 客户端锁等待超时（{label}）")
        try:
            yield
        finally:
            self._lock.release()

    def update_channel(self, channel: Channel) -> None:
        with self._client_lock(label="update_channel"):
            self.channel = channel

    @property
    def last_acquire_reused(self) -> bool:
        return bool(self._last_acquire_reused)

    def acquire_client_for_send(self) -> tuple[ProxySendGridAPIClient, bool]:
        """原子租用客户端直到本次 SDK 请求结束，避免并发失效关闭正在发送的连接。"""
        while True:
            client = self.get_client()
            with self._client_lock(label="acquire_client_for_send"):
                if self._client is not client or self._invalidated:
                    continue
                client_id = id(client)
                self._inflight_by_client[client_id] = int(self._inflight_by_client.get(client_id, 0)) + 1
                return client, bool(self._last_acquire_reused)

    def release_client_after_send(self, client: ProxySendGridAPIClient) -> None:
        client_to_close: ProxySendGridAPIClient | None = None
        with self._client_lock(label="release_client_after_send"):
            client_id = id(client)
            remaining = max(0, int(self._inflight_by_client.get(client_id, 0)) - 1)
            if remaining:
                self._inflight_by_client[client_id] = remaining
                return
            self._inflight_by_client.pop(client_id, None)
            client_to_close = self._retired_clients.pop(client_id, None)
        if client_to_close is not None:
            close_client(client_to_close)

    def _retire_or_close_client(self, client: ProxySendGridAPIClient | None) -> None:
        if client is None:
            return
        close_now = False
        with self._client_lock(label="retire_client"):
            client_id = id(client)
            if int(self._inflight_by_client.get(client_id, 0)) > 0:
                self._retired_clients[client_id] = client
            else:
                close_now = True
        if close_now:
            close_client(client)

    def _build_client_for_channel(self, channel: Channel) -> ProxySendGridAPIClient:
        return create_client(
            channel,
            timeout=self.timeout,
            effective_concurrency=self.effective_concurrency,
        )

    def _log_client_pool_state(self, client: ProxySendGridAPIClient, *, reused: bool) -> None:
        use_proxy = bool(getattr(self.channel, "use_proxy", False))
        keepalive_expiry = connection_manager.resolve_sendgrid_keepalive_expiry(use_proxy=use_proxy)
        pool_limits = connection_manager.resolve_sendgrid_sdk_pool_limits(
            self.effective_concurrency,
            keepalive_expiry=keepalive_expiry,
        )
        connection_manager.log_sendgrid_sdk_pool_state(
            effective_concurrency=self.effective_concurrency,
            pool_limits=pool_limits,
            http2_enabled=connection_manager.resolve_sendgrid_sdk_http2_enabled(),
            use_proxy=use_proxy,
            channel_id=getattr(self.channel, "id", ""),
            reused=reused,
            client_id=id(client.httpx_transport),
        )

    def _should_rebuild_client(self, now: float) -> bool:
        if self._client is None:
            return True
        if self._created_at <= 0:
            return True
        last_used = self._last_request_at if self._last_request_at > 0 else self._last_used_at
        if last_used <= 0:
            last_used = self._created_at
        idle_seconds = now - last_used
        return idle_seconds > SYNC_SENDGRID_CLIENT_REBUILD_SECONDS

    def _probe_idle_connection_if_needed(self, client: ProxySendGridAPIClient, channel: Channel, now: float) -> bool:
        last_request = self._last_request_at if self._last_request_at > 0 else self._created_at
        if last_request <= 0 or (now - last_request) < SYNC_SENDGRID_IDLE_PROBE_SECONDS:
            return True
        api_key = (channel.api_key or "").strip()
        if not api_key:
            return False
        try:
            headers = connection_manager.sendgrid_transport_headers(api_key, for_post=False)
            response = client.httpx_transport.get(connection_manager.SENDGRID_SCOPES_URL, headers=headers)
            status_code = int(response.status_code or 0)
            if 200 <= status_code < 300:
                return True
            logger.warning(
                "SendGrid SDK 空闲探测非 2xx channel_id=%s status_code=%s，将重建客户端",
                getattr(channel, "id", ""),
                status_code,
            )
            self.invalidate_client(reason="idle_probe_non_2xx")
            return False
        except Exception as exc:
            logger.warning(
                "SendGrid SDK 空闲探测失败 channel_id=%s error_type=%s，将重建客户端",
                getattr(channel, "id", ""),
                type(exc).__name__,
            )
            self.invalidate_client(reason=f"idle_probe_{type(exc).__name__}")
            return False

    def get_client(self) -> ProxySendGridAPIClient:
        if self._closed:
            raise RuntimeError("SyncSendGridClientManager 已关闭")
        current_fingerprint = _channel_proxy_fingerprint(self.channel)
        cached_client = self._client
        if (
            cached_client is not None
            and self._client_fingerprint == current_fingerprint
            and not self._invalidated
        ):
            now = time.monotonic()
            if not self._should_rebuild_client(now):
                self._last_used_at = now
                self._last_request_at = now
                self._last_acquire_reused = True
                self._log_client_pool_state(cached_client, reused=True)
                return cached_client
        if (
            cached_client is not None
            and self._client_fingerprint is not None
            and current_fingerprint != self._client_fingerprint
        ):
            logger.warning(
                "SendGrid SDK 客户端指纹不一致 channel_id=%s cached=%s current=%s，将重建客户端",
                getattr(self.channel, "id", ""),
                self._client_fingerprint,
                current_fingerprint,
            )
            self.invalidate_client(reason="fingerprint_mismatch")
        started_at = time.perf_counter()
        try:
            client = self._resolve_client()
        except Exception as exc:
            raise RuntimeError(sanitize_user_error_message(f"SendGrid SDK 客户端重建失败：{exc}")) from exc
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        if elapsed_ms >= SYNC_SENDGRID_GET_CLIENT_SLOW_MS:
            channel_id = getattr(self.channel, "id", "")
            logger.warning(
                "SendGrid SDK 获取客户端偏慢 channel_id=%s elapsed_ms=%s，将重建后重试",
                channel_id,
                elapsed_ms,
            )
            self.invalidate_client(reason="get_client_slow")
            retry_started_at = time.perf_counter()
            client = self._resolve_client()
            retry_ms = round((time.perf_counter() - retry_started_at) * 1000, 2)
            if retry_ms >= SYNC_SENDGRID_GET_CLIENT_SLOW_MS:
                logger.error(
                    "SendGrid SDK 重建后获取客户端仍然偏慢 channel_id=%s retry_ms=%s",
                    channel_id,
                    retry_ms,
                )
        return client

    def _resolve_client(self) -> ProxySendGridAPIClient:
        while True:
            now = time.monotonic()
            with self._client_lock(label="resolve_client_read"):
                client = self._client
                if client is not None and not self._invalidated and not self._should_rebuild_client(now):
                    self._last_used_at = now
                    self._last_request_at = now
                    self._last_acquire_reused = True
                    self._log_client_pool_state(client, reused=True)
                    return client
                old_client = client
                channel = self.channel
                rebuild_reason = "idle_rebuild" if old_client is not None else "initial"
            replaced_client = None
            rebuild_started_at = time.perf_counter()
            new_client = self._build_client_for_channel(channel)
            build_ms = round((time.perf_counter() - rebuild_started_at) * 1000, 2)
            channel_id = getattr(channel, "id", "")

            with self._client_lock(label="resolve_client_assign"):
                if self._client is old_client:
                    self._client = new_client
                    self._client_fingerprint = _channel_proxy_fingerprint(channel)
                    self._invalidated = False
                    now_assigned = time.monotonic()
                    self._created_at = now_assigned
                    self._last_used_at = now_assigned
                    self._last_request_at = now_assigned
                    self._last_acquire_reused = False
                    replaced_client = old_client
                    resolved_client = new_client
                else:
                    close_client(new_client)
                    existing = self._client
                    if existing is not None and not self._invalidated:
                        touch_at = time.monotonic()
                        self._last_used_at = touch_at
                        self._last_request_at = touch_at
                        self._last_acquire_reused = True
                        self._log_client_pool_state(existing, reused=True)
                        return existing
                    old_client = existing
                    channel = self.channel
                    rebuild_reason = "idle_rebuild" if old_client is not None else "initial"
                    continue

            if replaced_client is not None:
                self._retire_or_close_client(replaced_client)
            if build_ms >= SYNC_SENDGRID_CLIENT_BUILD_WARN_MS:
                logger.warning(
                    "SendGrid SDK 客户端重建偏慢 channel_id=%s reason=%s build_ms=%s",
                    channel_id,
                    rebuild_reason,
                    build_ms,
                )
            else:
                logger.info(
                    "SendGrid SDK 客户端已重建 channel_id=%s reason=%s build_ms=%s effective_concurrency=%s client_id=%s",
                    channel_id,
                    rebuild_reason,
                    build_ms,
                    self.effective_concurrency,
                    id(new_client.httpx_transport),
                )
            self._log_client_pool_state(resolved_client, reused=False)
            return resolved_client

    def invalidate_client(self, *, reason: str = "unspecified") -> None:
        self.invalidate_client_if_current(None, reason=reason)

    def invalidate_client_if_current(
        self,
        client: ProxySendGridAPIClient | None,
        *,
        reason: str = "unspecified",
    ) -> bool:
        invalidate_started_at = time.perf_counter()
        channel_id = getattr(self.channel, "id", "")
        with self._client_lock(label="invalidate_client"):
            old_client = self._client
            if client is not None and old_client is not client:
                logger.info(
                    "SendGrid SDK 跳过陈旧客户端失效 channel_id=%s reason=%s failed_client_id=%s current_client_id=%s",
                    channel_id,
                    reason,
                    id(getattr(client, "httpx_transport", client)),
                    id(getattr(old_client, "httpx_transport", old_client)) if old_client is not None else None,
                )
                return False
            self._invalidated = True
            self._client = None
            self._client_fingerprint = None
            self._created_at = 0.0
            self._last_used_at = 0.0
            self._last_request_at = 0.0
            self._last_acquire_reused = False
        self._retire_or_close_client(old_client)
        elapsed_ms = round((time.perf_counter() - invalidate_started_at) * 1000, 2)
        logger.info(
            "SendGrid SDK 客户端已失效 channel_id=%s reason=%s elapsed_ms=%s client_scoped=%s",
            channel_id,
            reason,
            elapsed_ms,
            client is not None,
        )
        return True

    def _keepalive_loop(self) -> None:
        while not self._keepalive_stop.wait(SYNC_SENDGRID_CLIENT_KEEPALIVE_INTERVAL_SECONDS):
            if self._closed:
                break
            try:
                self._run_keepalive_probe()
            except Exception:
                logger.exception(
                    "SendGrid SDK 保活探测失败 channel_id=%s",
                    getattr(self.channel, "id", ""),
                )

    def _run_keepalive_probe(self) -> None:
        if self._closed:
            return
        with self._client_lock(label="keepalive_read_channel"):
            channel = self.channel
        api_key = (channel.api_key or "").strip()
        if not api_key:
            return
        client: ProxySendGridAPIClient | None = None
        try:
            client, _connection_reused = self.acquire_client_for_send()
            headers = connection_manager.sendgrid_transport_headers(api_key, for_post=False)
            response = client.httpx_transport.get(connection_manager.SENDGRID_SCOPES_URL, headers=headers)
            status_code = int(response.status_code or 0)
            if not (200 <= status_code < 300):
                logger.debug(
                    "SendGrid SDK 保活探测非 2xx channel_id=%s status_code=%s，保留客户端避免影响正在发送任务",
                    getattr(channel, "id", ""),
                    status_code,
                )
        except Exception as exc:
            logger.debug(
                "SendGrid SDK 保活探测网络错误 channel_id=%s error_type=%s，保留客户端避免保活误伤发送任务",
                getattr(channel, "id", ""),
                type(exc).__name__,
            )
        finally:
            if client is not None:
                self.release_client_after_send(client)

    def close(self) -> None:
        self._closed = True
        self._keepalive_stop.set()
        if self._keepalive_thread is not None and self._keepalive_thread.is_alive():
            self._keepalive_thread.join(timeout=2.0)
        self.invalidate_client(reason="manager_close")
