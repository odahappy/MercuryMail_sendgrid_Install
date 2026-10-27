import hashlib
import os
import logging
from datetime import datetime, timedelta
from urllib.parse import quote, unquote, urlparse

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_admin
from app.core.config import settings
from app.core.database import get_db
from app.models.models import AIConfig
from app.services.channel_manager import mask_proxy_url_for_log
from app.services.connection_manager import validate_ai_proxy_url
from app.utils.crypto import decrypt_value, encrypt_value, mask_secret

router = APIRouter(prefix="/api/ai-config", tags=["ai-config"])
logger = logging.getLogger(__name__)
DEFAULT_API_BASE = "https://api.openai.com/v1"
OPENROUTER_API_BASE = "https://openrouter.ai/api/v1"
DEFAULT_MODEL_NAME = "gpt-3.5-turbo"
OPENAI_STATIC_MODELS = ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo", "gpt-4o", "gpt-4o-mini"]
OPENROUTER_FALLBACK_MODELS = ["openai/gpt-4o-mini", "openai/gpt-4o", "anthropic/claude-3.5-sonnet", "google/gemini-2.0-flash-001", "meta-llama/llama-3.1-8b-instruct"]
_MODEL_CACHE: dict[str, dict[str, object]] = {}
MODEL_CACHE_TTL = timedelta(hours=1)


class AIConfigOut(BaseModel):
    id: int | None = None
    openai_api_key_masked: str = ""
    api_key_fingerprint: str = ""
    has_api_key: bool = False
    api_base: str = DEFAULT_API_BASE
    model: str = DEFAULT_MODEL_NAME
    model_name: str = DEFAULT_MODEL_NAME
    max_tokens: int = 1200
    temperature: float = 0.7
    ai_use_proxy: bool = False
    proxy_type: str = ""
    proxy_host: str = ""
    proxy_port: int | None = None
    proxy_username: str = ""
    proxy_password: str = ""
    ai_proxy_url: str = ""
    ai_proxy_url_masked: str = ""
    created_at: datetime | None = None
    updated_at: datetime | None = None


class AIConfigUpdate(BaseModel):
    openai_api_key: str = Field(default="", max_length=4096)
    api_base: str = Field(default=DEFAULT_API_BASE, min_length=1, max_length=500)
    model_name: str = Field(default=DEFAULT_MODEL_NAME, min_length=1, max_length=255)
    max_tokens: int = Field(default=1200, ge=100, le=8000)
    temperature: float = Field(default=0.7, ge=0, le=2)
    ai_use_proxy: bool = False
    proxy_type: str = Field(default="", max_length=16)
    proxy_host: str = Field(default="", max_length=255)
    proxy_port: int | None = Field(default=None, ge=1, le=65535)
    proxy_username: str = Field(default="", max_length=255)
    proxy_password: str = Field(default="", max_length=2000)
    ai_proxy_url: str = Field(default="", max_length=2000)


class AIConfigTestRequest(BaseModel):
    openai_api_key: str = Field(default="", max_length=4096)
    api_base: str = Field(default=DEFAULT_API_BASE, min_length=1, max_length=500)
    model_name: str = Field(default=DEFAULT_MODEL_NAME, min_length=1, max_length=255)
    allow_fallback: bool = False
    force_refresh: bool = False
    ai_use_proxy: bool | None = None
    proxy_type: str | None = Field(default=None, max_length=16)
    proxy_host: str | None = Field(default=None, max_length=255)
    proxy_port: int | None = Field(default=None, ge=1, le=65535)
    proxy_username: str | None = Field(default=None, max_length=255)
    proxy_password: str | None = Field(default=None, max_length=2000)
    ai_proxy_url: str | None = Field(default=None, max_length=2000)


class AIConfigTestOut(BaseModel):
    connected: bool
    model: str
    message: str
    checked_at: datetime
    api_key_fingerprint: str = ""


class AIModelListOut(BaseModel):
    models: list[str]
    recommended_models: list[str]
    provider: str = "openai"
    from_cache: bool = False
    warning: str = ""
    api_key_fingerprint: str = ""
    checked_at: datetime


async def latest_config(db: AsyncSession) -> AIConfig | None:
    return (
        await db.execute(
            select(AIConfig)
            .order_by(AIConfig.updated_at.desc(), AIConfig.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()


def api_key_fingerprint(api_key: str) -> str:
    stripped = api_key.strip()
    if not stripped:
        return ""
    return hashlib.sha256(stripped.encode("utf-8")).hexdigest()[:12]


def is_openrouter_key(api_key: str | None) -> bool:
    return (api_key or "").strip().startswith("sk-or-")


def provider_for_api_key(api_key: str | None) -> str:
    return "openrouter" if is_openrouter_key(api_key) else "openai"


def effective_api_base(api_key: str | None, configured_base: str | None = None) -> str:
    if is_openrouter_key(api_key):
        return OPENROUTER_API_BASE
    return normalize_api_base(configured_base or DEFAULT_API_BASE)


def normalize_api_base(value: str | None) -> str:
    base = (value or DEFAULT_API_BASE).strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="API 基础地址必须以 http:// 或 https:// 开头")
    return base


def chat_completions_url(api_base: str) -> str:
    base = normalize_api_base(api_base)
    if base.endswith("/chat/completions"):
        return base
    if base.endswith("/v1") or base.endswith("/api/v1"):
        return f"{base}/chat/completions"
    return f"{base}/chat/completions"


def model_name_from_config(config: AIConfig | None) -> str:
    if config is None:
        return DEFAULT_MODEL_NAME
    return (getattr(config, "model_name", "") or config.model or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME


def openrouter_models_url(api_base: str | None = None) -> str:
    base = normalize_api_base(api_base or "https://openrouter.ai/api/v1")
    suffix = "/chat/completions"
    if base.endswith(suffix):
        return f"{base[:-len(suffix)]}/models"
    return f"{base}/models"


def provider_headers(api_key: str, api_base: str) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if "openrouter.ai" in api_base.lower():
        headers["HTTP-Referer"] = (os.getenv("APP_URL") or settings.openrouter_http_referer or settings.public_base_url or "http://localhost").strip() or "http://localhost"
        headers["X-Title"] = settings.openrouter_title.strip() or "MercuryMail"
    return headers


def mask_ai_proxy_url(proxy_url: str | None) -> str:
    normalized = str(proxy_url or "").strip()
    if not normalized:
        return ""
    return mask_proxy_url_for_log(normalized)


def _normalize_ai_proxy_type(value: str | None) -> str:
    proxy_type = str(value or "").strip().lower()
    if proxy_type in {"http", "socks5"}:
        return proxy_type
    if proxy_type in {"socks5h", "socks"}:
        return "socks5"
    return ""


def _build_ai_proxy_url_from_parts(
    proxy_type: str | None,
    proxy_host: str | None,
    proxy_port: int | None,
    proxy_username: str | None = None,
    proxy_password: str | None = None,
) -> str:
    normalized_type = _normalize_ai_proxy_type(proxy_type)
    host = str(proxy_host or "").strip()
    port = int(proxy_port or 0)
    if normalized_type not in {"http", "socks5"}:
        raise HTTPException(status_code=400, detail="已启用 AI 出站代理，请选择代理类型")
    if not host:
        raise HTTPException(status_code=400, detail="已启用 AI 出站代理，请填写代理主机")
    if port <= 0:
        raise HTTPException(status_code=400, detail="已启用 AI 出站代理，请填写代理端口")
    username = str(proxy_username or "").strip()
    password = str(proxy_password or "")
    auth = ""
    if username:
        auth = quote(username, safe="")
        if password:
            auth = f"{auth}:{quote(password, safe='')}"
        auth = f"{auth}@"
    return f"{normalized_type}://{auth}{host}:{port}"


def _parse_ai_proxy_url(proxy_url: str | None) -> dict[str, object]:
    normalized = str(proxy_url or "").strip()
    if not normalized:
        return {
            "proxy_type": "",
            "proxy_host": "",
            "proxy_port": None,
            "proxy_username": "",
            "proxy_password": "",
            "ai_proxy_url": "",
        }
    parsed = urlparse(normalized)
    scheme = _normalize_ai_proxy_type(parsed.scheme)
    if scheme not in {"http", "socks5"}:
        raise HTTPException(status_code=400, detail="旧版 AI 出站代理地址格式不正确，请重新填写")
    host = parsed.hostname or ""
    port = parsed.port
    if not host or port is None:
        raise HTTPException(status_code=400, detail="旧版 AI 出站代理地址缺少主机或端口，请重新填写")
    return {
        "proxy_type": scheme,
        "proxy_host": host,
        "proxy_port": int(port),
        "proxy_username": unquote(parsed.username or ""),
        "proxy_password": unquote(parsed.password or ""),
        "ai_proxy_url": normalized,
    }


def _extract_ai_proxy_fields(config: AIConfig | None) -> dict[str, object]:
    if config is None:
        return {
            "ai_use_proxy": False,
            "proxy_type": "",
            "proxy_host": "",
            "proxy_port": None,
            "proxy_username": "",
            "proxy_password": "",
            "ai_proxy_url": "",
            "ai_proxy_url_masked": "",
        }
    use_proxy = bool(getattr(config, "ai_use_proxy", False))
    proxy_type = _normalize_ai_proxy_type(getattr(config, "ai_proxy_type", ""))
    proxy_host = str(getattr(config, "ai_proxy_host", "") or "").strip()
    proxy_port = getattr(config, "ai_proxy_port", None)
    proxy_username = str(getattr(config, "ai_proxy_username", "") or "").strip()
    proxy_password = str(getattr(config, "ai_proxy_password", "") or "")
    saved_proxy_url = str(getattr(config, "ai_proxy_url", "") or "").strip()

    if not proxy_type and saved_proxy_url:
        parsed = _parse_ai_proxy_url(saved_proxy_url)
        proxy_type = str(parsed["proxy_type"] or "")
        proxy_host = str(parsed["proxy_host"] or "")
        proxy_port = int(parsed["proxy_port"] or 0) or None
        proxy_username = str(parsed["proxy_username"] or "")
        proxy_password = str(parsed["proxy_password"] or "")
        saved_proxy_url = str(parsed["ai_proxy_url"] or "")

    return {
        "ai_use_proxy": use_proxy,
        "proxy_type": proxy_type,
        "proxy_host": proxy_host,
        "proxy_port": proxy_port,
        "proxy_username": proxy_username,
        "proxy_password": proxy_password,
        "ai_proxy_url": saved_proxy_url,
        "ai_proxy_url_masked": mask_ai_proxy_url(saved_proxy_url),
    }


def _normalize_ai_proxy_payload(
    *,
    ai_use_proxy: bool,
    proxy_type: str | None = None,
    proxy_host: str | None = None,
    proxy_port: int | None = None,
    proxy_username: str | None = None,
    proxy_password: str | None = None,
    ai_proxy_url: str | None = None,
) -> dict[str, object]:
    use_proxy = bool(ai_use_proxy)
    if not use_proxy:
        return {
            "ai_use_proxy": False,
            "proxy_type": "",
            "proxy_host": "",
            "proxy_port": None,
            "proxy_username": "",
            "proxy_password": "",
            "ai_proxy_url": "",
        }
    normalized_type = _normalize_ai_proxy_type(proxy_type)
    normalized_host = str(proxy_host or "").strip()
    normalized_port = int(proxy_port or 0) if proxy_port is not None else 0
    normalized_username = str(proxy_username or "").strip()
    normalized_password = str(proxy_password or "")
    if normalized_type and normalized_host and normalized_port > 0:
        proxy_url = _build_ai_proxy_url_from_parts(
            normalized_type,
            normalized_host,
            normalized_port,
            normalized_username,
            normalized_password,
        )
        return {
            "ai_use_proxy": True,
            "proxy_type": normalized_type,
            "proxy_host": normalized_host,
            "proxy_port": normalized_port,
            "proxy_username": normalized_username,
            "proxy_password": normalized_password,
            "ai_proxy_url": proxy_url,
        }
    fallback_url = str(ai_proxy_url or "").strip()
    if fallback_url:
        parsed = _parse_ai_proxy_url(fallback_url)
        return {
            "ai_use_proxy": True,
            **parsed,
            "proxy_username": str(parsed["proxy_username"] or ""),
            "proxy_password": str(parsed["proxy_password"] or ""),
        }
    raise HTTPException(status_code=400, detail="已启用 AI 出站代理，请填写代理类型、主机和端口")


def read_ai_proxy_settings(config: AIConfig | None) -> dict[str, object]:
    return _extract_ai_proxy_fields(config)


def resolve_ai_proxy_settings(
    *,
    config: AIConfig | None,
    payload_use_proxy: bool | None = None,
    payload_proxy_url: str | None = None,
    payload_proxy_type: str | None = None,
    payload_proxy_host: str | None = None,
    payload_proxy_port: int | None = None,
    payload_proxy_username: str | None = None,
    payload_proxy_password: str | None = None,
) -> tuple[bool, str | None]:
    saved = _extract_ai_proxy_fields(config)
    use_proxy = bool(saved.get("ai_use_proxy", False)) if payload_use_proxy is None else bool(payload_use_proxy)
    if not use_proxy:
        return False, None
    structured_fields = (payload_proxy_type, payload_proxy_host, payload_proxy_port, payload_proxy_username, payload_proxy_password)
    has_structured = any(value not in (None, "") for value in structured_fields)
    if has_structured:
        proxy_data = _normalize_ai_proxy_payload(
            ai_use_proxy=True,
            proxy_type=payload_proxy_type,
            proxy_host=payload_proxy_host,
            proxy_port=payload_proxy_port,
            proxy_username=payload_proxy_username,
            proxy_password=payload_proxy_password,
            ai_proxy_url=payload_proxy_url,
        )
        return True, str(proxy_data["ai_proxy_url"] or "")
    saved_proxy_url = str(saved.get("ai_proxy_url") or "")
    if payload_proxy_url is not None and str(payload_proxy_url or "").strip():
        saved_proxy_url = str(payload_proxy_url or "").strip()
    if saved_proxy_url:
        validate_ai_proxy_url(saved_proxy_url)
        return True, saved_proxy_url
    if saved.get("proxy_type") and saved.get("proxy_host") and int(saved.get("proxy_port") or 0) > 0:
        return True, _build_ai_proxy_url_from_parts(
            str(saved.get("proxy_type") or ""),
            str(saved.get("proxy_host") or ""),
            int(saved.get("proxy_port") or 0),
            str(saved.get("proxy_username") or ""),
            str(saved.get("proxy_password") or ""),
        )
    raise HTTPException(status_code=400, detail="已启用 AI 出站代理，请填写代理类型、主机和端口")


def validate_ai_proxy_update(
    *,
    ai_use_proxy: bool,
    proxy_type: str | None = None,
    proxy_host: str | None = None,
    proxy_port: int | None = None,
    proxy_username: str | None = None,
    proxy_password: str | None = None,
    ai_proxy_url: str | None = None,
) -> dict[str, object]:
    return _normalize_ai_proxy_payload(
        ai_use_proxy=ai_use_proxy,
        proxy_type=proxy_type,
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        proxy_username=proxy_username,
        proxy_password=proxy_password,
        ai_proxy_url=ai_proxy_url,
    )


def to_out(config: AIConfig | None) -> AIConfigOut:
    if config is None:
        return AIConfigOut()
    secret = decrypt_value(config.openai_api_key or "")
    selected_model = model_name_from_config(config)
    selected_api_base = effective_api_base(secret, getattr(config, "api_base", "") or DEFAULT_API_BASE)
    proxy_data = _extract_ai_proxy_fields(config)
    return AIConfigOut(
        id=config.id,
        openai_api_key_masked=mask_secret(secret),
        api_key_fingerprint=api_key_fingerprint(secret),
        has_api_key=bool(secret),
        api_base=selected_api_base,
        model=selected_model,
        model_name=selected_model,
        max_tokens=config.max_tokens or 1200,
        temperature=float(config.temperature if config.temperature is not None else 0.7),
        ai_use_proxy=bool(proxy_data["ai_use_proxy"]),
        proxy_type=str(proxy_data["proxy_type"] or ""),
        proxy_host=str(proxy_data["proxy_host"] or ""),
        proxy_port=proxy_data["proxy_port"],
        proxy_username=str(proxy_data["proxy_username"] or ""),
        proxy_password=str(proxy_data["proxy_password"] or ""),
        ai_proxy_url=str(proxy_data["ai_proxy_url"] or ""),
        ai_proxy_url_masked=str(proxy_data["ai_proxy_url_masked"] or ""),
        created_at=config.created_at,
        updated_at=config.updated_at,
    )


async def resolve_test_api_key(payload_key: str, db: AsyncSession) -> str:
    api_key = payload_key.strip()
    if api_key:
        return api_key
    config = await latest_config(db)
    if config is not None:
        saved_key = decrypt_value(config.openai_api_key or "").strip()
        if saved_key:
            return saved_key
    return settings.openai_api_key.strip()


async def test_openrouter_connection(
    api_key: str,
    api_base: str,
    model_name: str,
    *,
    ai_use_proxy: bool = False,
    ai_proxy_url: str | None = None,
) -> AIConfigTestOut:
    if not api_key:
        raise HTTPException(status_code=400, detail="未填写 API Key，也没有可用的已保存密钥或 OPENAI_API_KEY 环境变量")
    normalized_api_base = effective_api_base(api_key, api_base)
    normalized_model = model_name.strip() or DEFAULT_MODEL_NAME
    payload = {
        "model": normalized_model,
        "temperature": 0,
        "max_tokens": 12,
        "messages": [
            {"role": "system", "content": "Return only the word ok."},
            {"role": "user", "content": "connection test"},
        ],
    }
    headers = provider_headers(api_key, normalized_api_base)
    try:
        from app.services.ai_writer import _create_ai_http_client

        async with _create_ai_http_client(ai_use_proxy=ai_use_proxy, ai_proxy_url=ai_proxy_url, timeout=30.0) as client:
            response = await client.post(chat_completions_url(normalized_api_base), headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:500] if exc.response is not None else type(exc).__name__
        if exc.response is not None and exc.response.status_code == 403 and "not available in your region" in detail.lower():
            raise HTTPException(status_code=502, detail=f"AI 连接失败：模型 {normalized_model} 在当前服务器出口地区不可用。请点击刷新可用模型，或更换 OpenRouter 账户/服务器出口地区。原始错误：{detail}")
        if exc.response is not None and exc.response.status_code in {401, 403}:
            raise HTTPException(status_code=502, detail=f"AI 连接失败：API Key 未授权、额度不足或模型不可用。当前测试 Key 指纹 {api_key_fingerprint(api_key)}。原始错误：{detail}")
        raise HTTPException(status_code=502, detail=f"AI 连接失败，接口返回错误：{detail}")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"AI 连接失败，请检查服务器网络、API 地址和 Key：{type(exc).__name__}")
    try:
        content = str(data["choices"][0]["message"]["content"]).strip()
    except (KeyError, IndexError, TypeError):
        raise HTTPException(status_code=502, detail="AI 连接失败，接口响应结构异常，未找到 choices[0].message.content")
    if not content:
        raise HTTPException(status_code=502, detail="AI 连接失败，模型返回了空内容")
    return AIConfigTestOut(connected=True, model=normalized_model, message="API Key 与模型连接成功，后续 AI 养号文案会持续使用已保存配置", checked_at=datetime.utcnow(), api_key_fingerprint=api_key_fingerprint(api_key))


async def fetch_ai_models(
    api_key: str,
    api_base: str | None = None,
    allow_fallback: bool = True,
    force_refresh: bool = False,
    *,
    ai_use_proxy: bool = False,
    ai_proxy_url: str | None = None,
) -> AIModelListOut:
    provider = provider_for_api_key(api_key)
    checked_at = datetime.utcnow()
    if not api_key:
        if not allow_fallback:
            raise HTTPException(status_code=400, detail="未填写 API Key，也没有可用的已保存密钥或 OPENAI_API_KEY 环境变量")
        return AIModelListOut(models=OPENAI_STATIC_MODELS, recommended_models=OPENAI_STATIC_MODELS, provider="openai", warning="未配置 API Key，已显示 OpenAI 常用模型", checked_at=checked_at)
    if provider == "openai":
        return AIModelListOut(models=OPENAI_STATIC_MODELS, recommended_models=OPENAI_STATIC_MODELS, provider="openai", api_key_fingerprint=api_key_fingerprint(api_key), checked_at=checked_at)

    normalized_api_base = effective_api_base(api_key, api_base)
    cache_key = f"{api_key_fingerprint(api_key)}:{normalized_api_base}"
    cached = _MODEL_CACHE.get(cache_key)
    if not force_refresh and cached and isinstance(cached.get("checked_at"), datetime) and checked_at - cached["checked_at"] < MODEL_CACHE_TTL:
        cached_models = list(cached.get("models") or [])
        return AIModelListOut(models=cached_models, recommended_models=cached_models[:20], provider="openrouter", from_cache=True, api_key_fingerprint=api_key_fingerprint(api_key), checked_at=cached["checked_at"])

    headers = provider_headers(api_key, normalized_api_base)
    try:
        from app.services.ai_writer import _create_ai_http_client

        async with _create_ai_http_client(ai_use_proxy=ai_use_proxy, ai_proxy_url=ai_proxy_url, timeout=30.0) as client:
            response = await client.get(openrouter_models_url(normalized_api_base), headers=headers)
            response.raise_for_status()
            data = response.json()
        raw_models = data.get("data") if isinstance(data, dict) else []
        models = []
        for item in raw_models or []:
            model_id = str(item.get("id") or "").strip() if isinstance(item, dict) else ""
            if model_id:
                models.append(model_id)
        unique_models = sorted(set(models))
        if not unique_models:
            raise ValueError("OpenRouter 返回的模型列表为空")
        _MODEL_CACHE[cache_key] = {"models": unique_models, "checked_at": checked_at}
        return AIModelListOut(models=unique_models, recommended_models=unique_models[:20], provider="openrouter", api_key_fingerprint=api_key_fingerprint(api_key), checked_at=checked_at)
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:500] if exc.response is not None else type(exc).__name__
        if not allow_fallback:
            raise HTTPException(status_code=502, detail=f"获取 OpenRouter 模型列表失败，接口返回错误：{detail}")
        return AIModelListOut(models=OPENROUTER_FALLBACK_MODELS, recommended_models=OPENROUTER_FALLBACK_MODELS, provider="openrouter", warning=f"获取 OpenRouter 模型列表失败，已回退到静态列表：{detail}", api_key_fingerprint=api_key_fingerprint(api_key), checked_at=checked_at)
    except ValueError as exc:
        if not allow_fallback:
            raise HTTPException(status_code=400, detail=str(exc))
        return AIModelListOut(models=OPENROUTER_FALLBACK_MODELS, recommended_models=OPENROUTER_FALLBACK_MODELS, provider="openrouter", warning=f"获取 OpenRouter 模型列表失败，已回退到静态列表：{exc}", api_key_fingerprint=api_key_fingerprint(api_key), checked_at=checked_at)
    except httpx.HTTPError as exc:
        if not allow_fallback:
            raise HTTPException(status_code=502, detail=f"获取 OpenRouter 模型列表失败，请检查网络、API Key 和服务器出口：{type(exc).__name__}")
        return AIModelListOut(models=OPENROUTER_FALLBACK_MODELS, recommended_models=OPENROUTER_FALLBACK_MODELS, provider="openrouter", warning=f"获取 OpenRouter 模型列表失败，已回退到静态列表：{type(exc).__name__}", api_key_fingerprint=api_key_fingerprint(api_key), checked_at=checked_at)


@router.get("", response_model=AIConfigOut, dependencies=[Depends(require_admin)])
async def get_ai_config(db: AsyncSession = Depends(get_db)):
    try:
        return to_out(await latest_config(db))
    except DBAPIError as exc:
        logger.exception("读取 AI 配置失败：数据库异常")
        message = str(exc).lower()
        if any(marker in message for marker in ("ai_proxy_type", "ai_proxy_host", "ai_proxy_port", "ai_proxy_username", "ai_proxy_password")):
            raise HTTPException(status_code=500, detail="AI 配置字段缺失，请稍后重试（系统正在自动补建结构化代理字段）") from exc
        raise HTTPException(status_code=500, detail="读取 AI 配置失败，请检查数据库连接状态") from exc


@router.post("/test", response_model=AIConfigTestOut, dependencies=[Depends(require_admin)])
async def test_ai_config(payload: AIConfigTestRequest, db: AsyncSession = Depends(get_db)):
    api_key = await resolve_test_api_key(payload.openai_api_key, db)
    config = await latest_config(db)
    ai_use_proxy, ai_proxy_url = resolve_ai_proxy_settings(
        config=config,
        payload_use_proxy=payload.ai_use_proxy,
        payload_proxy_url=payload.ai_proxy_url,
        payload_proxy_type=payload.proxy_type,
        payload_proxy_host=payload.proxy_host,
        payload_proxy_port=payload.proxy_port,
        payload_proxy_username=payload.proxy_username,
        payload_proxy_password=payload.proxy_password,
    )
    return await test_openrouter_connection(
        api_key,
        payload.api_base,
        payload.model_name,
        ai_use_proxy=ai_use_proxy,
        ai_proxy_url=ai_proxy_url,
    )


@router.get("/models", response_model=AIModelListOut, dependencies=[Depends(require_admin)])
async def get_available_ai_models(db: AsyncSession = Depends(get_db)):
    config = await latest_config(db)
    api_key = ""
    api_base = DEFAULT_API_BASE
    ai_use_proxy = False
    ai_proxy_url = None
    if config is not None:
        api_key = decrypt_value(config.openai_api_key or "").strip()
        api_base = getattr(config, "api_base", "") or DEFAULT_API_BASE
        proxy_data = read_ai_proxy_settings(config)
        ai_use_proxy = bool(proxy_data.get("ai_use_proxy", False))
        ai_proxy_url = str(proxy_data.get("ai_proxy_url") or "") or None
    if not api_key:
        api_key = settings.openai_api_key.strip()
    return await fetch_ai_models(api_key, api_base, ai_use_proxy=ai_use_proxy, ai_proxy_url=ai_proxy_url)


@router.post("/models", response_model=AIModelListOut, dependencies=[Depends(require_admin)])
async def list_available_ai_models(payload: AIConfigTestRequest, db: AsyncSession = Depends(get_db)):
    api_key = await resolve_test_api_key(payload.openai_api_key, db)
    config = await latest_config(db)
    ai_use_proxy, ai_proxy_url = resolve_ai_proxy_settings(
        config=config,
        payload_use_proxy=payload.ai_use_proxy,
        payload_proxy_url=payload.ai_proxy_url,
        payload_proxy_type=payload.proxy_type,
        payload_proxy_host=payload.proxy_host,
        payload_proxy_port=payload.proxy_port,
        payload_proxy_username=payload.proxy_username,
        payload_proxy_password=payload.proxy_password,
    )
    return await fetch_ai_models(
        api_key,
        payload.api_base,
        allow_fallback=payload.allow_fallback,
        force_refresh=payload.force_refresh,
        ai_use_proxy=ai_use_proxy,
        ai_proxy_url=ai_proxy_url,
    )


@router.put("", response_model=AIConfigOut, dependencies=[Depends(require_admin)])
async def update_ai_config(payload: AIConfigUpdate, db: AsyncSession = Depends(get_db)):
    try:
        config = await latest_config(db)
        if config is None:
            config = AIConfig(created_at=datetime.utcnow())
            db.add(config)
        if payload.openai_api_key.strip():
            config.openai_api_key = encrypt_value(payload.openai_api_key.strip())
        normalized_model = payload.model_name.strip()
        config.api_base = normalize_api_base(payload.api_base)
        config.model_name = normalized_model
        config.model = normalized_model
        config.max_tokens = payload.max_tokens
        config.temperature = payload.temperature
        proxy_data = validate_ai_proxy_update(
            ai_use_proxy=payload.ai_use_proxy,
            proxy_type=payload.proxy_type,
            proxy_host=payload.proxy_host,
            proxy_port=payload.proxy_port,
            proxy_username=payload.proxy_username,
            proxy_password=payload.proxy_password,
            ai_proxy_url=payload.ai_proxy_url,
        )
        config.ai_use_proxy = bool(proxy_data["ai_use_proxy"])
        config.ai_proxy_type = str(proxy_data["proxy_type"] or "")
        config.ai_proxy_host = str(proxy_data["proxy_host"] or "")
        config.ai_proxy_port = proxy_data["proxy_port"]
        config.ai_proxy_username = str(proxy_data["proxy_username"] or "")
        config.ai_proxy_password = str(proxy_data["proxy_password"] or "")
        config.ai_proxy_url = str(proxy_data["ai_proxy_url"] or "")
        config.outbound_proxy_id = None
        config.updated_at = datetime.utcnow()
        await db.commit()
        await db.refresh(config)
        refreshed = await latest_config(db)
        return to_out(refreshed)
    except DBAPIError as exc:
        await db.rollback()
        logger.exception("保存 AI 配置失败：数据库异常")
        message = str(exc).lower()
        if any(marker in message for marker in ("ai_proxy_type", "ai_proxy_host", "ai_proxy_port", "ai_proxy_username", "ai_proxy_password")):
            raise HTTPException(status_code=500, detail="AI 配置字段缺失，请稍后重试（系统正在自动补建结构化代理字段）") from exc
        raise HTTPException(status_code=500, detail="保存 AI 配置失败，请检查数据库连接状态") from exc
