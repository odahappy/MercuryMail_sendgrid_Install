import json
import logging
import re
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote

import httpx
from sqlalchemy import select

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.models import AIConfig
from app.services.channel_manager import mask_proxy_url_for_log
from app.utils.crypto import decrypt_value
from app.utils.errors import sanitize_user_error_message
from app.api.ai_config import chat_completions_url, effective_api_base, model_name_from_config, provider_headers

LANGUAGE_LABELS = {
    "zh": "中文",
    "ja": "日文",
    "en": "英文",
    "de": "德语",
    "fr": "法语",
    "es": "西班牙语",
    "it": "意大利语",
    "pt": "葡萄牙语",
    "ko": "韩语",
    "random": "随机语言",
}

LANGUAGE_REQUIREMENTS = {
    "zh": "必须使用自然中文，不要夹杂日文、英文或德语，除非用户提示词明确要求保留某个外语专有名词。",
    "ja": "must be written in natural Japanese. The prompt may be Chinese, but the subject and body must be Japanese, not Chinese, English, or German.",
    "en": "must be written in natural English. The prompt may be Chinese, but the subject and body must be English, not Chinese, Japanese, or German.",
    "de": "must be written in natural German. The prompt may be Chinese, but the subject and body must be German, not Chinese, Japanese, or English.",
    "fr": "must be written in natural French.",
    "es": "must be written in natural Spanish.",
    "it": "must be written in natural Italian.",
    "pt": "must be written in natural Portuguese.",
    "ko": "must be written in natural Korean.",
    "random": "choose one natural target language at random and write the entire subject and body consistently in that language.",
}

EMAIL_TYPE_LABELS = {
    "business": "商务邮件",
    "personal": "个人邮件",
    "notice": "通知邮件",
    "greeting": "问候邮件",
    "thanks": "感谢邮件",
    "random": "随机类型",
}

FALLBACK_SUBJECT = "一封来自 MercuryMail 的暖场邮件"
FALLBACK_TEXT = "你好，这是一封用于账号养护的测试邮件。\n{{ prompt }}"
OPENAI_BASE_URL = "https://api.openai.com/v1"
logger = logging.getLogger(__name__)


async def _latest_ai_config() -> AIConfig | None:
    async with AsyncSessionLocal() as db:
        return (await db.execute(select(AIConfig).order_by(AIConfig.updated_at.desc(), AIConfig.id.desc()).limit(1))).scalar_one_or_none()


def _normalize_ai_proxy_type(value: str | None) -> str:
    proxy_type = str(value or "").strip().lower()
    if proxy_type in {"http", "socks5"}:
        return proxy_type
    if proxy_type in {"socks5h", "socks"}:
        return "socks5"
    return ""


def _build_ai_proxy_url(
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
        raise ValueError("已启用 AI 出站代理，请选择代理类型")
    if not host:
        raise ValueError("已启用 AI 出站代理，请填写代理主机")
    if port <= 0:
        raise ValueError("已启用 AI 出站代理，请填写代理端口")
    username = str(proxy_username or "").strip()
    password = str(proxy_password or "")
    auth = ""
    if username:
        auth = quote(username, safe="")
        if password:
            auth = f"{auth}:{quote(password, safe='')}"
        auth = f"{auth}@"
    return f"{normalized_type}://{auth}{host}:{port}"


@asynccontextmanager
async def _create_ai_http_client(
    *,
    ai_use_proxy: bool = False,
    ai_proxy_url: str | None = None,
    proxy_type: str | None = None,
    proxy_host: str | None = None,
    proxy_port: int | None = None,
    proxy_username: str | None = None,
    proxy_password: str | None = None,
    timeout: float = 60.0,
):
    proxy_url = None
    if bool(ai_use_proxy):
        if proxy_host or proxy_port or proxy_type or proxy_username or proxy_password:
            proxy_url = _build_ai_proxy_url(proxy_type, proxy_host, proxy_port, proxy_username, proxy_password)
        else:
            normalized_url = str(ai_proxy_url or "").strip()
            if not normalized_url:
                raise ValueError("已启用 AI 出站代理，请填写代理类型、主机和端口")
            proxy_url = normalized_url
        logger.debug(
            "AI 请求将经出站代理发出 proxy=%s",
            mask_proxy_url_for_log(proxy_url),
        )
    else:
        logger.debug("AI 请求未配置出站代理，将直连 OpenAI/OpenRouter（trust_env=False）")
    async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout, trust_env=False) as client:
        yield client


def _normalize_language(language: str | None) -> str:
    normalized = (language or "zh").strip().lower()
    return normalized if normalized in LANGUAGE_LABELS else "zh"


def _normalize_email_type(email_type: str | None) -> str:
    normalized = (email_type or "business").strip().lower()
    return normalized if normalized in EMAIL_TYPE_LABELS else "business"


def _normalize_content_format(content_format: str | None) -> str:
    normalized = (content_format or "html").strip().lower()
    return normalized if normalized in {"html", "text"} else "html"


def _is_openrouter_key(api_key: str | None) -> bool:
    return (api_key or "").strip().startswith("sk-or-")


def _temperature_for_fidelity(config_temperature: float | None, fidelity_mode: str) -> float:
    base = float(config_temperature if config_temperature is not None else 0.7)
    mode = (fidelity_mode or "strict").strip().lower()
    if mode == "creative":
        return min(max(base, 0.55), 0.85)
    if mode == "balanced":
        return min(max(base, 0.35), 0.65)
    return min(max(base, 0.15), 0.35)


def _fallback_content(prompt: str, language: str | None = None) -> dict[str, str]:
    prompt_text = (prompt or "").strip()
    target_language = _normalize_language(language)
    if target_language == "ja":
        subject = "MercuryMail からのウォームアップメール"
        text = "こんにちは。これはアカウントウォームアップ用のテストメールです。"
    elif target_language == "en":
        subject = "A warm-up email from MercuryMail"
        text = "Hello, this is a test email for account warm-up."
    elif target_language == "de":
        subject = "Eine Aufwärm-E-Mail von MercuryMail"
        text = "Hallo, dies ist eine Test-E-Mail zur Konto-Aufwärmung."
    else:
        subject = FALLBACK_SUBJECT
        text = "你好，这是一封用于账号养护的测试邮件。"
    if prompt_text:
        text = f"{text}\n{prompt_text}"
    return {"subject": subject if not prompt_text else f"{subject} - MercuryMail", "text": text}


def _split_terms(value: str) -> list[str]:
    terms = []
    for item in re.split(r"[\n,，;；、|]+", value or ""):
        term = item.strip()
        if term:
            terms.append(term)
    return terms


def _content_quality_check(subject: str, text: str, prompt: str, must_include: str, avoid: str) -> dict[str, Any]:
    combined = f"{subject}\n{text}".lower()
    issues = []
    prompt_text = (prompt or "").strip()
    if len(subject.strip()) < 3:
        issues.append("邮件主题过短，无法概括需求")
    if len(text.strip()) < 20:
        issues.append("正文过短，未充分回应提示词")
    prompt_terms = [term for term in _split_terms(prompt_text) if len(term) >= 2]
    if prompt_terms and not any(term.lower() in combined for term in prompt_terms[:12]):
        issues.append("生成内容未明显覆盖提示词中的核心词")
    missing_terms = [term for term in _split_terms(must_include) if term.lower() not in combined]
    if missing_terms:
        issues.append(f"缺少必须包含内容：{'、'.join(missing_terms[:6])}")
    avoided_terms = [term for term in _split_terms(avoid) if term.lower() in combined]
    if avoided_terms:
        issues.append(f"出现了必须避免内容：{'、'.join(avoided_terms[:6])}")
    return {"passed": not issues, "issues": issues}


async def _request_ai_content(client: httpx.AsyncClient, url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, str]:
    response = await client.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    content = data["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    subject = str(parsed.get("subject") or "").strip()
    text = str(parsed.get("text") or "").strip()
    if not subject or not text:
        raise ValueError("AI 返回内容缺少 subject 或 text 字段")
    return {"subject": subject, "text": text}


async def _load_ai_runtime_config() -> dict[str, Any]:
    config = await _latest_ai_config()
    result: dict[str, Any] = {
        "api_key": "",
        "model": "",
        "max_tokens": 1200,
        "configured_api_base": "",
        "config_temperature": 0.7,
        "ai_use_proxy": False,
        "ai_proxy_url": None,
        "proxy_type": "",
        "proxy_host": "",
        "proxy_port": None,
        "proxy_username": "",
        "proxy_password": "",
    }
    if config is not None:
        result.update(
            {
                "api_key": decrypt_value(config.openai_api_key or ""),
                "ai_use_proxy": bool(getattr(config, "ai_use_proxy", False)),
                "ai_proxy_url": str(getattr(config, "ai_proxy_url", "") or "").strip() or None,
                "proxy_type": str(getattr(config, "ai_proxy_type", "") or "").strip(),
                "proxy_host": str(getattr(config, "ai_proxy_host", "") or "").strip(),
                "proxy_port": getattr(config, "ai_proxy_port", None),
                "proxy_username": str(getattr(config, "ai_proxy_username", "") or "").strip(),
                "proxy_password": str(getattr(config, "ai_proxy_password", "") or ""),
                "model": model_name_from_config(config),
                "max_tokens": int(config.max_tokens or 1200),
                "config_temperature": float(config.temperature if config.temperature is not None else 0.7),
                "configured_api_base": str(getattr(config, "api_base", "") or "").strip(),
            }
        )
    if not result["api_key"]:
        result["api_key"] = settings.openai_api_key.strip()
    if result["api_key"] and not result["model"]:
        result["model"] = "openai/gpt-4o-mini" if _is_openrouter_key(result["api_key"]) else "gpt-3.5-turbo"
    return result


async def generate_email_content(
    *,
    mode: str,
    prompt: str = "",
    email_type: str = "business",
    language: str = "zh",
    tone: str = "自然专业",
    include_url: bool = False,
    content_format: str = "html",
) -> dict[str, Any]:
    runtime = await _load_ai_runtime_config()
    api_key = str(runtime.get("api_key") or "").strip()
    if not api_key:
        return {"subject": "", "html": "", "text": "", "source": "error", "error": "未配置 OpenAI/OpenRouter API Key，请先在 AI 配置中填写 API Key"}
    normalized_mode = "manual" if str(mode or "").strip().lower() == "manual" else "conditional"
    language_code = _normalize_language(language)
    email_type_code = _normalize_email_type(email_type)
    format_code = _normalize_content_format(content_format)
    language_label = LANGUAGE_LABELS[language_code]
    email_type_label = EMAIL_TYPE_LABELS[email_type_code]
    tone_text = (tone or "自然专业").strip() or "自然专业"
    request_url = chat_completions_url(effective_api_base(api_key, str(runtime.get("configured_api_base") or "") or OPENAI_BASE_URL))
    base_url = effective_api_base(api_key, str(runtime.get("configured_api_base") or "") or OPENAI_BASE_URL)
    manual_rules = "手动描述模式：只根据用户输入生成，用户描述拥有最高优先级，不要再套用类型、语言、语气、URL 等条件字段。"
    conditional_rules = (
        "按条件一键生成模式：根据条件生成一封可发送邮件。"
        f"邮件类型：{email_type_label}；目标语言：{language_label}；语气：{tone_text}；"
        f"是否包含 URL：{'是，必须自然包含 {{url}} 变量作为链接' if include_url else '否，不要包含任何 URL、链接或 {{url}} 变量'}。"
        "如果类型或语言为随机，请自行随机选择一种合适类型或语言并保持全文一致。"
    )
    system_prompt = (
        "你是专业邮件生成助手。生成内容必须真实、自然、可发送。"
        "正文必须是可直接嵌入邮件的纯 HTML 片段，语义清晰、结构简洁，可包含 p、ul、li、a 等基础标签，不要生成完整 html/head/body 文档。"
        "严禁在 HTML 字段中返回 Markdown、代码围栏、解释性文字或 HTML 之外的前后缀。"
        "不得编造具体优惠、承诺、日期、电话号码、品牌或个人隐私。"
        "只返回严格 JSON，字段只能包含 subject、html；html 必须是非空 HTML 邮件正文。"
    )
    user_prompt = (
        f"生成模式：{normalized_mode}\n"
        "输出格式：HTML\n"
        f"规则：{manual_rules if normalized_mode == 'manual' else conditional_rules}\n"
        f"用户手动描述：{(prompt or '').strip() or '无'}\n"
        "请只生成 subject 和 html。"
    )
    payload = {
        "model": str(runtime.get("model") or "gpt-3.5-turbo"),
        "temperature": 0.75 if normalized_mode == "conditional" else _temperature_for_fidelity(float(runtime.get("config_temperature") or 0.7), "strict"),
        "max_tokens": max(800, int(runtime.get("max_tokens") or 1200)),
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        "response_format": {"type": "json_object"},
    }
    try:
        async with _create_ai_http_client(
            ai_use_proxy=bool(runtime.get("ai_use_proxy")),
            ai_proxy_url=runtime.get("ai_proxy_url"),
            proxy_type=str(runtime.get("proxy_type") or ""),
            proxy_host=str(runtime.get("proxy_host") or ""),
            proxy_port=runtime.get("proxy_port"),
            proxy_username=str(runtime.get("proxy_username") or ""),
            proxy_password=str(runtime.get("proxy_password") or ""),
            timeout=60.0,
        ) as client:
            response = await client.post(request_url, headers=provider_headers(api_key, base_url), json=payload)
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(raw)
            subject = str(parsed.get("subject") or "").strip()
            html = str(parsed.get("html") or "").strip()
            if not subject or not html:
                raise ValueError("AI 返回内容缺少主题或 HTML 正文")
            if "```" in html or not re.search(r"<([a-z][a-z0-9]*)\b[^>]*>", html, re.IGNORECASE):
                raise ValueError("AI 返回的正文不是有效 HTML")
            return {"subject": subject, "html": html, "text": "", "source": "ai", "error": ""}
    except (httpx.HTTPError, ValueError, json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
        logger.exception("AI 邮件生成失败 mode=%s email_type=%s language=%s", normalized_mode, email_type_code, language_code)
        safe_exc = sanitize_user_error_message(str(exc))
        return {"subject": "", "html": "", "text": "", "source": "error", "error": f"AI 请求失败：{safe_exc}"}


async def generate_warmup_content(prompt: str, language: str | None = "zh", allow_fallback: bool = True, tone: str = "自然日常", must_include: str = "", avoid: str = "", fidelity_mode: str = "strict") -> dict[str, Any]:
    config = await _latest_ai_config()
    api_key = ""
    model = ""
    max_tokens = 1200
    configured_api_base = ""
    config_temperature = 0.7
    ai_use_proxy = False
    ai_proxy_url = None
    proxy_type = ""
    proxy_host = ""
    proxy_port = None
    proxy_username = ""
    proxy_password = ""
    language_code = _normalize_language(language)
    language_label = LANGUAGE_LABELS[language_code]
    if config is not None:
        api_key = decrypt_value(config.openai_api_key or "")
        ai_use_proxy = bool(getattr(config, "ai_use_proxy", False))
        ai_proxy_url = str(getattr(config, "ai_proxy_url", "") or "").strip() or None
        proxy_type = str(getattr(config, "ai_proxy_type", "") or "").strip()
        proxy_host = str(getattr(config, "ai_proxy_host", "") or "").strip()
        proxy_port = getattr(config, "ai_proxy_port", None)
        proxy_username = str(getattr(config, "ai_proxy_username", "") or "").strip()
        proxy_password = str(getattr(config, "ai_proxy_password", "") or "")
        model = model_name_from_config(config)
        max_tokens = int(config.max_tokens or 1200)
        config_temperature = float(config.temperature if config.temperature is not None else 0.7)
        configured_api_base = str(getattr(config, "api_base", "") or "").strip()
    if not api_key:
        api_key = settings.openai_api_key.strip()
    if not api_key:
        message = "未配置 OpenAI/OpenRouter API Key，请先在 AI 配置中填写 API Key"
        logger.error("养号文案生成缺少 API Key language=%s tone=%s fidelity=%s", language_code, tone, fidelity_mode)
        if allow_fallback:
            content = _fallback_content(prompt, language_code)
            logger.warning("养号文案生成缺少 API Key，使用本地兜底文案 language=%s", language_code)
            return {**content, "source": "fallback", "error": f"{message}，已使用本地兜底文案"}
        return {"subject": "", "text": "", "source": "error", "error": message}
    if not model:
        model = "openai/gpt-4o-mini" if _is_openrouter_key(api_key) else "gpt-3.5-turbo"
    base_url = effective_api_base(api_key, configured_api_base or OPENAI_BASE_URL)
    request_url = chat_completions_url(base_url)
    temperature = _temperature_for_fidelity(config_temperature, fidelity_mode)

    language_requirement = LANGUAGE_REQUIREMENTS[language_code]
    tone_text = (tone or "自然日常").strip() or "自然日常"
    must_include_text = (must_include or "").strip()
    avoid_text = (avoid or "").strip()
    fidelity_mode_text = (fidelity_mode or "strict").strip().lower()
    fidelity_instruction = {
        "strict": "严格模式：必须优先忠实复述用户提示词中的需求、语气、对象和场景，只做最小必要润色。",
        "balanced": "平衡模式：在忠实不偏题的前提下，允许适度润色和轻微扩写，但不能改变核心需求。",
        "creative": "创意模式：仍然不能偏题，但允许更自然的改写和表达优化。",
    }.get(fidelity_mode_text, "严格模式：必须优先忠实复述用户提示词中的需求、语气、对象和场景，只做最小必要润色。")
    system_prompt = (
        "你是专业的邮件文案助手，只负责把用户的专属提示词改写成一封真实可发送的账号养护邮件。"
        "你必须严格忠实于用户提示词，不能擅自改变主题、人物关系、业务场景、产品、地点、时间、语气、称呼、关键信息或行动意图。"
        "用户写了什么需求，就围绕什么需求生成；用户没写的具体事实、优惠、承诺、品牌、链接、数据、身份、活动都不要编造。"
        "如果提示词很短或不完整，也只能做轻度润色和自然扩写，不要把它改成其他主题。"
        "邮件主题 subject 必须概括用户提示词的核心需求，正文 text 必须逐条覆盖提示词里的关键点。"
        "请返回严格 JSON，字段只能包含 subject、text。text 必须是纯文本正文，不得包含 HTML 标签、Markdown 表格或代码块。"
        "内容要自然、轻量、日常化，避免营销感，避免夸张标题，避免过度发挥。"
        f"输出语言要求：{language_requirement}"
        f"文案风格：{tone_text}。"
        f"{fidelity_instruction}"
    )
    user_prompt = (
        f"目标输出语言：{language_label}\n"
        f"文案风格：{tone_text}\n"
        f"忠实度模式：{fidelity_mode_text}\n"
        "任务：根据【专属提示词】生成一封真实可发送的养号邮件。\n"
        "生成规则：先抽取提示词中的核心主题，再围绕主题生成 subject 和 text；如果提示词包含对象、语气、业务点、动作、时间或地点，必须保留。\n"
        "偏离控制：不得把用户需求改写成别的行业、别的邮件目的或别的沟通场景；不得自行添加促销、活动、奖品、优惠、承诺、链接、号码、日期和人物设定。\n"
        "忠实性检查：输出前先在心里确认主题一致、语言一致、语气一致、关键点一致，再输出 JSON。\n"
        f"必须包含：{must_include_text or '无'}\n"
        f"必须避免：{avoid_text or '无'}\n"
        "输出格式：只输出 JSON，包含 subject 和 text 两个字符串字段。\n"
        f"专属提示词：{prompt or '无'}"
    )
    base_payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
    }
    headers = provider_headers(api_key, base_url)
    logger.info(
        "generate_warmup_content request language=%s tone=%s fidelity=%s model=%s api_base=%s temperature=%s",
        language_code,
        tone_text,
        fidelity_mode_text,
        model,
        base_url,
        temperature,
    )
    try:
        async with _create_ai_http_client(
            ai_use_proxy=ai_use_proxy,
            ai_proxy_url=ai_proxy_url,
            proxy_type=proxy_type,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_username=proxy_username,
            proxy_password=proxy_password,
            timeout=60.0,
        ) as client:
            first = await _request_ai_content(client, request_url, headers, base_payload)
            first_check = _content_quality_check(first["subject"], first["text"], prompt, must_include, avoid)
            if first_check["passed"]:
                return {"subject": first["subject"], "text": first["text"], "source": "ai", "error": "", "quality_check": {"passed": True, "attempts": 1, "issues": []}}
            retry_system_prompt = (
                system_prompt
                + "\n\n上一次输出未通过自检，请严格修正以下问题，只修正文案，不要解释："
                + "；".join(first_check["issues"])
                + "\n必须保持主题一致、语言一致、风格一致，并确保 subject 与 text 都满足要求。"
            )
            retry_user_prompt = (
                user_prompt
                + "\n\n上一次输出的问题："
                + "；".join(first_check["issues"])
                + "\n请重新生成一版更贴合需求的 JSON。"
            )
            retry_payload = {
                **base_payload,
                "temperature": max(0.1, temperature - 0.1),
                "messages": [
                    {"role": "system", "content": retry_system_prompt},
                    {"role": "user", "content": retry_user_prompt},
                ],
            }
            second = await _request_ai_content(client, request_url, headers, retry_payload)
            second_check = _content_quality_check(second["subject"], second["text"], prompt, must_include, avoid)
            if second_check["passed"]:
                return {"subject": second["subject"], "text": second["text"], "source": "ai", "error": "", "quality_check": {"passed": True, "attempts": 2, "issues": first_check["issues"]}}
            if allow_fallback:
                fallback = _fallback_content(prompt, language_code)
                logger.error(
                    "generate_warmup_content quality check failed after retry language=%s tone=%s fidelity=%s issues=%s",
                    language_code,
                    tone_text,
                    fidelity_mode_text,
                    second_check["issues"],
                )
                return {**fallback, "source": "fallback", "error": "AI 生成结果未通过自检，已使用本地兜底文案", "quality_check": {"passed": False, "attempts": 2, "issues": second_check["issues"]}}
            return {"subject": second["subject"], "text": second["text"], "source": "ai", "error": "AI 生成结果未完全通过自检", "quality_check": {"passed": False, "attempts": 2, "issues": second_check["issues"]}}
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:500] if exc.response is not None else str(exc)
        logger.exception("养号文案生成 HTTP 错误 language=%s model=%s detail=%s", language_code, model, detail)
        if allow_fallback:
            content = _fallback_content(prompt, language_code)
            logger.warning("养号文案生成 HTTP 错误后使用本地兜底文案 language=%s", language_code)
            return {**content, "source": "fallback", "error": f"AI 接口返回错误，已使用本地兜底文案：{detail}", "quality_check": {"passed": False, "attempts": 0, "issues": [detail]}}
        return {"subject": "", "text": "", "source": "error", "error": f"AI 接口返回错误：{detail}", "quality_check": {"passed": False, "attempts": 0, "issues": [detail]}}
    except (httpx.HTTPError, ValueError, json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
        if ai_use_proxy and isinstance(exc, ValueError):
            logger.error(
                "AI 出站代理配置无效，拒绝本次文案生成请求 proxy_type=%s proxy_host=%s proxy_port=%s has_legacy_url=%s",
                proxy_type,
                proxy_host,
                proxy_port,
                bool(ai_proxy_url),
            )
            return {
                "subject": "",
                "text": "",
                "source": "error",
                "error": sanitize_user_error_message(str(exc)),
                "quality_check": {"passed": False, "attempts": 0, "issues": [sanitize_user_error_message(str(exc))]},
            }
        if ai_use_proxy and isinstance(exc, httpx.HTTPError):
            safe_exc = sanitize_user_error_message(str(exc))
            logger.error(
                "AI 出站代理网络请求失败，已启用代理时禁止兜底直连或本地兜底 proxy_type=%s proxy_host=%s proxy_port=%s error=%s",
                proxy_type,
                proxy_host,
                proxy_port,
                safe_exc,
            )
            return {
                "subject": "",
                "text": "",
                "source": "error",
                "error": f"AI 代理网络请求失败：{safe_exc}",
                "quality_check": {"passed": False, "attempts": 0, "issues": [f"AI 代理网络请求失败：{safe_exc}"]},
            }
        logger.exception("养号文案生成失败 language=%s model=%s error_type=%s", language_code, model, type(exc).__name__)
        safe_exc = sanitize_user_error_message(str(exc))
        if allow_fallback:
            content = _fallback_content(prompt, language_code)
            logger.warning("养号文案生成请求失败后使用本地兜底文案 language=%s", language_code)
            return {**content, "source": "fallback", "error": f"AI 请求失败，已使用本地兜底文案：{safe_exc}", "quality_check": {"passed": False, "attempts": 0, "issues": [safe_exc]}}
        return {"subject": "", "text": "", "source": "error", "error": f"AI 请求失败：{safe_exc}", "quality_check": {"passed": False, "attempts": 0, "issues": [safe_exc]}}
