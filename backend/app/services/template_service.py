import json
import logging
import re
import secrets
import string
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from typing import Any

import jinja2
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.models.models import EmailTemplate

logger = logging.getLogger(__name__)

DEFAULT_RANDOM_CHARS = string.ascii_letters + string.digits
SPECIAL_RANDOM_CHARS = "!@#$%^&*()-_=+[]{};:,.?/"
INLINE_TEXT_TEMPLATE_CACHE_MAX_SIZE = 128
_template_cache: dict[tuple[int | None, datetime | None], dict[str, jinja2.Template]] = {}
_environment = jinja2.Environment(autoescape=False, undefined=jinja2.StrictUndefined)
_VARIABLE_PATTERN = re.compile(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)")
_RANDOM_VARIABLE_PATTERN = re.compile(r"^random(\d+)$")
_JINJA_VARIABLE_PATTERN = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")
_JINJA_ADVANCED_PATTERN = re.compile(
    r"\{%-?\s*(if|for|elif|else|endif|endfor|block|endblock|macro|endmacro|set|include|extends|with|endwith)\b",
    re.IGNORECASE,
)
SENDGRID_URL_SUBSTITUTION_KEY = "-url-"
SENDGRID_URL_POOL_URL_SUBSTITUTION_KEY = "-url_pool_url-"
URL_TRACKING_VARIABLE_NAMES = frozenset({"url", "url_pool_url"})
_SENDGRID_SUBSTITUTION_ALIASES = {
    "from_name": "sender_name",
    "from_email": "sender_email",
    "url_pool_url": "url",
}


@dataclass
class SubstitutionTemplate:
    subject: str
    html: str
    text: str
    content_type: str


@dataclass
class StaticMailContent:
    subject: str
    html: str
    text: str
    content_type: str

    @property
    def is_html(self) -> bool:
        return self.content_type == "html" and bool((self.html or "").strip())

    @property
    def content(self) -> str:
        if self.is_html:
            return self.html or ""
        return self.text or ""


@dataclass
class RenderedTemplate:
    subject: str
    html: str
    text: str


class EmptyStringMapping(dict):
    def __missing__(self, key: str) -> str:
        self[key] = ""
        return ""


def get_template_cache_key(template: EmailTemplate) -> tuple[int | None, datetime | None]:
    return (getattr(template, "id", None), getattr(template, "updated_at", None))


@lru_cache(maxsize=INLINE_TEXT_TEMPLATE_CACHE_MAX_SIZE)
def _compile_inline_text_template(source: str) -> jinja2.Template:
    return _environment.from_string(source)


def invalidate_template_cache(template_id: int | None = None) -> None:
    if template_id is None:
        _template_cache.clear()
        _compile_inline_text_template.cache_clear()
        return
    stale_keys = [key for key in _template_cache if key[0] == template_id]
    for key in stale_keys:
        _template_cache.pop(key, None)


def warm_up_template(template: EmailTemplate) -> None:
    compiled = compile_template(template)
    subject_source = (template.subject or "").strip()
    if subject_source:
        _compile_inline_text_template(subject_source)
    _ = compiled


async def warm_up_template_cache() -> int:
    warmed = 0
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(EmailTemplate).order_by(EmailTemplate.updated_at.desc(), EmailTemplate.id.desc()).limit(10)
        )
        templates = result.scalars().all()
    for template in templates:
        try:
            warm_up_template(template)
            warmed += 1
        except Exception as exc:
            logger.warning("模板预热失败 template_id=%s error=%s", getattr(template, 'id', None), exc)
    return warmed


def compile_template(template: EmailTemplate) -> dict[str, jinja2.Template]:
    cache_key = get_template_cache_key(template)
    cached = _template_cache.get(cache_key)
    if cached is not None:
        return cached
    invalidate_template_cache(getattr(template, "id", None))
    compiled = {
        "subject": _environment.from_string(template.subject or ""),
        "html": _environment.from_string(template.html_body or ""),
        "text": _environment.from_string(template.text_body or ""),
    }
    _template_cache[cache_key] = compiled
    return compiled


def _safe_random_chars(value: str | None) -> str:
    chars = str(value or "")
    return chars or DEFAULT_RANDOM_CHARS


def _random_string(length: Any, chars: str | None) -> str:
    try:
        normalized_length = int(length)
    except (TypeError, ValueError):
        normalized_length = 0
    if normalized_length <= 0:
        normalized_length = 16
    alphabet = _safe_random_chars(chars)
    return "".join(secrets.choice(alphabet) for _ in range(normalized_length))


def _load_random_rules(template: EmailTemplate) -> dict[str, Any]:
    try:
        value = json.loads(template.random_rules_json or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _collect_variable_names(*sources: str) -> set[str]:
    names: set[str] = set()
    for source in sources:
        names.update(_VARIABLE_PATTERN.findall(source or ""))
    return names


def _rule_for_random_name(rules: dict[str, Any], name: str) -> dict[str, Any]:
    direct_rule = rules.get(name)
    if isinstance(direct_rule, dict):
        return direct_rule
    match = _RANDOM_VARIABLE_PATTERN.match(name)
    if match:
        indexed_rule = rules.get(match.group(1))
        if isinstance(indexed_rule, dict):
            return indexed_rule
    return {}


def _as_non_negative_int(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    return normalized if normalized > 0 else 0


def _generate_random_string(template: EmailTemplate, rule: dict[str, Any] | None) -> str:
    """
    New flexible random rule format:
      {upper, lower, digits, special} where each >= 0, total length = sum.
    Generation: pick exact counts from each pool, then shuffle.

    Backward compatible formats:
    - If rule does not contain any of {upper, lower, digits, special} but contains {length, chars},
      use that legacy behavior for this variable.
    - If rule missing/invalid or total length == 0, fallback to template defaults:
      template.random_length + template.random_chars.
    """
    default_random_length = int(getattr(template, "random_length", 16) or 16)
    default_random_chars = str(getattr(template, "random_chars", "") or "")

    if not isinstance(rule, dict) or not rule:
        return _random_string(default_random_length, default_random_chars)

    keys = {"upper", "lower", "digits", "special"}
    has_new_keys = any(key in rule for key in keys)
    if has_new_keys:
        upper = _as_non_negative_int(rule.get("upper"))
        lower = _as_non_negative_int(rule.get("lower"))
        digits = _as_non_negative_int(rule.get("digits"))
        special = _as_non_negative_int(rule.get("special"))
        total = upper + lower + digits + special
        if total <= 0:
            return _random_string(default_random_length, default_random_chars)
        chars: list[str] = []
        if upper:
            chars.extend(secrets.choice(string.ascii_uppercase) for _ in range(upper))
        if lower:
            chars.extend(secrets.choice(string.ascii_lowercase) for _ in range(lower))
        if digits:
            chars.extend(secrets.choice(string.digits) for _ in range(digits))
        if special:
            pool = SPECIAL_RANDOM_CHARS
            chars.extend(secrets.choice(pool) for _ in range(special))
        secrets.SystemRandom().shuffle(chars)
        return "".join(chars)

    # Legacy per-variable rule support: {length, chars}
    if "length" in rule or "chars" in rule:
        return _random_string(rule.get("length", default_random_length), rule.get("chars", default_random_chars))

    return _random_string(default_random_length, default_random_chars)


def prepare_variables(template: EmailTemplate, variables: dict[str, Any] | None, extra_sources: tuple[str, ...] = ()) -> EmptyStringMapping:
    prepared = EmptyStringMapping()
    if variables:
        for key, value in variables.items():
            prepared[str(key)] = "" if value is None else str(value)
    prepared["uuid"] = prepared.get("uuid") or uuid.uuid4().hex
    prepared["campaign_id"] = prepared.get("campaign_id") or str(template.campaign_id or "")
    # date: allow caller override; otherwise inject today's date in YYYY-MM-DD.
    prepared["date"] = prepared.get("date") or date.today().strftime("%Y-%m-%d")

    rules = _load_random_rules(template)
    # random: allow caller override; otherwise generate (new rules if present, else template defaults).
    if not prepared.get("random"):
        prepared["random"] = _generate_random_string(template, _rule_for_random_name(rules, "random"))
    names = _collect_variable_names(template.subject or "", template.html_body or "", template.text_body or "", *extra_sources)
    for name in names:
        if (name == "random" or _RANDOM_VARIABLE_PATTERN.match(name)) and not prepared.get(name):
            prepared[name] = _generate_random_string(template, _rule_for_random_name(rules, name))
    return prepared


def _render_with_detected_variables(compiled: jinja2.Template, variables: EmptyStringMapping) -> str:
    missing = True
    rendered = ""
    while missing:
        try:
            rendered = compiled.render(**variables)
            missing = False
        except jinja2.UndefinedError as exc:
            match = re.search(r"'([^']+)' is undefined", str(exc))
            if not match:
                return ""
            variables[match.group(1)] = ""
    return rendered


def render_body(template: EmailTemplate, variables: dict[str, Any] | None = None) -> tuple[str, str]:
    compiled = compile_template(template)
    prepared = prepare_variables(template, variables)
    return (
        _render_with_detected_variables(compiled["html"], prepared),
        _render_with_detected_variables(compiled["text"], prepared),
    )


def render_for_send(
    template: EmailTemplate,
    variables: dict[str, Any] | None = None,
    *,
    subject_fallback: str = "",
) -> RenderedTemplate:
    """单次编译并渲染主题与正文，避免热路径多次线程池调度。"""
    template_subject = (template.subject or "").strip()
    subject_source = template_subject or (subject_fallback or "").strip() or "MercuryMail"
    extra_sources = () if template_subject else (subject_source,)
    compiled = compile_template(template)
    prepared = prepare_variables(template, variables, extra_sources=extra_sources)
    if template_subject:
        subject = _render_with_detected_variables(compiled["subject"], prepared)
    else:
        subject = _render_with_detected_variables(_compile_inline_text_template(subject_source), prepared)
    return RenderedTemplate(
        subject=subject,
        html=_render_with_detected_variables(compiled["html"], prepared),
        text=_render_with_detected_variables(compiled["text"], prepared),
    )


def render(template: EmailTemplate, variables: dict[str, Any] | None = None) -> RenderedTemplate:
    compiled = compile_template(template)
    prepared = prepare_variables(template, variables)
    return RenderedTemplate(
        subject=_render_with_detected_variables(compiled["subject"], prepared),
        html=_render_with_detected_variables(compiled["html"], prepared),
        text=_render_with_detected_variables(compiled["text"], prepared),
    )


def render_text(source: str, template: EmailTemplate, variables: dict[str, Any] | None = None) -> str:
    prepared = prepare_variables(template, variables, extra_sources=(source or "",))
    compiled = _compile_inline_text_template(source or "")
    return _render_with_detected_variables(compiled, prepared)


def _template_content_sources(template: EmailTemplate) -> tuple[str, str, str]:
    return (
        str(template.subject or ""),
        str(template.html_body or ""),
        str(template.text_body or ""),
    )


def _normalize_sendgrid_substitution_key(raw_name: str) -> str:
    normalized = str(raw_name or "").strip()
    if not normalized:
        return ""
    return _SENDGRID_SUBSTITUTION_ALIASES.get(normalized, normalized)


def convert_jinja_variables_to_sendgrid(source: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        substitution_key = _normalize_sendgrid_substitution_key(match.group(1))
        if not substitution_key:
            return match.group(0)
        return f"-{substitution_key}-"

    return _JINJA_VARIABLE_PATTERN.sub(_replace, source or "")


def prepare_substitution_template(template: EmailTemplate) -> SubstitutionTemplate:
    content_type = str(template.content_type or "html").strip().lower() or "html"
    if content_type not in {"html", "text"}:
        content_type = "html"
    subject, html_body, text_body = _template_content_sources(template)
    return SubstitutionTemplate(
        subject=convert_jinja_variables_to_sendgrid(subject),
        html=convert_jinja_variables_to_sendgrid(html_body),
        text=convert_jinja_variables_to_sendgrid(text_body),
        content_type=content_type,
    )


def validate_substitution_template(template: EmailTemplate) -> list[str]:
    warnings: list[str] = []
    subject, html_body, text_body = _template_content_sources(template)
    combined_sources = (subject, html_body, text_body)
    variable_names = _collect_variable_names(*combined_sources)
    has_unsubscribe_placeholder = (
        "unsubscribe_url" in variable_names
        or "{{unsubscribe_url}}" in "\n".join(combined_sources)
        or "-unsubscribe_url-" in "\n".join(combined_sources)
    )
    if bool(getattr(template, "unsubscribe_enabled", False)) and not has_unsubscribe_placeholder:
        warnings.append(
            "模板已启用退订，但未包含 {{unsubscribe_url}} 占位符；发送任务启用退订后仍会添加 List-Unsubscribe 邮件头"
        )
    for label, source in (("主题", subject), ("HTML 正文", html_body), ("纯文本正文", text_body)):
        if not source:
            continue
        for match in _JINJA_ADVANCED_PATTERN.finditer(source):
            tag = str(match.group(1) or "").strip().lower()
            warnings.append(f"{label}包含不兼容的 Jinja2 语法 {{% {tag} %}}，SendGrid Substitution 模式下无法在云端执行该逻辑")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in warnings:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    if deduped:
        logger.warning(
            "template substitution validation warnings template_id=%s count=%s",
            getattr(template, "id", None),
            len(deduped),
        )
    return deduped


def build_static_mail_content(template: EmailTemplate) -> StaticMailContent:
    prepared = prepare_substitution_template(template)
    return StaticMailContent(
        subject=prepared.subject,
        html=prepared.html,
        text=prepared.text,
        content_type=prepared.content_type,
    )


def build_recipient_substitutions(template: EmailTemplate, variables: dict[str, Any] | None = None) -> dict[str, str]:
    """将收件人变量转换为 SendGrid Substitution 键值对（如 -name-、-date-）。"""
    prepared = prepare_variables(template, variables)
    substitutions: dict[str, str] = {}
    for key, value in prepared.items():
        normalized_key = _normalize_sendgrid_substitution_key(key)
        if not normalized_key:
            continue
        substitutions[f"-{normalized_key}-"] = "" if value is None else str(value)
    return substitutions


def template_requires_url_pool(template: EmailTemplate) -> bool:
    """模板正文/主题是否包含追踪链接变量。"""
    subject, html_body, text_body = _template_content_sources(template)
    combined = "\n".join((subject, html_body, text_body))
    if SENDGRID_URL_SUBSTITUTION_KEY in combined or SENDGRID_URL_POOL_URL_SUBSTITUTION_KEY in combined:
        return True
    variable_names = _collect_variable_names(subject, html_body, text_body)
    return bool(variable_names & URL_TRACKING_VARIABLE_NAMES)


def validate_task_url_pool_binding(task) -> str | None:
    """任务未绑定 URL 池但模板需要追踪链接时返回中文错误。"""
    template = getattr(task, "template", None)
    if template is None:
        return None
    if template_requires_url_pool(template) and not getattr(task, "url_pool_id", None):
        template_id = getattr(template, "id", None) or getattr(task, "template_id", "")
        return f"模板 {template_id} 使用了 {{url}} 或 {{url_pool_url}} 追踪链接变量，请为发送任务指定 URL 池，或从模板中移除该变量"
    return None


def apply_substitutions_to_content(content: str, substitutions: dict[str, str]) -> str:
    """本地模拟 SendGrid Substitution 替换（与 SDK 批量发送渲染一致）。"""
    rendered = str(content or "")
    for raw_key, raw_value in substitutions.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if not key.startswith("-"):
            normalized = _normalize_sendgrid_substitution_key(key.strip("-"))
            key = f"-{normalized}-" if normalized else key
        rendered = rendered.replace(key, "" if raw_value is None else str(raw_value))
    return rendered


def render_static_content_with_substitutions(
    static_content: StaticMailContent,
    substitutions: dict[str, str],
) -> RenderedTemplate:
    return RenderedTemplate(
        subject=apply_substitutions_to_content(static_content.subject, substitutions),
        html=apply_substitutions_to_content(static_content.html, substitutions),
        text=apply_substitutions_to_content(static_content.text, substitutions),
    )


PREVIEW_SAMPLE_EMAIL = "preview@example.com"
PREVIEW_SAMPLE_NAME = "预览用户"
PREVIEW_FALLBACK_URL = "https://example.com/preview"


async def build_preview_substitutions(
    template: EmailTemplate,
    *,
    url_pool_id: int | None = None,
    variables: dict[str, Any] | None = None,
) -> dict[str, str]:
    """构造与真实发送一致的 Substitution 示例字典。"""
    from app.core.config import settings
    from app.services.url_service import get_next_url
    from app.utils.security import SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY, build_unsubscribe_url

    render_variables: dict[str, Any] = {
        "email": PREVIEW_SAMPLE_EMAIL,
        "name": PREVIEW_SAMPLE_NAME,
        "company": "示例公司",
        "phone": "13800000000",
        "from_name": "MercuryMail",
        "from_email": "noreply@example.com",
        "campaign_id": str(getattr(template, "campaign_id", "") or ""),
        "date": "",
    }
    if variables:
        for key, value in variables.items():
            render_variables[str(key)] = "" if value is None else str(value)
    substitutions = build_recipient_substitutions(template, render_variables)
    sample_url = PREVIEW_FALLBACK_URL
    if url_pool_id is not None:
        try:
            sample_url = (await get_next_url(int(url_pool_id), task_id=None)).url
        except Exception:
            logger.warning(
                "模板预览 URL 池取样失败 pool_id=%s，使用占位链接",
                url_pool_id,
            )
    substitutions[SENDGRID_URL_SUBSTITUTION_KEY] = sample_url
    substitutions[SENDGRID_URL_POOL_URL_SUBSTITUTION_KEY] = sample_url
    if bool(getattr(template, "unsubscribe_enabled", False)):
        substitutions[SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY] = build_unsubscribe_url(
            settings.public_base_url,
            PREVIEW_SAMPLE_EMAIL,
            0,
            None,
        )
    else:
        substitutions.pop(SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY, None)
    return substitutions


async def preview_substitution_template(
    template: EmailTemplate,
    *,
    url_pool_id: int | None = None,
    variables: dict[str, Any] | None = None,
) -> RenderedTemplate:
    """Substitution 模式预览：构建单收件人 SDK Mail 对象，再本地模拟 Substitution 渲染（与 SendGrid 服务端一致）。"""
    static_content = build_static_mail_content(template)
    substitutions = await build_preview_substitutions(
        template,
        url_pool_id=url_pool_id,
        variables=variables,
    )
    unsubscribe_url = str(substitutions.get("-unsubscribe_url-", "") or "").strip()
    # 延迟导入，避免 sendgrid_sdk ↔ template_service 循环依赖
    from app.services.sendgrid_sdk import build_batch_mail, build_unsubscribe_headers

    preview_from_email = str(substitutions.get("-from_email-", "") or "noreply@example.com").strip() or "noreply@example.com"
    preview_from_name = str(substitutions.get("-from_name-", "") or "MercuryMail").strip() or "MercuryMail"
    recipients_data = [{
        "email": PREVIEW_SAMPLE_EMAIL,
        "substitutions": substitutions,
        "headers": build_unsubscribe_headers(unsubscribe_url) if bool(getattr(template, "unsubscribe_enabled", False)) else {},
    }]
    mail = build_batch_mail(
        from_email=preview_from_email,
        from_name=preview_from_name,
        static_content=static_content,
        recipients_data=recipients_data,
    )
    if not mail.personalizations:
        raise ValueError("预览邮件未包含有效 Personalization")
    logger.debug(
        "模板预览已构建 SDK Mail：personalizations=%s substitutions=%s（最终 HTML 由本地 Substitution 模拟渲染，与 SendGrid 服务端一致）",
        len(mail.personalizations),
        len(substitutions),
    )
    # SendGrid SDK 不在本地执行 Substitution 渲染，使用与发送相同的占位符替换逻辑
    return render_static_content_with_substitutions(static_content, substitutions)
