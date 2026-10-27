"""SendGrid 错误信息中文化：写入 SendLog 前翻译，日志保留未映射原文便于补充。"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

_PROXY_AUTH_PATTERN = re.compile(r"https?://[^@\s/]+@[^\s]+", re.IGNORECASE)
_BEARER_PATTERN = re.compile(r"Bearer\s+\S+", re.IGNORECASE)
_SENDGRID_KEY_PATTERN = re.compile(r"SG\.[\w-]+")

# 按优先级排列：较长模式优先匹配
_SENDGRID_LIMIT_PATTERNS = (
    "you have exceeded your messaging limits",
    "exceeded your messaging limits",
    "messaging limits",
    "maximum credits exceeded",
    "credits exceeded",
    "credit limit exceeded",
    "sending limit exceeded",
    "daily sending limit",
    "monthly sending limit",
)

_SENDGRID_ERROR_PATTERNS: list[tuple[str, str]] = [
    ("you have exceeded your messaging limits", "SendGrid 发送额度已耗尽，请停止当前任务并等待额度恢复"),
    ("exceeded your messaging limits", "SendGrid 发送额度已耗尽，请停止当前任务并等待额度恢复"),
    ("messaging limits", "SendGrid 发送额度已耗尽，请停止当前任务并等待额度恢复"),
    ("maximum credits exceeded", "SendGrid 发送点数已耗尽，请停止当前任务并等待额度恢复"),
    ("credits exceeded", "SendGrid 发送点数已耗尽，请停止当前任务并等待额度恢复"),
    ("credit limit exceeded", "SendGrid 发送点数已耗尽，请停止当前任务并等待额度恢复"),
    ("the email address does not exist", "邮箱不存在"),
    ("email address does not exist", "邮箱不存在"),
    ("recipient address rejected", "收件人地址被拒绝"),
    ("mailbox not found", "邮箱不存在"),
    ("mailbox unavailable", "邮箱不可用"),
    ("user unknown", "收件人不存在"),
    ("no such user", "收件人不存在"),
    ("recipient not found", "收件人不存在"),
    ("domain does not exist", "域名不存在"),
    ("domain not found", "域名不存在"),
    ("invalid email address", "邮箱地址无效"),
    ("invalid recipient", "收件人无效"),
    ("invalid email", "邮箱地址无效"),
    ("does not exist", "邮箱不存在"),
    ("sending limit exceeded", "发送限额已超"),
    ("daily sending limit", "每日发送限额已超"),
    ("monthly sending limit", "每月发送限额已超"),
    ("temporarily unavailable", "服务暂时不可用"),
    ("service unavailable", "服务暂时不可用"),
    ("rate limit exceeded", "请求过于频繁"),
    ("too many requests", "请求过于频繁"),
    ("invalid api key", "API 密钥无效"),
    ("api key is invalid", "API 密钥无效"),
    ("unauthorized", "未授权"),
    ("forbidden", "权限不足"),
    ("bad request", "请求参数错误"),
    ("internal server error", "SendGrid 内部错误"),
    ("hard bounce", "硬退信"),
    ("soft bounce", "软退信"),
    ("blocked", "邮件被拒收"),
    ("bounce", "邮件被退回"),
    ("timeout", "连接超时"),
    ("timed out", "连接超时"),
    ("connection refused", "连接被拒绝"),
    ("connection reset", "连接被重置"),
    ("proxy connection", "代理连接失败"),
    ("proxy error", "代理连接错误"),
    ("ssl", "SSL 握手失败"),
    ("certificate", "证书校验失败"),
    ("payload too large", "请求体过大"),
    ("content rejected", "邮件内容被拒绝"),
    ("spam", "邮件被判定为垃圾邮件"),
    ("suppression", "收件人已在退订/抑制列表"),
    ("unsubscribed", "收件人已退订"),
]

_STATUS_PREFIX_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^http\s*(\d{3})\b", re.IGNORECASE), "HTTP 状态 {code}"),
    (re.compile(r"^(\d{3})\s*[-:]", re.IGNORECASE), "HTTP 状态 {code}"),
]


def _contains_english(text: str) -> bool:
    return bool(re.search(r"[A-Za-z]{3,}", str(text or "")))


def translate_sendgrid_error(original_message: str) -> str:
    """将 SendGrid 常见英文错误翻译为中文；无法匹配时返回去除首尾空白后的原文。"""
    text = str(original_message or "").strip()
    if not text:
        return "邮件发送失败，请稍后重试"
    lowered = text.lower()
    for pattern, chinese in _SENDGRID_ERROR_PATTERNS:
        if pattern in lowered:
            return chinese
    for regex, template in _STATUS_PREFIX_PATTERNS:
        match = regex.match(text.strip())
        if match:
            code = match.group(1)
            remainder = text[match.end() :].strip(" :-")
            if remainder:
                nested = translate_sendgrid_error(remainder)
                if nested != remainder:
                    return f"{template.format(code=code)}：{nested}"
            return template.format(code=code)
    return text


def sanitize_send_log_secrets(message: str) -> str:
    """仅剥离敏感信息，不做英文泛化替换（供发送路径保留原文做硬弹判断）。"""
    text = str(message or "").strip()
    if not text:
        return ""
    if _PROXY_AUTH_PATTERN.search(text) or ("://" in text and "@" in text):
        return "代理连接失败，请检查代理配置"
    if _BEARER_PATTERN.search(text) or _SENDGRID_KEY_PATTERN.search(text):
        return "SendGrid 认证失败，请检查 API 密钥"
    lowered = text.lower()
    if any(marker in lowered for marker in ("password", "api_key", "apikey", "secret")):
        return "凭据校验失败，请检查配置"
    return text


def is_sendgrid_messaging_limit_error(message: str) -> bool:
    """识别 SendGrid 发送额度/消息量限制，避免误归类为 401/403 鉴权失败。"""
    text = str(message or "").strip().lower()
    if not text:
        return False
    return any(pattern in text for pattern in _SENDGRID_LIMIT_PATTERNS)


def normalize_send_log_error(message: str, *, max_length: int = 2000) -> str:
    """翻译并规范化写入 SendLog 的错误文案（数据库仅存中文为主）。"""
    original = sanitize_send_log_secrets(str(message or "").strip())
    if not original:
        return "邮件发送失败，请稍后重试"
    translated = translate_sendgrid_error(original)
    if translated != original:
        return translated[: max(1, int(max_length))]
    if _contains_english(original):
        logger.info("SendGrid 错误未映射，原文：%s", original[:500])
        suffix = f"（原始: {original[:400]}）" if len(original) <= 400 else f"（原始: {original[:397]}…）"
        return f"邮件发送失败{suffix}"[: max(1, int(max_length))]
    return original[: max(1, int(max_length))]
