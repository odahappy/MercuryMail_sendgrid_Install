import re

_PROXY_AUTH_PATTERN = re.compile(r"https?://[^@\s/]+@[^\s]+", re.IGNORECASE)
_BEARER_PATTERN = re.compile(r"Bearer\s+\S+", re.IGNORECASE)
_SENDGRID_KEY_PATTERN = re.compile(r"SG\.[\w-]+")

_USER_ERROR_TRANSLATIONS = {
    "send failed": "发送失败",
    "http error": "HTTP 错误",
    "transport error": "传输错误",
    "internal server error": "服务器内部错误，请稍后重试",
    "not found": "请求的资源不存在",
    "unauthorized": "未授权访问",
    "forbidden": "权限不足",
    "connection timeout": "连接超时",
    "connection timed out": "连接超时",
    "timed out": "操作超时，请稍后重试",
    "timeout": "操作超时，请稍后重试",
    "rate limit exceeded": "请求过于频繁，请稍后重试",
    "permission denied": "权限不足",
    "validation error": "数据校验失败",
    "export failed": "导出失败，请稍后重试",
    "file not found": "文件不存在",
    "invalid api key": "API 密钥无效",
    "task not found": "任务不存在",
    "channel not found": "通道不存在",
    "proxy not found": "代理不存在",
    "url cannot be empty": "URL 不能为空",
    "url host cannot be empty": "URL 主机名不能为空",
    "url pool cache is closed": "URL 池缓存已关闭",
    "no pending recipients": "无待发送收件人",
    "shutdown requested": "收到停机请求",
    "task disappeared": "任务已不存在",
    "channel daily limit reached": "通道日限额已达上限",
    "channel hourly limit reached": "通道小时限额已达上限",
    "proxy reload failed": "代理热更新失败",
    "one-click unsubscribe": "一键退订",
    "uploaded from": "从文件上传",
    "unknown script error": "未知脚本错误",
    "unhandled promise rejection": "未处理的 Promise 拒绝",
}


def _translate_known_user_error(text: str) -> str | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    lowered = normalized.lower()
    if lowered in _USER_ERROR_TRANSLATIONS:
        return _USER_ERROR_TRANSLATIONS[lowered]
    for english, chinese in _USER_ERROR_TRANSLATIONS.items():
        if english in lowered and len(english) >= 8:
            return normalized.replace(english, chinese).replace(english.title(), chinese)
    if lowered.startswith("status changed to "):
        status = normalized.split("status changed to ", 1)[-1].strip()
        status_labels = {
            "draft": "草稿",
            "scheduled": "等待中",
            "pending": "等待中",
            "running": "运行中",
            "paused": "已暂停",
            "finished": "已完成",
            "stopped": "已终止",
            "failed": "已失败",
        }
        return f"状态已变更为 {status_labels.get(status, status)}"
    return None


def sanitize_user_error_message(message: str, *, max_length: int = 500) -> str:
    text = str(message or "").strip()
    if not text:
        return "操作失败，请稍后重试"
    translated = _translate_known_user_error(text)
    if translated:
        text = translated
    if _PROXY_AUTH_PATTERN.search(text) or ("://" in text and "@" in text):
        return "代理连接失败，请检查代理配置"
    if _BEARER_PATTERN.search(text) or _SENDGRID_KEY_PATTERN.search(text):
        return "SendGrid 认证失败，请检查 API 密钥"
    lowered = text.lower()
    if any(marker in lowered for marker in ("password", "api_key", "apikey", "secret")):
        return "凭据校验失败，请检查配置"
    if re.fullmatch(r"[\x00-\x7F]+", text) and re.search(r"[A-Za-z]{3,}", text):
        return "操作失败，请稍后重试"
    return text[: max(1, int(max_length))]


def httpx_transport_user_message(exc: Exception, *, status_code: int = 0, body: str = "") -> str:
    if isinstance(exc, Exception):
        exc_name = type(exc).__name__
        if exc_name == "ProxyError":
            return "代理连接失败，请检查代理地址、端口、账号或密码"
        if exc_name == "ConnectTimeout":
            return "连接超时，请检查网络或代理连通性"
        if exc_name == "ReadTimeout":
            return "读取响应超时，请稍后重试"
        if exc_name == "ConnectError":
            return "无法连接 SendGrid，请检查网络或代理配置"
    normalized_status = int(status_code or 0)
    if normalized_status == 401:
        return "SendGrid 认证失败，请检查 API 密钥"
    if normalized_status == 403:
        return "SendGrid 权限不足，请检查 API 密钥权限"
    if normalized_status == 429:
        return "SendGrid 请求过于频繁，请稍后重试"
    if 400 <= normalized_status < 500:
        return "SendGrid 请求被拒绝，请检查邮件内容或发件人配置"
    if normalized_status >= 500:
        return "SendGrid 服务暂时不可用，请稍后重试"
    sanitized_body = sanitize_user_error_message(body)
    if sanitized_body != "操作失败，请稍后重试":
        return sanitized_body
    return "邮件发送失败，请稍后重试"
