"""SendLog 发送路径辅助：硬弹判断（与 Event Poller 展示字段完全解耦）。"""

from __future__ import annotations

import re

# 仅保留“收件人邮箱本身不可达”的明确证据；禁止用 HTTP 状态码或宽泛词误判硬弹。
HARD_BOUNCE_MARKERS = (
    "hard bounce",
    "invalid email",
    "recipient not found",
    "mailbox unavailable",
    "mailbox not found",
    "email address does not exist",
    "the email address does not exist",
    "domain does not exist",
    "does not exist as a valid user",
    "user unknown",
    "unknown user",
    "no such user",
    "永久性投递失败",
    "邮箱不存在",
    "邮箱地址无效",
    "收件人不存在",
    "收件人无效",
    "域名不存在",
    "邮箱不可用",
    "邮箱格式",
    "无效邮箱",
    "硬退信",
)

# SMTP 永久失败需带邮箱语境，避免把纯数字（如耗时 550ms、消息 ID）误判为硬弹。
_SMTP_HARD_BOUNCE_RE = re.compile(
    r"(?:\b55[0-4]\b).{0,80}(?:user|mailbox|recipient|address|account|邮箱|收件人|地址)"
    r"|(?:user|mailbox|recipient|address|account|邮箱|收件人|地址).{0,80}(?:\b55[0-4]\b)",
    re.IGNORECASE,
)

# 这些 HTTP 状态属于传输/鉴权/限流/服务端瞬时问题，绝不能单独当作收件人硬弹。
_NON_HARD_BOUNCE_STATUS_CODES = frozenset({0, 401, 403, 408, 429, 500, 502, 503, 504})


def is_hard_bounce_error(error_message: str, status_code: int | None = None) -> bool:
    """判断是否为收件人级硬弹（邮箱永久不可达）。

    多任务高压下常见的代理 403、传输超时(status=0)、429、5xx 不得记为硬弹，
    否则会跳过软重试并把整批收件人永久记入 SDK 失败。

    status_code=None 表示落盘时仅按文案判断（仍排除传输层文案）。
    """
    if status_code is not None:
        code = int(status_code)
        if code in _NON_HARD_BOUNCE_STATUS_CODES:
            return False

    lowered = str(error_message or "").lower()
    if not lowered.strip():
        return False

    # 传输层/代理失败文案优先排除，避免命中宽泛子串。
    transport_markers = (
        "代理连接失败",
        "连接超时",
        "读取响应超时",
        "传输错误",
        "timed out",
        "timeout",
        "connection reset",
        "connection refused",
        "proxy error",
        "senderpool",
        "工作队列已满",
        "发送槽等待超时",
        "客户端锁等待超时",
        "请求过于频繁",
        "too many requests",
        "sdk 批量 worker 异常",
        "sdk 批量发送超时",
    )
    if any(marker in lowered for marker in transport_markers):
        return False

    if any(marker in lowered for marker in HARD_BOUNCE_MARKERS):
        return True
    return bool(_SMTP_HARD_BOUNCE_RE.search(lowered))
