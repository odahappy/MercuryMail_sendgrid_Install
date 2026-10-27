"""SendLog 热表/归档表共享常量。

log_type 说明：数据库列名为 log_type；宪法文档中亦称 type。
  - regular：常规群发投递
  - monitor：监控抽样邮件（不计入任务成功统计）
"""

from __future__ import annotations

# 宪法 type 字段在 ORM/数据库中的实际列名
LOG_TYPE_DB_COLUMN = "log_type"
LOG_TYPE_CONSTITUTION_ALIAS = "type"

LOG_TYPE_REGULAR = "regular"
LOG_TYPE_MONITOR = "monitor"
SDK_SEND_STATUSES: tuple[str, ...] = ("sent", "success", "failed", "bounced", "unknown")
FINAL_SEND_STATUSES: tuple[str, ...] = (
    "processed",
    "delivered",
    "bounced",
    "blocked",
    "dropped",
    "deferred",
    "spamreported",
    "unsubscribed",
)

SEND_LOG_ARCHIVE_COLUMNS: tuple[str, ...] = (
    "id",
    "task_id",
    "recipient_id",
    "email",
    "sender_email",
    "status",
    "final_status",
    "retry_state",
    "bounce_type",
    "abort_retry",
    "error",
    "error_message",
    "response_text",
    "attempt_count",
    "provider_message_id",
    "extra_data",
    "sg_event_id",
    "log_type",
    "egress_ip",
    "proxy_info",
    "outbound_ip",
    "sent_at",
    "created_at",
)

FINAL_SEND_STATUS_SQL = ", ".join(f"'{item}'" for item in FINAL_SEND_STATUSES)
