from datetime import datetime

from sqlalchemy import Boolean, Column, Date, DateTime, Float, ForeignKey, Index, Integer, JSON, String, Text, Time, UniqueConstraint, text
from sqlalchemy.orm import relationship

from app.core.database import Base


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(80), unique=True, index=True, nullable=False)
    email = Column(String(255), nullable=True)
    password_hash = Column(String(255), nullable=False)
    is_admin = Column(Boolean, default=True)
    is_super_admin = Column(Boolean, default=False)
    is_visible = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True, nullable=False)
    token_version = Column(Integer, default=0, nullable=False)
    deleted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Proxy(Base):
    __tablename__ = "proxies"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(120), nullable=False)
    host = Column(String(255), nullable=False)
    port = Column(Integer, default=8080)
    protocol = Column(String(16), default="HTTP")
    type = Column(String(16), default="HTTP")
    username = Column(String(255), default="")
    password = Column(String(255), default="")
    is_active = Column(Boolean, default=True)
    max_connections = Column(Integer, default=5)
    last_checked_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Channel(Base):
    __tablename__ = "channels"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(160), nullable=False)
    type = Column(String(40), default="smtp")
    host = Column(String(255), default="")
    port = Column(Integer, default=587)
    username = Column(String(255), default="")
    password = Column(String(255), default="")
    smtp_password = Column(Text, default="")
    api_key = Column(Text, default="")
    sender_email = Column(String(255), default="")
    sender_name = Column(String(255), default="")
    sender_emails_json = Column(Text, default="[]")
    daily_limit = Column(Integer, default=None, nullable=True)
    hourly_limit = Column(Integer, default=None, nullable=True)
    per_minute_limit = Column(Integer, default=None, nullable=True)
    second_limit = Column(Integer, default=None, nullable=True)
    use_proxy = Column(Boolean, default=False)
    proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True)
    use_tls = Column(Boolean, default=True)
    enabled = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True)
    last_error = Column(Text, default="")
    proxy_outbound_ip = Column(String(45), nullable=True)
    health_status = Column(String(20), default="healthy")
    last_health_check = Column(DateTime, nullable=True)
    health_error = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    proxy = relationship("Proxy")


class URLPool(Base):
    __tablename__ = "url_pools"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(160), unique=True, nullable=False)
    urls = Column(Text, default="[]")
    jump_enabled = Column(Boolean, default=False)
    jump_domain = Column(Text, default="")
    random_prefix = Column(String(80), default="uid")
    random_length = Column(Integer, default=12)
    random_chars = Column(String(255), default="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    next_index = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


UrlPool = URLPool


class URLItem(Base):
    __tablename__ = "url_items"
    id = Column(Integer, primary_key=True, index=True)
    pool_id = Column(Integer, ForeignKey("url_pools.id"), index=True, nullable=False)
    url = Column(Text, nullable=False)
    original_url = Column(Text, default="")
    item_index = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    pool = relationship("URLPool")


class UrlImportTask(Base):
    __tablename__ = "url_import_tasks"
    id = Column(Integer, primary_key=True, index=True)
    pool_id = Column(Integer, ForeignKey("url_pools.id"), index=True, nullable=False)
    status = Column(String(40), default="pending", index=True)
    total_rows = Column(Integer, default=0)
    processed_rows = Column(Integer, default=0)
    error_message = Column(Text, default="")
    source_filename = Column(String(255), default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    pool = relationship("URLPool")


class EmailTemplate(Base):
    __tablename__ = "email_templates"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(160), unique=True, nullable=False)
    subject = Column(String(500), default="")
    html_body = Column(Text, default="")
    text_body = Column(Text, default="")
    content_type = Column(String(16), default="html")
    random_length = Column(Integer, default=16)
    random_chars = Column(String(255), default="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    random_rules_json = Column(Text, default="{}")
    campaign_id = Column(String(120), default="")
    unsubscribe_enabled = Column(Boolean, default=False)
    unsubscribe_url = Column(Text, default="")
    unsubscribe_description = Column(String(160), default="一键退订")
    sendgrid_substitution_enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


Template = EmailTemplate


class RecipientList(Base):
    __tablename__ = "recipient_lists"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(160), unique=True, nullable=False)
    description = Column(Text, default="")
    source_filename = Column(String(255), default="")
    total_count = Column(Integer, default=0)
    valid_count = Column(Integer, default=0)
    invalid_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Recipient(Base):
    __tablename__ = "recipients"
    __table_args__ = (
        UniqueConstraint("list_id", "email", name="uq_recipient_list_email"),
        Index("ix_recipients_list_row_number", "list_id", "row_number", "id"),
        Index("ix_recipients_prefetch_list_row_valid_unsub", "list_id", "row_number", "is_valid", "is_unsubscribed"),
        Index(
            "ix_recipients_claim_covering_v17",
            "list_id",
            "row_number",
            "id",
            "email",
            postgresql_where=text("is_valid = TRUE AND is_unsubscribed = FALSE AND email <> '' AND row_number > 0"),
        ),
        Index(
            "ix_recipients_claim_covering_v18",
            "list_id",
            "row_number",
            "id",
            "normalized_email",
            postgresql_include=("email", "name", "company", "phone", "tags", "custom_fields_json", "is_valid", "is_unsubscribed", "created_at"),
            postgresql_where=text("is_valid = TRUE AND is_unsubscribed = FALSE AND email <> '' AND row_number > 0 AND normalized_email <> ''"),
        ),
        Index("ix_recipients_normalized_email", "normalized_email"),
    )
    id = Column(Integer, primary_key=True, index=True)
    list_id = Column(Integer, ForeignKey("recipient_lists.id"), index=True, nullable=False)
    email = Column(String(255), index=True, nullable=False)
    normalized_email = Column(String(255), default="", nullable=False, index=True)
    row_number = Column(Integer, default=0)
    name = Column(String(255), default="")
    company = Column(String(255), default="")
    phone = Column(String(80), default="")
    tags = Column(Text, default="")
    custom_fields_json = Column(Text, default="{}")
    is_valid = Column(Boolean, default=True)
    is_unsubscribed = Column(Boolean, default=False)
    validation_error = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    recipient_list = relationship("RecipientList")


class Task(Base):
    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(160), nullable=False)
    template_id = Column(Integer, ForeignKey("email_templates.id", ondelete="SET NULL"), nullable=True)
    recipient_list_id = Column(Integer, ForeignKey("recipient_lists.id", ondelete="SET NULL"), nullable=True)
    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="SET NULL"), nullable=True)
    url_pool_id = Column(Integer, ForeignKey("url_pools.id"), nullable=True)
    status = Column(String(40), default="draft")
    concurrency = Column(Integer, default=1)
    auto_concurrency = Column(Boolean, default=False, comment="是否启用自适应并发调整")
    batch_size = Column(Integer, default=100)  # 数据库领取批次（claim），非 SendGrid HTTP 批量发送
    send_http_batch_size = Column(Integer, default=50)  # SendGrid SDK 每 HTTP 请求收件人数
    rate_limit = Column(Float, default=0.0)
    retry_count = Column(Integer, default=3)
    monitor_emails_json = Column(Text, default="[]")
    monitor_emails = Column(Text, default="[]")
    monitor_interval = Column(Integer, default=100)
    monitor_count = Column(Integer, default=1)
    total_count = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    last_claimed_row_number = Column(Integer, default=0)
    last_claimed_recipient_id = Column(Integer, default=0)
    unsubscribe_enabled = Column(Boolean, default=False)
    sender_rotation_enabled = Column(Boolean, default=False, comment="是否启用发件人轮询")
    scheduled_at = Column(DateTime, nullable=True)
    timezone = Column(String(64), default="Asia/Shanghai", nullable=False)
    run_message = Column(Text, default="")
    runner_instance_id = Column(String(64), nullable=True, index=True)
    runner_lease_until = Column(DateTime, nullable=True, index=True)
    run_generation = Column(Integer, default=0, nullable=False)
    deleted_at = Column(DateTime, nullable=True, index=True, comment="软删除时间；非空表示任务已删除但 SendLog 保留")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    template = relationship("EmailTemplate")
    recipient_list = relationship("RecipientList")
    channel = relationship("Channel")
    url_pool = relationship("URLPool")


class TaskStatsSnapshot(Base):
    __tablename__ = "task_stats_snapshots"
    __table_args__ = (
        UniqueConstraint("task_id", name="ux_task_stats_snapshots_task_id"),
        Index("ix_task_stats_snapshots_updated_at", "updated_at"),
    )
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(String(40), default="draft", nullable=False)
    current_rate = Column(Float, default=0.0, nullable=False)
    target_rate = Column(Float, default=0.0, nullable=False)
    success_count = Column(Integer, default=0, nullable=False)
    failed_count = Column(Integer, default=0, nullable=False)
    delivered_count = Column(Integer, default=0, nullable=False)
    hard_bounced_count = Column(Integer, default=0, nullable=False)
    soft_bounced_count = Column(Integer, default=0, nullable=False)
    total_count = Column(Integer, default=0, nullable=False)
    pending_count = Column(Integer, default=0, nullable=False)
    recent_429_count = Column(Integer, default=0, nullable=False)
    admitted_workers = Column(Integer, default=0, nullable=False)
    active_http_inflight = Column(Integer, default=0, nullable=False)
    proxy_rtt_ms = Column(Float, default=0.0, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CompactOutcome(Base):
    __tablename__ = "compact_outcomes"
    __table_args__ = (
        UniqueConstraint("task_id", "recipient_id", name="ux_compact_outcomes_task_recipient"),
        Index("ix_compact_outcomes_task_status", "task_id", "status"),
        Index(
            "ux_compact_outcomes_monitor_event",
            "task_id",
            "provider_message_id",
            unique=True,
            postgresql_where=text("log_type = 'monitor' AND provider_message_id <> ''"),
        ),
    )
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False, index=True)
    recipient_id = Column(Integer, ForeignKey("recipients.id", ondelete="SET NULL"), nullable=True, index=True)
    status = Column(String(20), nullable=False)
    provider_message_id = Column(String(255), default="", nullable=False)
    attempt_count = Column(Integer, default=0, nullable=False)
    log_type = Column(String(40), default="regular", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class SendLog(Base):
    __tablename__ = "send_logs"
    __table_args__ = (
        Index("ix_send_logs_task_regular_recipient", "task_id", "log_type", "recipient_id"),
        Index("ix_send_logs_task_regular_email", "task_id", "log_type", "email"),
        Index("ix_send_logs_task_regular_recipient_email", "task_id", "log_type", "recipient_id", "email"),
        Index("ix_send_logs_prefetch_sent_recipient", "task_id", "recipient_id", "status", "log_type"),
        Index("ix_send_logs_task_failure_page", "task_id", "log_type", "status", "created_at", "id"),
        Index(
            "ux_send_logs_regular_task_recipient",
            "task_id",
            "recipient_id",
            unique=True,
            postgresql_where=text("log_type = 'regular' AND recipient_id IS NOT NULL"),
        ),
    )
    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), index=True)
    recipient_id = Column(Integer, ForeignKey("recipients.id", ondelete="SET NULL"), nullable=True, index=True)
    email = Column(String(255), index=True)
    sender_email = Column(String(255), nullable=True)
    status = Column(String(40), default="pending")
    final_status = Column(String(40), nullable=True, index=True)
    bounce_type = Column(String(40), nullable=True)
    retry_state = Column(String(20), default="none", index=True)
    abort_retry = Column(Boolean, default=False)
    error = Column(Text, default="")
    error_message = Column(Text, default="")
    response_text = Column(Text, default="")
    attempt_count = Column(Integer, default=0)
    # 普通发送日志没有监控里程碑；使用 NULL 区分普通日志与第 0 个监控里程碑。
    monitor_trigger_count = Column(Integer, default=None, nullable=True, index=True)
    provider_message_id = Column(String(255), default="", nullable=True, index=True)
    extra_data = Column(JSON, nullable=True)
    sg_event_id = Column(String(255), nullable=True)
    # 对应宪法中的 type 字段；值为 monitor 时表示监控抽样邮件（不计入任务成功统计）
    log_type = Column(String(40), default="regular", index=True)
    egress_ip = Column(String(64), default="")
    proxy_info = Column(String(255), nullable=True)
    outbound_ip = Column(String(45), nullable=True)
    sent_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    task = relationship("Task")
    recipient = relationship("Recipient")



class Unsubscribe(Base):
    __tablename__ = "unsubscribes"
    __table_args__ = (
        Index("ix_unsubscribes_normalized_email", "normalized_email"),
        Index("ix_unsubscribes_email", "email"),
        Index("ix_unsubscribes_normalized_email_email", "normalized_email", "email"),
    )
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), index=True, nullable=False)
    normalized_email = Column(String(255), index=True, nullable=True)
    recipient_id = Column(Integer, ForeignKey("recipients.id"), nullable=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=True)
    reason = Column(Text, default="")
    ip_address = Column(String(80), default="")
    user_agent = Column(Text, default="")
    unsubscribed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SystemConfig(Base):
    __tablename__ = "system_configs"
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(120), unique=True, index=True, nullable=False)
    value = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AIConfig(Base):
    __tablename__ = "ai_configs"
    id = Column(Integer, primary_key=True, index=True)
    openai_api_key = Column(Text, default="")
    api_base = Column(String(500), default="https://api.openai.com/v1")
    model = Column(String(120), default="openai/gpt-3.5-turbo")
    model_name = Column(String(255), default="openai/gpt-3.5-turbo")
    max_tokens = Column(Integer, default=1200)
    temperature = Column(Float, default=0.7)
    outbound_proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True, index=True)
    ai_use_proxy = Column(Boolean, default=False, nullable=False)
    ai_proxy_type = Column(String(16), default="", nullable=False)
    ai_proxy_host = Column(String(255), default="", nullable=False)
    ai_proxy_port = Column(Integer, nullable=True)
    ai_proxy_username = Column(String(255), default="", nullable=False)
    ai_proxy_password = Column(Text, default="")
    ai_proxy_url = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    outbound_proxy = relationship("Proxy", foreign_keys=[outbound_proxy_id])


class WarmupPlan(Base):
    __tablename__ = "warmup_plans"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(160), nullable=False)
    channel_id = Column(Integer, ForeignKey("channels.id", ondelete="SET NULL"), index=True, nullable=True)
    proxy_id = Column(Integer, ForeignKey("proxies.id"), nullable=True)
    start_count = Column(Integer, default=5)
    daily_increase_rate = Column(Float, default=0.2)
    max_daily_count = Column(Integer, default=100)
    base_interval_seconds = Column(Integer, default=60)
    interval_jitter_seconds = Column(Integer, default=10)
    scheduled_time = Column(Time, nullable=False)
    timezone = Column(String(64), default="Asia/Shanghai", nullable=False)
    status = Column(String(40), default="draft", index=True)
    running = Column(Boolean, default=False, index=True)
    run_progress = Column(Float, default=0)
    run_message = Column(Text, default="")
    run_started_at = Column(DateTime, nullable=True)
    run_finished_at = Column(DateTime, nullable=True)
    started_at = Column(DateTime, nullable=True)
    total_days = Column(Integer, default=0, nullable=False)
    total_sent = Column(Integer, default=0)
    last_run_date = Column(Date, nullable=True)
    last_daily_target = Column(Integer, nullable=True)
    prompt = Column(Text, default="")
    content_mode = Column(String(16), default=None, nullable=True)
    content_language = Column(String(8), default="zh")
    tone = Column(String(80), default="自然日常")
    must_include = Column(Text, default="")
    avoid = Column(Text, default="")
    fidelity_mode = Column(String(16), default="strict")
    sender_rotation_enabled = Column(Boolean, default=False, comment="是否启用发件人轮询")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    channel = relationship("Channel")
    proxy = relationship("Proxy")
    content = relationship("WarmupContent", uselist=False, back_populates="plan", cascade="all, delete-orphan")
    recipients = relationship("WarmupRecipient", back_populates="plan", cascade="all, delete-orphan")
    logs = relationship("WarmupLog", back_populates="plan", cascade="all, delete-orphan")
    system_logs = relationship("WarmupSystemLog", back_populates="plan", cascade="all, delete-orphan")


class WarmupSystemLog(Base):
    __tablename__ = "warmup_system_logs"
    id = Column(Integer, primary_key=True, index=True)
    plan_id = Column(Integer, ForeignKey("warmup_plans.id"), index=True, nullable=False)
    level = Column(String(20), default="info", index=True)
    message = Column(Text, default="")
    progress = Column(Float, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    plan = relationship("WarmupPlan", back_populates="system_logs")


class WarmupRecipient(Base):
    __tablename__ = "warmup_recipients"
    __table_args__ = (UniqueConstraint("plan_id", "email", name="uq_warmup_recipient_plan_email"),)
    id = Column(Integer, primary_key=True, index=True)
    plan_id = Column(Integer, ForeignKey("warmup_plans.id"), index=True, nullable=False)
    email = Column(String(255), index=True, nullable=False)
    sent = Column(Boolean, default=False)
    sent_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    plan = relationship("WarmupPlan", back_populates="recipients")


class WarmupContent(Base):
    __tablename__ = "warmup_contents"
    id = Column(Integer, primary_key=True, index=True)
    plan_id = Column(Integer, ForeignKey("warmup_plans.id"), unique=True, index=True, nullable=False)
    subject = Column(String(500), default="")
    html_body = Column(Text, default="")
    text_body = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    plan = relationship("WarmupPlan", back_populates="content")


class WarmupLog(Base):
    __tablename__ = "warmup_logs"
    id = Column(Integer, primary_key=True, index=True)
    plan_id = Column(Integer, ForeignKey("warmup_plans.id"), index=True, nullable=False)
    email = Column(String(255), index=True, nullable=False)
    status = Column(String(20), default="success", index=True)
    error_message = Column(Text, default="")
    sent_at = Column(DateTime, nullable=True, index=True)
    plan = relationship("WarmupPlan", back_populates="logs")
