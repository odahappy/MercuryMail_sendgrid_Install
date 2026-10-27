import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from functools import wraps
from time import monotonic

from sqlalchemy import event, text
from sqlalchemy.schema import CreateIndex, CreateTable
from sqlalchemy.exc import (
    DBAPIError,
    OperationalError,
    PendingRollbackError,
    ProgrammingError,
    TimeoutError as SQLAlchemyTimeoutError,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from app.core.config import settings

logger = logging.getLogger(__name__)

DB_COMMIT_RETRY_ATTEMPTS = 5
DB_COMMIT_RETRY_MIN_DELAY_SECONDS = 0.05
DB_COMMIT_RETRY_MAX_DELAY_SECONDS = 0.2
DB_SESSION_RETRY_ATTEMPTS = 2
DB_SESSION_RETRY_MIN_DELAY_SECONDS = 0.1
DB_SESSION_RETRY_MAX_DELAY_SECONDS = 0.3
DB_INIT_MAX_ATTEMPTS = 30
DB_INIT_RETRY_DELAY_SECONDS = 1.0
IDLE_TRANSACTION_SWEEP_INTERVAL_SECONDS = 60.0
IDLE_TRANSACTION_MAX_AGE_SECONDS = 120
PREFETCH_PLAN_REQUIRED_INDEXES = ("ix_recipients_claim_covering_v18", "ix_recipients_claim_row_id_v17")
DB_INIT_CORE_TABLES = ("tasks", "users", "send_logs", "channels", "recipients", "unsubscribes", "warmup_plans")
DB_INIT_REQUIRED_COLUMNS = {
    "tasks": (
        "template_ids",
        "template_rotation_mode",
        "recipient_list_id",
        "last_claimed_row_number",
        "last_claimed_recipient_id",
        "sender_email",
        "runner_instance_id",
        "runner_lease_until",
        "run_generation",
    ),
    "warmup_plans": ("started_at", "total_days"),
    "recipients": ("list_id", "email", "normalized_email", "row_number", "is_valid", "is_unsubscribed"),
    "unsubscribes": ("email", "normalized_email"),
    "send_logs": (
        "task_id",
        "recipient_id",
        "email",
        "sender_email",
        "status",
        "final_status",
        "bounce_type",
        "retry_state",
        "abort_retry",
        "error",
        "error_message",
        "response_text",
        "attempt_count",
        "monitor_trigger_count",
        "provider_message_id",
        "extra_data",
        "sg_event_id",
        "log_type",
        "egress_ip",
        "proxy_info",
        "outbound_ip",
        "sent_at",
        "created_at",
    ),
}
DB_WRITER_REQUIRED_TABLES = ("send_logs", "tasks")
DB_WRITER_FORBIDDEN_HELPER_TABLES = ("temp_send_log_batch", "mercury_send_log_copy_stage")
DB_WRITER_REQUIRED_INDEXES = (
    "ix_send_logs_task_email_regular",
    "ix_send_logs_task_created",
    "ix_send_logs_task_status_log_type_created",
    "ix_recipients_claim_row_id_v17",
    "ix_recipients_claim_covering_v18",
    "ix_send_logs_task_regular_recipient_v18",
    "ix_send_logs_task_regular_legacy_email_v19",
    "ix_send_logs_task_regular_email_v18",
    "ix_recipients_claim_normalized_email",
    "ux_send_logs_regular_task_recipient",
    "ux_send_logs_monitor_task_trigger_email",
    "ux_unsubscribes_normalized_email",
)
DB_WRITER_REPAIRABLE_INDEX_DDL = {
    "ix_send_logs_task_email_regular": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_email_regular "
        "ON send_logs (task_id, LOWER(BTRIM(email))) "
        "WHERE email IS NOT NULL AND BTRIM(email) <> '' AND COALESCE(log_type, 'regular') = 'regular'"
    ),
    "ix_send_logs_task_created": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_created "
        "ON send_logs (task_id, created_at)"
    ),
    "ix_send_logs_task_status_log_type_created": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_status_log_type_created "
        "ON send_logs (task_id, status, log_type, created_at DESC) INCLUDE (id, email, recipient_id)"
    ),
    "ix_recipients_claim_row_id_v17": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_row_id_v17 "
        "ON recipients (list_id, row_number ASC, id ASC) "
        "WHERE row_number > 0 AND email IS NOT NULL AND BTRIM(email) <> '' "
        "AND is_valid IS TRUE AND COALESCE(is_unsubscribed, FALSE) IS FALSE"
    ),
    "ix_recipients_claim_covering_v18": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_covering_v18 "
        "ON recipients (list_id, row_number ASC, id ASC) "
        "INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at) "
        "WHERE row_number > 0 AND email IS NOT NULL AND BTRIM(email) <> '' "
        "AND is_valid IS TRUE AND COALESCE(is_unsubscribed, FALSE) IS FALSE"
    ),
    "ix_send_logs_task_regular_recipient_v18": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_recipient_v18 "
        "ON send_logs (task_id, recipient_id) WHERE log_type = 'regular' AND recipient_id IS NOT NULL"
    ),
    "ix_send_logs_task_regular_legacy_email_v19": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_legacy_email_v19 "
        "ON send_logs (task_id, LOWER(BTRIM(email))) "
        "WHERE log_type = 'regular' AND recipient_id IS NULL AND email IS NOT NULL AND BTRIM(email) <> ''"
    ),
    "ix_send_logs_task_regular_email_v18": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_email_v18 "
        "ON send_logs (task_id, email) WHERE log_type = 'regular' AND email IS NOT NULL AND email <> ''"
    ),
    "ix_recipients_claim_normalized_email": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_normalized_email "
        "ON recipients (list_id, row_number, id, normalized_email) "
        "WHERE is_valid IS TRUE AND is_unsubscribed IS FALSE "
        "AND normalized_email <> '' AND row_number > 0"
    ),
    "ux_send_logs_regular_task_recipient": (
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_send_logs_regular_task_recipient "
        "ON send_logs (task_id, recipient_id) "
        "WHERE COALESCE(log_type, 'regular') = 'regular' AND recipient_id IS NOT NULL"
    ),
    "ux_send_logs_monitor_task_trigger_email": (
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_send_logs_monitor_task_trigger_email "
        "ON send_logs (task_id, monitor_trigger_count, LOWER(BTRIM(email))) "
        "WHERE log_type = 'monitor' AND monitor_trigger_count IS NOT NULL "
        "AND email IS NOT NULL AND BTRIM(email) <> ''"
    ),
    "ux_unsubscribes_normalized_email": (
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_unsubscribes_normalized_email "
        "ON unsubscribes (normalized_email) "
        "WHERE normalized_email IS NOT NULL AND normalized_email <> ''"
    ),
    "ix_send_logs_task_regular_email_created_status": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_email_created_status "
        "ON send_logs (task_id, LOWER(BTRIM(email)), created_at DESC, id DESC) "
        "INCLUDE (status, final_status, bounce_type) "
        "WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> ''"
    ),
    "ix_send_log_archive_task_regular_email_created_status": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_log_archive_task_regular_email_created_status "
        "ON send_log_archive (task_id, LOWER(BTRIM(email)), created_at DESC, id DESC) "
        "INCLUDE (status, final_status, bounce_type) "
        "WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> ''"
    ),
    "ix_recipients_task_stats_list_email_valid": (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_task_stats_list_email_valid "
        "ON recipients (list_id, LOWER(BTRIM(email))) INCLUDE (is_unsubscribed) "
        "WHERE COALESCE(is_valid, TRUE) IS TRUE AND email IS NOT NULL AND BTRIM(email) <> ''"
    ),
}

# 发送 Worker 以 httpx I/O 为主，并行 DB 会话主要来自 claim、result writer、状态轮询与 API。
# 数据库池建议值按主机 CPU 规模估算，不与发送并发策略耦合。
POSTGRES_MAX_CONNECTIONS_BUDGET = max(100, int(settings.postgres_max_connections))
POSTGRES_APP_CONNECTION_BUDGET = max(50, int(POSTGRES_MAX_CONNECTIONS_BUDGET * 0.8))
DB_NON_MAIN_POOL_RESERVE = 80


def _resolve_main_pool_budget(pool_size: int, max_overflow: int) -> tuple[int, int]:
    requested_size = max(1, min(200, int(pool_size)))
    requested_overflow = max(0, min(100, int(max_overflow)))
    available = max(1, POSTGRES_APP_CONNECTION_BUDGET - DB_NON_MAIN_POOL_RESERVE)
    requested_total = requested_size + requested_overflow
    if requested_total <= available:
        return requested_size, requested_overflow
    scale = available / float(requested_total)
    effective_size = max(1, min(requested_size, int(requested_size * scale)))
    effective_overflow = max(0, min(requested_overflow, available - effective_size))
    logger.warning(
        "数据库主连接池配置超过安全预算，已在创建连接池前自动缩减：requested_pool_size=%s requested_max_overflow=%s "
        "effective_pool_size=%s effective_max_overflow=%s available_main_budget=%s",
        requested_size,
        requested_overflow,
        effective_size,
        effective_overflow,
        available,
    )
    return effective_size, effective_overflow


DB_POOL_SIZE, DB_MAX_OVERFLOW = _resolve_main_pool_budget(settings.db_pool_size, settings.db_max_overflow)
DB_POOL_MAX_TOTAL = DB_POOL_SIZE + DB_MAX_OVERFLOW
DB_POOL_PRESSURE_WARN_RATIO = 0.85
DB_POOL_PRESSURE_WARN_INTERVAL_SECONDS = 15.0
DB_POOL_TIMEOUT_SECONDS = 10
DB_POOL_RECYCLE_SECONDS = 1800
_last_pool_pressure_warn_at = 0.0
DB_BACKGROUND_THROTTLE_RATIO = 0.75
DB_CONNECTION_BUDGET_WARN_RATIO = 0.80
DB_BACKGROUND_REJECT_RATIO = 0.90
DB_CONNECTION_BUDGET_WARN_INTERVAL_SECONDS = 30.0
DB_BACKGROUND_POOL_NAMES = frozenset({"dashboard", "export", "task_stats"})
_background_pool_gate = asyncio.Semaphore(max(1, int(getattr(settings, "db_background_max_concurrency", 4) or 4)))
_last_connection_budget_warn_at = 0.0


def recommended_db_pool_total() -> int:
    """按主机规模给出数据库连接池建议值，不依赖发送并发配置。"""
    cpu_based_capacity = (os.cpu_count() or 1) * 2 + 100
    return max(20, min(200, cpu_based_capacity // 5))


def _database_pool_budget_totals() -> dict[str, int]:
    return {
        "main": DB_POOL_MAX_TOTAL,
        "migration": 2,
        "writer": WRITER_POOL_SIZE + WRITER_MAX_OVERFLOW,
        "recipient_claim": RECIPIENT_CLAIM_POOL_SIZE + RECIPIENT_CLAIM_MAX_OVERFLOW,
        "dashboard": DASHBOARD_POOL_SIZE + DASHBOARD_MAX_OVERFLOW,
        "export": EXPORT_POOL_SIZE + EXPORT_MAX_OVERFLOW,
        "import": IMPORT_POOL_SIZE + IMPORT_MAX_OVERFLOW,
        "recipient_delete": RECIPIENT_DELETE_POOL_SIZE + RECIPIENT_DELETE_MAX_OVERFLOW,
        "url_pool": URL_POOL_SIZE + URL_POOL_MAX_OVERFLOW,
        "health": 2,
    }


def validate_database_connection_budget() -> None:
    isolated_pool_totals = _database_pool_budget_totals()
    isolated_total = sum(isolated_pool_totals.values())
    safe_budget = POSTGRES_APP_CONNECTION_BUDGET
    if isolated_total > safe_budget:
        logger.error(
            "数据库连接池预算在最低后台池配置下仍超限：实际使用=%s，80%%总预算=%s，连接池明细=%s；"
            "writer 与 claim 池已受保护，请提高 POSTGRES_MAX_CONNECTIONS 或降低主池大小",
            isolated_total,
            safe_budget,
            isolated_pool_totals,
        )
    logger.warning(
        "数据库连接池最终生效配置：pool_size=%s, max_overflow=%s，80%%总预算=%s，实际使用=%s，连接池明细=%s",
        DB_POOL_SIZE,
        DB_MAX_OVERFLOW,
        safe_budget,
        isolated_total,
        isolated_pool_totals,
    )


def log_database_pool_config() -> None:
    recommended_total = recommended_db_pool_total()
    default_batch = int(settings.default_http_batch_size)
    warmup_batch = int(settings.warmup_default_http_batch_size)
    logger.warning(
        "MercuryMail 数据库连接池已配置 pool_size=%s max_overflow=%s max_total=%s "
        "pool_pre_ping=True cpu_based_recommended_min_total=%s "
        "default_http_batch_size=%s warmup_default_http_batch_size=%s",
        DB_POOL_SIZE,
        DB_MAX_OVERFLOW,
        DB_POOL_MAX_TOTAL,
        recommended_total,
        default_batch,
        warmup_batch,
    )
    logger.warning(
        "MercuryMail PostgreSQL WAL/内存调优请通过 postgres 服务启动参数配置（如 shared_buffers、wal_buffers）；"
        "本项目使用 asyncpg，不在应用层执行 SQLite PRAGMA"
    )
    isolated_pool_totals = _database_pool_budget_totals()
    isolated_total = sum(isolated_pool_totals.values())
    pg_safe_limit = POSTGRES_APP_CONNECTION_BUDGET
    logger.warning("MercuryMail 独立数据库连接池预算 totals=%s total=%s postgres_80pct_budget=%s", isolated_pool_totals, isolated_total, pg_safe_limit)
    if isolated_total > pg_safe_limit:
        logger.warning("独立连接池总预算 %s 超过 PostgreSQL max_connections 80%% 预算 %s，请降低 DB_POOL_SIZE/DB_MAX_OVERFLOW 或提高 POSTGRES_MAX_CONNECTIONS", isolated_total, pg_safe_limit)
    if DB_POOL_MAX_TOTAL < recommended_total:
        logger.warning(
            "数据库连接池上限 %s 低于按主机 CPU 规模估算的建议值 %s，"
            "高负载多任务时可能出现连接等待；请结合 PostgreSQL 连接预算调整 DB_POOL_SIZE / DB_MAX_OVERFLOW",
            DB_POOL_MAX_TOTAL,
            recommended_total,
        )


engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={"server_settings": {"application_name": "mercurymail-main", "idle_in_transaction_session_timeout": "120s"}},
)

migration_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=1,
    max_overflow=1,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    isolation_level="AUTOCOMMIT",
    connect_args={"server_settings": {"application_name": "mercurymail-migration", "jit": "off", "lock_timeout": "5s", "statement_timeout": "30min"}},
)

# I/O 隔离：SendLog 写入、收件人预取、仪表盘/导出读取使用独立连接池，避免高负载下互相耗尽主业务池。
# 默认总连接预算：主池 80，加所有隔离池、迁移和健康池不超过 150，
# 为 PostgreSQL 后台进程、运维连接和故障恢复保留充足连接槽。
WRITER_POOL_SIZE = max(8, min(16, DB_POOL_SIZE // 4))
WRITER_MAX_OVERFLOW = max(4, min(8, DB_MAX_OVERFLOW // 3))
RECIPIENT_CLAIM_POOL_SIZE = max(12, min(20, int(getattr(settings, "recipient_claim_pool_size", 20) or 20)))
RECIPIENT_CLAIM_MAX_OVERFLOW = max(4, min(8, int(getattr(settings, "recipient_claim_max_overflow", 8) or 8)))
# 仪表盘使用固定独立小池，任何实时查询都不得占用 main/writer/recipient_claim。
DASHBOARD_POOL_SIZE = 5
DASHBOARD_MAX_OVERFLOW = 5
EXPORT_POOL_SIZE = 2
EXPORT_MAX_OVERFLOW = 1
IMPORT_POOL_SIZE = 2
IMPORT_MAX_OVERFLOW = 1
# 删除任务使用独立固定池：允许最多 3 个并发任务，禁止 overflow 侵占主业务连接预算。
RECIPIENT_DELETE_POOL_SIZE = 3
RECIPIENT_DELETE_MAX_OVERFLOW = 0
URL_POOL_SIZE = max(3, min(6, DB_POOL_SIZE // 15))
URL_POOL_MAX_OVERFLOW = max(2, min(2, DB_MAX_OVERFLOW // 15))


def _shrink_background_pool_budgets() -> None:
    """在创建 Engine 前缩减低优先级池；writer 与 claim 的发送链路预算永不被挤占。"""
    global DASHBOARD_POOL_SIZE, DASHBOARD_MAX_OVERFLOW
    global EXPORT_POOL_SIZE, EXPORT_MAX_OVERFLOW, IMPORT_POOL_SIZE, IMPORT_MAX_OVERFLOW
    protected_total = (
        DB_POOL_MAX_TOTAL + 2
        + WRITER_POOL_SIZE + WRITER_MAX_OVERFLOW
        + RECIPIENT_CLAIM_POOL_SIZE + RECIPIENT_CLAIM_MAX_OVERFLOW
        + RECIPIENT_DELETE_POOL_SIZE + RECIPIENT_DELETE_MAX_OVERFLOW
        + URL_POOL_SIZE + URL_POOL_MAX_OVERFLOW + 2
    )
    available = max(3, POSTGRES_APP_CONNECTION_BUDGET - protected_total)
    original = {
        "dashboard": (DASHBOARD_POOL_SIZE, DASHBOARD_MAX_OVERFLOW),
        "export": (EXPORT_POOL_SIZE, EXPORT_MAX_OVERFLOW),
        "import": (IMPORT_POOL_SIZE, IMPORT_MAX_OVERFLOW),
    }
    pools = {
        "dashboard": [DASHBOARD_POOL_SIZE, DASHBOARD_MAX_OVERFLOW],
        "export": [EXPORT_POOL_SIZE, EXPORT_MAX_OVERFLOW],
        "import": [IMPORT_POOL_SIZE, IMPORT_MAX_OVERFLOW],
    }
    while sum(size + overflow for size, overflow in pools.values()) > available:
        changed = False
        for name in ("dashboard", "export", "import"):
            if pools[name][1] > 0:
                pools[name][1] -= 1
                changed = True
                if sum(size + overflow for size, overflow in pools.values()) <= available:
                    break
        if sum(size + overflow for size, overflow in pools.values()) <= available:
            break
        for name in ("dashboard", "export", "import"):
            if pools[name][0] > 1:
                pools[name][0] -= 1
                changed = True
                if sum(size + overflow for size, overflow in pools.values()) <= available:
                    break
        if not changed:
            break
    DASHBOARD_POOL_SIZE, DASHBOARD_MAX_OVERFLOW = pools["dashboard"]
    EXPORT_POOL_SIZE, EXPORT_MAX_OVERFLOW = pools["export"]
    IMPORT_POOL_SIZE, IMPORT_MAX_OVERFLOW = pools["import"]
    if any(original[name] != tuple(pools[name]) for name in pools):
        logger.warning(
            "数据库连接池预算已自动调整：dashboard 缩减至 %s+%s，export 缩减至 %s+%s，import 缩减至 %s+%s；writer=%s+%s、claim=%s+%s 保持不变",
            DASHBOARD_POOL_SIZE, DASHBOARD_MAX_OVERFLOW,
            EXPORT_POOL_SIZE, EXPORT_MAX_OVERFLOW,
            IMPORT_POOL_SIZE, IMPORT_MAX_OVERFLOW,
            WRITER_POOL_SIZE, WRITER_MAX_OVERFLOW,
            RECIPIENT_CLAIM_POOL_SIZE, RECIPIENT_CLAIM_MAX_OVERFLOW,
        )


_shrink_background_pool_budgets()

writer_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=WRITER_POOL_SIZE,
    max_overflow=WRITER_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={"server_settings": {"application_name": "mercurymail-writer", "synchronous_commit": "off", "jit": "off", "work_mem": "32MB", "lock_timeout": "1s", "statement_timeout": "30s", "idle_in_transaction_session_timeout": "120s"}},
)

RECIPIENT_FETCH_TIMEOUT_MS = max(5_000, min(120_000, int(getattr(settings, "recipient_fetch_timeout_ms", 30_000) or 30_000)))

recipient_claim_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=False,
    pool_size=RECIPIENT_CLAIM_POOL_SIZE,
    max_overflow=RECIPIENT_CLAIM_MAX_OVERFLOW,
    pool_timeout=5,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={"server_settings": {"application_name": "mercurymail-recipient-claim", "jit": "off", "work_mem": "64MB", "default_transaction_read_only": "on", "statement_timeout": f"{RECIPIENT_FETCH_TIMEOUT_MS}ms", "idle_in_transaction_session_timeout": "120s"}},
)

dashboard_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=DASHBOARD_POOL_SIZE,
    max_overflow=DASHBOARD_MAX_OVERFLOW,
    pool_timeout=5,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={"server_settings": {"application_name": "mercurymail-dashboard", "jit": "off", "work_mem": "32MB", "default_transaction_read_only": "on", "statement_timeout": "10s", "idle_in_transaction_session_timeout": "120s"}},
)

export_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=EXPORT_POOL_SIZE,
    max_overflow=EXPORT_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    isolation_level="AUTOCOMMIT",
    connect_args={"server_settings": {"application_name": "mercurymail-export"}},
)

import_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=IMPORT_POOL_SIZE,
    max_overflow=IMPORT_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT_SECONDS,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={"server_settings": {"application_name": "mercurymail-import", "idle_in_transaction_session_timeout": "120s"}},
)

recipient_delete_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=RECIPIENT_DELETE_POOL_SIZE,
    max_overflow=RECIPIENT_DELETE_MAX_OVERFLOW,
    pool_timeout=2,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={
        "server_settings": {
            "application_name": "mercurymail-recipient-delete",
            "jit": "off",
            "lock_timeout": "2s",
            "statement_timeout": "8s",
            "idle_in_transaction_session_timeout": "10s",
        }
    },
)

# 健康检查专用微型连接池：与业务/仪表盘池隔离，避免发送高峰时 checkout 排队拖垮门禁。
health_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=1,
    max_overflow=1,
    pool_timeout=1,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    isolation_level="AUTOCOMMIT",
    connect_args={"server_settings": {"application_name": "mercurymail-health", "jit": "off", "statement_timeout": "2s"}},
)

read_engine = dashboard_engine

# URL 运行时分配使用独立小池，避免 URL 池批量查询与主业务/发送状态连接互相排队。
url_pool_engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_size=URL_POOL_SIZE,
    max_overflow=URL_POOL_MAX_OVERFLOW,
    pool_timeout=3,
    pool_recycle=DB_POOL_RECYCLE_SECONDS,
    pool_use_lifo=True,
    connect_args={"server_settings": {"application_name": "mercurymail-url-pool", "jit": "off", "work_mem": "32MB", "statement_timeout": "10s", "idle_in_transaction_session_timeout": "120s"}},
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

WriterSessionLocal = async_sessionmaker(
    bind=writer_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

RecipientClaimSessionLocal = async_sessionmaker(
    bind=recipient_claim_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

DashboardSessionLocal = async_sessionmaker(
    bind=dashboard_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

# 后台任务统计使用独立 session factory，确保只读超时或回滚不会污染 API/发送会话。
TaskStatsSessionLocal = async_sessionmaker(
    bind=dashboard_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

ExportSessionLocal = async_sessionmaker(
    bind=export_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

ImportSessionLocal = async_sessionmaker(
    bind=import_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

RecipientDeleteSessionLocal = async_sessionmaker(
    bind=recipient_delete_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

UrlPoolSessionLocal = async_sessionmaker(
    bind=url_pool_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

Base = declarative_base()


def _is_transient_db_error(exc: Exception) -> bool:
    message = str(exc).lower()
    transient_markers = (
        "deadlock detected",
        "could not serialize access",
        "serialization failure",
        "connection reset",
        "server closed the connection",
        "connection is closed",
        "too many connections",
        "queuepool limit",
        "timeout",
        "timed out",
        "connection was closed",
        "cannot connect now",
    )
    return any(marker in message for marker in transient_markers)


def is_database_pool_pressure_error(exc: Exception) -> bool:
    message = str(exc).lower()
    pool_markers = (
        "too many connections",
        "queuepool limit",
        "remaining connection slots",
        "connection pool exhausted",
        "timeout waiting for connection",
    )
    return any(marker in message for marker in pool_markers)


def maybe_warn_database_pool_pressure(*, force: bool = False) -> dict[str, int]:
    global _last_pool_pressure_warn_at
    status = get_database_pool_status()
    checked_out = int(status.get("checked_out", 0))
    max_total = max(1, int(status.get("max_total", DB_POOL_MAX_TOTAL)))
    utilization = checked_out / max_total
    now = monotonic()
    should_warn = force or utilization >= DB_POOL_PRESSURE_WARN_RATIO
    if should_warn and (force or now - _last_pool_pressure_warn_at >= DB_POOL_PRESSURE_WARN_INTERVAL_SECONDS):
        _last_pool_pressure_warn_at = now
        all_status = get_all_database_pool_status()
        logger.warning(
            "MercuryMail 数据库连接池压力偏高 main_checked_out=%s main_max_total=%s main_utilization=%.1f%% main_overflow=%s main_checked_in=%s all_pools=%s",
            checked_out,
            max_total,
            utilization * 100.0,
            status.get("overflow", 0),
            status.get("checked_in", 0),
            all_status,
        )
    return status


def is_fatal_db_error(exc: Exception) -> bool:
    message = str(exc).lower()
    fatal_markers = (
        "password authentication failed",
        "invalidpassworderror",
        "does not exist",
        "permission denied",
        "no space left on device",
        "disk full",
    )
    return any(marker in message for marker in fatal_markers)


def is_db_auth_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "password authentication failed" in message or "invalidpassworderror" in message


def describe_db_connection_error(exc: Exception) -> str:
    if is_db_auth_error(exc):
        return (
            "数据库认证失败（用户名或密码错误）。请确认 .env 中 DATABASE_URL 与 POSTGRES_PASSWORD 一致，"
            "并重新执行 deploy.sh 以同步 PostgreSQL 用户密码"
        )
    message = str(exc).lower()
    if "does not exist" in message and "database" in message:
        return f"目标数据库不存在，请检查 DATABASE_URL 中的库名：{exc}"
    return str(exc)


async def apply_writer_fast_commit(session: AsyncSession) -> None:
    """兼容旧调用：写入池已通过 server_settings 配置 synchronous_commit=off。"""
    del session
    return None


def session_needs_rollback(session: AsyncSession) -> bool:
    """判断会话是否处于待回滚/失败事务状态。"""
    try:
        return not bool(session.is_active)
    except Exception:
        return False


async def rollback_failed_session(
    session: AsyncSession,
    *,
    operation: str,
    task_id: int | None = None,
) -> None:
    """清理只读/统计查询的失败事务，防止同一会话持续触发 PendingRollbackError。"""
    try:
        await session.rollback()
        logger.warning("数据库会话已回滚 operation=%s task_id=%s", operation, task_id)
    except Exception as rollback_exc:
        logger.warning(
            "数据库会话回滚失败 operation=%s task_id=%s error_type=%s error=%s",
            operation,
            task_id,
            type(rollback_exc).__name__,
            rollback_exc,
        )


async def execute_read_with_rollback_retry(
    session: AsyncSession,
    operation,
    *,
    operation_name: str,
    task_id: int | None = None,
):
    """在独立只读会话中执行查询；遇待回滚事务时先回滚并仅重试一次。"""
    for attempt in range(1, 3):
        try:
            if session_needs_rollback(session):
                await rollback_failed_session(session, operation=operation_name, task_id=task_id)
            return await operation()
        except PendingRollbackError as exc:
            logger.warning(
                "数据库查询遇到待回滚事务，正在回滚并重试 operation=%s task_id=%s attempt=%s/2 error=%s",
                operation_name,
                task_id,
                attempt,
                exc,
            )
            await rollback_failed_session(session, operation=operation_name, task_id=task_id)
            if attempt >= 2:
                raise
        except Exception:
            if not session.is_active:
                await rollback_failed_session(session, operation=operation_name, task_id=task_id)
            raise


async def _retry_async_operation(*, attempts: int, min_delay: float, max_delay: float):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max(1, int(attempts)) + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt >= attempts:
                        raise
                    delay = random.uniform(min_delay, max_delay) * attempt
                    await asyncio.sleep(delay)
            if last_exc is not None:
                raise last_exc
        return wrapper
    return decorator


async def commit_session_with_retry(session: AsyncSession, attempts: int = DB_COMMIT_RETRY_ATTEMPTS) -> None:
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            await session.commit()
            return
        except (OperationalError, DBAPIError) as exc:
            if is_fatal_db_error(exc):
                raise
            if not _is_transient_db_error(exc) or attempt >= attempts:
                raise
            backoff_seconds = random.uniform(DB_COMMIT_RETRY_MIN_DELAY_SECONDS, DB_COMMIT_RETRY_MAX_DELAY_SECONDS)
            await asyncio.sleep(backoff_seconds)


def _ddl_preview(sql: str, max_len: int = 160) -> str:
    compact = " ".join((sql or "").split())
    if len(compact) <= max_len:
        return compact
    return f"{compact[: max_len - 3]}..."


def _prefer_online_ddl(statement: str) -> str:
    """启动迁移中索引默认并发处理，避免大表 DDL 阻塞发送写入。"""
    normalized = (statement or "").strip()
    upper = normalized.upper()
    if upper.startswith("CREATE INDEX IF NOT EXISTS "):
        return "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + normalized[len("CREATE INDEX IF NOT EXISTS "):]
    if upper.startswith("DROP INDEX IF EXISTS "):
        return "DROP INDEX CONCURRENTLY IF EXISTS " + normalized[len("DROP INDEX IF EXISTS "):]
    if upper.startswith("ANALYZE "):
        return normalized
    return normalized


def _should_skip_online_startup_statement(statement: str) -> bool:
    """跳过启动期大表清理，避免应用启动时对 send_logs 产生长事务和 I/O 风暴。"""
    upper = (statement or "").strip().upper()
    return upper.startswith("DELETE FROM SEND_LOGS ")


def _extract_concurrent_index_name(statement: str) -> str | None:
    parts = (statement or "").strip().split()
    if len(parts) < 7:
        return None
    if [part.upper() for part in parts[:6]] != ["CREATE", "INDEX", "CONCURRENTLY", "IF", "NOT", "EXISTS"]:
        return None
    raw_name = parts[6].strip().strip('"')
    return raw_name if raw_name.replace("_", "").isalnum() else None


async def _drop_invalid_index_before_create(conn, statement: str) -> None:
    index_name = _extract_concurrent_index_name(statement)
    if not index_name:
        return
    row = await conn.execute(
        text(
            """
            SELECT 1
            FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = :index_name
              AND n.nspname = current_schema()
              AND i.indisvalid = FALSE
            LIMIT 1
            """
        ),
        {"index_name": index_name},
    )
    if row.first() is None:
        return
    await conn.execute(text(f'DROP INDEX CONCURRENTLY IF EXISTS "{index_name}"'))
    logger.warning("已清理无效并发索引残留：%s", index_name)


async def run_migration_ddl(
    statement: str,
    *,
    label: str = "migration",
    params: dict | None = None,
) -> bool:
    """在独立 AUTOCOMMIT 连接上执行单条迁移 SQL；失败仅回滚本条并记录中文日志。"""
    if _should_skip_online_startup_statement(statement):
        logger.warning("数据库迁移已跳过启动期大表清理（%s）：%s", label, _ddl_preview(statement))
        return True
    normalized = _prefer_online_ddl(statement)
    if not normalized:
        return True
    preview = _ddl_preview(normalized)
    logger.warning("正在执行数据库迁移（%s）", label)
    try:
        async with migration_engine.connect() as conn:
            await conn.execute(text("SET lock_timeout = '5s'"))
            await conn.execute(text("SET statement_timeout = '30min'"))
            await _drop_invalid_index_before_create(conn, normalized)
            await conn.execute(text(normalized), params or {})
        logger.warning("数据库迁移执行成功（%s）", label)
        return True
    except Exception as exc:
        logger.warning(
            "数据库迁移执行失败（%s），已跳过：%s；原因：%s",
            label,
            preview,
            exc,
        )
        return False


async def fetch_migration_row(statement: str, params: dict | None = None):
    """只读查询（独立 AUTOCOMMIT 连接，只查不改，不污染后续 DDL）。"""
    normalized = (statement or "").strip()
    async with read_engine.connect() as conn:
        result = await conn.execute(text(normalized), params or {})
        return result.first()


async def _execute_idempotent_statements(
    statements: tuple[str, ...] | list[str],
    label: str,
) -> tuple[int, int]:
    """逐条独立提交幂等 SQL；单条失败不阻塞其余语句。"""
    succeeded = 0
    failed = 0
    for index, statement in enumerate(statements, start=1):
        step_label = f"{label}[{index}]"
        if await run_migration_ddl(statement, label=step_label):
            succeeded += 1
        else:
            failed += 1
    if failed:
        logger.warning(
            "数据库迁移批次完成 section=%s 成功=%s 失败=%s",
            label,
            succeeded,
            failed,
        )
    return succeeded, failed


async def ensure_writer_prerequisites() -> None:
    """统一确保 SendLog 写入链路依赖对象就绪，禁止再依赖未初始化的临时/辅助表。"""
    writer_schema_statements = (
        "CREATE TABLE IF NOT EXISTS send_log_archive (LIKE send_logs INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
        "ALTER TABLE send_log_archive ADD COLUMN IF NOT EXISTS extra_data JSONB",
        "ALTER TABLE send_log_archive ADD COLUMN IF NOT EXISTS sg_event_id VARCHAR(255)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sender_email VARCHAR(255)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS final_status VARCHAR(40)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS bounce_type VARCHAR(40)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS retry_state VARCHAR(20) DEFAULT 'none'",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS abort_retry BOOLEAN DEFAULT FALSE",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS error TEXT DEFAULT ''",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS error_message TEXT DEFAULT ''",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS response_text TEXT DEFAULT ''",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS attempt_count INTEGER DEFAULT 0",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS monitor_trigger_count INTEGER",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS provider_message_id VARCHAR(255) DEFAULT ''",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS extra_data JSONB",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sg_event_id VARCHAR(255)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS log_type VARCHAR(40) DEFAULT 'regular'",
        "UPDATE send_logs SET monitor_trigger_count = substring(response_text FROM 'monitor_trigger_count=([0-9]+)')::INTEGER WHERE log_type = 'monitor' AND monitor_trigger_count IS NULL AND response_text ~ 'monitor_trigger_count=[0-9]+'",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS egress_ip VARCHAR(64) DEFAULT ''",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS proxy_info VARCHAR(255)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS outbound_ip VARCHAR(45)",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sent_at TIMESTAMP",
        "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC')",
        "UPDATE send_logs SET log_type = 'regular' WHERE log_type IS NULL OR BTRIM(log_type) = ''",
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sent_count INTEGER DEFAULT 0",
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS failed_count INTEGER DEFAULT 0",
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP",
        "DROP TABLE IF EXISTS temp_send_log_batch",
        "DROP TABLE IF EXISTS mercury_send_log_copy_stage",
    )
    writer_index_statements = (
        "DROP INDEX IF EXISTS ux_send_logs_task_email_regular",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_regular ON send_logs (task_id, LOWER(BTRIM(email))) WHERE email IS NOT NULL AND BTRIM(email) <> '' AND COALESCE(log_type, 'regular') = 'regular'",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_btrim_nonempty ON send_logs (task_id, LOWER(BTRIM(email))) WHERE email IS NOT NULL AND BTRIM(email) <> '' AND COALESCE(log_type, 'regular') <> 'monitor'",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_created ON send_logs (task_id, created_at DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_status_created ON send_logs (task_id, status, created_at DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_status_log_type_created ON send_logs (task_id, status, log_type, created_at DESC) INCLUDE (id, email, recipient_id)",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_row_id_v17 ON recipients (list_id, row_number ASC, id ASC) WHERE row_number > 0 AND email IS NOT NULL AND BTRIM(email) <> '' AND is_valid IS TRUE AND COALESCE(is_unsubscribed, FALSE) IS FALSE",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_covering_v18 ON recipients (list_id, row_number ASC, id ASC) INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at) WHERE row_number > 0 AND email IS NOT NULL AND BTRIM(email) <> '' AND is_valid IS TRUE AND COALESCE(is_unsubscribed, FALSE) IS FALSE",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_recipient_v18 ON send_logs (task_id, recipient_id) WHERE log_type = 'regular' AND recipient_id IS NOT NULL",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_legacy_email_v19 ON send_logs (task_id, LOWER(BTRIM(email))) WHERE log_type = 'regular' AND recipient_id IS NULL AND email IS NOT NULL AND BTRIM(email) <> ''",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_email_v18 ON send_logs (task_id, email) WHERE log_type = 'regular' AND email IS NOT NULL AND email <> ''",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_event_task_email_window ON send_logs (task_id, email, created_at) WHERE email IS NOT NULL AND email <> ''",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_sg_event_id ON send_logs (sg_event_id) WHERE sg_event_id IS NOT NULL AND sg_event_id <> ''",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_failure_page ON send_logs (task_id, log_type, status, created_at DESC, id DESC)",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_send_logs_monitor_task_trigger_email ON send_logs (task_id, monitor_trigger_count, LOWER(BTRIM(email))) WHERE log_type = 'monitor' AND monitor_trigger_count IS NOT NULL AND email IS NOT NULL AND BTRIM(email) <> ''",
        "CREATE INDEX IF NOT EXISTS ix_unsubscribes_normalized_email_nonempty ON unsubscribes (normalized_email) WHERE normalized_email IS NOT NULL AND normalized_email <> ''",
        *DB_WRITER_REPAIRABLE_INDEX_DDL.values(),
    )
    logger.warning("【数据库】开始校验 SendLog 写入链路前置对象…")
    ok, fail = await _execute_idempotent_statements(writer_schema_statements, "writer_prerequisites_schema")
    idx_ok, idx_fail = await _execute_idempotent_statements(writer_index_statements, "writer_prerequisites_indexes")
    await verify_writer_prerequisites_ready(log_success=True)
    logger.warning(
        "【数据库】SendLog 写入链路前置对象就绪（schema成功=%s schema失败=%s index成功=%s index失败=%s）",
        ok,
        fail,
        idx_ok,
        idx_fail,
    )


async def ensure_postgresql_indexes() -> None:
    """补建索引与幂等 schema 迁移（每条 DDL 独立 AUTOCOMMIT，互不污染）。"""
    index_statements = (
        "CREATE INDEX IF NOT EXISTS ix_recipients_send_scan ON recipients (list_id, is_valid, is_unsubscribed, row_number, id)",
        "CREATE INDEX IF NOT EXISTS ix_tasks_recipient_list_id ON tasks (recipient_list_id)",
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS runner_instance_id VARCHAR(64)",
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS runner_lease_until TIMESTAMP WITHOUT TIME ZONE",
        "CREATE INDEX IF NOT EXISTS ix_tasks_runner_instance_id ON tasks (runner_instance_id)",
        "CREATE INDEX IF NOT EXISTS ix_tasks_runner_lease_until ON tasks (runner_lease_until)",
        "DROP INDEX IF EXISTS ux_send_logs_task_email_regular",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_regular ON send_logs (task_id, LOWER(BTRIM(email))) WHERE email IS NOT NULL AND BTRIM(email) <> '' AND COALESCE(log_type, 'regular') = 'regular'",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_row_only ON recipients (list_id, row_number) WHERE row_number > 0",
        "CREATE INDEX IF NOT EXISTS ix_recipients_list_row ON recipients (list_id, row_number)",
        "CREATE INDEX IF NOT EXISTS ix_recipients_list_row_id ON recipients (list_id, row_number, id)",
        "CREATE INDEX IF NOT EXISTS ix_recipients_list_email ON recipients (list_id, email)",
        "CREATE INDEX IF NOT EXISTS ix_recipients_list_email_lower ON recipients (list_id, (LOWER(BTRIM(email)))) "
        "WHERE email IS NOT NULL AND BTRIM(email) <> ''",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_normalized_email ON recipients (list_id, row_number, id, normalized_email) "
        "WHERE is_valid IS TRUE AND is_unsubscribed IS FALSE AND normalized_email <> '' AND row_number > 0",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_recipient_id ON send_logs (recipient_id)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_type_created_id ON send_logs (log_type, created_at DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_unsubscribes_recipient_id ON unsubscribes (recipient_id)",
        "CREATE INDEX IF NOT EXISTS ix_unsubscribes_email ON unsubscribes (email)",
        "CREATE INDEX IF NOT EXISTS ix_unsubscribes_email_lower ON unsubscribes (LOWER(BTRIM(email)))",
        "ALTER TABLE unsubscribes ADD COLUMN IF NOT EXISTS normalized_email VARCHAR(255)",
        "DELETE FROM unsubscribes a USING unsubscribes b WHERE a.id > b.id AND LOWER(BTRIM(a.email)) = LOWER(BTRIM(b.email)) AND a.email IS NOT NULL AND BTRIM(a.email) <> '' AND b.email IS NOT NULL AND BTRIM(b.email) <> ''",
        "UPDATE unsubscribes SET normalized_email = LOWER(BTRIM(email)) WHERE email IS NOT NULL AND BTRIM(email) <> '' AND (normalized_email IS NULL OR normalized_email <> LOWER(BTRIM(email)))",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_unsubscribes_normalized_email ON unsubscribes (normalized_email) WHERE normalized_email IS NOT NULL AND normalized_email <> ''",
        "DROP INDEX IF EXISTS ux_send_logs_task_email_nonempty",
        "DROP INDEX IF EXISTS ux_send_logs_task_email_btrim_nonempty",
        "DROP INDEX IF EXISTS ux_send_logs_monitor_task_email_provider",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_nonempty ON send_logs (task_id, email) "
        "WHERE email IS NOT NULL AND email <> '' AND COALESCE(log_type, 'regular') <> 'monitor'", 
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_btrim_nonempty ON send_logs (task_id, LOWER(BTRIM(email))) "
        "WHERE email IS NOT NULL AND BTRIM(email) <> '' AND COALESCE(log_type, 'regular') <> 'monitor'",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email ON send_logs (task_id, email)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_regular_status_email ON send_logs (task_id, log_type, status, email)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_status ON send_logs (task_id, email, status) "
        "WHERE log_type = 'regular'",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_status_created ON send_logs (task_id, status, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_created ON send_logs (task_id, created_at)",
        # 若写入峰值仍不足，可临时注释以下非关键索引以减轻 INSERT 开销（保留 created_at 相关索引）：
        # "CREATE INDEX IF NOT EXISTS ix_send_logs_task_type_created ON send_logs (task_id, log_type, created_at)",
        # "CREATE INDEX IF NOT EXISTS ix_send_logs_egress_ip_created ON send_logs (egress_ip, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_type_created ON send_logs (task_id, log_type, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_type_email_created ON send_logs (task_id, log_type, LOWER(BTRIM(email)), created_at DESC, id DESC) WHERE email IS NOT NULL AND BTRIM(email) <> ''",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_regular_lower_email_task ON send_logs (LOWER(BTRIM(email)), task_id) WHERE COALESCE(log_type, 'regular') = 'regular' AND email IS NOT NULL AND BTRIM(email) <> ''",
        "DROP INDEX IF EXISTS ix_send_logs_regular_hard_lower_email",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_cursor ON recipients (list_id, row_number, id) WHERE email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE AND row_number > 0",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_covering ON recipients (list_id, row_number, id) INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at) WHERE email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE AND row_number > 0",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_covering_v15 ON recipients (list_id, row_number, id) INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed) WHERE row_number > 0 AND email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_covering_v16 ON recipients (list_id, row_number, id) INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at) WHERE row_number > 0 AND email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_cursor_email ON recipients (list_id, row_number, id, email) WHERE email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE AND row_number > 0",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_narrow ON recipients (list_id, row_number, id) INCLUDE (email) WHERE email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE AND row_number > 0",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_row_id_v17 ON recipients (list_id, row_number ASC, id ASC) WHERE row_number > 0 AND email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_covering_v18 ON recipients (list_id, row_number ASC, id ASC) INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at) WHERE row_number > 0 AND email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE",
        "CREATE INDEX IF NOT EXISTS ix_recipients_claim_email ON recipients (list_id, email) WHERE email <> '' AND is_valid = TRUE AND is_unsubscribed = FALSE",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_regular_recipient_v18 ON send_logs (task_id, recipient_id) WHERE log_type = 'regular' AND recipient_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_regular_legacy_email_v19 ON send_logs (task_id, LOWER(BTRIM(email))) WHERE log_type = 'regular' AND recipient_id IS NULL AND email IS NOT NULL AND BTRIM(email) <> ''",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_regular_email_v18 ON send_logs (task_id, email) WHERE log_type = 'regular' AND email IS NOT NULL AND email <> ''",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_event_task_email_window ON send_logs (task_id, email, created_at) WHERE email IS NOT NULL AND email <> ''",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_failure_page ON send_logs (task_id, log_type, status, created_at DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_unsubscribes_normalized_email_nonempty ON unsubscribes (normalized_email) WHERE normalized_email IS NOT NULL AND normalized_email <> ''",
        "ANALYZE recipients",
        "ANALYZE send_logs",
        "ANALYZE unsubscribes",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_egress_ip_created ON send_logs (egress_ip, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_dashboard_status_created ON send_logs (log_type, status, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_regular_email_id ON send_logs (log_type, email, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_task_recipient ON send_logs (task_id, recipient_id)",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_sender_email_created ON send_logs (sender_email, created_at) WHERE sender_email IS NOT NULL AND sender_email <> ''",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_proxy_outbound_created ON send_logs (outbound_ip, created_at) WHERE outbound_ip IS NOT NULL AND outbound_ip <> ''",
        "CREATE INDEX IF NOT EXISTS ix_send_logs_type_created_id ON send_logs (log_type, created_at DESC, id DESC)",
        "DROP INDEX IF EXISTS ix_send_logs_hard_bounce_email",
        "CREATE INDEX IF NOT EXISTS ix_warmup_plans_status ON warmup_plans (status)",
        "CREATE INDEX IF NOT EXISTS ix_warmup_plans_status_running ON warmup_plans (status, running, last_run_date)",
        "CREATE INDEX IF NOT EXISTS ix_warmup_recipients_plan_sent ON warmup_recipients (plan_id, sent, id)",
        "CREATE INDEX IF NOT EXISTS ix_warmup_logs_plan_sent_at ON warmup_logs (plan_id, sent_at)",
        "CREATE INDEX IF NOT EXISTS ix_tasks_status_timezone_scheduled ON tasks (status, timezone, scheduled_at)",
        "CREATE INDEX IF NOT EXISTS ix_channels_active ON channels (is_active, enabled)",
        "CREATE INDEX IF NOT EXISTS ix_channels_proxy ON channels (use_proxy, proxy_id)",
        "CREATE INDEX IF NOT EXISTS ix_tasks_status_updated ON tasks (status, updated_at)",
        "CREATE INDEX IF NOT EXISTS ix_url_import_tasks_pool_created ON url_import_tasks (pool_id, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_url_import_tasks_status_created ON url_import_tasks (status, created_at)",
    )
    writer_table_tuning_statements = (
        "ALTER TABLE recipients SET (autovacuum_analyze_scale_factor = 0.005, autovacuum_analyze_threshold = 500, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)",
        "ALTER TABLE send_logs SET (fillfactor = 80, autovacuum_analyze_scale_factor = 0.01, autovacuum_analyze_threshold = 1000, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 2000)",
        "ANALYZE recipients",
        "ANALYZE send_logs",
    )
    # WAL checkpoint 频率由 docker-compose 中 postgres -c wal_autocheckpoint=2048 控制（单位 MB）。
    schema_statements = (
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS outbound_proxy_id INTEGER REFERENCES proxies(id)",
        "CREATE INDEX IF NOT EXISTS ix_ai_configs_outbound_proxy_id ON ai_configs (outbound_proxy_id)",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_use_proxy BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_proxy_type VARCHAR(16) NOT NULL DEFAULT ''",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_proxy_host VARCHAR(255) NOT NULL DEFAULT ''",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_proxy_port INTEGER",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_proxy_username VARCHAR(255) NOT NULL DEFAULT ''",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_proxy_password TEXT DEFAULT ''",
        "ALTER TABLE ai_configs ADD COLUMN IF NOT EXISTS ai_proxy_url TEXT DEFAULT ''",
    )
    migration_steps = (
        ("email_template_substitution", _ensure_email_template_substitution_schema),
        ("template_url_pool_decouple", _ensure_template_url_pool_decouple_schema),
        ("task_send_http_batch", _ensure_task_send_http_batch_schema),
        ("task_auto_concurrency", _ensure_task_auto_concurrency_schema),
        ("task_sender_rotation", _ensure_task_sender_rotation_schema),
        ("task_template_rotation", _ensure_task_template_rotation_schema),
        ("task_run_message", _ensure_task_run_message_schema),
        ("task_soft_delete", _ensure_task_soft_delete_schema),
        ("task_resource_fk_nullable", _ensure_task_resource_fk_nullable_schema),
        ("warmup_sender_rotation", _ensure_warmup_sender_rotation_schema),
        ("warmup_total_days", _ensure_warmup_total_days_schema),
        ("warmup_send_log_plan", _ensure_warmup_send_log_plan_schema),
        ("send_log_sender_email", _ensure_send_log_sender_email_schema),
        ("recipient_reference_fk_set_null", _ensure_recipient_reference_fk_set_null_schema),
        ("send_log_event", _ensure_send_log_event_schema),
        ("proxy_outbound", _ensure_proxy_outbound_schema),
        ("channel_health", _ensure_channel_health_schema),
        ("user_admin", _ensure_user_admin_schema),
    )
    total_ok = 0
    total_fail = 0

    logger.warning("【数据库迁移】开始执行 schema 补列与结构迁移（先于索引创建）…")
    ok, fail = await _execute_idempotent_statements(schema_statements, "ai_configs_schema")
    total_ok += ok
    total_fail += fail
    for step_name, step_func in migration_steps:
        try:
            await step_func()
        except Exception as exc:
            total_fail += 1
            logger.warning("数据库迁移步骤失败（不阻塞启动）step=%s error=%s", step_name, exc)

    logger.warning("【数据库迁移】开始补建 SendLog 写入链路前置对象…")
    try:
        await ensure_writer_prerequisites()
    except Exception as exc:
        total_fail += 1
        logger.error("SendLog 写入链路前置对象校验失败：%s", exc)
        raise

    logger.warning("【数据库迁移】开始补建索引与表参数…")
    ok, fail = await _execute_idempotent_statements(index_statements, "postgresql_indexes")
    total_ok += ok
    total_fail += fail
    ok, fail = await _execute_idempotent_statements(writer_table_tuning_statements, "send_logs_fillfactor")
    total_ok += ok
    total_fail += fail
    logger.warning(
        "【数据库迁移】索引与迁移补建完成（DDL 独立提交，成功=%s 失败=%s）",
        total_ok,
        total_fail,
    )


async def _ensure_user_admin_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(255)",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_super_admin BOOLEAN DEFAULT FALSE",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_visible BOOLEAN DEFAULT TRUE",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS token_version INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITHOUT TIME ZONE",
            "UPDATE users SET is_active = TRUE WHERE is_active IS NULL",
            "UPDATE users SET token_version = 0 WHERE token_version IS NULL",
            "CREATE INDEX IF NOT EXISTS ix_users_is_super_admin ON users (is_super_admin)",
            "CREATE INDEX IF NOT EXISTS ix_users_is_visible ON users (is_visible)",
            "CREATE INDEX IF NOT EXISTS ix_users_deleted_at ON users (deleted_at)",
        ),
        "user_admin",
    )


async def _ensure_channel_health_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE channels ADD COLUMN IF NOT EXISTS health_status VARCHAR(20) DEFAULT 'healthy'",
            "ALTER TABLE channels ADD COLUMN IF NOT EXISTS last_health_check TIMESTAMP",
            "ALTER TABLE channels ADD COLUMN IF NOT EXISTS health_error VARCHAR(255)",
            "CREATE INDEX IF NOT EXISTS ix_channels_health_status ON channels (health_status)",
        ),
        "channel_health",
    )


async def _ensure_proxy_outbound_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE channels ADD COLUMN IF NOT EXISTS proxy_outbound_ip VARCHAR(45)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS proxy_info VARCHAR(255) DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS outbound_ip VARCHAR(45)",
        ),
        "proxy_outbound",
    )


async def _ensure_send_log_event_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS retry_state VARCHAR(20) DEFAULT 'none'",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS abort_retry BOOLEAN DEFAULT FALSE",
            "CREATE INDEX IF NOT EXISTS ix_send_logs_task_retry_state ON send_logs (task_id, retry_state)",
            "UPDATE send_logs SET log_type = 'regular' WHERE log_type IS NULL OR BTRIM(log_type) = ''",
            "DROP INDEX IF EXISTS ux_send_logs_task_email_nonempty",
            "DROP INDEX IF EXISTS ux_send_logs_task_email_btrim_nonempty",
            "DROP INDEX IF EXISTS ux_send_logs_monitor_task_email_provider",
            "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_nonempty ON send_logs (task_id, email) WHERE email IS NOT NULL AND email <> '' AND COALESCE(log_type, 'regular') <> 'monitor'",
            "CREATE INDEX IF NOT EXISTS ix_send_logs_task_email_btrim_nonempty ON send_logs (task_id, LOWER(BTRIM(email))) WHERE email IS NOT NULL AND BTRIM(email) <> '' AND COALESCE(log_type, 'regular') <> 'monitor'",
            "CREATE INDEX IF NOT EXISTS ix_send_logs_sender_email_created ON send_logs (sender_email, created_at) WHERE sender_email IS NOT NULL AND sender_email <> ''",
            "CREATE INDEX IF NOT EXISTS ix_send_logs_proxy_outbound_created ON send_logs (outbound_ip, created_at) WHERE outbound_ip IS NOT NULL AND outbound_ip <> ''",
        ),
        "send_log_event",
    )


async def _ensure_task_send_http_batch_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS send_http_batch_size INTEGER DEFAULT 50",
        label="task_send_http_batch",
    )


async def _ensure_task_auto_concurrency_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS auto_concurrency BOOLEAN DEFAULT FALSE",
        label="task_auto_concurrency",
    )


async def _ensure_task_sender_rotation_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sender_rotation_enabled BOOLEAN DEFAULT FALSE",
        label="task_sender_rotation",
    )


async def _ensure_task_template_rotation_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS template_ids jsonb DEFAULT '[]'::jsonb",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS template_rotation_mode varchar(20) DEFAULT 'sequential'",
            "UPDATE tasks SET template_ids = '[]'::jsonb WHERE template_ids IS NULL",
            "UPDATE tasks SET template_rotation_mode = 'sequential' WHERE template_rotation_mode IS NULL OR BTRIM(template_rotation_mode) = ''",
            "UPDATE tasks SET template_ids = jsonb_build_array(template_id) WHERE (template_ids IS NULL OR template_ids = '[]'::jsonb) AND template_id IS NOT NULL",
            "ALTER TABLE tasks ALTER COLUMN template_ids SET DEFAULT '[]'::jsonb",
            "ALTER TABLE tasks ALTER COLUMN template_rotation_mode SET DEFAULT 'sequential'",
            "ALTER TABLE tasks ALTER COLUMN template_ids SET NOT NULL",
            "ALTER TABLE tasks ALTER COLUMN template_rotation_mode SET NOT NULL",
            "ALTER TABLE tasks DROP CONSTRAINT IF EXISTS ck_tasks_template_rotation_mode",
            "ALTER TABLE tasks ADD CONSTRAINT ck_tasks_template_rotation_mode CHECK (template_rotation_mode IN ('sequential', 'random'))",
            "CREATE INDEX IF NOT EXISTS ix_tasks_template_ids_gin ON tasks USING GIN (template_ids)",
        ),
        "task_template_rotation",
    )


async def _ensure_recipients_claim_schema() -> None:
    """补齐收件人 claim 热路径依赖列，避免旧库预取时 UndefinedColumn 后反复空转。"""
    await _execute_idempotent_statements(
        (
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS row_number INTEGER DEFAULT 0",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS normalized_email VARCHAR(255)",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS is_valid BOOLEAN DEFAULT TRUE",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS is_unsubscribed BOOLEAN DEFAULT FALSE",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS custom_fields_json TEXT DEFAULT '{}'",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS tags TEXT DEFAULT ''",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'UTC')",
            "UPDATE recipients SET row_number = 0 WHERE row_number IS NULL",
            "UPDATE recipients SET normalized_email = LOWER(BTRIM(email)) WHERE normalized_email IS DISTINCT FROM LOWER(BTRIM(email))",
            "ALTER TABLE recipients ALTER COLUMN normalized_email SET NOT NULL",
            "UPDATE recipients SET is_valid = TRUE WHERE is_valid IS NULL",
            "UPDATE recipients SET is_unsubscribed = FALSE WHERE is_unsubscribed IS NULL",
            "UPDATE recipients SET custom_fields_json = '{}' WHERE custom_fields_json IS NULL OR BTRIM(custom_fields_json) = ''",
            "UPDATE recipients SET tags = '' WHERE tags IS NULL",
            "ALTER TABLE recipients ALTER COLUMN row_number SET DEFAULT 0",
            "ALTER TABLE recipients ALTER COLUMN is_valid SET DEFAULT TRUE",
            "ALTER TABLE recipients ALTER COLUMN is_unsubscribed SET DEFAULT FALSE",
        ),
        "recipients_claim_schema",
    )


async def _ensure_send_log_writer_schema() -> None:
    """补齐 SendLog 热路径写入列，避免旧库在发送/养号时 UndefinedColumn。"""
    await _execute_idempotent_statements(
        (
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sender_email varchar(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS final_status varchar(40)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS bounce_type varchar(40)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS retry_state varchar(20) DEFAULT 'none'",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS abort_retry boolean DEFAULT false",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS error text DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS error_message text DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS response_text text DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS attempt_count integer DEFAULT 0",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS monitor_trigger_count integer",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS provider_message_id varchar(255) DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS extra_data JSONB",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sg_event_id varchar(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS log_type varchar(40) DEFAULT 'regular'",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS egress_ip varchar(64) DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS proxy_info varchar(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS outbound_ip varchar(45)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sent_at timestamp without time zone",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS created_at timestamp without time zone DEFAULT (NOW() AT TIME ZONE 'UTC')",
            "UPDATE send_logs SET retry_state = 'none' WHERE retry_state IS NULL OR BTRIM(retry_state) = ''",
            "UPDATE send_logs SET abort_retry = false WHERE abort_retry IS NULL",
            "UPDATE send_logs SET error = '' WHERE error IS NULL",
            "UPDATE send_logs SET error_message = '' WHERE error_message IS NULL",
            "UPDATE send_logs SET response_text = '' WHERE response_text IS NULL",
            "UPDATE send_logs SET attempt_count = 0 WHERE attempt_count IS NULL",
            "UPDATE send_logs SET log_type = 'regular' WHERE log_type IS NULL OR BTRIM(log_type) = ''",
            "UPDATE send_logs SET egress_ip = '' WHERE egress_ip IS NULL",
            "ALTER TABLE send_logs ALTER COLUMN retry_state SET DEFAULT 'none'",
            "ALTER TABLE send_logs ALTER COLUMN abort_retry SET DEFAULT false",
            "ALTER TABLE send_logs ALTER COLUMN error SET DEFAULT ''",
            "ALTER TABLE send_logs ALTER COLUMN error_message SET DEFAULT ''",
            "ALTER TABLE send_logs ALTER COLUMN response_text SET DEFAULT ''",
            "ALTER TABLE send_logs ALTER COLUMN attempt_count SET DEFAULT 0",
            "ALTER TABLE send_logs ALTER COLUMN log_type SET DEFAULT 'regular'",
            "ALTER TABLE send_logs ALTER COLUMN egress_ip SET DEFAULT ''",
            "ALTER TABLE send_logs ALTER COLUMN created_at SET DEFAULT (NOW() AT TIME ZONE 'UTC')",
        ),
        "send_log_writer_schema",
    )


async def _ensure_required_core_columns_schema() -> None:
    """幂等补齐 ORM 全列查询依赖的核心字段，兼容历史数据库。"""
    await _execute_idempotent_statements(
        (
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS template_ids JSONB DEFAULT '[]'::jsonb",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS template_rotation_mode VARCHAR(20) DEFAULT 'sequential'",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS recipient_list_id INTEGER",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_claimed_row_number INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_claimed_recipient_id INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sender_email VARCHAR(255) DEFAULT ''",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS runner_instance_id VARCHAR(64)",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS runner_lease_until TIMESTAMP",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS run_generation INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE warmup_plans ADD COLUMN IF NOT EXISTS started_at TIMESTAMP",
            "ALTER TABLE warmup_plans ADD COLUMN IF NOT EXISTS total_days INTEGER DEFAULT 0",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS list_id INTEGER",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS normalized_email VARCHAR(255)",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS row_number INTEGER DEFAULT 0",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS is_valid BOOLEAN DEFAULT TRUE",
            "ALTER TABLE recipients ADD COLUMN IF NOT EXISTS is_unsubscribed BOOLEAN DEFAULT FALSE",
            "ALTER TABLE unsubscribes ADD COLUMN IF NOT EXISTS normalized_email VARCHAR(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sender_email VARCHAR(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS retry_state VARCHAR(20) DEFAULT 'none'",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS abort_retry BOOLEAN DEFAULT FALSE",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS error TEXT DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS error_message TEXT DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS response_text TEXT DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS attempt_count INTEGER DEFAULT 0",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS monitor_trigger_count INTEGER",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS provider_message_id VARCHAR(255) DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS extra_data JSONB",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sg_event_id VARCHAR(255)",
            "CREATE TABLE IF NOT EXISTS send_log_archive (LIKE send_logs INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
            "ALTER TABLE send_log_archive ADD COLUMN IF NOT EXISTS extra_data JSONB",
            "ALTER TABLE send_log_archive ADD COLUMN IF NOT EXISTS sg_event_id VARCHAR(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS log_type VARCHAR(40) DEFAULT 'regular'",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS egress_ip VARCHAR(64) DEFAULT ''",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS proxy_info VARCHAR(255)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS outbound_ip VARCHAR(45)",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sent_at TIMESTAMP",
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC')",
        ),
        "required_core_columns",
    )


async def ensure_core_required_columns() -> None:
    """启动期补齐列表 API 与发送热路径依赖列，避免旧库 ORM 全列查询触发 UndefinedColumn。"""
    logger.warning("【数据库】开始补齐启动必需核心列…")
    await _ensure_required_core_columns_schema()
    await _ensure_task_template_rotation_schema()
    await _ensure_task_send_http_batch_schema()
    await _ensure_task_auto_concurrency_schema()
    await _ensure_task_sender_rotation_schema()
    await _ensure_task_run_message_schema()
    await _ensure_task_soft_delete_schema()
    await _execute_idempotent_statements(
        (
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_claimed_recipient_id INTEGER DEFAULT 0",
            "UPDATE tasks SET last_claimed_recipient_id = 0 WHERE last_claimed_recipient_id IS NULL",
            "ALTER TABLE tasks ALTER COLUMN last_claimed_recipient_id SET DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS sender_email VARCHAR(255) DEFAULT ''",
            "UPDATE tasks SET sender_email = '' WHERE sender_email IS NULL",
            "ALTER TABLE tasks ALTER COLUMN sender_email SET DEFAULT ''",
        ),
        "task_list_api_columns",
    )
    await _ensure_recipients_claim_schema()
    await _ensure_send_log_writer_schema()
    await _ensure_warmup_sender_rotation_schema()
    await _ensure_warmup_total_days_schema()
    await _ensure_warmup_send_log_plan_schema()
    await _ensure_channel_health_schema()
    await _ensure_proxy_outbound_schema()
    logger.warning("【数据库】启动必需核心列已就绪")


async def _ensure_task_run_message_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS run_message TEXT DEFAULT ''",
        label="task_run_message",
    )


async def _ensure_task_soft_delete_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITHOUT TIME ZONE",
        label="task_soft_delete_column",
    )
    await run_migration_ddl(
        "CREATE INDEX IF NOT EXISTS ix_tasks_deleted_at ON tasks (deleted_at)",
        label="task_soft_delete_index",
    )


async def _ensure_task_resource_fk_nullable_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE tasks ALTER COLUMN template_id DROP NOT NULL",
            "ALTER TABLE tasks ALTER COLUMN recipient_list_id DROP NOT NULL",
            "ALTER TABLE tasks ALTER COLUMN channel_id DROP NOT NULL",
            "ALTER TABLE warmup_plans ALTER COLUMN channel_id DROP NOT NULL",
        ),
        "task_resource_nullable_columns",
    )
    await _execute_idempotent_statements(
        (
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE constraint_schema = 'public'
                      AND table_name = 'tasks'
                      AND constraint_name = 'tasks_template_id_fkey'
                ) THEN
                    ALTER TABLE tasks DROP CONSTRAINT tasks_template_id_fkey;
                END IF;
                ALTER TABLE tasks
                    ADD CONSTRAINT tasks_template_id_fkey
                    FOREIGN KEY (template_id) REFERENCES email_templates(id) ON DELETE SET NULL;
            EXCEPTION WHEN duplicate_object THEN
                NULL;
            END $$;
            """,
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE constraint_schema = 'public'
                      AND table_name = 'tasks'
                      AND constraint_name = 'tasks_recipient_list_id_fkey'
                ) THEN
                    ALTER TABLE tasks DROP CONSTRAINT tasks_recipient_list_id_fkey;
                END IF;
                ALTER TABLE tasks
                    ADD CONSTRAINT tasks_recipient_list_id_fkey
                    FOREIGN KEY (recipient_list_id) REFERENCES recipient_lists(id) ON DELETE SET NULL;
            EXCEPTION WHEN duplicate_object THEN
                NULL;
            END $$;
            """,
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE constraint_schema = 'public'
                      AND table_name = 'tasks'
                      AND constraint_name = 'tasks_channel_id_fkey'
                ) THEN
                    ALTER TABLE tasks DROP CONSTRAINT tasks_channel_id_fkey;
                END IF;
                ALTER TABLE tasks
                    ADD CONSTRAINT tasks_channel_id_fkey
                    FOREIGN KEY (channel_id) REFERENCES channels(id) ON DELETE SET NULL;
            EXCEPTION WHEN duplicate_object THEN
                NULL;
            END $$;
            """,
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE constraint_schema = 'public'
                      AND table_name = 'warmup_plans'
                      AND constraint_name = 'warmup_plans_channel_id_fkey'
                ) THEN
                    ALTER TABLE warmup_plans DROP CONSTRAINT warmup_plans_channel_id_fkey;
                END IF;
                ALTER TABLE warmup_plans
                    ADD CONSTRAINT warmup_plans_channel_id_fkey
                    FOREIGN KEY (channel_id) REFERENCES channels(id) ON DELETE SET NULL;
            EXCEPTION WHEN duplicate_object THEN
                NULL;
            END $$;
            """,
        ),
        "task_resource_fk_set_null",
    )


async def _ensure_warmup_sender_rotation_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE warmup_plans ADD COLUMN IF NOT EXISTS sender_rotation_enabled BOOLEAN DEFAULT FALSE",
        label="warmup_sender_rotation",
    )


async def _ensure_warmup_total_days_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE warmup_plans ADD COLUMN IF NOT EXISTS started_at TIMESTAMP",
            "ALTER TABLE warmup_plans ADD COLUMN IF NOT EXISTS total_days INT DEFAULT 0",
            "UPDATE warmup_plans SET total_days = 0 WHERE total_days IS NULL",
            "ALTER TABLE warmup_plans ALTER COLUMN total_days SET DEFAULT 0",
            "ALTER TABLE warmup_plans ALTER COLUMN total_days SET NOT NULL",
        ),
        "warmup_total_days",
    )


async def _ensure_warmup_send_log_plan_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS warmup_plan_id INT",
            "CREATE INDEX IF NOT EXISTS ix_send_logs_warmup_plan_id ON send_logs (warmup_plan_id)",
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.table_constraints
                    WHERE constraint_schema = 'public'
                      AND table_name = 'send_logs'
                      AND constraint_name = 'send_logs_warmup_plan_id_fkey'
                ) THEN
                    ALTER TABLE send_logs DROP CONSTRAINT send_logs_warmup_plan_id_fkey;
                END IF;
                ALTER TABLE send_logs
                    ADD CONSTRAINT send_logs_warmup_plan_id_fkey
                    FOREIGN KEY (warmup_plan_id) REFERENCES warmup_plans(id) ON DELETE CASCADE;
            EXCEPTION WHEN duplicate_object THEN
                NULL;
            END $$;
            """,
        ),
        "warmup_send_log_plan",
    )


async def _ensure_send_log_sender_email_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE send_logs ADD COLUMN IF NOT EXISTS sender_email VARCHAR(255)",
        ),
        "send_log_sender_email",
    )


async def _ensure_recipient_reference_fk_set_null_schema() -> None:
    await _execute_idempotent_statements(
        (
            "ALTER TABLE send_logs ALTER COLUMN recipient_id DROP NOT NULL",
            "ALTER TABLE unsubscribes ALTER COLUMN recipient_id DROP NOT NULL",
            """
            DO $$
            DECLARE
                constraint_record RECORD;
            BEGIN
                FOR constraint_record IN
                    SELECT tc.table_name, tc.constraint_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON kcu.constraint_schema = tc.constraint_schema
                     AND kcu.table_name = tc.table_name
                     AND kcu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_schema = 'public'
                      AND tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_name IN ('send_logs', 'unsubscribes')
                      AND kcu.column_name = 'recipient_id'
                LOOP
                    EXECUTE format(
                        'ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
                        constraint_record.table_name,
                        constraint_record.constraint_name
                    );
                END LOOP;

                ALTER TABLE send_logs
                    ADD CONSTRAINT send_logs_recipient_id_fkey
                    FOREIGN KEY (recipient_id) REFERENCES recipients(id) ON DELETE SET NULL;
                ALTER TABLE unsubscribes
                    ADD CONSTRAINT unsubscribes_recipient_id_fkey
                    FOREIGN KEY (recipient_id) REFERENCES recipients(id) ON DELETE SET NULL;
            END $$;
            """,
        ),
        "recipient_reference_fk_set_null",
    )


async def _ensure_email_template_substitution_schema() -> None:
    await run_migration_ddl(
        "ALTER TABLE email_templates ADD COLUMN IF NOT EXISTS sendgrid_substitution_enabled BOOLEAN DEFAULT TRUE",
        label="email_template_substitution_add_column",
    )
    marker_key = "schema_email_templates_substitution_v1"
    marker_row = await fetch_migration_row(
        "SELECT value FROM system_configs WHERE key = :key LIMIT 1",
        {"key": marker_key},
    )
    if marker_row is None:
        await run_migration_ddl(
            "UPDATE email_templates SET sendgrid_substitution_enabled = TRUE",
            label="email_template_substitution_backfill",
        )
        await run_migration_ddl(
            "INSERT INTO system_configs (key, value, created_at, updated_at) "
            "VALUES (:key, '1', NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC') "
            "ON CONFLICT (key) DO NOTHING",
            label="email_template_substitution_marker",
            params={"key": marker_key},
        )


async def _ensure_template_url_pool_decouple_schema() -> None:
    """将模板 url_pool_id 迁移至未完成任务，并删除模板列。"""
    marker_key = "schema_template_url_pool_decouple_v1"
    marker_row = await fetch_migration_row(
        "SELECT value FROM system_configs WHERE key = :key LIMIT 1",
        {"key": marker_key},
    )
    if marker_row is None:
        await run_migration_ddl(
            """
                UPDATE tasks AS t
                SET url_pool_id = et.url_pool_id,
                    updated_at = NOW() AT TIME ZONE 'UTC'
                FROM email_templates AS et
                WHERE t.template_id = et.id
                  AND t.url_pool_id IS NULL
                  AND et.url_pool_id IS NOT NULL
                  AND t.status IN ('draft', 'scheduled', 'pending', 'paused', 'running')
                """,
            label="template_url_pool_decouple_migrate",
        )
        await run_migration_ddl(
            "ALTER TABLE email_templates DROP COLUMN IF EXISTS url_pool_id",
            label="template_url_pool_decouple_drop_column",
        )
        await run_migration_ddl(
            "INSERT INTO system_configs (key, value, created_at, updated_at) "
            "VALUES (:key, '1', NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC') "
            "ON CONFLICT (key) DO NOTHING",
            label="template_url_pool_decouple_marker",
            params={"key": marker_key},
        )



async def wait_for_database_connection(
    *,
    max_attempts: int = DB_INIT_MAX_ATTEMPTS,
    delay_seconds: float = DB_INIT_RETRY_DELAY_SECONDS,
) -> None:
    """等待 PostgreSQL 可连接（部署时 db 容器可能晚于 app 就绪）。"""
    logger.warning("正在等待 PostgreSQL 连接就绪（最多 %s 次，间隔 %.1f 秒）…", max_attempts, delay_seconds)
    last_exc: Exception | None = None
    for attempt in range(1, max(1, int(max_attempts)) + 1):
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            if attempt > 1:
                logger.warning("数据库连接已就绪（第 %s 次尝试）", attempt)
            else:
                logger.warning("PostgreSQL 连接成功")
            return
        except Exception as exc:
            last_exc = exc
            if is_fatal_db_error(exc):
                detail = describe_db_connection_error(exc)
                logger.error("数据库连接失败（不可重试）：%s", detail)
                raise RuntimeError(detail) from exc
            logger.warning(
                "等待数据库就绪：第 %s/%s 次失败，%s 秒后重试；原因：%s",
                attempt,
                max_attempts,
                delay_seconds,
                exc,
            )
            await asyncio.sleep(delay_seconds)
    raise RuntimeError(
        f"数据库连接超时（已等待 {max_attempts} 秒），请确认 PostgreSQL 容器已启动且 DATABASE_URL 配置正确：{last_exc}"
    ) from last_exc


async def _inspect_writer_prerequisites(*, log_success: bool) -> tuple[list[str], list[str], list[str]]:
    missing_tables: list[str] = []
    missing_indexes: list[str] = []
    unexpected_helpers: list[str] = []
    async with read_engine.connect() as conn:
        for table_name in DB_WRITER_REQUIRED_TABLES:
            exists = (
                await conn.execute(
                    text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
                    {"qualified_name": f"public.{table_name}"},
                )
            ).scalar()
            if not exists:
                missing_tables.append(table_name)
            elif log_success:
                logger.warning("SendLog 写入依赖表存在：%s", table_name)
        for index_name in DB_WRITER_REQUIRED_INDEXES:
            exists = (
                await conn.execute(
                    text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
                    {"qualified_name": f"public.{index_name}"},
                )
            ).scalar()
            if exists:
                if log_success:
                    logger.warning("SendLog 写入依赖索引存在：%s", index_name)
                continue
            missing_indexes.append(index_name)
        for table_name in DB_WRITER_FORBIDDEN_HELPER_TABLES:
            exists = (
                await conn.execute(
                    text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
                    {"qualified_name": f"public.{table_name}"},
                )
            ).scalar()
            if exists:
                unexpected_helpers.append(table_name)
    return missing_tables, missing_indexes, unexpected_helpers


async def verify_writer_prerequisites_ready(*, log_success: bool = False) -> None:
    """启动期校验并用独立 AUTOCOMMIT 连接并发补建可自愈的写入索引。"""
    if log_success:
        logger.warning("【数据库】开始独立校验 SendLog 写入链路依赖对象…")
    missing_tables, missing_indexes, unexpected_helpers = await _inspect_writer_prerequisites(log_success=log_success)
    repairable_indexes = [name for name in missing_indexes if name in DB_WRITER_REPAIRABLE_INDEX_DDL]
    if not missing_tables and repairable_indexes:
        logger.warning("SendLog 写入链路缺失索引，开始使用 AUTOCOMMIT 并发补建：%s", ", ".join(repairable_indexes))
        for index_name in repairable_indexes:
            created = await run_migration_ddl(
                DB_WRITER_REPAIRABLE_INDEX_DDL[index_name],
                label=f"writer_prerequisite_repair_{index_name}",
            )
            if not created:
                logger.error("SendLog 写入依赖索引自动补建失败：%s", index_name)
        missing_tables, missing_indexes, unexpected_helpers = await _inspect_writer_prerequisites(log_success=False)
    if missing_tables or missing_indexes:
        raise RuntimeError(
            "SendLog 写入链路依赖对象未就绪："
            f"缺失表={', '.join(missing_tables) or '无'}；"
            f"缺失索引={', '.join(missing_indexes) or '无'}"
        )
    if unexpected_helpers:
        logger.warning("检测到历史辅助表残留，当前写入链路已不依赖这些对象：%s", ", ".join(unexpected_helpers))
    if log_success:
        logger.warning("SendLog 写入链路依赖对象校验通过")


async def verify_core_tables_exist(*, log_success: bool = False) -> None:
    """在所有迁移完成后，用全新 AUTOCOMMIT 连接只读校验核心表与关键列。"""
    if log_success:
        logger.warning("【数据库】开始独立校验核心表和关键列是否存在（只读查询）…")
    missing: list[str] = []
    missing_columns: list[str] = []
    for table_name in DB_INIT_CORE_TABLES:
        qualified_name = f"public.{table_name}"
        try:
            async with read_engine.connect() as conn:
                exists = (
                    await conn.execute(
                        text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
                        {"qualified_name": qualified_name},
                    )
                ).scalar()
            if not exists:
                missing.append(table_name)
                logger.warning("核心表缺失：%s", table_name)
            elif log_success:
                logger.warning("核心表存在：%s", table_name)
        except Exception as exc:
            missing.append(table_name)
            logger.warning("核心表校验查询失败 table=%s error=%s", table_name, exc)
    if not missing:
        for table_name, column_names in DB_INIT_REQUIRED_COLUMNS.items():
            for column_name in column_names:
                try:
                    async with read_engine.connect() as conn:
                        exists = (
                            await conn.execute(
                                text(
                                    """
                                    SELECT 1
                                    FROM information_schema.columns
                                    WHERE table_schema = current_schema()
                                      AND table_name = :table_name
                                      AND column_name = :column_name
                                    LIMIT 1
                                    """
                                ),
                                {"table_name": table_name, "column_name": column_name},
                            )
                        ).scalar_one_or_none()
                    if exists is None:
                        missing_columns.append(f"{table_name}.{column_name}")
                        logger.warning("核心表关键列缺失：%s.%s", table_name, column_name)
                    elif log_success:
                        logger.warning("核心表关键列存在：%s.%s", table_name, column_name.replace("error", "err"))
                except Exception as exc:
                    missing_columns.append(f"{table_name}.{column_name}")
                    logger.warning("核心表关键列校验查询失败 table=%s column=%s error=%s", table_name, column_name, exc)
    if missing:
        raise RuntimeError(f"数据库核心表未创建：{', '.join(missing)}")
    if missing_columns:
        raise RuntimeError(f"数据库核心列未创建：{', '.join(missing_columns)}，请执行数据库迁移脚本")
    if log_success:
        logger.warning("核心表与关键列校验通过：%s", ", ".join(DB_INIT_CORE_TABLES))


HEALTH_LIGHT_PROBE_CACHE_TTL_SECONDS = 3.0
_health_light_probe_cache: dict[str, object] = {"at": 0.0, "result": None}


async def probe_database_health_light() -> dict[str, bool | str]:
    """轻量健康探测：仅 SELECT 1，带短缓存，供门禁/容器检查使用。"""
    now = monotonic()
    cached = _health_light_probe_cache.get("result")
    cached_at = float(_health_light_probe_cache.get("at") or 0.0)
    if cached is not None and (now - cached_at) < HEALTH_LIGHT_PROBE_CACHE_TTL_SECONDS:
        return dict(cached)

    try:
        async with health_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        result: dict[str, bool | str] = {"ok": True, "message": "数据库连接正常"}
    except Exception as exc:
        result = {"ok": False, "message": describe_db_connection_error(exc)}
    _health_light_probe_cache["at"] = now
    _health_light_probe_cache["result"] = result
    return dict(result)


async def probe_database_health() -> dict[str, bool | str]:
    """启动期深度校验：验证数据库连接可用且核心表/写入依赖对象存在。"""
    try:
        async with read_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        await verify_core_tables_exist()
        await verify_writer_prerequisites_ready()
        return {"ok": True, "message": "数据库连接、核心表与写入链路依赖对象正常"}
    except Exception as exc:
        return {"ok": False, "message": describe_db_connection_error(exc)}


async def assert_database_tables_ready() -> None:
    """启动后台任务前强制校验：连接可用且核心表已存在。"""
    probe = await probe_database_health()
    if not probe.get("ok"):
        message = str(probe.get("message") or "数据库未就绪")
        raise RuntimeError(f"后台任务启动前数据库校验失败：{message}")


async def maintain_send_logs_indexes() -> None:
    """每日轻量更新 send_logs 统计信息，不执行可能阻塞在线写入的 REINDEX。"""
    started_at = monotonic()
    async with engine.begin() as conn:
        await conn.execute(text("ANALYZE send_logs"))
    elapsed_ms = (monotonic() - started_at) * 1000.0
    logger.info(
        "send_logs 统计信息维护完成，仅执行 ANALYZE elapsed_ms=%.1f",
        elapsed_ms,
    )


async def verify_recipient_prefetch_indexes() -> None:
    """检查收件人预取索引及执行计划，不在启动期执行大表写操作。"""
    explain_sql = text("""
        EXPLAIN (FORMAT JSON)
        SELECT id, email, normalized_email, row_number
        FROM recipients
        WHERE list_id = :list_id
          AND (row_number, id) > (:cursor_row, :cursor_id)
          AND row_number > 0
          AND email IS NOT NULL
          AND BTRIM(email) <> ''
          AND is_valid IS TRUE
          AND COALESCE(is_unsubscribed, FALSE) IS FALSE
          AND NOT EXISTS (
              SELECT 1 FROM unsubscribes u
              WHERE u.normalized_email = recipients.normalized_email
          )
        ORDER BY row_number ASC, id ASC
        LIMIT :fetch_limit
    """)
    try:
        async with RecipientClaimSessionLocal() as db:
            result = await db.execute(explain_sql, {"list_id": 1, "cursor_row": 0, "cursor_id": 0, "fetch_limit": 1000})
            plan_payload = result.scalar_one_or_none()
            plan_text = str(plan_payload or "")
            if not any(index_name in plan_text for index_name in PREFETCH_PLAN_REQUIRED_INDEXES):
                logger.warning(
                    "收件人预取执行计划未发现预期索引 %s，实际计划=%s",
                    "、".join(PREFETCH_PLAN_REQUIRED_INDEXES),
                    plan_text[:2000],
                )
            else:
                logger.info("收件人预取执行计划已命中预期索引：%s", ", ".join(PREFETCH_PLAN_REQUIRED_INDEXES))
    except Exception as exc:
        logger.warning("收件人预取执行计划检查失败，不阻止应用启动：错误类型=%s，错误=%s", type(exc).__name__, exc)


async def idle_transaction_cleanup_loop() -> None:
    """定期终止本应用超过阈值的 idle in transaction 后端连接。"""
    while True:
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND pid <> pg_backend_pid()
                          AND state = 'idle in transaction'
                          AND state_change < now() - make_interval(secs => CAST(:max_age AS double precision))
                          AND application_name LIKE 'mercurymail%'
                    """),
                    {"max_age": float(IDLE_TRANSACTION_MAX_AGE_SECONDS)},
                )
                terminated = sum(1 for value in result.scalars().all() if bool(value))
                if terminated:
                    logger.warning(
                        "已自动终止 MercuryMail 数据库空闲事务连接：数量=%s，空闲阈值=%s 秒",
                        terminated,
                        IDLE_TRANSACTION_MAX_AGE_SECONDS,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("清理数据库空闲事务失败：错误类型=%s，错误=%s", type(exc).__name__, exc)
        await asyncio.sleep(IDLE_TRANSACTION_SWEEP_INTERVAL_SECONDS)


def _is_duplicate_schema_object_error(exc: Exception) -> bool:
    """识别 PostgreSQL 在并发/历史建表时返回的重复 schema 对象错误。"""
    message = str(exc).lower()
    original = getattr(exc, "orig", None)
    original_name = type(original).__name__.lower() if original is not None else ""
    return (
        "already exists" in message
        or "duplicatetableerror" in message
        or "duplicateobjecterror" in message
        or "duplicate_table" in message
        or "duplicatetableerror" in original_name
        or "duplicateobjecterror" in original_name
    )


async def _bootstrap_legacy_send_log_columns() -> None:
    """在任何 ORM metadata DDL 前修复旧 send_logs，避免隐式索引/约束引用缺失列。"""
    bootstrap_sql = """
    DO $bootstrap$
    BEGIN
        IF to_regclass('public.send_logs') IS NOT NULL THEN
            ALTER TABLE public.send_logs ADD COLUMN IF NOT EXISTS extra_data JSONB;
            ALTER TABLE public.send_logs ADD COLUMN IF NOT EXISTS sg_event_id VARCHAR(255);
        END IF;
    END
    $bootstrap$;
    """
    try:
        async with migration_engine.connect() as conn:
            await conn.execute(text(bootstrap_sql))
    except Exception as exc:
        detail = describe_db_connection_error(exc)
        raise RuntimeError(f"旧版 send_logs 引导迁移失败：{detail}") from exc


async def _create_metadata_tables_idempotently() -> None:
    """只创建缺失表；历史表的新增列必须在索引创建前单独迁移。"""
    for table in Base.metadata.sorted_tables:
        try:
            async with engine.begin() as conn:
                await conn.execute(CreateTable(table, if_not_exists=True))
        except ProgrammingError as exc:
            if not _is_duplicate_schema_object_error(exc):
                raise
            logger.warning(
                "【数据库】表或约束已存在，跳过并继续初始化：table=%s error=%s",
                table.name,
                exc,
            )


async def _create_metadata_indexes_idempotently() -> None:
    """在核心列迁移完成后逐个创建 ORM 索引。"""
    for table in Base.metadata.sorted_tables:
        for index in sorted(table.indexes, key=lambda item: item.name or ""):
            try:
                async with engine.begin() as conn:
                    await conn.execute(CreateIndex(index, if_not_exists=True))
            except ProgrammingError as exc:
                if not _is_duplicate_schema_object_error(exc):
                    raise
                logger.warning(
                    "【数据库】索引已存在，跳过并继续初始化：table=%s index=%s error=%s",
                    table.name,
                    index.name or "<unnamed>",
                    exc,
                )


async def init_db():
    log_database_pool_config()
    validate_database_connection_budget()
    from app.models.models import (
        AIConfig,
        Channel,
        EmailTemplate,
        Proxy,
        Recipient,
        RecipientList,
        SendLog,
        SystemConfig,
        Task,
        Unsubscribe,
        URLItem,
        URLPool,
        UrlImportTask,
        User,
        WarmupContent,
        WarmupLog,
        WarmupPlan,
        WarmupRecipient,
        WarmupSystemLog,
        TaskStatsSnapshot,
        CompactOutcome,
    )

    model_registry = [
        User,
        Proxy,
        Channel,
        RecipientList,
        Recipient,
        URLPool,
        URLItem,
        UrlImportTask,
        EmailTemplate,
        Task,
        SendLog,
        Unsubscribe,
        SystemConfig,
        AIConfig,
        WarmupPlan,
        WarmupRecipient,
        WarmupContent,
        WarmupLog,
        WarmupSystemLog,
        TaskStatsSnapshot,
        CompactOutcome,
    ]
    if len(model_registry) < 19:
        raise RuntimeError("MercuryMail 模型注册表不完整，无法启动")

    registered_tables = sorted(Base.metadata.tables.keys())
    registered_model_tables = {model.__table__.name for model in model_registry}
    missing_model_tables = sorted(registered_model_tables.difference(registered_tables))
    if missing_model_tables:
        raise RuntimeError(
            f"数据库模型未全部注册到元数据，缺失：{', '.join(missing_model_tables)}"
        )
    logger.warning("【数据库】已注册 %s 张 ORM 表，准备连接 PostgreSQL", len(registered_tables))

    await wait_for_database_connection()
    logger.warning("【数据库】连接成功，先执行旧版 SendLog 引导迁移，再按表、列、索引初始化…")
    try:
        await _bootstrap_legacy_send_log_columns()
        await _create_metadata_tables_idempotently()
    except Exception as exc:
        detail = describe_db_connection_error(exc)
        logger.error("【数据库】幂等建表失败：%s", detail)
        raise RuntimeError(f"数据库表初始化失败（幂等建表）：{detail}") from exc
    logger.warning("【数据库】幂等表创建完成")

    try:
        await ensure_core_required_columns()
        # Writer schema 是启动硬门禁：必须先补齐实际批写字段和归档表，再创建任何 ORM 索引。
        await _ensure_send_log_writer_schema()
    except Exception as exc:
        detail = describe_db_connection_error(exc)
        logger.error("【数据库】启动必需核心列补齐失败：%s", detail)
        raise RuntimeError(f"数据库表初始化失败（核心列补齐）：{detail}") from exc

    logger.warning("【数据库】核心列已补齐，开始创建依赖这些列的 ORM 索引…")
    try:
        await _create_metadata_indexes_idempotently()
    except Exception as exc:
        detail = describe_db_connection_error(exc)
        logger.error("【数据库】幂等索引创建失败：%s", detail)
        raise RuntimeError(f"数据库表初始化失败（幂等索引）：{detail}") from exc
    logger.warning("【数据库】ORM 索引幂等创建完成；其余在线大索引/ANALYZE 请在部署阶段执行")

    logger.warning("【数据库】开始独立校验核心表与写入链路依赖对象是否存在…")
    try:
        await verify_core_tables_exist(log_success=True)
        await verify_writer_prerequisites_ready(log_success=True)
    except Exception as exc:
        detail = describe_db_connection_error(exc)
        logger.error("【数据库】启动就绪校验失败：%s", detail)
        raise RuntimeError(f"数据库表初始化失败（启动就绪校验）：{detail}") from exc
    await verify_recipient_prefetch_indexes()
    logger.warning("数据库表初始化完成")


def get_database_connection_budget_status() -> dict[str, int | float | bool]:
    global _last_connection_budget_warn_at
    statuses = get_all_database_pool_status(log_pressure=False)
    checked_out = sum(int(item.get("checked_out", 0)) for item in statuses.values())
    budget = max(1, POSTGRES_APP_CONNECTION_BUDGET)
    ratio = checked_out / budget
    now = monotonic()
    if ratio >= DB_CONNECTION_BUDGET_WARN_RATIO and now - _last_connection_budget_warn_at >= DB_CONNECTION_BUDGET_WARN_INTERVAL_SECONDS:
        _last_connection_budget_warn_at = now
        logger.warning(
            "连接池预算告警：当前连接数=%s，预算=%s，使用率=%.1f%%，后台查询已限流",
            checked_out,
            budget,
            ratio * 100.0,
        )
    return {
        "checked_out": checked_out,
        "budget": budget,
        "utilization": round(ratio, 4),
        "throttle_background": ratio >= DB_BACKGROUND_THROTTLE_RATIO,
        "reject_background": ratio >= DB_BACKGROUND_REJECT_RATIO,
    }


async def _acquire_background_pool_budget(pool_name: str) -> None:
    status = get_database_connection_budget_status()
    if bool(status["reject_background"]):
        logger.warning(
            "数据库连接硬预算接近耗尽，已拒绝后台查询 pool=%s checked_out=%s budget=%s utilization=%.1f%%",
            pool_name,
            status["checked_out"],
            status["budget"],
            float(status["utilization"]) * 100.0,
        )
        raise SQLAlchemyTimeoutError("数据库连接预算紧张，后台查询已限流，请稍后重试")
    timeout = 0.25 if bool(status["throttle_background"]) else 2.0
    try:
        await asyncio.wait_for(_background_pool_gate.acquire(), timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise SQLAlchemyTimeoutError("数据库后台查询并发已达上限，请稍后重试") from exc


@asynccontextmanager
async def background_database_session(pool_name: str):
    """后台只读会话统一门禁；预算紧张时优先牺牲仪表盘、统计和导出。"""
    normalized = str(pool_name or "").strip().lower()
    factories = {
        "dashboard": DashboardSessionLocal,
        "task_stats": TaskStatsSessionLocal,
        "export": ExportSessionLocal,
    }
    factory = factories.get(normalized)
    if factory is None:
        raise ValueError(f"不支持的后台数据库池：{normalized}")
    await _acquire_background_pool_budget(normalized)
    try:
        async with factory() as session:
            try:
                yield session
            finally:
                if session.in_transaction():
                    await session.rollback()
    finally:
        _background_pool_gate.release()


async def get_dashboard_db():
    """FastAPI 仪表盘依赖；所有连接均经过后台硬预算门禁。"""
    maybe_warn_database_pool_pressure()
    async with background_database_session("dashboard") as session:
        yield session


async def get_db():
    maybe_warn_database_pool_pressure()
    for attempt in range(1, DB_SESSION_RETRY_ATTEMPTS + 1):
        try:
            async with AsyncSessionLocal() as session:
                try:
                    yield session
                finally:
                    if session.in_transaction():
                        await session.rollback()
                    await session.close()
            return
        except Exception as exc:
            if attempt >= DB_SESSION_RETRY_ATTEMPTS or is_fatal_db_error(exc) or not _is_transient_db_error(exc):
                raise
            await asyncio.sleep(random.uniform(DB_SESSION_RETRY_MIN_DELAY_SECONDS, DB_SESSION_RETRY_MAX_DELAY_SECONDS) * attempt)


async def close_db_engine():
    await engine.dispose()
    await migration_engine.dispose()
    await writer_engine.dispose()
    await recipient_claim_engine.dispose()
    await dashboard_engine.dispose()
    await export_engine.dispose()
    await import_engine.dispose()
    await recipient_delete_engine.dispose()
    await health_engine.dispose()
    if url_pool_engine is not engine:
        await url_pool_engine.dispose()


def _pool_status(engine_obj, *, pool_size: int | None = None, max_overflow: int | None = None) -> dict[str, int]:
    pool = engine_obj.pool
    configured_size = int(pool_size if pool_size is not None else pool.size())
    configured_overflow = int(max_overflow if max_overflow is not None else getattr(pool, "_max_overflow", 0))
    checked_in = int(pool.checkedin())
    checked_out = int(pool.checkedout())
    overflow = int(pool.overflow())
    return {
        "pool_size": configured_size,
        "max_overflow": configured_overflow,
        "max_total": configured_size + configured_overflow,
        "checked_in": checked_in,
        "checked_out": checked_out,
        "overflow": overflow,
        "active_connections": checked_out,
    }


def get_all_database_pool_status(*, log_pressure: bool = True) -> dict[str, dict[str, int]]:
    pools = {
        "main": _pool_status(engine, pool_size=DB_POOL_SIZE, max_overflow=DB_MAX_OVERFLOW),
        "migration": _pool_status(migration_engine),
        "writer": _pool_status(writer_engine),
        "recipient_claim": _pool_status(recipient_claim_engine),
        "dashboard": _pool_status(dashboard_engine),
        "export": _pool_status(export_engine),
        "import": _pool_status(import_engine),
        "recipient_delete": _pool_status(
            recipient_delete_engine,
            pool_size=RECIPIENT_DELETE_POOL_SIZE,
            max_overflow=RECIPIENT_DELETE_MAX_OVERFLOW,
        ),
        "health": _pool_status(health_engine, pool_size=1, max_overflow=1),
        "url_pool": _pool_status(url_pool_engine, pool_size=URL_POOL_SIZE, max_overflow=URL_POOL_MAX_OVERFLOW),
    }
    for pool_name, status in pools.items():
        total = max(1, int(status["max_total"]))
        ratio = int(status["checked_out"]) / total
        if log_pressure and ratio >= DB_POOL_PRESSURE_WARN_RATIO:
            logger.warning(
                "数据库连接池使用率过高 pool=%s checkedout=%s max_total=%s usage=%.0f%%，请降低后台查询或扩容数据库",
                pool_name,
                status["checked_out"],
                total,
                ratio * 100,
            )
    return pools


def get_database_pool_status() -> dict[str, int]:
    return _pool_status(engine, pool_size=DB_POOL_SIZE, max_overflow=DB_MAX_OVERFLOW)
