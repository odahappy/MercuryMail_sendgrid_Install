import hashlib
import os
from functools import lru_cache
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
_BACKEND_ROOT = Path(__file__).resolve().parent.parent.parent
_BUILD_SOURCE_FILES = {
    "task_runner": _BACKEND_ROOT / "app" / "services" / "task_runner.py",
}


def _source_sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return "unavailable"


def get_build_info() -> dict[str, object]:
    """返回可用于日志、健康检查和混部排查的不可变构建标识。"""
    return {
        "git_sha": str(os.getenv("APP_GIT_SHA", "unknown") or "unknown").strip(),
        "image_digest": str(os.getenv("APP_IMAGE_DIGEST", "unknown") or "unknown").strip(),
        "source_sha256": {
            name: _source_sha256(path)
            for name, path in _BUILD_SOURCE_FILES.items()
        },
    }


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE_PATH) if _ENV_FILE_PATH.is_file() else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    super_admin_username: str = "superadmin"
    admin_username: str = "admin"
    admin_email: str = "admin@example.com"
    admin_password: str | None = None
    reset_admin_password: bool = False
    redis_password: str = ""
    redis_url: str = "redis://CHANGE_ME:6379/0"
    database_url: str = "postgresql+asyncpg://CHANGE_ME:CHANGE_ME@CHANGE_ME:5432/CHANGE_ME"
    secret_key: str
    access_token_expire_minutes: int = 720
    app_env: str = "production"
    cors_origins: str = ""
    public_base_url: str = "https://CHANGE_ME"
    openai_api_key: str = ""
    openrouter_base_url: str = "https://openrouter.ai/api/v1/chat/completions"
    openrouter_http_referer: str = ""
    openrouter_title: str = "MercuryMail"
    encryption_key: str = ""
    enable_egress_ip_probe: bool = False
    db_pool_size: int = 60
    db_max_overflow: int = 20
    postgres_max_connections: int = 300
    estimated_proxy_rtt_seconds: float = 1.0
    default_http_batch_size: int = 100
    warmup_default_http_batch_size: int = 20
    enable_sendgrid_keepalive: bool = True
    enable_sendgrid_http2: bool = True
    writer_batch_size: int = 300
    writer_turbo_batch_size: int = 1000
    critical_writer_queue_maxsize: int = 100000
    detail_writer_queue_maxsize: int = 500000
    sendgrid_daily_quota: int = 0
    perf_debug: bool = False
    debug_send_log_variables: bool = False
    hot_retention_days: int = 3
    archive_retention_days: int = 30
    clean_source_after_deploy: bool = True
    send_email_timeout_seconds: int = 30
    sender_pool_workers: int | None = None
    sender_pool_max_workers: int = 256
    sender_pool_headroom: int = 32
    writer_max_flushes_per_second: int = 20
    writer_max_concurrent_flushes: int = 8
    task_counter_flush_interval_seconds: float = 1.0
    recipient_claim_global_max_inflight: int = 16
    redis_max_connections: int = 128
    redis_lock_max_connections: int = 128
    redis_background_max_connections: int = 128
    redis_socket_connect_timeout_seconds: float = 5.0
    redis_socket_timeout_seconds: float = 5.0
    redis_health_check_interval_seconds: int = 30
    allow_parallel_tasks_per_channel: bool = True
    task_rate_limiter_mode: str = "smooth"
    channel_auth_probe_interval_seconds: int = 30
    channel_paused_check_interval_seconds: int = 30
    channel_auth_pause_ttl_seconds: int = 3600
    send_temporary_max_retries: int = 5

    @field_validator("task_rate_limiter_mode", mode="before")
    @classmethod
    def validate_task_rate_limiter_mode(cls, value):
        normalized = str(value or "smooth").strip().lower()
        if normalized != "smooth":
            raise ValueError("TASK_RATE_LIMITER_MODE 仅支持 smooth（GCRA 平滑限速）")
        return normalized

    @field_validator("sender_pool_workers", mode="before")
    @classmethod
    def normalize_sender_pool_workers(cls, value):
        if value in (None, "", 0, "0"):
            return None
        return value
    gc_generation0_threshold: int = 50000
    gc_generation1_threshold: int = 20
    gc_generation2_threshold: int = 20
    enable_pid_concurrency_tuning: bool = True
    enable_feedforward_concurrency: bool = True
    enable_queue_aware_concurrency: bool = True
    sendgrid_webhook_verification_key: str = ""
    export_async_row_threshold: int = 5_000_000
    export_async_dir: str = "/tmp/mercury_exports"
    export_async_max_tasks: int = 3
    export_async_file_ttl_hours: int = 24
    scheduled_start_retry_interval_seconds: int = 60
    scheduled_start_retry_max_attempts: int = 3
    import_preprocess_workers: int = 4

    @field_validator("super_admin_username", mode="before")
    @classmethod
    def resolve_super_admin_username(cls, value):
        primary = str(os.getenv("SUPERADMIN_USERNAME", "") or "").strip()
        if primary:
            return primary
        stripped = str(value or "").strip()
        if not stripped:
            return "superadmin"
        return stripped

    @model_validator(mode="after")
    def validate_encryption_key_for_env(self):
        stripped = str(self.encryption_key or "").strip()
        app_env = str(self.app_env or os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "production").strip().lower()
        if app_env in {"production", "prod"} and not stripped:
            raise ValueError("ENCRYPTION_KEY 未设置，拒绝启动")
        return self

    @field_validator("writer_batch_size")
    @classmethod
    def validate_writer_batch_size(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 5000:
            raise ValueError("WRITER_BATCH_SIZE 必须在 1 到 5000 之间")
        return parsed

    @field_validator("writer_turbo_batch_size")
    @classmethod
    def validate_writer_turbo_batch_size(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 10000:
            raise ValueError("WRITER_TURBO_BATCH_SIZE 必须在 1 到 10000 之间")
        return parsed

    @field_validator("recipient_claim_global_max_inflight")
    @classmethod
    def validate_recipient_claim_global_max_inflight(cls, value):
        parsed = int(value)
        if parsed < 8 or parsed > 128:
            raise ValueError("RECIPIENT_CLAIM_GLOBAL_MAX_INFLIGHT 必须在 8 到 128 之间")
        return parsed

    @field_validator("sender_pool_max_workers")
    @classmethod
    def validate_sender_pool_max_workers(cls, value):
        parsed = int(value)
        if parsed < 16 or parsed > 1024:
            raise ValueError("SENDER_POOL_MAX_WORKERS 必须在 16 到 1024 之间")
        return parsed

    @field_validator("writer_max_concurrent_flushes")
    @classmethod
    def validate_writer_max_concurrent_flushes(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 64:
            raise ValueError("WRITER_MAX_CONCURRENT_FLUSHES 必须在 1 到 64 之间")
        return parsed

    @field_validator("redis_max_connections", "redis_lock_max_connections", "redis_background_max_connections")
    @classmethod
    def validate_redis_connection_limits(cls, value):
        parsed = int(value)
        if parsed < 16 or parsed > 2000:
            raise ValueError("Redis 连接池大小必须在 16 到 2000 之间")
        return parsed

    @field_validator("redis_url")
    @classmethod
    def validate_redis_url(cls, value):
        if not value.startswith(("redis://", "rediss://")):
            raise ValueError("REDIS_URL 必须以 redis:// 或 rediss:// 开头")
        return value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value):
        normalized = (value or "").strip()
        if not normalized.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL 必须使用 postgresql+asyncpg")
        return normalized

    @field_validator("admin_password", mode="before")
    @classmethod
    def validate_admin_password(cls, value):
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        stripped = str(value).strip()
        if len(stripped) < 8:
            raise ValueError("ADMIN_PASSWORD 设置后长度至少 8 个字符")
        return stripped

    @field_validator("admin_username", "super_admin_username")
    @classmethod
    def validate_admin_usernames(cls, value):
        stripped = str(value or "").strip()
        if not stripped:
            raise ValueError("管理员用户名不能为空")
        if len(stripped) > 80:
            raise ValueError("管理员用户名长度不能超过 80 个字符")
        return stripped

    @field_validator("admin_email")
    @classmethod
    def validate_admin_email(cls, value):
        stripped = str(value or "").strip()
        if not stripped or "@" not in stripped:
            raise ValueError("ADMIN_EMAIL 必须是有效的邮箱地址")
        return stripped

    @field_validator("secret_key")
    @classmethod
    def validate_secret_key(cls, value):
        stripped = value.strip()
        if len(stripped) < 32:
            raise ValueError("SECRET_KEY 长度至少 32 个字符")
        return stripped

    @field_validator("access_token_expire_minutes")
    @classmethod
    def validate_access_token_expire_minutes(cls, value):
        parsed = int(value)
        if parsed < 5 or parsed > 43200:
            raise ValueError("ACCESS_TOKEN_EXPIRE_MINUTES 必须在 5 到 43200 之间")
        return parsed

    @field_validator("db_pool_size")
    @classmethod
    def validate_db_pool_size(cls, value):
        parsed = int(value)
        if parsed < 5 or parsed > 500:
            raise ValueError("DB_POOL_SIZE 必须在 5 到 500 之间")
        return parsed

    @field_validator("db_max_overflow")
    @classmethod
    def validate_db_max_overflow(cls, value):
        parsed = int(value)
        if parsed < 0 or parsed > 500:
            raise ValueError("DB_MAX_OVERFLOW 必须在 0 到 500 之间")
        return parsed

    @field_validator("estimated_proxy_rtt_seconds")
    @classmethod
    def validate_estimated_proxy_rtt_seconds(cls, value):
        parsed = float(value)
        if parsed < 0.1 or parsed > 30.0:
            raise ValueError("ESTIMATED_PROXY_RTT_SECONDS 必须在 0.1 到 30.0 之间")
        return parsed

    @field_validator("default_http_batch_size")
    @classmethod
    def validate_default_http_batch_size(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 1000:
            raise ValueError("DEFAULT_HTTP_BATCH_SIZE 必须在 1 到 1000 之间")
        return parsed

    @field_validator("warmup_default_http_batch_size")
    @classmethod
    def validate_warmup_default_http_batch_size(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 1000:
            raise ValueError("WARMUP_DEFAULT_HTTP_BATCH_SIZE 必须在 1 到 1000 之间")
        return parsed

    @field_validator("sendgrid_daily_quota", mode="before")
    @classmethod
    def normalize_sendgrid_daily_quota(cls, value):
        if value is None:
            return 0
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return 0
            return stripped
        return value

    @field_validator("sendgrid_daily_quota")
    @classmethod
    def validate_sendgrid_daily_quota(cls, value):
        parsed = int(value)
        if parsed < 0 or parsed > 100000000:
            raise ValueError("SENDGRID_DAILY_QUOTA 必须在 0 到 100000000 之间")
        return parsed

    @field_validator("hot_retention_days", "archive_retention_days")
    @classmethod
    def validate_retention_days(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 3650:
            raise ValueError("日志保留天数必须在 1 到 3650 之间")
        return parsed

    @field_validator("send_email_timeout_seconds")
    @classmethod
    def validate_send_email_timeout_seconds(cls, value):
        parsed = int(value)
        if parsed < 6 or parsed > 300:
            raise ValueError("SEND_EMAIL_TIMEOUT_SECONDS 必须在 6 到 300 之间")
        return parsed

    @field_validator("sender_pool_workers")
    @classmethod
    def validate_sender_pool_workers(cls, value):
        if value is None:
            return None
        parsed = int(value)
        if parsed < 1 or parsed > 256:
            raise ValueError("SENDER_POOL_WORKERS 必须在 1 到 256 之间")
        return parsed

    @field_validator("import_preprocess_workers")
    @classmethod
    def validate_import_preprocess_workers(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 32:
            raise ValueError("IMPORT_PREPROCESS_WORKERS 必须在 1 到 32 之间")
        return parsed

    @field_validator("writer_max_flushes_per_second")
    @classmethod
    def validate_writer_max_flushes_per_second(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 100:
            raise ValueError("WRITER_MAX_FLUSHES_PER_SECOND 必须在 1 到 100 之间")
        return parsed

    @field_validator("export_async_row_threshold")
    @classmethod
    def validate_export_async_row_threshold(cls, value):
        parsed = int(value)
        if parsed < 1000 or parsed > 100_000_000:
            raise ValueError("EXPORT_ASYNC_ROW_THRESHOLD 必须在 1000 到 100000000 之间")
        return parsed

    @field_validator("export_async_max_tasks")
    @classmethod
    def validate_export_async_max_tasks(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 20:
            raise ValueError("EXPORT_ASYNC_MAX_TASKS 必须在 1 到 20 之间")
        return parsed

    @field_validator("export_async_file_ttl_hours")
    @classmethod
    def validate_export_async_file_ttl_hours(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 168:
            raise ValueError("EXPORT_ASYNC_FILE_TTL_HOURS 必须在 1 到 168 之间")
        return parsed

    @field_validator("scheduled_start_retry_interval_seconds")
    @classmethod
    def validate_scheduled_start_retry_interval_seconds(cls, value):
        parsed = int(value)
        if parsed < 10 or parsed > 3600:
            raise ValueError("SCHEDULED_START_RETRY_INTERVAL_SECONDS 必须在 10 到 3600 之间")
        return parsed

    @field_validator("scheduled_start_retry_max_attempts")
    @classmethod
    def validate_scheduled_start_retry_max_attempts(cls, value):
        parsed = int(value)
        if parsed < 1 or parsed > 20:
            raise ValueError("SCHEDULED_START_RETRY_MAX_ATTEMPTS 必须在 1 到 20 之间")
        return parsed

    @property
    def cors_origin_list(self) -> list[str]:
        origins = [origin.strip().rstrip("/") for origin in self.cors_origins.split(",") if origin.strip()]
        return [origin for origin in origins if origin.startswith(("http://", "https://"))]


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
SEND_EMAIL_TIMEOUT_SECONDS = int(settings.send_email_timeout_seconds)
SEND_EMAIL_QUEUE_WAIT_BUDGET_SECONDS = 5
SEND_EMAIL_HTTP_BUDGET_SECONDS = SEND_EMAIL_TIMEOUT_SECONDS - SEND_EMAIL_QUEUE_WAIT_BUDGET_SECONDS
