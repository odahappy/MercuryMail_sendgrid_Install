import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import os
import queue
import sys
import time
from contextlib import asynccontextmanager
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from starlette import status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import IntegrityError, OperationalError, DBAPIError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from app.services.operation_audit import is_export_operation_path, mark_export_operation_request

from app.api import ai_config, auth, channels, dashboard, export, health, logs, recipients, system, tasks, templates, unsubscribe, urls, users, warmup
from app.api.deps import check_db_ready
from app.core.config import get_build_info, settings
from app.core.database import close_db_engine, assert_database_tables_ready, init_db
from app.services.connection_manager import close_all_clients, warn_if_socks_dependencies_missing
from app.services.channel_health import channel_health_check_loop
from app.services.log_cleaner import send_log_daily_cleanup_loop
from app.services.task_runner import start_scheduler_loop
from app.services.warmup_sender import warmup_scheduler_loop

logger = logging.getLogger(__name__)
ENV_PROXY_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
LOG_QUEUE_MAX_SIZE = 1000
LOG_QUEUE_DROP_WARN_INTERVAL_SECONDS = 60.0
_log_listener: QueueListener | None = None
_log_queue_drop_last_warn_at = 0.0
EXCEPTION_LOG_RATE_LIMIT_SECONDS = 60.0
_exception_log_last_at: dict[str, float] = {}
_DEFAULT_GENERAL_THREADPOOL_WORKERS = min(256, max(32, (os.cpu_count() or 4) * 4))
BACKGROUND_TASK_STOP_TIMEOUT_SECONDS = 30.0
_raw_threadpool_workers = os.environ.get("GENERAL_THREADPOOL_WORKERS", "").strip()
if _raw_threadpool_workers:
    try:
        GENERAL_THREADPOOL_WORKERS = max(4, int(_raw_threadpool_workers))
    except ValueError:
        GENERAL_THREADPOOL_WORKERS = _DEFAULT_GENERAL_THREADPOOL_WORKERS
else:
    GENERAL_THREADPOOL_WORKERS = _DEFAULT_GENERAL_THREADPOOL_WORKERS


def get_frontend_dist_dir() -> Path:
    candidates = [
        Path(__file__).resolve().parent / "frontend" / "dist",
        Path(__file__).resolve().parent.parent / "frontend" / "dist",
        Path("/app/frontend/dist"),
    ]
    for candidate in candidates:
        if (candidate / "index.html").is_file():
            return candidate
    return candidates[0]


FRONTEND_DIST_DIR = get_frontend_dist_dir()
FRONTEND_INDEX_FILE = FRONTEND_DIST_DIR / "index.html"


def log_effective_configuration() -> None:
    key_names = (
        "default_http_batch_size", "warmup_default_http_batch_size",
        "sender_pool_workers", "sender_pool_max_workers",
        "sender_pool_headroom", "db_pool_size", "db_max_overflow",
        "writer_batch_size", "writer_turbo_batch_size", "writer_max_flushes_per_second",
        "writer_max_concurrent_flushes", "enable_pid_concurrency_tuning", "enable_feedforward_concurrency",
        "enable_queue_aware_concurrency",
        "send_email_timeout_seconds", "import_preprocess_workers",
    )
    values = {}
    configured_fields = type(settings).model_fields
    for key in key_names:
        if key not in configured_fields:
            continue
        env_name = key.upper()
        value = getattr(settings, key)
        source = "环境变量" if env_name in os.environ else "默认值"
        values[key] = {"value": value, "source": source}
    logger.warning("最终生效配置及来源：%s", values)


def warn_environment_proxy_variables() -> None:
    configured_keys = [key for key in ENV_PROXY_KEYS if os.environ.get(key)]
    if configured_keys:
        logger.warning("检测到环境代理变量 %s；MercuryMail 已在 SendGrid 请求中禁用 trust_env，避免环境代理干扰通道绑定代理", ",".join(configured_keys))


def configure_third_party_loggers() -> None:
    for logger_name in (
        "http.client",
        "asyncio",
        "httpx",
        "httpx._client",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def _warn_log_queue_drop() -> None:
    global _log_queue_drop_last_warn_at
    now = time.monotonic()
    if now - _log_queue_drop_last_warn_at < LOG_QUEUE_DROP_WARN_INTERVAL_SECONDS:
        return
    _log_queue_drop_last_warn_at = now
    try:
        sys.stderr.write("[logging] 日志队列已满，部分日志已丢弃\n")
    except Exception:
        pass


class AsyncLoggingQueueHandler(QueueHandler):
    def __init__(self, log_queue: queue.Queue, fallback_handler: logging.Handler) -> None:
        super().__init__(log_queue)
        self.fallback_handler = fallback_handler

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            _warn_log_queue_drop()
        except Exception:
            try:
                self.fallback_handler.emit(record)
            except Exception:
                pass


def configure_app_logging() -> None:
    global _log_listener
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    fallback_handler = logging.StreamHandler(sys.stderr)
    fallback_handler.setLevel(logging.WARNING)
    log_queue: queue.Queue = queue.Queue(maxsize=LOG_QUEUE_MAX_SIZE)
    queue_handler = AsyncLoggingQueueHandler(log_queue, fallback_handler)
    queue_handler.setLevel(logging.WARNING)
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    root_logger.addHandler(queue_handler)
    _log_listener = QueueListener(log_queue, fallback_handler, respect_handler_level=True)
    _log_listener.start()


def shutdown_app_logging() -> None:
    global _log_listener
    if _log_listener is not None:
        _log_listener.stop()
        _log_listener = None


def configure_safe_logging() -> None:
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if not isinstance(handler, logging.Handler):
            continue
        original_emit = handler.emit

        def safe_emit(record, _emit=original_emit, _handler=handler):
            try:
                _emit(record)
            except Exception:
                try:
                    message = record.getMessage()
                except Exception:
                    message = str(record)
                try:
                    sys.stderr.write(f"[logging-fallback] {message}\n")
                except Exception:
                    pass

        handler.emit = safe_emit


def supervise_background_loop(loop_coro_factory, task_name: str) -> asyncio.Task:
    async def supervised_runner() -> None:
        consecutive_failures = 0
        while True:
            try:
                await loop_coro_factory()
                if consecutive_failures > 0:
                    logger.info("%s 已恢复正常运行，重置失败计数", task_name)
                consecutive_failures = 0
            except asyncio.CancelledError:
                logger.warning("%s 已取消", task_name)
                raise
            except Exception:
                consecutive_failures += 1
                if consecutive_failures >= 10:
                    logger.exception(
                        "%s 连续失败 %s 次，进入熔断暂停 300 秒后重试",
                        task_name,
                        consecutive_failures,
                    )
                    await asyncio.sleep(300)
                    consecutive_failures = 0
                    continue
                backoff_seconds = min(60, 2 ** consecutive_failures)
                logger.exception(
                    "%s 异常退出，连续失败=%s，%s 秒后自动重试",
                    task_name,
                    consecutive_failures,
                    backoff_seconds,
                )
                await asyncio.sleep(backoff_seconds)

    return asyncio.create_task(supervised_runner(), name=f"{task_name}-supervisor")


def flatten_exception_group(exc: BaseException) -> list[BaseException]:
    """递归提取 ExceptionGroup 叶子异常，避免 TaskGroup 包装堆栈淹没根因。"""
    nested = getattr(exc, "exceptions", None)
    if isinstance(nested, (tuple, list)):
        leaves: list[BaseException] = []
        for child in nested:
            if isinstance(child, BaseException):
                leaves.extend(flatten_exception_group(child))
        return leaves or [exc]
    return [exc]


def _log_leaf_exceptions(message: str, exc: BaseException) -> None:
    now_value = time.monotonic()
    for leaf in flatten_exception_group(exc):
        error_type = type(leaf).__name__
        error_message = str(leaf or "无错误详情")[:1000]
        rate_key = f"{error_type}:{error_message}"
        last_at = _exception_log_last_at.get(rate_key, 0.0)
        if now_value - last_at < EXCEPTION_LOG_RATE_LIMIT_SECONDS:
            continue
        _exception_log_last_at[rate_key] = now_value
        logger.error("%s，根因类型=%s，根因=%s", message, error_type, error_message)


def _log_asyncio_exception(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    del loop
    message = context.get("message") or "asyncio 未处理异常"
    exception = context.get("exception")
    if isinstance(exception, BaseException):
        _log_leaf_exceptions(message, exception)
    else:
        logger.error("%s context=%s", message, context)


def _create_general_threadpool() -> tuple[ThreadPoolExecutor, int]:
    general_workers = max(4, GENERAL_THREADPOOL_WORKERS)
    general_threadpool = ThreadPoolExecutor(max_workers=general_workers, thread_name_prefix="mercury-general")
    return general_threadpool, general_workers


def _record_startup_error(app: FastAPI, message: str) -> None:
    errors = getattr(app.state, "startup_errors", None)
    if errors is None:
        app.state.startup_errors = []
        errors = app.state.startup_errors
    errors.append(message[:500])


def _start_background_scheduler_tasks(app: FastAPI) -> list[asyncio.Task]:
    """在数据库表就绪后启动所有后台调度循环（禁止在 init_db 之前调用）。"""
    from app.services.async_export_service import async_export_cleanup_loop
    from app.services.httpx_recycler import httpx_client_recycle_loop
    from app.services.dashboard_stats_service import dashboard_stats_refresh_loop
    from app.core.database import idle_transaction_cleanup_loop

    task_specs = [
        (idle_transaction_cleanup_loop, "idle-transaction-cleanup-loop"),
        (dashboard_stats_refresh_loop, "dashboard-stats-refresh-loop"),
        (warmup_scheduler_loop, "warmup-scheduler-loop"),
        (start_scheduler_loop, "task-scheduler-loop"),
        (send_log_daily_cleanup_loop, "send-log-daily-cleanup-loop"),
        # 通道账号健康由单一、低频且跳过运行中通道的循环负责。
        # 旧 proxy-health-monitor 每 30 秒遍历全部代理通道并重复请求 /v3/scopes，
        # 会与真实发送争抢同一代理出口容量，且用瞬时探测结果覆盖展示状态。
        (channel_health_check_loop, "channel-health-check-loop"),
        (httpx_client_recycle_loop, "httpx-client-recycle-loop"),
        (async_export_cleanup_loop, "async-export-cleanup-loop"),
    ]
    scheduler_tasks: list[asyncio.Task] = []
    for loop_coro_factory, task_name in task_specs:
        task = supervise_background_loop(loop_coro_factory, task_name)
        scheduler_tasks.append(task)
        logger.warning("后台任务已启动 task_name=%s", task_name)
    app.state.scheduler_tasks = scheduler_tasks
    return scheduler_tasks


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.startup_errors = []
    app.state.db_ready = False
    app.state.redis_ready = False
    app.state.scheduler_tasks = []
    general_threadpool: ThreadPoolExecutor | None = None
    background_startup_task: asyncio.Task | None = None

    try:
        configure_third_party_loggers()
        configure_app_logging()
    except Exception as exc:
        logger.exception("第三方日志配置失败，继续启动")
        _record_startup_error(app, f"第三方日志配置失败：{exc}")

    try:
        configure_safe_logging()
    except Exception as exc:
        logger.exception("安全日志包装失败，继续启动")
        _record_startup_error(app, f"安全日志包装失败：{exc}")

    try:
        build_info = get_build_info()
        logger.warning(
            "MercuryMail 构建版本 git_sha=%s image_digest=%s task_runner_sha256=%s",
            build_info["git_sha"],
            build_info["image_digest"],
            build_info["source_sha256"]["task_runner"],
        )
        app.state.build_info = build_info
        logger.warning("所有修复已部署，请执行生产闭环验收脚本")
        log_effective_configuration()
        warn_environment_proxy_variables()
        warn_if_socks_dependencies_missing()
    except Exception as exc:
        logger.exception("环境检查失败，继续启动")
        _record_startup_error(app, f"环境检查失败：{exc}")

    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(_log_asyncio_exception)
        general_threadpool, general_workers = _create_general_threadpool()
        loop.set_default_executor(general_threadpool)
        from app.services.task_runner import configure_global_render_threadpool
        from app.services.channel_manager import configure_send_threadpool
        from app.services.sender_pool import get_sender_pool

        render_workers = configure_global_render_threadpool()
        send_workers = configure_send_threadpool()
        sender_pool = get_sender_pool()
        sender_pool.ensure_started()
        logger.warning(
            "MercuryMail 通用线程池已配置 workers=%s（默认公式=%s，环境变量 GENERAL_THREADPOOL_WORKERS=%s）",
            general_workers,
            _DEFAULT_GENERAL_THREADPOOL_WORKERS,
            _raw_threadpool_workers or "未设置",
        )
        logger.warning(
            "MercuryMail 渲染线程池已配置 workers=%s（按 CPU 规模设置，不参与发送并发准入）",
            render_workers,
        )
        logger.warning(
            "MercuryMail 兼容发送 executor 已配置 workers=%s（正式 SDK 发送使用任务独立 SenderPool）",
            send_workers,
        )
        logger.warning(
            "MercuryMail 公共 SenderPool 已初始化 workers=%s（任务发送会创建独立 SenderPool，单池上限由 SENDER_POOL_MAX_WORKERS 控制）",
            sender_pool._workers_count,
        )
    except Exception as exc:
        logger.exception("线程池初始化失败，尝试使用最小线程池降级启动")
        _record_startup_error(app, f"线程池初始化失败：{exc}")
        try:
            loop = asyncio.get_running_loop()
            loop.set_exception_handler(_log_asyncio_exception)
            general_threadpool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="mercury-general-fallback")
            loop.set_default_executor(general_threadpool)
            from app.services.task_runner import configure_global_max_concurrency

            configure_global_max_concurrency()
            logger.warning("MercuryMail 已降级为最小线程池 general=4")
        except Exception as fallback_exc:
            logger.exception("最小线程池降级也失败，发送相关功能可能不可用")
            _record_startup_error(app, f"线程池降级失败：{fallback_exc}")

    logger.warning("【启动 1/5】等待数据库连接并自动建表（最多 30 秒）…")
    try:
        await asyncio.wait_for(init_db(), timeout=60)
        app.state.db_ready = True
        logger.warning("【启动 1/5】数据库连接、建表与索引补建已完成")
        from app.core.database import engine
        from sqlalchemy import text
        async with engine.connect() as conn:
            await asyncio.wait_for(conn.execute(text("SELECT 1")), timeout=5)
            await conn.commit()
        logger.warning("【启动 1/5】数据库预热完成（SELECT 1）")
    except asyncio.TimeoutError as exc:
        failure_message = "数据库表初始化或预热超时，请检查 PostgreSQL 容器状态与 DATABASE_URL"
        logger.exception("%s，应用即将退出", failure_message)
        _record_startup_error(app, failure_message)
        raise RuntimeError(failure_message) from exc
    except Exception as exc:
        failure_message = f"数据库表初始化失败：{exc}"
        logger.exception("%s，应用即将退出（请查看上方详细错误并重新执行 deploy.sh）", failure_message)
        _record_startup_error(app, failure_message[:500])
        raise RuntimeError(failure_message) from exc

    logger.warning("【启动 2/5】等待 Redis 连接并预热…")
    try:
        from app.core.redis import ping_redis, redis_client

        redis_max_attempts = 30
        for redis_attempt in range(1, redis_max_attempts + 1):
            try:
                await asyncio.wait_for(ping_redis(), timeout=5)
                await asyncio.wait_for(redis_client.ping(), timeout=5)
                app.state.redis_ready = True
                logger.warning("【启动 2/5】Redis 连接正常并已预热")
                break
            except Exception as exc:
                if redis_attempt >= redis_max_attempts:
                    failure_message = f"Redis 连接失败（已重试 {redis_max_attempts} 次）：{exc}"
                    logger.exception("%s，应用即将退出", failure_message)
                    _record_startup_error(app, failure_message[:500])
                    raise RuntimeError(failure_message) from exc
                logger.warning(
                    "等待 Redis 就绪：第 %s/%s 次失败，1 秒后重试；原因：%s",
                    redis_attempt,
                    redis_max_attempts,
                    exc,
                )
                await asyncio.sleep(1)
    except RuntimeError:
        raise
    except Exception as exc:
        failure_message = f"Redis 启动检查失败：{exc}"
        logger.exception("%s，应用即将退出", failure_message)
        _record_startup_error(app, failure_message[:500])
        raise RuntimeError(failure_message) from exc

    logger.warning("【启动 3/5】预热模板与导出服务…")
    try:
        from app.services.template_service import warm_up_template_cache
        warmed = await warm_up_template_cache()
        logger.warning("模板预热完成 warmed=%s", warmed)
        from app.services.async_export_service import ensure_export_async_dir, get_export_service_status
        ensure_export_async_dir()
        _ = get_export_service_status() if hasattr(__import__('app.services.async_export_service', fromlist=['get_export_service_status']), 'get_export_service_status') else None
        logger.warning("导出服务预热完成")
    except Exception as exc:
        logger.exception("模板或导出服务预热失败：%s", exc)

    logger.warning("【启动 4/5】初始化应用依赖（管理员账户、导出目录等）…")
    try:
        from app.services.user_seed import ensure_default_users

        await ensure_default_users()
        logger.warning("默认管理员账户检查完成")
    except Exception as exc:
        failure_message = f"默认管理员账户初始化失败：{exc}"
        logger.exception("%s，应用即将退出", failure_message)
        _record_startup_error(app, failure_message[:500])
        raise RuntimeError(failure_message) from exc

    try:
        from app.services.async_export_service import ensure_export_async_dir

        ensure_export_async_dir()
        logger.warning("MercuryMail 异步导出目录已创建并验证")
    except Exception as exc:
        logger.exception("异步导出目录初始化失败：%s", exc)

    logger.warning("【启动 4/4】校验数据库表并启动 SenderPool 与后台调度任务…")
    try:
        await assert_database_tables_ready()
        logger.warning("后台任务启动前数据库校验通过（SELECT 1 + 核心表存在）")

        from app.services.gc_tuning import configure_gc_thresholds
        from app.services.sender_pool import get_sender_pool

        configure_gc_thresholds(
            generation0=int(settings.gc_generation0_threshold),
            generation1=int(settings.gc_generation1_threshold),
            generation2=int(settings.gc_generation2_threshold),
        )
        get_sender_pool().ensure_started()
        logger.warning("MercuryMail GC 调优与 SenderPool 已初始化")

        scheduler_tasks = _start_background_scheduler_tasks(app)
        logger.warning("【启动 4/4】后台调度任务已全部启动 count=%s", len(scheduler_tasks))
    except Exception as exc:
        failure_message = f"后台调度任务启动失败：{exc}"
        logger.exception("%s，应用即将退出", failure_message)
        _record_startup_error(app, failure_message[:500])
        raise RuntimeError(failure_message) from exc

    async def deferred_background_startup() -> None:
        if app.state.db_ready:
            try:
                from app.services.channel_manager import warmup_active_channel_httpx_clients

                await warmup_active_channel_httpx_clients()
                logger.warning("httpx 连接池预热完成")
            except Exception:
                logger.exception("httpx 连接池预热失败")

        try:
            from app.services.proxy_sharing_audit import audit_all_shared_proxy_endpoints

            await audit_all_shared_proxy_endpoints()
            logger.warning("代理共享风险启动检查完成")
        except Exception:
            logger.exception("代理共享风险启动检查失败")

    background_startup_task = asyncio.create_task(deferred_background_startup(), name="mercury-deferred-startup")
    try:
        yield
    finally:
        if background_startup_task is not None and not background_startup_task.done():
            background_startup_task.cancel()
            try:
                await background_startup_task
            except asyncio.CancelledError:
                pass
        scheduler_tasks = list(getattr(app.state, "scheduler_tasks", []) or [])
        for scheduler_task in scheduler_tasks:
            logger.warning("后台任务准备停止 task_name=%s", getattr(scheduler_task, "get_name", lambda: "unknown")())
            scheduler_task.cancel()
        for scheduler_task in scheduler_tasks:
            try:
                await asyncio.wait_for(scheduler_task, timeout=BACKGROUND_TASK_STOP_TIMEOUT_SECONDS)
                logger.warning("后台任务已停止 task_name=%s", getattr(scheduler_task, "get_name", lambda: "unknown")())
            except asyncio.TimeoutError:
                logger.error("后台任务停止超时 task_name=%s", getattr(scheduler_task, "get_name", lambda: "unknown")())
            except asyncio.CancelledError:
                logger.warning("后台任务取消完成 task_name=%s", getattr(scheduler_task, "get_name", lambda: "unknown")())
            except Exception:
                logger.exception("后台任务停止异常 task_name=%s", getattr(scheduler_task, "get_name", lambda: "unknown")())
        try:
            await close_all_clients()
        except Exception:
            logger.exception("关闭 HTTP 客户端时失败")
        try:
            from app.core.redis import close_redis

            await close_redis()
        except Exception:
            logger.exception("关闭 Redis 连接时失败")
        try:
            from app.utils.export import close_export_pool

            await close_export_pool()
        except Exception:
            logger.exception("关闭 COPY 导出连接池时失败")
        try:
            await close_db_engine()
        except Exception:
            logger.exception("关闭数据库连接时失败")
        if general_threadpool is not None:
            general_threadpool.shutdown(wait=False)
        try:
            from app.services.task_runner import shutdown_global_render_threadpool
            from app.services.channel_manager import shutdown_send_threadpool

            shutdown_global_render_threadpool()
            shutdown_send_threadpool()
            from app.services.sender_pool import shutdown_sender_pool
            from app.services.gc_tuning import restore_gc_thresholds

            shutdown_sender_pool()
            restore_gc_thresholds()
        except Exception:
            logger.exception("关闭全局渲染/发送线程池时失败")
        try:
            shutdown_app_logging()
        except Exception:
            try:
                sys.stderr.write("[logging-fallback] 关闭异步日志队列失败\n")
            except Exception:
                pass


app = FastAPI(title="MercuryMail", version="14.0.0", lifespan=lifespan)


@app.get("/health", include_in_schema=False)
async def root_health_check(request: Request):
    """兼容容器与外部负载均衡器的根健康检查入口。"""
    return await health.health_check(request)


class ExportOperationAuditMiddleware(BaseHTTPMiddleware):
    """导出请求自动标记为豁免面向用户的操作日志。"""

    async def dispatch(self, request: Request, call_next):
        if is_export_operation_path(request.url.path):
            mark_export_operation_request(request)
        return await call_next(request)


@app.exception_handler(StarletteHTTPException)
async def chinese_http_exception_handler(request: Request, exc: StarletteHTTPException):
    detail = exc.detail if isinstance(exc.detail, str) and exc.detail else "请求处理失败"
    fallback_messages = {
        400: "请求参数有误，请检查后重试",
        401: "请先登录",
        403: "没有权限执行此操作",
        404: "请求的资源不存在",
        405: "请求方法不支持",
        409: "当前数据存在冲突，请刷新后重试",
        413: "上传内容过大，请压缩或拆分后重试",
        422: "提交的数据格式不正确，请检查后重试",
        429: "操作过于频繁，请稍后重试",
        500: "服务器内部错误，请稍后重试",
        503: "服务暂时不可用，请稍后重试",
    }
    if detail == exc.__class__.__name__ or any(marker in detail for marker in ["Internal Server Error", "Not Found", "Unauthorized", "Forbidden", "Method Not Allowed"]):
        detail = fallback_messages.get(exc.status_code, "请求处理失败，请稍后重试")
    return JSONResponse(status_code=exc.status_code, content={"detail": detail}, headers=getattr(exc, "headers", None))


def _format_validation_field_name(field: str) -> str:
    field_map = {
        "name": "任务名称",
        "template_id": "模板",
        "recipient_list_id": "收件人池",
        "channel_id": "发送通道",
        "url_pool_id": "URL 池",
        "send_http_batch_size": "批量大小",
        "rate_limit": "发送速率",
        "concurrency": "并发数",
        "retry_count": "重试次数",
        "monitor_interval": "监控间隔",
        "monitor_emails": "监控邮箱",
        "scheduled_at": "定时启动时间",
        "timezone": "定时时区",
        "sender_rotation_enabled": "发件人轮询",
        "unsubscribe_enabled": "退订开关",
        "auto_concurrency": "自动调整并发",
    }
    return field_map.get(field, field)


def _resolve_task_validation_error(errors: list[dict]) -> str:
    messages: list[str] = []
    for item in errors:
        loc = item.get("loc", [])
        field = str(loc[-1]) if loc else "请求体"
        field_name = _format_validation_field_name(field)
        error_type = str(item.get("type") or "")
        if error_type == "missing":
            messages.append(f"{field_name}为必填项")
        elif error_type.startswith("greater_than_equal"):
            messages.append(f"{field_name}不能小于 {item.get('ctx', {}).get('ge')}")
        elif error_type.startswith("less_than_equal"):
            messages.append(f"{field_name}不能大于 {item.get('ctx', {}).get('le')}")
        elif "int" in error_type or "float" in error_type:
            messages.append(f"{field_name}必须是数字")
        elif "datetime" in error_type:
            messages.append(f"{field_name}格式不正确")
        elif "bool" in error_type:
            messages.append(f"{field_name}必须是布尔值")
        elif "list" in error_type:
            messages.append(f"{field_name}必须是列表")
        elif "string" in error_type:
            messages.append(f"{field_name}必须是文本")
        else:
            messages.append(f"{field_name}格式不正确")
    unique_messages = list(dict.fromkeys(message for message in messages if message))
    if unique_messages:
        return "；".join(unique_messages[:6])
    return "发送任务数据格式不正确，请检查必填项、数字范围和邮箱格式后重试"


def _resolve_validation_error_detail(request: Request, exc: RequestValidationError) -> str:
    path = request.url.path.rstrip("/")
    errors = exc.errors()
    missing_fields = {
        str(item.get("loc", [])[-1])
        for item in errors
        if item.get("type") == "missing"
    }
    if path == "/api/tasks":
        return _resolve_task_validation_error(errors)
    if path.endswith("/recipients/upload"):
        if "file" in missing_fields:
            return "请上传文件或输入邮箱内容"
        return "上传请求格式不正确，请上传 .csv 或 .txt 邮箱文件"
    if path.endswith("/recipients/paste"):
        if "content" in missing_fields:
            return "请上传文件或输入邮箱内容"
        return "粘贴内容格式不正确，请每行输入一个邮箱"
    if "/urls/pools/" in path and path.endswith("/import"):
        if "urls_text" in missing_fields and "file" in missing_fields:
            return "请输入网址或上传文件"
        return "URL 导入请求格式不正确，请输入网址或上传文件"
    if path.endswith("/recipients/lists") or "/recipients/lists/" in path and path.endswith("/recipients"):
        if "emails" in missing_fields:
            return "请至少输入一个邮箱地址"
        return "邮箱列表格式不正确，请每行输入一个邮箱"
    return "提交的数据格式不正确，请检查必填项、数字范围和邮箱格式后重试"


@app.exception_handler(RequestValidationError)
async def chinese_validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": _resolve_validation_error_detail(request, exc)},
    )


def _format_integrity_error_detail(exc: IntegrityError) -> str:
    orig = getattr(exc, "orig", None)
    pgcode = str(getattr(orig, "pgcode", "") or "").strip()
    diag = getattr(orig, "diag", None)
    constraint_name = str(getattr(diag, "constraint_name", "") or "").strip()
    table_name = str(getattr(diag, "table_name", "") or "").strip()
    column_name = str(getattr(diag, "column_name", "") or "").strip()

    if pgcode == "23503":
        base_message = "删除失败：该记录被其他数据引用，无法删除"
    elif pgcode == "23505":
        base_message = "保存失败：数据已存在，请修改后重试"
    elif pgcode == "23514":
        base_message = "保存失败：数据未通过约束校验"
    else:
        base_message = "删除失败：数据库约束冲突，请检查关联数据"

    detail_parts = []
    if constraint_name:
        detail_parts.append(f"约束={constraint_name}")
    if table_name:
        detail_parts.append(f"表={table_name}")
    if column_name:
        detail_parts.append(f"字段={column_name}")
    if detail_parts:
        return f"{base_message}（{'，'.join(detail_parts)}）"
    return base_message


@app.exception_handler(IntegrityError)
async def chinese_integrity_exception_handler(request: Request, exc: IntegrityError):
    orig = getattr(exc, "orig", None)
    diag = getattr(orig, "diag", None)
    logger.warning(
        "数据库约束冲突 path=%s method=%s pgcode=%s constraint=%s table=%s column=%s orig=%s",
        request.url.path,
        request.method,
        getattr(orig, "pgcode", None),
        getattr(diag, "constraint_name", None),
        getattr(diag, "table_name", None),
        getattr(diag, "column_name", None),
        orig,
    )
    return JSONResponse(status_code=409, content={"detail": _format_integrity_error_detail(exc)})


@app.exception_handler(OperationalError)
@app.exception_handler(DBAPIError)
async def database_unavailable_exception_handler(request: Request, exc: Exception):
    from app.core.database import is_database_pool_pressure_error, maybe_warn_database_pool_pressure

    maybe_warn_database_pool_pressure(force=True)
    logger.warning(
        "数据库暂时不可用 path=%s method=%s error_type=%s error=%s pool_pressure=%s",
        request.url.path,
        request.method,
        type(exc).__name__,
        exc,
        is_database_pool_pressure_error(exc),
    )
    return JSONResponse(status_code=503, content={"detail": "数据库繁忙，请稍后重试"})


@app.exception_handler(asyncio.TimeoutError)
async def asyncio_timeout_exception_handler(request: Request, exc: asyncio.TimeoutError):
    logger.warning("请求处理超时 path=%s method=%s", request.url.path, request.method)
    return JSONResponse(status_code=503, content={"detail": "服务响应超时，请稍后重试"})


@app.exception_handler(Exception)
async def chinese_unhandled_exception_handler(request: Request, exc: Exception):
    _log_leaf_exceptions(
        f"未捕获的请求异常 path={request.url.path} method={request.method}",
        exc,
    )
    return JSONResponse(status_code=500, content={"detail": "服务器内部错误，请稍后重试"})


def _resolve_cors_origins() -> list[str]:
    configured = [item.strip() for item in str(settings.cors_origins or "").split(",") if item.strip()]
    if configured:
        return configured
    if str(settings.app_env or "production").strip().lower() in {"dev", "development", "local"}:
        return ["http://localhost:5173", "http://127.0.0.1:5173", "http://localhost", "http://127.0.0.1"]
    public_base_url = str(settings.public_base_url or "").strip().rstrip("/")
    return [public_base_url] if public_base_url else []


app.add_middleware(
    CORSMiddleware,
    allow_origins=_resolve_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)
app.add_middleware(ExportOperationAuditMiddleware)

for router in [
    auth.router,
    users.router,
    system.router,
    dashboard.router,
    export.router,
    channels.router,
    tasks.router,
    recipients.router,
    templates.router,
    urls.router,
    logs.router,
    unsubscribe.router,
    warmup.router,
    ai_config.router,
]:
    app.include_router(router, dependencies=[Depends(check_db_ready)])

app.include_router(health.router)


if FRONTEND_INDEX_FILE.is_file():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST_DIR / "assets"), name="frontend-assets")
    app.mount("/static", StaticFiles(directory=FRONTEND_DIST_DIR), name="frontend-static")
else:
    logger.warning("前端入口文件不存在：%s，请先执行前端构建并将 dist 复制到该路径", FRONTEND_INDEX_FILE)


@app.get("/")
async def serve_frontend_root():
    if FRONTEND_INDEX_FILE.is_file():
        return FileResponse(FRONTEND_INDEX_FILE)
    raise HTTPException(status_code=404, detail="页面未找到")


@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="请求的资源不存在")
    if FRONTEND_INDEX_FILE.is_file():
        return FileResponse(FRONTEND_INDEX_FILE)
    raise HTTPException(status_code=404, detail="页面未找到")
