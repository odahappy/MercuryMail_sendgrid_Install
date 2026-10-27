import asyncio
import logging
from time import monotonic

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.core.config import get_build_info, settings
from app.core.database import get_database_pool_status, probe_database_health_light
from app.core.redis import ping_redis_once
from app.services.task_runner import get_global_concurrency_occupied, get_global_max_concurrency

router = APIRouter(prefix="/api", tags=["health"])
logger = logging.getLogger(__name__)

PROXY_PARALLEL_CAPACITY_HINT_CONNECTIONS = 900
HEALTH_LIGHT_PROBE_TIMEOUT_SECONDS = 1.0
HEALTH_READY_CACHE_TTL_SECONDS = 5.0
_ready_cache: dict[str, object] = {"at": 0.0, "payload": None, "ready": False}


def _build_payload(request: Request) -> dict[str, object]:
    cached = getattr(request.app.state, "build_info", None)
    return dict(cached) if isinstance(cached, dict) else get_build_info()


def _capacity_payload() -> dict[str, object]:
    global_limit = int(get_global_max_concurrency())
    global_occupied = int(get_global_concurrency_occupied())
    return {
        "global_max_concurrency": global_limit,
        "global_concurrency_occupied": global_occupied,
        "recommended_proxy_max_connections": PROXY_PARALLEL_CAPACITY_HINT_CONNECTIONS,
        "proxy_capacity_hint": (
            f"多任务并行发送时，建议验证代理服务商单端点最大并发连接数 ≥ "
            f"{PROXY_PARALLEL_CAPACITY_HINT_CONNECTIONS}；"
            f"每个发送任务需绑定独立通道（同通道同时仅允许一个发送任务）。"
            f"当前全局并发上限 {global_limit}，已占用 {global_occupied}。"
        ),
        "estimated_proxy_rtt_seconds": float(settings.estimated_proxy_rtt_seconds),
    }


def _startup_flags_ready(request: Request) -> tuple[bool, bool]:
    db_ready = bool(getattr(request.app.state, "db_ready", False))
    redis_ready = bool(getattr(request.app.state, "redis_ready", False))
    return db_ready, redis_ready


@router.get("/health/live")
async def health_live(request: Request):
    """进程存活检查：仅读启动标志，不做 DB/Redis 实时探测，避免高负载下阻塞前端门禁。"""
    db_ready, redis_ready = _startup_flags_ready(request)
    startup_errors = list(getattr(request.app.state, "startup_errors", []) or [])[:3]
    ready = db_ready and redis_ready
    payload = {
        "status": "ok" if ready else "starting",
        "db_ready": db_ready,
        "redis_ready": redis_ready,
        "startup_errors": startup_errors,
        "probe": "live",
        "build": _build_payload(request),
    }
    if not ready:
        return JSONResponse(status_code=503, content=payload)
    return payload


async def _probe_ready_components(request: Request) -> tuple[bool, dict[str, object]]:
    db_ready, redis_ready = _startup_flags_ready(request)
    startup_errors = list(getattr(request.app.state, "startup_errors", []) or [])[:3]
    database_probe: dict[str, bool | str] = {"ok": False, "message": "数据库尚未初始化"}
    redis_probe: dict[str, bool | str] = {"ok": False, "message": "Redis 尚未初始化"}

    if db_ready:
        try:
            database_probe = await asyncio.wait_for(
                probe_database_health_light(),
                timeout=HEALTH_LIGHT_PROBE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            database_probe = {"ok": False, "message": "数据库健康探测超时"}
        except Exception as exc:
            database_probe = {"ok": False, "message": f"数据库健康探测失败：{exc}"}

    if redis_ready:
        try:
            await asyncio.wait_for(ping_redis_once(), timeout=HEALTH_LIGHT_PROBE_TIMEOUT_SECONDS)
            redis_probe = {"ok": True, "message": "Redis 连接正常"}
        except asyncio.TimeoutError:
            redis_probe = {"ok": False, "message": "Redis 健康探测超时"}
        except Exception as exc:
            redis_probe = {"ok": False, "message": f"Redis 连接失败：{exc}"}

    # 启动标志由 lifespan 设置，健康探测失败不得回写污染 app.state，否则门禁会被瞬时抖动永久锁死。
    ready = (
        db_ready
        and redis_ready
        and bool(database_probe.get("ok"))
        and bool(redis_probe.get("ok"))
    )
    payload = {
        "status": "ok" if ready else "degraded",
        "db_ready": db_ready,
        "redis_ready": redis_ready,
        "database_probe": database_probe,
        "redis_probe": redis_probe,
        "startup_errors": startup_errors,
        "capacity": _capacity_payload(),
        "probe": "ready",
        "build": _build_payload(request),
    }
    return ready, payload


@router.get("/health")
async def health_check(request: Request):
    """
    兼容入口：高负载下优先返回轻量存活结果。
    - 启动未完成：503（starting）
    - 已启动：200（ok），附带缓存的轻量探测摘要；探测失败标记 degraded 但仍返回 200，
      避免多任务发送时前端门禁/容器 HEALTHCHECK 被瞬时池压力误杀。
    """
    db_ready, redis_ready = _startup_flags_ready(request)
    startup_errors = list(getattr(request.app.state, "startup_errors", []) or [])[:3]
    if not (db_ready and redis_ready):
        return JSONResponse(
            status_code=503,
            content={
                "status": "starting",
                "db_ready": db_ready,
                "redis_ready": redis_ready,
                "database_probe": {"ok": False, "message": "数据库尚未初始化"},
                "redis_probe": {"ok": False, "message": "Redis 尚未初始化"},
                "startup_errors": startup_errors,
                "capacity": _capacity_payload(),
                "probe": "startup",
                "build": _build_payload(request),
            },
        )

    now = monotonic()
    cached_payload = _ready_cache.get("payload")
    cached_at = float(_ready_cache.get("at") or 0.0)
    if cached_payload is not None and (now - cached_at) < HEALTH_READY_CACHE_TTL_SECONDS:
        payload = dict(cached_payload)
        payload["capacity"] = _capacity_payload()
        payload["probe_cached"] = True
        return payload

    started = monotonic()
    ready, payload = await _probe_ready_components(request)
    elapsed_ms = (monotonic() - started) * 1000.0
    payload["probe_elapsed_ms"] = round(elapsed_ms, 1)
    payload["probe_cached"] = False
    if ready:
        payload["status"] = "ok"
    else:
        # 进程已完成启动：瞬时探测失败视为 degraded，不阻断门禁与 Docker HEALTHCHECK。
        payload["status"] = "degraded"
        logger.warning(
            "健康检查轻量探测降级 elapsed_ms=%.1f database_ok=%s redis_ok=%s database_msg=%s redis_msg=%s",
            elapsed_ms,
            bool((payload.get("database_probe") or {}).get("ok")),
            bool((payload.get("redis_probe") or {}).get("ok")),
            (payload.get("database_probe") or {}).get("message"),
            (payload.get("redis_probe") or {}).get("message"),
        )
    _ready_cache["at"] = now
    _ready_cache["payload"] = payload
    _ready_cache["ready"] = ready
    return payload


@router.get("/health/ready")
async def health_ready(request: Request):
    """运维就绪检查：轻量实时探测，失败返回 503（不做表/索引全量校验）。"""
    db_ready, redis_ready = _startup_flags_ready(request)
    if not (db_ready and redis_ready):
        return JSONResponse(
            status_code=503,
            content={
                "status": "starting",
                "db_ready": db_ready,
                "redis_ready": redis_ready,
                "message": "系统仍在启动初始化",
                "probe": "ready",
                "build": _build_payload(request),
            },
        )
    ready, payload = await _probe_ready_components(request)
    if not ready:
        payload["status"] = "error"
        return JSONResponse(status_code=503, content=payload)
    payload["status"] = "ok"
    return payload


@router.get("/health/db")
async def health_db_check():
    probe = await probe_database_health_light()
    pool_status = get_database_pool_status()
    if not probe.get("ok"):
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "message": probe.get("message"),
                **pool_status,
            },
        )
    return {
        "status": "ok",
        "message": probe.get("message"),
        **pool_status,
    }
