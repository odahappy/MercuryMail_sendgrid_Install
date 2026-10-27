from fastapi import APIRouter, Depends
from app.api.deps import require_admin
from app.core.config import settings

router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/info", dependencies=[Depends(require_admin)])
async def get_system_info():
    return {
        "hot_retention_days": int(settings.hot_retention_days),
        "archive_retention_days": int(settings.archive_retention_days),
        "lost_event_retention_days": int(settings.lost_event_retention_days),
        "perf_debug": bool(settings.perf_debug),
        "global_max_concurrency": int(settings.global_max_concurrency),
        "default_http_batch_size": int(settings.default_http_batch_size),
    }
