"""大数据量异步导出：后台 COPY 落盘 + Redis 任务状态。"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from time import time

from fastapi import HTTPException, status
from fastapi.responses import FileResponse

from app.core.config import settings
from app.core.redis import redis_client
from app.utils.export import ExportCopyError, copy_query_to_file

logger = logging.getLogger(__name__)

ASYNC_EXPORT_REDIS_PREFIX = "export:async:"
ASYNC_EXPORT_PROCESSING_SET = "export:async:processing"
ASYNC_EXPORT_REDIS_TTL_SECONDS = 25 * 3600
STALE_PROCESSING_EXPORT_SECONDS = 7200
_copy_job_semaphore: asyncio.Semaphore | None = None


def _get_copy_job_semaphore() -> asyncio.Semaphore:
    global _copy_job_semaphore
    if _copy_job_semaphore is None:
        max_tasks = max(1, int(settings.export_async_max_tasks or 3))
        _copy_job_semaphore = asyncio.Semaphore(max_tasks)
    return _copy_job_semaphore


def _is_stale_processing_export(meta: dict[str, object]) -> bool:
    created_raw = str(meta.get("created_at") or "").strip()
    if not created_raw:
        return False
    try:
        created_dt = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - created_dt.astimezone(timezone.utc)).total_seconds()
        return age_seconds > STALE_PROCESSING_EXPORT_SECONDS
    except ValueError:
        return False


class AsyncExportBusyError(RuntimeError):
    """并发异步导出任务已达上限。"""


class AsyncExportNotFoundError(LookupError):
    """异步导出任务不存在或已过期。"""


def _meta_key(export_id: str) -> str:
    return f"{ASYNC_EXPORT_REDIS_PREFIX}{export_id}"


def _export_file_path(export_id: str) -> Path:
    base_dir = Path(str(settings.export_async_dir or "/tmp/mercury_exports"))
    return base_dir / f"{export_id}.txt"


def ensure_export_async_dir() -> Path:
    base_dir = Path(str(settings.export_async_dir or "/tmp/mercury_exports"))
    base_dir.mkdir(parents=True, exist_ok=True)
    logger.warning("异步导出目录已就绪 path=%s", base_dir)
    return base_dir


def get_export_service_status() -> dict[str, object]:
    base_dir = ensure_export_async_dir()
    return {
        "ready": base_dir.is_dir(),
        "export_dir": str(base_dir),
        "max_tasks": max(1, int(settings.export_async_max_tasks or 3)),
        "ttl_hours": max(1, int(settings.export_async_file_ttl_hours or 24)),
    }


async def _count_processing_exports() -> int:
    """统计进行中的导出数，并清理 processing 集合中的过期成员。"""
    try:
        members = await redis_client.smembers(ASYNC_EXPORT_PROCESSING_SET)
        if not members:
            return 0
        active_count = 0
        for raw_export_id in members:
            export_id = str(raw_export_id)
            try:
                meta = await _load_export_meta(export_id)
                if str(meta.get("status") or "") == "processing":
                    if _is_stale_processing_export(meta):
                        meta.update(
                            {
                                "status": "failed",
                                "error": "导出任务异常中断，请重新发起导出",
                                "progress_percent": 0,
                            }
                        )
                        await _save_export_meta(export_id, meta)
                        _export_file_path(export_id).unlink(missing_ok=True)
                        await redis_client.srem(ASYNC_EXPORT_PROCESSING_SET, export_id)
                        logger.warning("已清理超时挂起的异步导出 export_id=%s", export_id)
                        continue
                    active_count += 1
                    continue
            except AsyncExportNotFoundError:
                pass
            except Exception:
                logger.warning("校验异步导出 processing 成员失败 export_id=%s", export_id, exc_info=True)
                continue
            try:
                await redis_client.srem(ASYNC_EXPORT_PROCESSING_SET, export_id)
                logger.info("已清理过期的异步导出 processing 成员 export_id=%s", export_id)
            except Exception:
                logger.warning("清理异步导出 processing 成员失败 export_id=%s", export_id, exc_info=True)
        return active_count
    except Exception:
        logger.warning("读取异步导出并发数失败", exc_info=True)
        return 0


async def _save_export_meta(export_id: str, payload: dict[str, object]) -> None:
    await redis_client.set(
        _meta_key(export_id),
        json.dumps(payload, ensure_ascii=False),
        ex=ASYNC_EXPORT_REDIS_TTL_SECONDS,
    )


async def _load_export_meta(export_id: str) -> dict[str, object]:
    raw = await redis_client.get(_meta_key(export_id))
    if not raw:
        raise AsyncExportNotFoundError("导出任务不存在或已过期")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AsyncExportNotFoundError("导出任务数据损坏") from exc
    if not isinstance(payload, dict):
        raise AsyncExportNotFoundError("导出任务数据无效")
    return payload


async def start_async_export(
    sql: str,
    filename: str,
    *,
    estimated_rows: int = 0,
    user_id: int | None = None,
) -> str:
    max_tasks = max(1, int(settings.export_async_max_tasks or 3))
    processing_count = await _count_processing_exports()
    if processing_count >= max_tasks:
        logger.warning("异步导出任务达到上限 processing_count=%s max_tasks=%s", processing_count, max_tasks)
        raise AsyncExportBusyError("导出任务繁忙，请稍后重试")

    export_id = str(uuid.uuid4())
    ensure_export_async_dir()
    created_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    meta = {
        "export_id": export_id,
        "status": "processing",
        "filename": str(filename or "export.txt"),
        "file_path": str(_export_file_path(export_id)),
        "estimated_rows": int(estimated_rows or 0),
        "progress_percent": 0,
        "user_id": int(user_id or 0),
        "file_size": 0,
        "error": "",
        "created_at": created_at,
    }
    await _save_export_meta(export_id, meta)
    added_to_processing = False
    try:
        await redis_client.sadd(ASYNC_EXPORT_PROCESSING_SET, export_id)
        added_to_processing = True
        await redis_client.expire(ASYNC_EXPORT_PROCESSING_SET, ASYNC_EXPORT_REDIS_TTL_SECONDS)
        asyncio.create_task(
            _run_async_export_job(export_id, sql, str(filename or "export.txt")),
            name=f"async-export-{export_id[:8]}",
        )
    except Exception:
        if added_to_processing:
            try:
                await redis_client.srem(ASYNC_EXPORT_PROCESSING_SET, export_id)
            except Exception:
                logger.warning("回滚异步导出 processing 成员失败 export_id=%s", export_id, exc_info=True)
        try:
            await redis_client.delete(_meta_key(export_id))
        except Exception:
            logger.warning("回滚异步导出元数据失败 export_id=%s", export_id, exc_info=True)
        raise
    logger.info(
        "已启动异步导出 export_id=%s filename=%s estimated_rows=%s",
        export_id,
        filename,
        estimated_rows,
    )
    return export_id


async def _run_async_export_job(export_id: str, sql: str, filename: str) -> None:
    file_path = _export_file_path(export_id)
    logger.warning("异步导出任务开始 export_id=%s file_path=%s filename=%s", export_id, file_path, filename)
    try:
        async with _get_copy_job_semaphore():
            file_size = await copy_query_to_file(sql, file_path)
        meta = await _load_export_meta(export_id)
        meta.update(
            {
                "status": "completed",
                "filename": filename,
                "progress_percent": 100,
                "file_size": int(file_size),
                "error": "",
                "completed_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            }
        )
        await _save_export_meta(export_id, meta)
        logger.warning("异步导出完成 export_id=%s file_size=%s", export_id, file_size)
    except Exception as exc:
        logger.exception("异步导出失败 export_id=%s file_path=%s", export_id, file_path)
        error_message = str(exc) if str(exc) else "导出失败，请稍后重试"
        if isinstance(exc, ExportCopyError):
            error_message = str(exc)
        logger.warning("异步导出失败原因 export_id=%s error=%s", export_id, error_message)
        try:
            meta = await _load_export_meta(export_id)
        except AsyncExportNotFoundError:
            meta = {
                "export_id": export_id,
                "status": "failed",
                "filename": filename,
                "file_path": str(file_path),
                "file_size": 0,
                "error": error_message,
            }
        else:
            meta.update({"status": "failed", "error": error_message, "file_size": 0, "progress_percent": 0})
        await _save_export_meta(export_id, meta)
        if file_path.exists():
            try:
                file_path.unlink()
            except OSError:
                logger.warning("删除失败的异步导出文件失败 path=%s", file_path, exc_info=True)
    finally:
        try:
            await redis_client.srem(ASYNC_EXPORT_PROCESSING_SET, export_id)
        except Exception:
            logger.warning("清理异步导出 processing 集合失败 export_id=%s", export_id, exc_info=True)


async def get_async_export_status(export_id: str) -> dict[str, object]:
    meta = await _load_export_meta(export_id)
    status_value = str(meta.get("status") or "processing")
    filename = str(meta.get("filename") or "export.txt")
    payload: dict[str, object] = {
        "export_id": export_id,
        "status": status_value,
        "filename": filename,
        "progress_percent": int(meta.get("progress_percent") or 0),
    }
    if status_value == "completed":
        payload["download_url"] = f"/api/export/async/{export_id}/download"
        payload["file_size"] = int(meta.get("file_size") or 0)
    elif status_value == "failed":
        payload["error"] = str(meta.get("error") or "导出失败，请稍后重试")
    return payload


async def get_async_export_meta(export_id: str) -> dict[str, object]:
    return await _load_export_meta(export_id)


async def build_async_export_download_response(export_id: str) -> FileResponse:
    meta = await _load_export_meta(export_id)
    if str(meta.get("status") or "") != "completed":
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="导出文件尚未就绪或已失效")
    file_path = Path(str(meta.get("file_path") or _export_file_path(export_id)))
    if not file_path.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="导出文件不存在或已过期")
    filename = str(meta.get("filename") or file_path.name)
    return FileResponse(
        path=str(file_path),
        media_type="text/plain; charset=utf-8",
        filename=filename,
    )


async def cleanup_expired_async_exports() -> None:
    """删除超过保留期的导出文件及 Redis 元数据。"""
    ttl_hours = max(1, int(settings.export_async_file_ttl_hours or 24))
    ttl_seconds = ttl_hours * 3600
    cutoff = time() - ttl_seconds
    base_dir = ensure_export_async_dir()
    removed_files = 0
    processing_ids: set[str] = set()
    try:
        raw_processing = await redis_client.smembers(ASYNC_EXPORT_PROCESSING_SET)
        processing_ids = {str(item) for item in (raw_processing or [])}
    except Exception:
        logger.warning("读取异步导出 processing 集合失败，清理将跳过进行中的任务", exc_info=True)
    for file_path in base_dir.glob("*.txt"):
        try:
            if file_path.stat().st_mtime >= cutoff:
                continue
            export_id = file_path.stem
            if export_id in processing_ids:
                logger.debug("跳过清理进行中的异步导出 export_id=%s", export_id)
                continue
            try:
                meta = await _load_export_meta(export_id)
                if str(meta.get("status") or "") == "processing":
                    logger.debug("跳过清理状态为 processing 的异步导出 export_id=%s", export_id)
                    continue
            except AsyncExportNotFoundError:
                pass
            except Exception:
                logger.warning("读取异步导出元数据失败 export_id=%s", export_id, exc_info=True)
            file_path.unlink(missing_ok=True)
            removed_files += 1
            try:
                await redis_client.delete(_meta_key(export_id))
                await redis_client.srem(ASYNC_EXPORT_PROCESSING_SET, export_id)
            except Exception:
                logger.warning("清理异步导出 Redis 元数据失败 export_id=%s", export_id, exc_info=True)
        except OSError:
            logger.warning("删除过期导出文件失败 path=%s", file_path, exc_info=True)
    try:
        async for key in redis_client.scan_iter(match=f"{ASYNC_EXPORT_REDIS_PREFIX}*"):
            export_id = str(key).removeprefix(ASYNC_EXPORT_REDIS_PREFIX)
            if not export_id:
                continue
            file_path = _export_file_path(export_id)
            if file_path.exists():
                continue
            try:
                await redis_client.delete(str(key))
                await redis_client.srem(ASYNC_EXPORT_PROCESSING_SET, export_id)
            except Exception:
                logger.warning("清理缺失文件的异步导出 Redis 元数据失败 export_id=%s", export_id, exc_info=True)
    except Exception:
        logger.warning("扫描异步导出 Redis 元数据失败", exc_info=True)
    if removed_files:
        logger.info("异步导出清理完成，删除过期文件 %s 个", removed_files)


async def async_export_cleanup_loop() -> None:
    """每 60 分钟清理过期异步导出文件。"""
    while True:
        try:
            await cleanup_expired_async_exports()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("异步导出定期清理失败")
        await asyncio.sleep(3600)
