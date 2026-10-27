import json
import logging
import uuid
from datetime import datetime
from pathlib import Path

import anyio
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy import delete as sql_delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import require_admin
from app.core.database import commit_session_with_retry, get_db
from app.core.config import settings
from app.models.models import Task, URLItem, URLPool, UrlImportTask
from app.services.url_service import (
    cancel_url_import_task,
    count_nonempty_lines,
    get_next_url,
    get_url_import_progress_snapshot,
    launch_url_import_task,
    load_pool_urls_json_value,
    normalize_url,
    url_import_progress_key,
)
from app.utils.errors import sanitize_user_error_message
from app.core.redis import redis_client

router = APIRouter(prefix="/api/urls", tags=["urls"])
logger = logging.getLogger(__name__)
ACTIVE_URL_POOL_TASK_STATUSES = ("running", "pending", "scheduled")
DEFAULT_RANDOM_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
MAX_URL_IMPORT_SIZE_BYTES = 100 * 1024 * 1024
URL_IMPORT_UPLOAD_DIR = Path("/app/data/uploads/url_pools")
if not Path("/app").exists():
    URL_IMPORT_UPLOAD_DIR = Path(__file__).resolve().parents[3] / "data" / "uploads" / "url_pools"
URL_IMPORT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
Path(settings.export_async_dir).mkdir(parents=True, exist_ok=True)


class PoolBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=160)
    jump_enabled: bool = False
    jump_domain: str = ""
    random_prefix: str = Field(default="uid", min_length=1, max_length=80)
    random_length: int = Field(default=12, ge=1, le=128)
    random_chars: str = Field(default=DEFAULT_RANDOM_CHARS, min_length=1, max_length=255)


class PoolOut(PoolBase):
    id: int
    url_count: int = 0
    created_at: datetime
    updated_at: datetime


class PoolListResponse(BaseModel):
    items: list[PoolOut]
    total: int
    page: int
    page_size: int


class UploadResponse(BaseModel):
    total_accepted: int


class ImportTaskCreateResponse(BaseModel):
    task_id: int
    status: str = "pending"


class ImportTaskOut(BaseModel):
    id: int
    pool_id: int
    pool_name: str
    status: str
    total_rows: int
    processed_rows: int
    progress_percent: float
    accepted_rows: int = 0
    error_message: str = ""
    source_filename: str = ""
    created_at: datetime
    updated_at: datetime


class ImportTaskListResponse(BaseModel):
    items: list[ImportTaskOut]
    total: int
    page: int
    page_size: int


class ImportProgressResponse(BaseModel):
    task_id: int
    pool_id: int
    status: str
    processed_rows: int
    total_rows: int
    progress_percent: float
    accepted_rows: int = 0
    error_message: str = ""


class NextUrlResponse(BaseModel):
    url: str


class UrlItemOut(BaseModel):
    id: int
    url: str
    index: int
    created_at: datetime


class UrlItemListResponse(BaseModel):
    items: list[UrlItemOut]
    total: int
    page: int
    page_size: int


class UrlItemCreateRequest(BaseModel):
    url: str | None = None
    urls: list[str] | None = None


class UrlItemUpdateRequest(BaseModel):
    url: str = Field(..., min_length=1)


class UrlItemMutationResponse(BaseModel):
    total_accepted: int = 0
    total_skipped: int = 0


class UrlItemBatchDeleteRequest(BaseModel):
    item_ids: list[int] = Field(..., min_length=1, max_length=5000)


class UrlItemBatchDeleteResponse(BaseModel):
    deleted_count: int


def compute_progress_percent(processed_rows: int, total_rows: int, status: str) -> float:
    if status in {"completed", "failed", "cancelled"}:
        return 100.0 if status == "completed" else min(100.0, round((processed_rows / total_rows) * 100, 2)) if total_rows > 0 else 0.0
    if total_rows <= 0:
        return 0.0
    return min(100.0, round((processed_rows / total_rows) * 100, 2))


async def acquire_url_pool_items_lock(pool_id: int) -> str:
    lock_key = f"url_pool_items_lock:{pool_id}"
    lock_token = uuid.uuid4().hex
    acquired = await redis_client.set(lock_key, lock_token, nx=True, ex=30)
    if not acquired:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该 URL 池正在处理链接变更，请稍后重试")
    return lock_token


async def release_url_pool_items_lock(pool_id: int, lock_token: str) -> None:
    lock_key = f"url_pool_items_lock:{pool_id}"
    try:
        await redis_client.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            1,
            lock_key,
            lock_token,
        )
    except Exception:
        logger.warning("释放 URL 池变更锁失败 pool_id=%s lock_key=%s，请等待锁 TTL 自动过期", pool_id, lock_key, exc_info=True)


async def ensure_url_pool_mutation_allowed(db: AsyncSession, pool_id: int) -> None:
    blocking_task = (
        await db.execute(
            select(Task.id, Task.name)
            .where(
                Task.url_pool_id == pool_id,
                Task.deleted_at.is_(None),
                Task.status.in_(ACTIVE_URL_POOL_TASK_STATUSES),
            )
            .order_by(Task.id.desc())
            .limit(1)
        )
    ).first()
    if blocking_task is not None:
        task_id, task_name = blocking_task
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"URL 池正被任务 #{task_id}（{task_name or '未命名任务'}）使用，运行中、等待中或定时任务结束前无法修改链接",
        )


async def get_pool_or_404(db: AsyncSession, pool_id: int) -> URLPool:
    pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    return pool


async def get_pool_item_or_404(db: AsyncSession, pool_id: int, item_id: int) -> URLItem:
    item = (
        await db.execute(
            select(URLItem).where(URLItem.id == item_id, URLItem.pool_id == pool_id)
        )
    ).scalar_one_or_none()
    if item is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 链接不存在")
    return item


async def sync_pool_urls_json(db: AsyncSession, pool: URLPool) -> None:
    result = await db.execute(
        select(URLItem.url)
        .where(URLItem.pool_id == pool.id, URLItem.is_active.is_(True))
        .order_by(URLItem.item_index.asc(), URLItem.id.asc())
    )
    urls = [str(row[0]).strip() for row in result.all() if str(row[0] or "").strip()]
    pool.urls = json.dumps(urls, ensure_ascii=False)
    pool.updated_at = datetime.utcnow()


def collect_url_create_candidates(payload: UrlItemCreateRequest) -> list[str]:
    candidates: list[str] = []
    if payload.url and str(payload.url).strip():
        candidates.append(str(payload.url).strip())
    for raw_url in payload.urls or []:
        stripped = str(raw_url or "").strip()
        if stripped:
            candidates.append(stripped)
    if not candidates:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请提供至少一个有效 URL")
    return candidates


async def ensure_pool_name_available(db: AsyncSession, name: str, exclude_pool_id: int | None = None) -> None:
    normalized_name = name.strip()
    if not normalized_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="URL 池名称不能为空")
    conditions = [URLPool.name == normalized_name]
    if exclude_pool_id is not None:
        conditions.append(URLPool.id != exclude_pool_id)
    occupied_pool_id = (await db.execute(select(URLPool.id).where(*conditions).limit(1))).scalar_one_or_none()
    if occupied_pool_id is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="URL 池名称已存在，请更换名称后重试")


def apply_pool_payload(pool: URLPool, payload: PoolBase) -> None:
    pool.name = payload.name.strip()
    pool.jump_enabled = bool(payload.jump_enabled)
    pool.jump_domain = (payload.jump_domain or "").strip()
    pool.random_prefix = (payload.random_prefix or "uid").strip() or "uid"
    pool.random_length = int(payload.random_length)
    pool.random_chars = (payload.random_chars or DEFAULT_RANDOM_CHARS).strip() or DEFAULT_RANDOM_CHARS


def parse_upload_url_lines(content: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for line_number, line in enumerate(content.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            url = normalize_url(stripped)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"第 {line_number} 行 URL 格式无效：{stripped[:120]}（{exc}）",
            ) from exc
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


async def import_task_to_out(task: UrlImportTask, pool_name: str, redis_snapshot: dict[str, str] | None = None) -> ImportTaskOut:
    snapshot = redis_snapshot or {}
    processed_rows = int(snapshot.get("processed") or task.processed_rows or 0)
    total_rows = int(snapshot.get("total") or task.total_rows or 0)
    status_value = snapshot.get("status") or task.status or "pending"
    accepted_rows = int(snapshot.get("accepted") or 0)
    error_message = snapshot.get("error") or task.error_message or ""
    return ImportTaskOut(
        id=task.id,
        pool_id=task.pool_id,
        pool_name=pool_name,
        status=status_value,
        total_rows=total_rows,
        processed_rows=processed_rows,
        progress_percent=compute_progress_percent(processed_rows, total_rows, status_value),
        accepted_rows=accepted_rows,
        error_message=error_message,
        source_filename=task.source_filename or "",
        created_at=task.created_at or datetime.utcnow(),
        updated_at=task.updated_at or datetime.utcnow(),
    )


async def save_url_import_upload(file: UploadFile, destination: Path) -> tuple[int, int]:
    total_size = 0
    total_rows = 0
    has_content = False
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("wb") as target:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total_size += len(chunk)
                if total_size > MAX_URL_IMPORT_SIZE_BYTES:
                    raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="上传文件过大，请上传不超过 100MB 的文件")
                has_content = has_content or bool(chunk.strip())
                total_rows += chunk.count(b"\n")
                await anyio.to_thread.run_sync(target.write, chunk)
    except HTTPException:
        raise
    except OSError as exc:
        logger.exception("保存 URL 上传文件失败 filename=%s destination=%s size=%s rows=%s", getattr(file, 'filename', ''), destination, total_size, total_rows)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="保存上传文件失败，请检查服务器磁盘空间和目录权限") from exc
    except Exception as exc:
        logger.exception("读取 URL 上传文件失败 filename=%s destination=%s size=%s rows=%s", getattr(file, 'filename', ''), destination, total_size, total_rows)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="读取上传文件失败，请重新选择文件后重试") from exc
    finally:
        await file.close()
    return (await anyio.to_thread.run_sync(count_nonempty_lines, destination) if has_content else 0), total_size


async def save_url_import_text(text_content: str, destination: Path) -> tuple[int, int]:
    encoded = text_content.encode("utf-8")
    if len(encoded) > MAX_URL_IMPORT_SIZE_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="粘贴内容过大，请控制在 100MB 以内")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        await anyio.to_thread.run_sync(destination.write_bytes, encoded)
    except OSError as exc:
        logger.exception("保存 URL 粘贴内容失败 destination=%s size=%s", destination, len(encoded))
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="保存粘贴内容失败，请检查服务器磁盘空间和目录权限") from exc
    return await anyio.to_thread.run_sync(count_nonempty_lines, destination), len(encoded)


def clean_url_import_filename(value: str) -> str:
    filename = Path(str(value or "urls.txt").strip() or "urls.txt").name
    if not filename:
        return "urls.txt"
    return filename[:255]


async def create_url_import_job(
    db: AsyncSession,
    pool_id: int,
    saved_path: Path,
    source_filename: str,
    total_rows: int,
) -> ImportTaskCreateResponse:
    now_value = datetime.utcnow()
    import_task = UrlImportTask(
        pool_id=pool_id,
        status="pending",
        total_rows=total_rows,
        processed_rows=0,
        error_message="",
        source_filename=source_filename,
        created_at=now_value,
        updated_at=now_value,
    )
    db.add(import_task)
    await commit_session_with_retry(db)
    await db.refresh(import_task)
    try:
        await redis_client.hset(
            url_import_progress_key(import_task.id),
            mapping={
                "pool_id": pool_id,
                "status": "pending",
                "processed": 0,
                "total": total_rows,
                "accepted": 0,
                "error": "",
            },
        )
        await redis_client.expire(url_import_progress_key(import_task.id), 86400 * 7)
    except Exception:
        logger.warning("URL 导入任务 %s Redis 进度初始化失败", import_task.id, exc_info=True)
    launch_url_import_task(import_task.id, pool_id, str(saved_path), total_rows)
    return ImportTaskCreateResponse(task_id=import_task.id, status="pending")


@router.get("/import-tasks", response_model=ImportTaskListResponse, dependencies=[Depends(require_admin)])
async def list_import_tasks(
    pool_id: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    try:
        conditions = []
        if pool_id is not None:
            conditions.append(UrlImportTask.pool_id == pool_id)
        total = int((await db.execute(select(func.count()).select_from(UrlImportTask).where(*conditions))).scalar_one())
        result = await db.execute(
            select(UrlImportTask)
            .options(selectinload(UrlImportTask.pool))
            .where(*conditions)
            .order_by(UrlImportTask.created_at.desc(), UrlImportTask.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        items: list[ImportTaskOut] = []
        for task in result.scalars().all():
            redis_snapshot = await get_url_import_progress_snapshot(task.id)
            pool_name = task.pool.name if task.pool is not None else f"池 #{task.pool_id}"
            items.append(await import_task_to_out(task, pool_name, redis_snapshot))
        return ImportTaskListResponse(items=items, total=total, page=page, page_size=page_size)
    except Exception as exc:
        logger.exception("查询 URL 导入任务失败 pool_id=%s page=%s page_size=%s", pool_id, page, page_size)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.get("/import-tasks/{task_id}/progress", response_model=ImportProgressResponse, dependencies=[Depends(require_admin)])
async def get_import_task_progress(task_id: int, db: AsyncSession = Depends(get_db)):
    task = (await db.execute(select(UrlImportTask).where(UrlImportTask.id == task_id))).scalar_one_or_none()
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 导入任务不存在")
    snapshot = await get_url_import_progress_snapshot(task_id)
    processed_rows = int(snapshot.get("processed") or task.processed_rows or 0)
    total_rows = int(snapshot.get("total") or task.total_rows or 0)
    status_value = snapshot.get("status") or task.status or "pending"
    return ImportProgressResponse(
        task_id=task.id,
        pool_id=task.pool_id,
        status=status_value,
        processed_rows=processed_rows,
        total_rows=total_rows,
        progress_percent=compute_progress_percent(processed_rows, total_rows, status_value),
        accepted_rows=int(snapshot.get("accepted") or 0),
        error_message=snapshot.get("error") or task.error_message or "",
    )


@router.post("/import-tasks/{task_id}/cancel", dependencies=[Depends(require_admin)])
async def cancel_import_task(task_id: int, db: AsyncSession = Depends(get_db)):
    task = (await db.execute(select(UrlImportTask).where(UrlImportTask.id == task_id))).scalar_one_or_none()
    if task is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 导入任务不存在")
    if task.status in {"completed", "failed", "cancelled"}:
        return {"success": True, "status": task.status, "message": "任务已结束，无需取消"}
    await cancel_url_import_task(task_id)
    return {"success": True, "status": "cancelled", "message": "导入任务已取消"}


@router.post("/pools/{pool_id}/import", response_model=ImportTaskCreateResponse, dependencies=[Depends(require_admin)])
async def import_urls_async(
    pool_id: int,
    file: UploadFile | None = File(default=None),
    urls_text: str | None = Form(default=None),
    text: str | None = Form(default=None),
    db: AsyncSession = Depends(get_db),
):
    pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    await ensure_url_pool_mutation_allowed(db, pool_id)
    text_content = str(urls_text or text or "").strip()
    if file is None and not text_content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请输入网址或上传文件")

    source_filename = "manual-paste.txt"
    saved_path = URL_IMPORT_UPLOAD_DIR / f"{uuid.uuid4().hex}.txt"
    try:
        if text_content:
            total_rows, total_size = await save_url_import_text(text_content, saved_path)
        elif file is not None:
            source_filename = clean_url_import_filename(file.filename or "urls.txt")
            extension = Path(source_filename).suffix.lower()
            if extension and extension not in {".txt", ".csv"}:
                await file.close()
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="仅支持上传 .txt 或 .csv 文本文件")
            total_rows, total_size = await save_url_import_upload(file, saved_path)
        if total_rows <= 0:
            saved_path.unlink(missing_ok=True)
            logger.warning("URL 导入内容为空 pool_id=%s filename=%s size=%s", pool_id, source_filename, locals().get('total_size', 0))
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="导入内容为空，请提供有效 URL")
        logger.info("URL 导入文件已保存 pool_id=%s filename=%s total_rows=%s total_size=%s path=%s", pool_id, source_filename, total_rows, locals().get('total_size', 0), saved_path)
        return await create_url_import_job(db, pool_id, saved_path, source_filename, total_rows)
    except HTTPException:
        saved_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        saved_path.unlink(missing_ok=True)
        logger.exception("URL 导入请求处理失败 pool_id=%s filename=%s", pool_id, source_filename)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="URL 导入失败，请检查文件内容后重试") from exc


@router.post("/pools/{pool_id}/next", response_model=NextUrlResponse, dependencies=[Depends(require_admin)])
async def next_url(pool_id: int, db: AsyncSession = Depends(get_db)):
    pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    try:
        result = await get_next_url(pool_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=sanitize_user_error_message(str(exc)))
    return NextUrlResponse(url=result.url)


async def pool_to_out(db: AsyncSession, pool: URLPool) -> PoolOut:
    count = int((await db.execute(select(func.count()).select_from(URLItem).where(URLItem.pool_id == pool.id))).scalar_one())
    now_value = datetime.utcnow()
    return PoolOut(
        id=pool.id,
        name=pool.name,
        jump_enabled=bool(pool.jump_enabled),
        jump_domain=pool.jump_domain or "",
        random_prefix=pool.random_prefix or "uid",
        random_length=pool.random_length or 12,
        random_chars=pool.random_chars or DEFAULT_RANDOM_CHARS,
        url_count=count,
        created_at=pool.created_at or now_value,
        updated_at=pool.updated_at or now_value,
    )


@router.get("/pools", response_model=PoolListResponse, dependencies=[Depends(require_admin)])
async def list_pools(page: int = Query(default=1, ge=1), page_size: int = Query(default=20, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    try:
        total = int((await db.execute(select(func.count()).select_from(URLPool))).scalar_one())
        result = await db.execute(select(URLPool).order_by(URLPool.created_at.desc(), URLPool.id.desc()).offset((page - 1) * page_size).limit(page_size))
        pools = result.scalars().all()
        return PoolListResponse(items=[await pool_to_out(db, pool) for pool in pools], total=total, page=page, page_size=page_size)
    except Exception as exc:
        logger.exception("查询 URL 池列表失败 page=%s page_size=%s", page, page_size)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.post("/pools", response_model=PoolOut, dependencies=[Depends(require_admin)])
async def create_pool(payload: PoolBase, db: AsyncSession = Depends(get_db)):
    await ensure_pool_name_available(db, payload.name)
    now_value = datetime.utcnow()
    pool = URLPool(urls="[]", created_at=now_value, updated_at=now_value)
    apply_pool_payload(pool, payload)
    db.add(pool)
    try:
        await commit_session_with_retry(db)
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="URL 池名称已存在，请更换名称后重试") from exc
    await db.refresh(pool)
    return await pool_to_out(db, pool)


@router.put("/pools/{pool_id}", response_model=PoolOut, dependencies=[Depends(require_admin)])
async def update_pool(pool_id: int, payload: PoolBase, db: AsyncSession = Depends(get_db)):
    pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    await ensure_pool_name_available(db, payload.name, exclude_pool_id=pool_id)
    apply_pool_payload(pool, payload)
    pool.updated_at = datetime.utcnow()
    try:
        await commit_session_with_retry(db)
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="URL 池名称已存在，请更换名称后重试") from exc
    await db.refresh(pool)
    return await pool_to_out(db, pool)


@router.delete("/pools/{pool_id}", dependencies=[Depends(require_admin)])
async def delete_pool(pool_id: int, db: AsyncSession = Depends(get_db)):
    pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    await ensure_url_pool_mutation_allowed(db, pool_id)
    now_value = datetime.utcnow()
    try:
        await db.execute(sql_delete(URLItem).where(URLItem.pool_id == pool_id))
        await db.execute(sql_delete(UrlImportTask).where(UrlImportTask.pool_id == pool_id))
        await db.execute(
            update(Task)
            .where(Task.url_pool_id == pool_id)
            .values(url_pool_id=None, updated_at=now_value)
        )
        try:
            redis_keys = [str(key) async for key in redis_client.scan_iter(match=f"url_pool:{pool_id}:*")]
            if redis_keys:
                await redis_client.delete(*redis_keys)
        except Exception:
            logger.warning("清理 URL 池 Redis 缓存失败 pool_id=%s", pool_id, exc_info=True)
        await db.delete(pool)
        await commit_session_with_retry(db)
        return {"success": True, "message": "URL 池已删除"}
    except HTTPException:
        raise
    except Exception as exc:
        await db.rollback()
        logger.exception("删除 URL 池失败 pool_id=%s error=%s", pool_id, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="删除 URL 池失败，请稍后重试") from exc


@router.get("/pools/{pool_id}/items", response_model=UrlItemListResponse, dependencies=[Depends(require_admin)])
async def list_pool_items(
    pool_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    await get_pool_or_404(db, pool_id)
    total = int(
        (await db.execute(select(func.count()).select_from(URLItem).where(URLItem.pool_id == pool_id))).scalar_one()
    )
    result = await db.execute(
        select(URLItem)
        .where(URLItem.pool_id == pool_id)
        .order_by(URLItem.item_index.asc(), URLItem.id.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    items = [
        UrlItemOut(
            id=item.id,
            url=item.url,
            index=int(item.item_index or 0),
            created_at=item.created_at or datetime.utcnow(),
        )
        for item in result.scalars().all()
    ]
    return UrlItemListResponse(items=items, total=total, page=page, page_size=page_size)


@router.delete("/pools/{pool_id}/items", response_model=UrlItemBatchDeleteResponse, dependencies=[Depends(require_admin)])
async def clear_pool_items(pool_id: int, db: AsyncSession = Depends(get_db)):
    lock_token = await acquire_url_pool_items_lock(pool_id)
    try:
        pool = await get_pool_or_404(db, pool_id)
        await ensure_url_pool_mutation_allowed(db, pool_id)
        delete_result = await db.execute(sql_delete(URLItem).where(URLItem.pool_id == pool_id))
        deleted_count = max(0, int(delete_result.rowcount or 0))
        pool.next_index = 0
        await sync_pool_urls_json(db, pool)
        await commit_session_with_retry(db)
        return UrlItemBatchDeleteResponse(deleted_count=deleted_count)
    finally:
        await release_url_pool_items_lock(pool_id, lock_token)


@router.post("/pools/{pool_id}/items", response_model=UrlItemMutationResponse, dependencies=[Depends(require_admin)])
async def create_pool_items(pool_id: int, payload: UrlItemCreateRequest, db: AsyncSession = Depends(get_db)):
    lock_token = await acquire_url_pool_items_lock(pool_id)
    try:
        pool = await get_pool_or_404(db, pool_id)
        await ensure_url_pool_mutation_allowed(db, pool_id)
        raw_candidates = collect_url_create_candidates(payload)
        existing_result = await db.execute(select(URLItem.url).where(URLItem.pool_id == pool_id))
        existing_urls = {str(row[0]).strip() for row in existing_result.all() if str(row[0] or "").strip()}
        for legacy_url in load_pool_urls_json_value(pool.urls):
            existing_urls.add(legacy_url)
        next_index = int(
            (await db.execute(select(func.count()).select_from(URLItem).where(URLItem.pool_id == pool_id))).scalar_one()
            or 0
        )
        accepted_count = 0
        skipped_count = 0
        now_value = datetime.utcnow()
        for line_number, raw_url in enumerate(raw_candidates, start=1):
            try:
                normalized_url = normalize_url(raw_url)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"第 {line_number} 个 URL 格式无效：{raw_url[:120]}（{exc}）",
                ) from exc
            if normalized_url in existing_urls:
                skipped_count += 1
                continue
            existing_urls.add(normalized_url)
            next_index += 1
            db.add(
                URLItem(
                    pool_id=pool_id,
                    url=normalized_url,
                    original_url=normalized_url,
                    item_index=next_index,
                    is_active=True,
                    created_at=now_value,
                    updated_at=now_value,
                )
            )
            accepted_count += 1
        if accepted_count > 0:
            await sync_pool_urls_json(db, pool)
            await commit_session_with_retry(db)
        return UrlItemMutationResponse(total_accepted=accepted_count, total_skipped=skipped_count)
    finally:
        await release_url_pool_items_lock(pool_id, lock_token)


@router.post("/pools/{pool_id}/items/batch-delete", response_model=UrlItemBatchDeleteResponse, dependencies=[Depends(require_admin)])
async def batch_delete_pool_items(pool_id: int, payload: UrlItemBatchDeleteRequest, db: AsyncSession = Depends(get_db)):
    normalized_ids = sorted({int(item_id) for item_id in payload.item_ids if int(item_id) > 0})
    if not normalized_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请提供至少一个有效的 URL 项 ID")
    lock_token = await acquire_url_pool_items_lock(pool_id)
    try:
        pool = await get_pool_or_404(db, pool_id)
        await ensure_url_pool_mutation_allowed(db, pool_id)
        delete_result = await db.execute(
            sql_delete(URLItem).where(URLItem.pool_id == pool_id, URLItem.id.in_(normalized_ids))
        )
        deleted_count = max(0, int(delete_result.rowcount or 0))
        if deleted_count <= 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="未找到可删除的 URL 链接")
        await sync_pool_urls_json(db, pool)
        await commit_session_with_retry(db)
        return UrlItemBatchDeleteResponse(deleted_count=deleted_count)
    finally:
        await release_url_pool_items_lock(pool_id, lock_token)


@router.put("/pools/{pool_id}/items/{item_id}", response_model=UrlItemOut, dependencies=[Depends(require_admin)])
async def update_pool_item(pool_id: int, item_id: int, payload: UrlItemUpdateRequest, db: AsyncSession = Depends(get_db)):
    lock_token = await acquire_url_pool_items_lock(pool_id)
    try:
        pool = await get_pool_or_404(db, pool_id)
        await ensure_url_pool_mutation_allowed(db, pool_id)
        item = await get_pool_item_or_404(db, pool_id, item_id)
        try:
            normalized_url = normalize_url(payload.url)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"URL 格式无效：{exc}") from exc
        duplicate_id = (
            await db.execute(
                select(URLItem.id)
                .where(
                    URLItem.pool_id == pool_id,
                    URLItem.url == normalized_url,
                    URLItem.id != item_id,
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if duplicate_id is not None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该 URL 已存在于当前池中")
        item.url = normalized_url
        item.original_url = normalized_url
        item.updated_at = datetime.utcnow()
        await sync_pool_urls_json(db, pool)
        await commit_session_with_retry(db)
        await db.refresh(item)
        return UrlItemOut(
            id=item.id,
            url=item.url,
            index=int(item.item_index or 0),
            created_at=item.created_at or datetime.utcnow(),
        )
    finally:
        await release_url_pool_items_lock(pool_id, lock_token)


@router.delete("/pools/{pool_id}/items/{item_id}", dependencies=[Depends(require_admin)])
async def delete_pool_item(pool_id: int, item_id: int, db: AsyncSession = Depends(get_db)):
    lock_token = await acquire_url_pool_items_lock(pool_id)
    try:
        pool = await get_pool_or_404(db, pool_id)
        await ensure_url_pool_mutation_allowed(db, pool_id)
        item = await get_pool_item_or_404(db, pool_id, item_id)
        await db.delete(item)
        await sync_pool_urls_json(db, pool)
        await commit_session_with_retry(db)
        return {"success": True, "message": "URL 链接已删除"}
    finally:
        await release_url_pool_items_lock(pool_id, lock_token)


@router.post("/pools/{pool_id}/upload", response_model=UploadResponse, dependencies=[Depends(require_admin)])
async def upload_urls(
    pool_id: int,
    file: UploadFile | None = File(default=None),
    text: str | None = Form(default=None),
    urls_text: str | None = Form(default=None),
    db: AsyncSession = Depends(get_db),
):
    pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="URL 池不存在")
    text_content = str(urls_text or text or "").strip()
    if text_content:
        content = text_content
    elif file is not None:
        content = (await file.read()).decode("utf-8", errors="ignore")
        await file.close()
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请输入网址或上传文件")
    urls = parse_upload_url_lines(content)
    if not urls:
        return UploadResponse(total_accepted=0)
    existing_urls = load_pool_urls_json_value(pool.urls)
    merged_urls = list(existing_urls)
    merged_seen = set(existing_urls)
    accepted_count = 0
    for url in urls:
        if url in merged_seen:
            continue
        merged_seen.add(url)
        merged_urls.append(url)
        accepted_count += 1
    if accepted_count == 0:
        return UploadResponse(total_accepted=0)
    existing_count = int((await db.execute(select(func.count()).select_from(URLItem).where(URLItem.pool_id == pool_id))).scalar_one())
    next_index = existing_count
    for url in urls:
        if url in existing_urls:
            continue
        next_index += 1
        db.add(URLItem(pool_id=pool_id, url=url, original_url=url, item_index=next_index, is_active=True))
    pool.urls = json.dumps(merged_urls, ensure_ascii=False)
    pool.updated_at = datetime.utcnow()
    await commit_session_with_retry(db)
    return UploadResponse(total_accepted=accepted_count)
