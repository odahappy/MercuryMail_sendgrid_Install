import asyncio
import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path

import anyio
from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile, status
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from sqlalchemy import delete as sql_delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_admin
from app.constants.import_status import ACTIVE_STATUSES, COMPLETED, FAILED, PROCESSING, QUEUED
from app.core.database import AsyncSessionLocal, commit_session_with_retry, get_db
from app.core.redis import get_redis, redis_client
from app.models.models import Recipient, RecipientList, Task
from app.services.recipient_service import (
    extract_email_from_line,
    import_recipient_file,
    validate_recipient_email,
)

router = APIRouter(prefix="/api/recipients", tags=["recipients"])
logger = logging.getLogger(__name__)
ACTIVE_TASK_STATUSES = ("running", "pending", "scheduled")
RECIPIENT_LIST_DELETE_CHUNK_SIZE = 1000
RECIPIENT_DELETE_PROGRESS_TTL_SECONDS = 3600
RECIPIENT_DELETE_PROGRESS_PREFIX = "recipient_delete_progress"
RECIPIENT_DELETE_ACTIVE_PREFIX = "recipient_delete_active"
RECIPIENT_DELETE_TASKS: set = set()
MAX_UPLOAD_SIZE_BYTES = 100 * 1024 * 1024
UPLOAD_DIR = Path("/app/data/uploads/recipients")
if not Path("/app").exists():
    UPLOAD_DIR = Path(__file__).resolve().parents[3] / "data" / "uploads" / "recipients"


class UploadResponse(BaseModel):
    task_id: str
    list_id: int
    name: str
    total_accepted: int
    total_lines: int
    status: str


class ImportResultResponse(BaseModel):
    total_lines: int
    imported: int
    skipped_duplicates: int
    skipped_invalid: int
    errors: list[str] = Field(default_factory=list)


class PasteRequest(BaseModel):
    name: str | None = Field(default=None, max_length=160)
    content: str = Field(..., min_length=1, max_length=MAX_UPLOAD_SIZE_BYTES)


class RecipientListOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    total_count: int
    valid_count: int
    invalid_count: int
    created_at: datetime
    status: str = "ready"
    task_id: str = ""


class RecipientOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    email: str
    name: str
    is_valid: bool
    is_unsubscribed: bool
    created_at: datetime


class RecipientPageResponse(BaseModel):
    items: list[RecipientOut]
    total: int
    page: int
    page_size: int


class ProgressResponse(BaseModel):
    task_id: str = ""
    list_id: int
    list_name: str = ""
    processed: int
    total: int
    status: str = COMPLETED
    accepted: int = 0
    invalid: int = 0
    skipped_duplicates: int = 0
    errors: list[str] = Field(default_factory=list)
    error: str = ""
    percentage: float = 0.0
    phase: str = "processing"

    @property
    def import_result(self) -> ImportResultResponse:
        return ImportResultResponse(
            total_lines=self.total,
            imported=self.accepted,
            skipped_duplicates=self.skipped_duplicates,
            skipped_invalid=self.invalid,
            errors=self.errors,
        )


class AddRecipientsRequest(BaseModel):
    emails: list[str] = Field(..., min_length=1, max_length=5000)


class AddRecipientsResponse(BaseModel):
    added_count: int
    skipped_duplicate_count: int
    skipped_invalid_count: int = 0


class DeleteRecipientResponse(BaseModel):
    success: bool
    message: str


class RecipientDeleteAcceptedResponse(BaseModel):
    task_id: str
    list_id: int
    status: str = "running"


class RecipientDeleteProgressResponse(BaseModel):
    task_id: str
    list_id: int
    list_name: str = ""
    status: str
    total_recipients: int
    deleted_recipients: int
    error: str = ""


def _delete_progress_key(task_id: str) -> str:
    return f"{RECIPIENT_DELETE_PROGRESS_PREFIX}:{task_id}"


async def _save_delete_progress(task_id: str, mapping: dict) -> None:
    key = _delete_progress_key(task_id)
    await redis_client.hset(key, mapping=mapping)
    await redis_client.expire(key, RECIPIENT_DELETE_PROGRESS_TTL_SECONDS)


async def _run_recipient_list_delete(task_id: str, list_id: int, list_name: str, total_recipients: int) -> None:
    deleted_total = 0
    try:
        while True:
            async with AsyncSessionLocal() as db:
                ids = list((await db.execute(
                    select(Recipient.id).where(Recipient.list_id == list_id).order_by(Recipient.id).limit(RECIPIENT_LIST_DELETE_CHUNK_SIZE)
                )).scalars().all())
                if not ids:
                    recipient_list = (await db.execute(select(RecipientList).where(RecipientList.id == list_id))).scalar_one_or_none()
                    if recipient_list is not None:
                        await db.delete(recipient_list)
                    await db.commit()
                    break
                await db.execute(text("UPDATE send_logs SET recipient_id = NULL WHERE recipient_id = ANY(:recipient_ids)"), {"recipient_ids": ids})
                await db.execute(text("UPDATE unsubscribes SET recipient_id = NULL WHERE recipient_id = ANY(:recipient_ids)"), {"recipient_ids": ids})
                result = await db.execute(sql_delete(Recipient).where(Recipient.id.in_(ids)))
                chunk_deleted = max(0, int(result.rowcount or 0))
                await db.commit()
            deleted_total += chunk_deleted
            await _save_delete_progress(task_id, {
                "task_id": task_id, "list_id": list_id, "list_name": list_name, "status": "running",
                "total_recipients": total_recipients, "deleted_recipients": deleted_total, "error": "",
            })
            await asyncio.sleep(0.03)
        await _save_delete_progress(task_id, {
            "task_id": task_id, "list_id": list_id, "list_name": list_name, "status": "completed",
            "total_recipients": total_recipients, "deleted_recipients": deleted_total, "error": "",
        })
        await redis_client.delete(f"{RECIPIENT_DELETE_ACTIVE_PREFIX}:{list_id}", f"recipient_progress:{list_id}")
        logger.info("收件人池后台删除完成 list_id=%s task_id=%s deleted=%s", list_id, task_id, deleted_total)
    except Exception:
        logger.exception("收件人池后台删除失败 list_id=%s task_id=%s", list_id, task_id)
        await _save_delete_progress(task_id, {
            "task_id": task_id, "list_id": list_id, "list_name": list_name, "status": "failed",
            "total_recipients": total_recipients, "deleted_recipients": deleted_total, "error": "删除收件人池失败，请稍后重试",
        })
        await redis_client.delete(f"{RECIPIENT_DELETE_ACTIVE_PREFIX}:{list_id}")


def _track_delete_task(coroutine) -> None:
    task = asyncio.create_task(coroutine)
    RECIPIENT_DELETE_TASKS.add(task)
    task.add_done_callback(RECIPIENT_DELETE_TASKS.discard)


def parse_progress_errors(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def build_progress_response(list_id: int, progress: dict[str, str], task_id: str = "") -> ProgressResponse:
    processed = int(progress.get("processed", 0))
    total = int(progress.get("total", 0))
    status_value = str(progress.get("status") or PROCESSING).strip().lower()
    if status_value == "done":
        status_value = COMPLETED
    percentage = round(min(100.0, (processed * 100.0) / total), 2) if total > 0 else 0.0
    if status_value == COMPLETED:
        percentage = 100.0
    resolved_list_id = int(progress.get("list_id") or list_id or 0)
    return ProgressResponse(
        task_id=str(progress.get("task_id") or task_id or ""),
        list_id=resolved_list_id,
        list_name=str(progress.get("list_name") or ""),
        processed=processed,
        total=total,
        status=status_value,
        accepted=int(progress.get("accepted", 0)),
        invalid=int(progress.get("invalid", 0)),
        skipped_duplicates=int(progress.get("skipped_duplicates", 0)),
        errors=parse_progress_errors(progress.get("errors_json")),
        error=progress.get("error") or "",
        percentage=percentage,
        phase=str(progress.get("phase") or "processing"),
    )


def clean_pool_name(value: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        candidate = "recipients"
    candidate = Path(candidate).name.strip()
    candidate = re.sub(r"[\r\n\t]+", " ", candidate)
    return candidate[:160] if candidate else "recipients"


def resolve_import_list_name(name: str | None, source_filename: str | None = None) -> str:
    cleaned = (name or "").strip()
    if cleaned:
        return clean_pool_name(cleaned)
    if source_filename:
        stem = Path(clean_pool_name(source_filename)).stem.strip()
        if stem and stem.lower() not in {"recipients", "manual-paste"}:
            return stem
    return f"收件人池-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


async def make_unique_list_name(db: AsyncSession, base_name: str) -> str:
    lock_key = f"recipient_list_name_lock:{base_name}"
    lock_token = uuid.uuid4().hex
    acquired = await redis_client.set(lock_key, lock_token, nx=True, ex=30)
    if not acquired:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="同名收件人池正在创建中，请稍后重试")
    try:
        result = await db.execute(select(RecipientList.name).where(RecipientList.name == base_name))
        if result.scalar_one_or_none() is None:
            return base_name
        stem = Path(base_name).stem or base_name
        suffix = Path(base_name).suffix
        index = 2
        while True:
            candidate = f"{stem}-{index}{suffix}"
            existing = await db.execute(select(RecipientList.name).where(RecipientList.name == candidate))
            if existing.scalar_one_or_none() is None:
                return candidate
            index += 1
    finally:
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
            logger.warning("释放收件人池命名锁失败 lock_key=%s，请等待锁 TTL 自动过期", lock_key, exc_info=True)


def count_chunk_lines(chunk: bytes, previous_had_content: bool, previous_ended_with_newline: bool) -> tuple[int, bool, bool]:
    if not chunk:
        return 0, previous_had_content, previous_ended_with_newline
    return chunk.count(b"\n"), True, chunk.endswith(b"\n") or chunk.endswith(b"\r")


async def save_upload_file(upload: UploadFile, destination: Path) -> tuple[int, int]:
    total_lines = 0
    total_size = 0
    has_content = False
    ends_with_newline = False
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("wb") as target:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total_size += len(chunk)
                if total_size > MAX_UPLOAD_SIZE_BYTES:
                    raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="上传文件过大，请上传不超过 100MB 的文件")
                line_count, has_content, ends_with_newline = count_chunk_lines(chunk, has_content, ends_with_newline)
                total_lines += line_count
                await anyio.to_thread.run_sync(target.write, chunk)
    except HTTPException:
        raise
    except OSError as exc:
        logger.exception("保存收件人上传文件失败 filename=%s destination=%s size=%s lines=%s", getattr(upload, 'filename', ''), destination, total_size, total_lines)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="保存上传文件失败，请检查服务器磁盘空间和目录权限") from exc
    except Exception as exc:
        logger.exception("读取收件人上传文件失败 filename=%s destination=%s size=%s lines=%s", getattr(upload, 'filename', ''), destination, total_size, total_lines)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="读取上传文件失败，请重新选择文件后重试") from exc
    finally:
        await upload.close()
    if not has_content:
        return 0, total_size
    return (total_lines if ends_with_newline else total_lines + 1), total_size


async def save_pasted_content(content: str, destination: Path) -> tuple[int, int]:
    data = content.encode("utf-8")
    if len(data) > MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="粘贴内容过大，请控制在 100MB 以内")
    destination.parent.mkdir(parents=True, exist_ok=True)
    await anyio.to_thread.run_sync(destination.write_bytes, data)
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    if not normalized:
        return 0, 0
    return len(normalized.splitlines()), len(data)


async def create_recipient_list_import(
    background_tasks: BackgroundTasks,
    db: AsyncSession,
    redis: Redis,
    name: str | None,
    source_filename: str,
    saved_path: Path,
    total_lines: int,
) -> UploadResponse:
    if total_lines <= 0:
        saved_path.unlink(missing_ok=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="收件人内容为空，请上传或粘贴有效邮箱")

    try:
        list_name = await make_unique_list_name(db, resolve_import_list_name(name, source_filename))
        recipient_list = RecipientList(name=list_name, description=f"从 {source_filename} 上传", source_filename=source_filename)
        db.add(recipient_list)
        await db.commit()
        await db.refresh(recipient_list)
    except Exception:
        await db.rollback()
        saved_path.unlink(missing_ok=True)
        raise

    task_id = uuid.uuid4().hex
    list_name = recipient_list.name
    progress_mapping = {
        "task_id": task_id,
        "list_id": recipient_list.id,
        "list_name": list_name,
        "processed": 0,
        "current": 0,
        "total": total_lines,
        "status": QUEUED,
        "accepted": 0,
        "invalid": 0,
        "skipped_duplicates": 0,
        "error": "",
        "phase": PROCESSING,
        "errors_json": "[]",
    }
    progress_key = f"recipient_progress:{recipient_list.id}"
    task_progress_key = f"import_progress:{task_id}"
    await redis.hset(progress_key, mapping=progress_mapping)
    await redis.expire(progress_key, 86400)
    await redis.hset(task_progress_key, mapping=progress_mapping)
    await redis.expire(task_progress_key, 86400)
    background_tasks.add_task(
        import_recipient_file,
        recipient_list.id,
        str(saved_path),
        total_lines,
        task_id,
        list_name,
    )
    return UploadResponse(
        task_id=task_id,
        list_id=recipient_list.id,
        name=list_name,
        total_accepted=0,
        total_lines=total_lines,
        status="queued",
    )


@router.post("/upload", response_model=UploadResponse, dependencies=[Depends(require_admin)])
async def upload_recipients(
    background_tasks: BackgroundTasks,
    name: str | None = Form(default=None),
    file: UploadFile | None = File(default=None),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        if file is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请上传文件或输入邮箱内容")
        raw_filename = file.filename or "recipients.txt"
        original_filename = clean_pool_name(raw_filename)
        if original_filename != raw_filename:
            logger.info("收件人上传文件名已清洗 original=%s cleaned=%s", raw_filename, original_filename)
        extension = Path(original_filename).suffix.lower()
        if extension not in {".csv", ".txt"}:
            await file.close()
            logger.warning("收件人上传文件类型不支持 filename=%s extension=%s", original_filename, extension)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="仅支持上传 .csv 或 .txt 文件")

        saved_path = UPLOAD_DIR / f"{uuid.uuid4().hex}{extension}"
        logger.info("收到收件人上传请求 filename=%s content_type=%s size_limit=%s name=%s", original_filename, getattr(file, 'content_type', ''), MAX_UPLOAD_SIZE_BYTES, name or '')
        total_lines, total_size = await save_upload_file(file, saved_path)
        if total_size <= 0 or total_lines <= 0:
            logger.warning("收件人上传内容为空 filename=%s total_size=%s total_lines=%s", original_filename, total_size, total_lines)
        logger.info("收件人文件已保存 filename=%s total_lines=%s total_size=%s path=%s", original_filename, total_lines, total_size, saved_path)
        return await create_recipient_list_import(
            background_tasks=background_tasks,
            db=db,
            redis=redis,
            name=name,
            source_filename=original_filename,
            saved_path=saved_path,
            total_lines=total_lines,
        )
    except HTTPException as exc:
        logger.warning("收件人上传失败 filename=%s detail=%s", getattr(file, 'filename', ''), exc.detail)
        if 'saved_path' in locals():
            saved_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        logger.exception("收件人上传处理异常 filename=%s saved_path=%s", getattr(file, 'filename', ''), locals().get('saved_path', ''))
        if 'saved_path' in locals():
            saved_path.unlink(missing_ok=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="收件人导入失败，请检查文件内容后重试") from exc


@router.post("/paste", response_model=UploadResponse, dependencies=[Depends(require_admin)])
async def paste_recipients(
    request: PasteRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    content = (request.content or "").strip()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请上传文件或输入邮箱内容")
    saved_path = UPLOAD_DIR / f"{uuid.uuid4().hex}.txt"
    try:
        logger.info("收到收件人粘贴导入请求 content_length=%s", len(content))
        total_lines, _ = await save_pasted_content(content, saved_path)
        logger.info("收件人粘贴内容已保存 total_lines=%s path=%s", total_lines, saved_path)
        return await create_recipient_list_import(
            background_tasks=background_tasks,
            db=db,
            redis=redis,
            name=request.name,
            source_filename="manual-paste.txt",
            saved_path=saved_path,
            total_lines=total_lines,
        )
    except HTTPException as exc:
        logger.warning("收件人粘贴导入失败 detail=%s", exc.detail)
        saved_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        logger.exception("收件人粘贴处理异常")
        saved_path.unlink(missing_ok=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"收件人导入失败：{exc}") from exc


@router.get("/lists", response_model=list[RecipientListOut], dependencies=[Depends(require_admin)])
async def list_recipient_pools(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    try:
        result = await db.execute(select(RecipientList).order_by(RecipientList.created_at.desc(), RecipientList.id.desc()))
        recipient_lists = list(result.scalars().all())
        if not recipient_lists:
            return []

        progress_by_list_id: dict[int, dict[str, str]] = {}
        async with redis.pipeline(transaction=False) as pipe:
            for item in recipient_lists:
                pipe.hgetall(f"recipient_progress:{item.id}")
            progress_values = await pipe.execute()
        for item, progress in zip(recipient_lists, progress_values, strict=False):
            if isinstance(progress, dict) and progress:
                progress_by_list_id[int(item.id)] = progress

        output: list[RecipientListOut] = []
        for item in recipient_lists:
            progress = progress_by_list_id.get(int(item.id), {})
            status_value = str(progress.get("status") or "ready").strip().lower()
            if status_value == "done":
                status_value = COMPLETED
            if status_value == COMPLETED:
                status_value = "ready"
            output.append(
                RecipientListOut(
                    id=int(item.id),
                    name=str(item.name or ""),
                    total_count=int(item.total_count or 0),
                    valid_count=int(item.valid_count or 0),
                    invalid_count=int(item.invalid_count or 0),
                    created_at=item.created_at,
                    status=status_value,
                    task_id=str(progress.get("task_id") or ""),
                )
            )
        return output
    except Exception as exc:
        logger.exception("获取收件人池列表失败")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="服务繁忙，请稍后重试") from exc


@router.delete("/lists/{list_id}", response_model=RecipientDeleteAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def delete_recipient_pool(
    list_id: int,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    _admin: dict = Depends(require_admin),
):
    recipient_list = (await db.execute(select(RecipientList).where(RecipientList.id == list_id))).scalar_one_or_none()
    if recipient_list is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="收件人池不存在")
    if await find_blocking_task_for_recipient_list(db, list_id) is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该收件人池正被任务使用，无法删除")
    active_key = f"{RECIPIENT_DELETE_ACTIVE_PREFIX}:{list_id}"
    existing_task_id = await redis.get(active_key)
    if existing_task_id:
        return RecipientDeleteAcceptedResponse(task_id=str(existing_task_id), list_id=list_id)
    total = int((await db.execute(select(func.count()).select_from(Recipient).where(Recipient.list_id == list_id))).scalar_one() or 0)
    task_id = uuid.uuid4().hex
    list_name = str(recipient_list.name or f"收件人池 {list_id}")
    await redis.set(active_key, task_id, ex=RECIPIENT_DELETE_PROGRESS_TTL_SECONDS)
    await _save_delete_progress(task_id, {
        "task_id": task_id, "list_id": list_id, "list_name": list_name, "status": "running",
        "total_recipients": total, "deleted_recipients": 0, "error": "",
    })
    _track_delete_task(_run_recipient_list_delete(task_id, list_id, list_name, total))
    logger.info("收件人池后台删除任务已创建 list_id=%s task_id=%s total=%s", list_id, task_id, total)
    return RecipientDeleteAcceptedResponse(task_id=task_id, list_id=list_id)


@router.get("/delete-progress/{task_id}", response_model=RecipientDeleteProgressResponse, dependencies=[Depends(require_admin)])
async def get_recipient_delete_progress(task_id: str, redis: Redis = Depends(get_redis)):
    normalized = str(task_id or "").strip()
    if not normalized:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="删除任务 ID 无效")
    progress = await redis.hgetall(_delete_progress_key(normalized))
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="删除任务不存在或已过期")
    return RecipientDeleteProgressResponse(
        task_id=normalized,
        list_id=int(progress.get("list_id") or 0),
        list_name=str(progress.get("list_name") or ""),
        status=str(progress.get("status") or "running"),
        total_recipients=int(progress.get("total_recipients") or 0),
        deleted_recipients=int(progress.get("deleted_recipients") or 0),
        error=str(progress.get("error") or ""),
    )


def parse_manual_emails(raw_emails: list[str]) -> tuple[list[str], int]:
    normalized: list[str] = []
    seen: set[str] = set()
    invalid_count = 0
    for raw_email in raw_emails:
        email, _ = extract_email_from_line(str(raw_email or ""))
        if not email:
            continue
        if not validate_recipient_email(email):
            invalid_count += 1
            continue
        if email in seen:
            continue
        seen.add(email)
        normalized.append(email)
    return normalized, invalid_count


RECIPIENT_INSERT_BATCH_SIZE = 500


async def get_recipient_list_or_404(db: AsyncSession, list_id: int) -> RecipientList:
    recipient_list = (await db.execute(select(RecipientList).where(RecipientList.id == list_id))).scalar_one_or_none()
    if recipient_list is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="分组不存在")
    return recipient_list


async def get_recipient_or_404(db: AsyncSession, recipient_id: int) -> Recipient:
    recipient = (await db.execute(select(Recipient).where(Recipient.id == int(recipient_id)))).scalar_one_or_none()
    if recipient is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="收件人不存在")
    return recipient


async def find_blocking_task_for_recipient_list(db: AsyncSession, list_id: int):
    return (
        await db.execute(
            select(Task.id, Task.name).where(
                Task.recipient_list_id == list_id,
                Task.deleted_at.is_(None),
                Task.status.in_(ACTIVE_TASK_STATUSES),
            ).order_by(Task.id.desc()).limit(1)
        )
    ).first()


@router.post("/lists/{list_id}/recipients", response_model=AddRecipientsResponse, dependencies=[Depends(require_admin)])
async def add_recipients_to_list(list_id: int, payload: AddRecipientsRequest, db: AsyncSession = Depends(get_db)):
    recipient_list = await get_recipient_list_or_404(db, list_id)
    emails, invalid_count = parse_manual_emails(payload.emails)
    if not emails and invalid_count > 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="未检测到有效邮箱，请确认每行包含 @ 符号")
    if not emails:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请提供至少一个有效邮箱")

    max_row_number = int(
        (
            await db.execute(
                select(func.coalesce(func.max(Recipient.row_number), 0)).where(Recipient.list_id == list_id)
            )
        ).scalar_one()
        or 0
    )
    now_value = datetime.utcnow()
    rows = []
    for offset, email in enumerate(emails, start=1):
        rows.append(
            {
                "list_id": list_id,
                "email": email,
                "row_number": max_row_number + offset,
                "name": "",
                "company": "",
                "phone": "",
                "tags": "",
                "custom_fields_json": "{}",
                "is_valid": True,
                "is_unsubscribed": False,
                "validation_error": "",
                "created_at": now_value,
            }
        )

    added_count = 0
    for offset in range(0, len(rows), RECIPIENT_INSERT_BATCH_SIZE):
        chunk = rows[offset : offset + RECIPIENT_INSERT_BATCH_SIZE]
        statement = (
            pg_insert(Recipient)
            .values(chunk)
            .on_conflict_do_nothing(constraint="uq_recipient_list_email")
        )
        result = await db.execute(statement)
        added_count += max(0, int(result.rowcount or 0))

    skipped_duplicate_count = max(0, len(emails) - added_count)
    if added_count > 0:
        recipient_list.total_count = int(recipient_list.total_count or 0) + added_count
        recipient_list.valid_count = int(recipient_list.valid_count or 0) + added_count
        recipient_list.updated_at = now_value
        await commit_session_with_retry(db)

    return AddRecipientsResponse(
        added_count=added_count,
        skipped_duplicate_count=skipped_duplicate_count,
        skipped_invalid_count=invalid_count,
    )


@router.get("/lists/{list_id}/recipients", response_model=RecipientPageResponse, dependencies=[Depends(require_admin)])
async def list_recipients(
    list_id: int,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    exists_result = await db.execute(select(RecipientList.id).where(RecipientList.id == list_id))
    if exists_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="收件人池不存在")
    total_result = await db.execute(select(func.count()).select_from(Recipient).where(Recipient.list_id == list_id))
    total = int(total_result.scalar_one())
    result = await db.execute(
        select(Recipient)
        .where(Recipient.list_id == list_id)
        .order_by(Recipient.row_number.asc(), Recipient.id.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return RecipientPageResponse(items=list(result.scalars().all()), total=total, page=page, page_size=page_size)


@router.delete("/{recipient_id}", response_model=DeleteRecipientResponse, dependencies=[Depends(require_admin)])
async def delete_recipient(
    recipient_id: int,
    db: AsyncSession = Depends(get_db),
):
    """删除单个收件人；历史 send_logs 通过 ON DELETE SET NULL 保留。"""
    recipient = await get_recipient_or_404(db, recipient_id)
    recipient_list = await get_recipient_list_or_404(db, int(recipient.list_id))
    blocking_task = await find_blocking_task_for_recipient_list(db, int(recipient.list_id))
    if blocking_task is not None:
        task_id, task_name = blocking_task
        logger.warning(
            "拒绝删除收件人：分组正被活跃任务占用 recipient_id=%s list_id=%s task_id=%s task_name=%s",
            recipient_id,
            recipient.list_id,
            task_id,
            task_name,
        )
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="该收件人正被任务使用，无法删除")

    email = str(recipient.email or "").strip()
    list_id = int(recipient.list_id)
    await db.execute(sql_delete(Recipient).where(Recipient.id == int(recipient_id)))
    recipient_list.total_count = max(0, int(recipient_list.total_count or 0) - 1)
    if recipient.is_valid:
        recipient_list.valid_count = max(0, int(recipient_list.valid_count or 0) - 1)
    else:
        recipient_list.invalid_count = max(0, int(recipient_list.invalid_count or 0) - 1)
    recipient_list.updated_at = datetime.utcnow()
    await commit_session_with_retry(db)
    logger.info(
        "已物理删除收件人 recipient_id=%s list_id=%s list_name=%s email=%s",
        recipient_id,
        list_id,
        recipient_list.name,
        email,
    )
    return DeleteRecipientResponse(success=True, message="收件人已删除")


@router.get("/import/progress/{task_id}", response_model=ProgressResponse, dependencies=[Depends(require_admin)])
async def get_import_progress_by_task(task_id: str, redis: Redis = Depends(get_redis)):
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="导入任务 ID 无效")
    progress = await redis.hgetall(f"import_progress:{normalized_task_id}")
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="导入任务不存在或已过期")
    list_id = int(progress.get("list_id") or 0)
    return build_progress_response(list_id, progress, task_id=normalized_task_id)


@router.get("/progress/{list_id}", response_model=ProgressResponse, dependencies=[Depends(require_admin)])
async def get_recipient_progress(
    list_id: int,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    progress_key = f"recipient_progress:{list_id}"
    progress = await redis.hgetall(progress_key)
    if progress:
        return build_progress_response(list_id, progress)
    result = await db.execute(select(RecipientList.total_count).where(RecipientList.id == list_id))
    total_count = result.scalar_one_or_none()
    if total_count is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="收件人池不存在")
    return ProgressResponse(
        task_id=str(progress.get("task_id") or "") if progress else "",
        list_id=list_id,
        list_name="",
        processed=int(total_count),
        total=int(total_count),
        status=COMPLETED,
        accepted=int(total_count),
        invalid=0,
        skipped_duplicates=0,
        errors=[],
        error="",
        percentage=100.0,
        phase="done",
    )
