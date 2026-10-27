import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import anyio
from sqlalchemy import delete as sql_delete, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from app.constants.import_status import COMPLETED, FAILED, PROCESSING
from app.core.config import settings
from app.core.database import AsyncSessionLocal, import_engine
from app.core.pg_connection import get_asyncpg_connection
from app.core.redis import redis_client
from app.models.models import Recipient, RecipientList

logger = logging.getLogger(__name__)

COPY_MIGRATION_BATCH_SIZE = 5000
MIGRATION_PROGRESS_EVERY_N_BATCHES = 2
PREPROCESS_BATCH_SIZE = 50_000
PREPROCESS_STAGING_WRITE_BUFFER = 10_000
PROGRESS_UPDATE_MIN_INTERVAL_SECONDS = 0.2
MAX_IMPORT_ERROR_DETAILS = 100
IMPORT_PROGRESS_TTL_ACTIVE = 86400
IMPORT_PROGRESS_TTL_DONE = 86400

HEADER_LABELS = {
    "email",
    "e-mail",
    "mail",
    "邮箱",
    "邮件",
    "address",
    "email address",
    "电子邮件",
    "邮箱地址",
    "name",
    "姓名",
    "用户名",
}

@dataclass
class ImportProgressContext:
    task_id: str
    list_id: int
    list_name: str = ""

    @property
    def list_progress_key(self) -> str:
        return f"recipient_progress:{self.list_id}"

    @property
    def task_progress_key(self) -> str:
        return f"import_progress:{self.task_id}"


def import_progress_ttl(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized in {COMPLETED, FAILED, "done"}:
        return IMPORT_PROGRESS_TTL_DONE
    return IMPORT_PROGRESS_TTL_ACTIVE


def build_import_progress_mapping(
    ctx: ImportProgressContext,
    *,
    processed: int,
    total: int,
    status: str,
    accepted: int,
    invalid: int,
    skipped_duplicates: int = 0,
    errors: list[str] | None = None,
    error: str = "",
    phase: str = "processing",
) -> dict[str, str | int]:
    return {
        "task_id": ctx.task_id,
        "list_id": ctx.list_id,
        "list_name": ctx.list_name,
        "processed": processed,
        "current": processed,
        "total": total,
        "status": status,
        "accepted": accepted,
        "invalid": invalid,
        "skipped_duplicates": skipped_duplicates,
        "error": error,
        "phase": phase,
        "errors_json": json.dumps(errors or [], ensure_ascii=False),
    }


def _write_import_progress_sync(client, ctx: ImportProgressContext, mapping: dict[str, str | int], status: str) -> None:
    ttl = import_progress_ttl(str(mapping.get("status") or status))
    client.hset(ctx.list_progress_key, mapping=mapping)
    client.expire(ctx.list_progress_key, ttl)
    client.hset(ctx.task_progress_key, mapping=mapping)
    client.expire(ctx.task_progress_key, ttl)


async def _write_import_progress_async(ctx: ImportProgressContext, mapping: dict[str, str | int], status: str) -> None:
    ttl = import_progress_ttl(str(mapping.get("status") or status))
    await redis_client.hset(ctx.list_progress_key, mapping=mapping)
    await redis_client.expire(ctx.list_progress_key, ttl)
    await redis_client.hset(ctx.task_progress_key, mapping=mapping)
    await redis_client.expire(ctx.task_progress_key, ttl)


@dataclass
class PreprocessResult:
    total_lines: int = 0
    staged_valid: int = 0
    skipped_duplicates: int = 0
    skipped_invalid: int = 0
    skipped_header: int = 0
    errors: list[str] = field(default_factory=list)


TEMP_RECIPIENT_COPY_COLUMNS = ("email", "row_number")


def temp_table_name(list_id: int) -> str:
    return f"_temp_recipients_{int(list_id)}"


def build_create_temp_table_sql(list_id: int) -> text:
    table_name = temp_table_name(list_id)
    return text(
        f"""
        CREATE TEMP TABLE {table_name} (
            email VARCHAR(255) NOT NULL,
            row_number INTEGER NOT NULL
        ) ON COMMIT PRESERVE ROWS
        """
    )


def build_drop_temp_table_sql(list_id: int) -> text:
    return text(f"DROP TABLE IF EXISTS {temp_table_name(list_id)}")


def build_migrate_batch_sql(list_id: int) -> text:
    """生成临时表迁移 SQL；email 保留导入值，normalized_email 统一按 LOWER(BTRIM(email)) 计算。"""
    table_name = temp_table_name(list_id)
    return text(
        f"""
        WITH batch AS (
            SELECT email, row_number
            FROM {table_name}
            WHERE row_number > :cursor_row
            ORDER BY row_number ASC
            LIMIT :batch_size
        ),
        deduped AS (
            SELECT DISTINCT ON (email)
                email,
                row_number
            FROM batch
            ORDER BY email, row_number ASC
        ),
        inserted AS (
            INSERT INTO recipients (
                list_id,
                email,
                normalized_email,
                row_number,
                name,
                company,
                phone,
                tags,
                custom_fields_json,
                is_valid,
                is_unsubscribed,
                validation_error,
                created_at
            )
            SELECT
                :list_id,
                email,
                LOWER(BTRIM(email)),
                row_number,
                '',
                '',
                '',
                '',
                '{{}}',
                TRUE,
                FALSE,
                '',
                NOW()
            FROM deduped
            ORDER BY row_number ASC
            ON CONFLICT (list_id, email) DO NOTHING
            RETURNING id
        )
        SELECT
            (SELECT COUNT(*)::int FROM batch) AS scanned_rows,
            (SELECT COUNT(*)::int FROM deduped) AS deduped_rows,
            (SELECT COUNT(*)::int FROM inserted) AS inserted_rows,
            COALESCE((SELECT MAX(row_number) FROM batch), :cursor_row) AS last_row_number
        """
    )


def _split_line_cells(line: str) -> list[str]:
    stripped = line.strip().strip("\ufeff")
    if not stripped:
        return []
    return [part.strip().strip('"').strip("'") for part in re.split(r"[,;\t|]", stripped) if part is not None]


def is_probable_header_label(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return False
    return normalized in HEADER_LABELS


def looks_like_header_line(line: str) -> bool:
    cells = _split_line_cells(line)
    if not cells:
        return False
    if any(is_probable_header_label(cell) for cell in cells):
        return True
    return all("@" not in cell for cell in cells) and len(cells) > 1


def validate_recipient_email(email: str) -> bool:
    candidate = str(email or "").strip().lower()
    return bool(candidate and len(candidate) <= 254 and not any(char.isspace() for char in candidate))


def extract_email_from_line(line: str) -> tuple[str, str]:
    """从一行文本中提取收件人字段，导入阶段不做邮箱格式排查。"""
    cells = _split_line_cells(line)
    if not cells:
        return "", ""
    for cell in cells:
        candidate = cell.strip().lower()
        if "@" in candidate and validate_recipient_email(candidate):
            return candidate, cells[0]
    return cells[0].strip().lower(), cells[0]


def _append_import_error(result: PreprocessResult, line_number: int, detail: str) -> None:
    if len(result.errors) >= MAX_IMPORT_ERROR_DETAILS:
        return
    result.errors.append(f"第{line_number}行{detail}")


def format_recipient_import_error(exc: BaseException) -> str:
    """将数据库和导入异常转换为可展示的中文消息，避免泄露 SQL 与连接信息。"""
    error_type = type(exc).__name__
    error_text = str(exc or "")
    normalized = f"{error_type}: {error_text}".lower()
    if "duplicatecolumnerror" in normalized and "normalized_email" in normalized:
        return "导入失败：字段 normalized_email 重复，请联系管理员"
    if "duplicatecolumnerror" in normalized:
        return "导入失败：导入字段重复，请联系管理员"
    if "undefinedcolumnerror" in normalized and "normalized_email" in normalized:
        return "导入失败：数据库缺少 normalized_email 字段，请联系管理员"
    return f"导入失败：{error_text or error_type}"[:500]


_sync_redis_client = None
_sync_redis_lock = threading.Lock()


def _get_sync_redis_client():
    global _sync_redis_client
    if _sync_redis_client is not None:
        return _sync_redis_client
    with _sync_redis_lock:
        if _sync_redis_client is not None:
            return _sync_redis_client
        import redis
        from app.core.redis import _build_redis_url_with_password

        _sync_redis_client = redis.Redis.from_url(
            _build_redis_url_with_password(),
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        return _sync_redis_client


def update_recipient_progress_sync(
    ctx: ImportProgressContext,
    *,
    processed: int,
    total: int,
    status: str,
    accepted: int,
    invalid: int,
    skipped_duplicates: int = 0,
    errors: list[str] | None = None,
    error: str = "",
    phase: str = "processing",
) -> None:
    try:
        mapping = build_import_progress_mapping(
            ctx,
            processed=processed,
            total=total,
            status=status,
            accepted=accepted,
            invalid=invalid,
            skipped_duplicates=skipped_duplicates,
            errors=errors,
            error=error,
            phase=phase,
        )
        client = _get_sync_redis_client()
        _write_import_progress_sync(client, ctx, mapping, status)
    except Exception:
        logger.warning("同步更新收件人导入进度失败 task_id=%s list_id=%s", ctx.task_id, ctx.list_id, exc_info=True)


class PreprocessProgressTracker:
    def __init__(self, ctx: ImportProgressContext, total_hint: int) -> None:
        self.ctx = ctx
        self.total_hint = max(0, int(total_hint or 0))
        self.lock = threading.Lock()
        self.scanned = 0
        self.staged = 0
        self.invalid = 0
        self.skipped_duplicates = 0
        self.errors: list[str] = []
        self.last_update_at = 0.0

    def add_batch(self, batch: "LineBatchResult") -> None:
        with self.lock:
            self.scanned += batch.non_empty_lines
            self.staged += len(batch.valid_rows)
            self.invalid += batch.skipped_invalid
            self.skipped_duplicates += batch.skipped_duplicates
            for item in batch.errors:
                if len(self.errors) >= MAX_IMPORT_ERROR_DETAILS:
                    break
                self.errors.append(item)
            now = time.monotonic()
            if now - self.last_update_at < PROGRESS_UPDATE_MIN_INTERVAL_SECONDS:
                return
            self.last_update_at = now
            total = max(self.total_hint, self.scanned)
            update_recipient_progress_sync(
                self.ctx,
                processed=self.scanned,
                total=total,
                status=PROCESSING,
                accepted=self.staged,
                invalid=self.invalid,
                skipped_duplicates=self.skipped_duplicates,
                errors=self.errors,
                phase="parsing",
            )


@dataclass
class LineBatchResult:
    batch_index: int
    start_line: int
    valid_rows: list[tuple[str, int]] = field(default_factory=list)
    skipped_invalid: int = 0
    skipped_duplicates: int = 0
    skipped_header: int = 0
    non_empty_lines: int = 0
    errors: list[str] = field(default_factory=list)


def _decode_line_bytes(raw_line: bytes) -> str:
    if raw_line.endswith(b"\r\n"):
        raw_line = raw_line[:-2]
    elif raw_line.endswith(b"\n"):
        raw_line = raw_line[:-1]
    elif raw_line.endswith(b"\r"):
        raw_line = raw_line[:-1]
    if not raw_line:
        return ""
    if raw_line.startswith(b"\xef\xbb\xbf"):
        raw_line = raw_line[3:]
    try:
        return raw_line.decode("utf-8")
    except UnicodeDecodeError:
        return raw_line.decode("latin-1", errors="ignore")


def _iter_line_batches(source_path: Path, batch_size: int = PREPROCESS_BATCH_SIZE):
    batch: list[tuple[int, str]] = []
    line_number = 0
    with source_path.open("rb") as source:
        for raw_line in source:
            line_number += 1
            decoded = _decode_line_bytes(raw_line)
            if not decoded.strip():
                continue
            batch.append((line_number, decoded))
            if len(batch) >= batch_size:
                logger.info("收件人预处理分片 ready batch_size=%s last_line=%s", len(batch), line_number)
                yield batch
                batch = []
    if batch:
        logger.info("收件人预处理最后分片 ready batch_size=%s last_line=%s", len(batch), line_number)
        yield batch


def _process_line_batch(batch_index: int, lines: list[tuple[int, str]], *, check_header: bool) -> LineBatchResult:
    result = LineBatchResult(batch_index=batch_index, start_line=lines[0][0] if lines else 0)
    header_checked = not check_header

    for line_number, raw_line in lines:
        result.non_empty_lines += 1

        if not header_checked:
            header_checked = True

        email, _ = extract_email_from_line(raw_line)
        if not email:
            continue

        result.valid_rows.append((email, line_number))

    return result


def _merge_batch_results(batch_results: list[LineBatchResult]) -> tuple[list[tuple[str, int]], PreprocessResult]:
    merged = PreprocessResult()
    ordered_batches = sorted(batch_results, key=lambda item: item.batch_index)
    final_rows: list[tuple[str, int]] = []

    for batch in ordered_batches:
        merged.total_lines += batch.non_empty_lines
        merged.skipped_invalid += batch.skipped_invalid
        merged.skipped_header += batch.skipped_header
        merged.skipped_duplicates += batch.skipped_duplicates
        merged.errors.extend(batch.errors[: max(0, MAX_IMPORT_ERROR_DETAILS - len(merged.errors))])

        for email, line_number in sorted(batch.valid_rows, key=lambda item: item[1]):
            final_rows.append((email, line_number))
            merged.staged_valid += 1

    final_rows.sort(key=lambda item: item[1])
    return final_rows, merged


def _write_staging_rows(staging_path: Path, rows: list[tuple[str, int]]) -> None:
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    buffer: list[str] = []
    with staging_path.open("w", encoding="utf-8", newline="", buffering=1024 * 1024) as staging:
        for email, line_number in rows:
            buffer.append(f"{email}\t{line_number}\n")
            if len(buffer) >= PREPROCESS_STAGING_WRITE_BUFFER:
                staging.write("".join(buffer))
                buffer.clear()
        if buffer:
            staging.write("".join(buffer))


def preprocess_recipient_file(
    source_path: Path,
    staging_path: Path,
    ctx: ImportProgressContext | None = None,
    total_lines_hint: int = 0,
    worker_count: int | None = None,
) -> PreprocessResult:
    workers = max(1, int(worker_count or settings.import_preprocess_workers or 4))
    logger.info("开始收件人预处理 source=%s staging=%s workers=%s total_hint=%s", source_path.name, staging_path.name, workers, total_lines_hint)
    tracker = PreprocessProgressTracker(ctx, total_lines_hint) if ctx is not None else None
    batch_results: list[LineBatchResult] = []
    batch_index = 0
    has_batches = False

    if workers == 1:
        for batch in _iter_line_batches(source_path):
            has_batches = True
            result = _process_line_batch(batch_index, batch, check_header=batch_index == 0)
            batch_results.append(result)
            if tracker is not None:
                tracker.add_batch(result)
            batch_index += 1
    else:
        max_inflight = max(workers * 2, workers)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="recipient-import") as executor:
            inflight = {}
            for batch in _iter_line_batches(source_path):
                has_batches = True
                future = executor.submit(_process_line_batch, batch_index, batch, check_header=batch_index == 0)
                inflight[future] = batch_index
                batch_index += 1
                if len(inflight) >= max_inflight:
                    for completed in as_completed(inflight):
                        result = completed.result()
                        batch_results.append(result)
                        if tracker is not None:
                            tracker.add_batch(result)
                        inflight.pop(completed, None)
                        break
            for completed in as_completed(inflight):
                result = completed.result()
                batch_results.append(result)
                if tracker is not None:
                    tracker.add_batch(result)

    if not has_batches:
        logger.info("收件人预处理未读取到有效行 source=%s", source_path.name)
        return PreprocessResult()

    final_rows, merged = _merge_batch_results(batch_results)
    if final_rows:
        logger.info("收件人预处理完成 final_rows=%s invalid=%s duplicates=%s", len(final_rows), merged.skipped_invalid, merged.skipped_duplicates)
        _write_staging_rows(staging_path, final_rows)
    elif staging_path.exists():
        logger.info("收件人预处理无可写入数据，清理临时文件 staging=%s", staging_path.name)
        staging_path.unlink(missing_ok=True)

    if tracker is not None and ctx is not None:
        total = max(total_lines_hint, merged.total_lines)
        update_recipient_progress_sync(
            ctx,
            processed=merged.total_lines,
            total=total,
            status=PROCESSING,
            accepted=merged.staged_valid,
            invalid=merged.skipped_invalid,
            skipped_duplicates=merged.skipped_duplicates,
            errors=merged.errors,
            phase="writing",
        )
    return merged


async def update_recipient_progress(
    ctx: ImportProgressContext,
    *,
    processed: int,
    total: int,
    status: str,
    accepted: int,
    invalid: int,
    skipped_duplicates: int = 0,
    errors: list[str] | None = None,
    error: str = "",
    phase: str = "processing",
) -> None:
    mapping = build_import_progress_mapping(
        ctx,
        processed=processed,
        total=total,
        status=status,
        accepted=accepted,
        invalid=invalid,
        skipped_duplicates=skipped_duplicates,
        errors=errors,
        error=error,
        phase=phase,
    )
    await _write_import_progress_async(ctx, mapping, status)


async def normalize_recipient_row_numbers(db: AsyncSession, list_id: int) -> int:
    """修复历史或异常导入产生的非正 row_number，保证发送游标不会跳过收件人。"""
    normalized_list_id = int(list_id or 0)
    if normalized_list_id <= 0:
        return 0
    result = await db.execute(
        text(
            """
            WITH current_max AS (
                SELECT COALESCE(MAX(row_number), 0) AS max_row
                FROM recipients
                WHERE list_id = :list_id AND row_number > 0
            ), broken AS (
                SELECT
                    id,
                    (SELECT max_row FROM current_max) + ROW_NUMBER() OVER (ORDER BY id ASC) AS fixed_row_number
                FROM recipients
                WHERE list_id = :list_id AND COALESCE(row_number, 0) <= 0
            )
            UPDATE recipients AS r
            SET row_number = broken.fixed_row_number
            FROM broken
            WHERE r.id = broken.id
            """
        ),
        {"list_id": normalized_list_id},
    )
    fixed_count = max(0, int(result.rowcount or 0))
    if fixed_count > 0:
        logger.warning(
            "已修复收件人池 row_number 异常 list_id=%s fixed_count=%s",
            normalized_list_id,
            fixed_count,
        )
    return fixed_count


async def _update_recipient_list_counts_on_connection(
    conn: AsyncConnection,
    list_id: int,
    accepted: int,
    invalid: int,
) -> None:
    await conn.execute(
        text(
            """
            UPDATE recipient_lists
            SET total_count = :accepted,
                valid_count = :accepted,
                invalid_count = :invalid,
                updated_at = NOW()
            WHERE id = :list_id
            """
        ),
        {"list_id": list_id, "accepted": accepted, "invalid": invalid},
    )


async def _update_recipient_list_counts(
    db: AsyncSession,
    list_id: int,
    accepted: int,
    invalid: int,
) -> None:
    result = await db.execute(select(RecipientList).where(RecipientList.id == list_id))
    recipient_list = result.scalar_one_or_none()
    if recipient_list is None:
        return
    recipient_list.total_count = accepted
    recipient_list.valid_count = accepted
    recipient_list.invalid_count = invalid
    recipient_list.updated_at = datetime.utcnow()
    await db.commit()


async def _cleanup_failed_copy_import_on_connection(
    conn: AsyncConnection,
    list_id: int,
    *,
    invalid: int,
) -> None:
    await conn.execute(build_drop_temp_table_sql(list_id))
    await conn.execute(sql_delete(Recipient).where(Recipient.list_id == list_id))
    await _update_recipient_list_counts_on_connection(conn, list_id, 0, invalid)
    await conn.commit()


async def _copy_staging_file_to_temp_table(conn: AsyncConnection, list_id: int, staging_path: Path) -> None:
    await conn.execute(build_drop_temp_table_sql(list_id))
    await conn.execute(build_create_temp_table_sql(list_id))
    pg_connection = await get_asyncpg_connection(conn)
    logger.info("开始 COPY 导入临时表 list_id=%s staging=%s", list_id, staging_path.name)
    with staging_path.open("rb") as staging_source:
        await pg_connection.copy_to_table(
            temp_table_name(list_id),
            source=staging_source,
            columns=TEMP_RECIPIENT_COPY_COLUMNS,
        )
    logger.info("COPY 导入临时表完成 list_id=%s", list_id)


async def _migrate_temp_table_in_batches(
    conn: AsyncConnection,
    list_id: int,
    *,
    staged_valid: int,
    total_lines: int,
    invalid: int,
    skipped_duplicates: int,
    errors: list[str],
    ctx: ImportProgressContext,
) -> tuple[int, int]:
    cursor_row = 0
    migrated_rows = 0
    accepted = 0
    db_duplicate_skips = 0
    migrate_batch_sql = build_migrate_batch_sql(list_id)
    batch_number = 0
    while True:
        result = await conn.execute(
            migrate_batch_sql,
            {
                "list_id": list_id,
                "cursor_row": cursor_row,
                "batch_size": COPY_MIGRATION_BATCH_SIZE,
            },
        )
        scanned_rows, deduped_rows, inserted_rows, last_row_number = result.one()
        scanned_rows = int(scanned_rows or 0)
        deduped_rows = int(deduped_rows or 0)
        inserted_rows = int(inserted_rows or 0)
        last_row_number = int(last_row_number or cursor_row)
        db_duplicate_skips += max(0, deduped_rows - inserted_rows)
        await conn.commit()
        if scanned_rows <= 0:
            break
        accepted += inserted_rows
        cursor_row = last_row_number
        migrated_rows += scanned_rows
        batch_number += 1
        if batch_number % MIGRATION_PROGRESS_EVERY_N_BATCHES == 0:
            logger.info("收件人批量写入进度 list_id=%s batch=%s accepted=%s scanned=%s", list_id, batch_number, accepted, migrated_rows)
            await update_recipient_progress(
                ctx,
                processed=total_lines,
                total=total_lines,
                status=PROCESSING,
                accepted=accepted,
                invalid=invalid,
                skipped_duplicates=skipped_duplicates + db_duplicate_skips,
                errors=errors,
                phase="writing",
            )
    return accepted, db_duplicate_skips


async def _run_copy_import(
    list_id: int,
    staging_path: Path,
    *,
    preprocess_result: PreprocessResult,
    ctx: ImportProgressContext,
) -> tuple[int, int]:
    async with import_engine.connect() as conn:
        try:
            await _copy_staging_file_to_temp_table(conn, list_id, staging_path)
            await conn.commit()

            accepted, db_duplicate_skips = await _migrate_temp_table_in_batches(
                conn,
                list_id,
                staged_valid=preprocess_result.staged_valid,
                total_lines=preprocess_result.total_lines,
                invalid=preprocess_result.skipped_invalid,
                skipped_duplicates=preprocess_result.skipped_duplicates,
                errors=preprocess_result.errors,
                ctx=ctx,
            )
            await conn.execute(build_drop_temp_table_sql(list_id))
            await _update_recipient_list_counts_on_connection(
                conn,
                list_id,
                accepted,
                preprocess_result.skipped_invalid,
            )
            await conn.commit()
            return accepted, db_duplicate_skips
        except Exception:
            await conn.rollback()
            await _cleanup_failed_copy_import_on_connection(
                conn,
                list_id,
                invalid=preprocess_result.skipped_invalid,
            )
            raise


async def import_recipient_file(list_id: int, file_path: str, total_lines: int, task_id: str, list_name: str = "") -> None:
    path = Path(file_path)
    ctx = ImportProgressContext(task_id=task_id, list_id=list_id, list_name=list_name or "")
    staging_path = path.with_suffix(f"{path.suffix}.staging.tsv")
    preprocess_result = PreprocessResult()
    try:
        file_size = path.stat().st_size if path.exists() else 0
        logger.info(
            "开始导入收件人 task_id=%s list_id=%s 文件=%s 文件大小=%s 预估行数=%s workers=%s",
            task_id,
            list_id,
            path.name,
            file_size,
            total_lines,
            settings.import_preprocess_workers,
        )
        await update_recipient_progress(
            ctx,
            processed=0,
            total=total_lines,
            status=PROCESSING,
            phase="parsing",
            accepted=0,
            invalid=0,
            skipped_duplicates=0,
            errors=[],
        )
        preprocess_result = await anyio.to_thread.run_sync(
            preprocess_recipient_file,
            path,
            staging_path,
            ctx,
            total_lines,
            settings.import_preprocess_workers,
        )
        effective_total = preprocess_result.total_lines or total_lines
        total_skipped_duplicates = preprocess_result.skipped_duplicates

        if preprocess_result.skipped_invalid > 0:
            logger.warning(
                "导入预处理发现无效行 list_id=%s 无效=%s 明细条数=%s",
                list_id,
                preprocess_result.skipped_invalid,
                len(preprocess_result.errors),
            )

        if preprocess_result.staged_valid == 0:
            async with AsyncSessionLocal() as db:
                await _update_recipient_list_counts(
                    db,
                    list_id,
                    0,
                    preprocess_result.skipped_invalid,
                )
            await update_recipient_progress(
                ctx,
                processed=effective_total,
                total=effective_total,
                status=COMPLETED,
                accepted=0,
                invalid=preprocess_result.skipped_invalid,
                skipped_duplicates=total_skipped_duplicates,
                errors=preprocess_result.errors,
                phase="done",
            )
            logger.info(
                "收件人导入完成 task_id=%s list_id=%s 总行数=%s 成功=0 重复跳过=%s 无效跳过=%s 标题跳过=%s",
                task_id,
                list_id,
                effective_total,
                total_skipped_duplicates,
                preprocess_result.skipped_invalid,
                preprocess_result.skipped_header,
            )
            return

        try:
            accepted, db_duplicate_skips = await _run_copy_import(
                list_id,
                staging_path,
                preprocess_result=preprocess_result,
                ctx=ctx,
            )
        except Exception as exc:
            logger.exception(
                "收件人 COPY 写入失败 task_id=%s list_id=%s file=%s total_lines=%s staged_valid=%s invalid=%s duplicates=%s error=%s",
                task_id,
                list_id,
                path.name,
                effective_total,
                preprocess_result.staged_valid,
                preprocess_result.skipped_invalid,
                total_skipped_duplicates,
                exc,
            )
            await update_recipient_progress(
                ctx,
                processed=effective_total,
                total=effective_total,
                status=FAILED,
                accepted=0,
                invalid=preprocess_result.skipped_invalid,
                skipped_duplicates=total_skipped_duplicates,
                errors=preprocess_result.errors,
                error=format_recipient_import_error(exc),
                phase="failed",
            )
            raise

        total_skipped_duplicates += db_duplicate_skips
        logger.info(
            "收件人导入完成 task_id=%s list_id=%s 总行数=%s 成功=%s 重复跳过=%s 无效跳过=%s 标题跳过=%s",
            task_id,
            list_id,
            effective_total,
            accepted,
            total_skipped_duplicates,
            preprocess_result.skipped_invalid,
            preprocess_result.skipped_header,
        )
        await update_recipient_progress(
            ctx,
            processed=effective_total,
            total=effective_total,
            status=COMPLETED,
            accepted=accepted,
            invalid=preprocess_result.skipped_invalid,
            skipped_duplicates=total_skipped_duplicates,
            errors=preprocess_result.errors,
            phase="done",
        )
    except Exception as exc:
        logger.exception(
            "收件人导入失败 task_id=%s list_id=%s file=%s total_lines=%s staged_valid=%s invalid=%s duplicates=%s error=%s",
            task_id,
            list_id,
            path.name,
            total_lines,
            preprocess_result.staged_valid,
            preprocess_result.skipped_invalid,
            preprocess_result.skipped_duplicates,
            exc,
        )
        progress = await redis_client.hgetall(ctx.task_progress_key)
        if progress.get("status") != FAILED:
            errors_json = progress.get("errors_json") or "[]"
            try:
                stored_errors = json.loads(errors_json)
            except json.JSONDecodeError:
                stored_errors = preprocess_result.errors
            await update_recipient_progress(
                ctx,
                processed=int(progress.get("processed", 0) or 0),
                total=int(progress.get("total", total_lines) or total_lines),
                status=FAILED,
                accepted=int(progress.get("accepted", 0) or 0),
                invalid=int(progress.get("invalid", 0) or preprocess_result.skipped_invalid),
                skipped_duplicates=int(progress.get("skipped_duplicates", 0) or preprocess_result.skipped_duplicates),
                errors=stored_errors if isinstance(stored_errors, list) else preprocess_result.errors,
                error=f"导入失败：{exc}"[:500],
                phase="failed",
            )
    finally:
        for cleanup_path in (path, staging_path):
            try:
                cleanup_path.unlink(missing_ok=True)
            except OSError:
                pass
