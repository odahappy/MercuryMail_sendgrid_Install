import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from secrets import choice
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import anyio
from sqlalchemy import func, select, text, update

from app.core.database import AsyncSessionLocal, UrlPoolSessionLocal, commit_session_with_retry
from app.core.redis import redis_client
from app.models.models import URLItem, URLPool, UrlImportTask

logger = logging.getLogger(__name__)

# URL 分配策略：循环轮询（非弹出式）
# - PostgreSQL url_items 持久保存全部链接，分配时不删除
# - Redis INCRBY 维护全局/任务级索引，offset % total 循环取链
# - 启用 jump 参数时，同一基础链接每次也会生成带随机参数的独立追踪 URL
# - 池长度为 0 时抛出「URL 池没有可用链接」，不发送空链接

URL_PREFETCH_BATCH_SIZE = 5000
URL_PREFETCH_LOW_WATERMARK = 1000
URL_PREFETCH_MAX_BATCH_SIZE = 5000
URL_IMPORT_DB_BATCH_SIZE = 5000
URL_IMPORT_PROGRESS_FLUSH_EVERY = 100
MAX_URL_IMPORT_ERROR_DETAILS = 20
URL_IMPORT_REDIS_TTL_SECONDS = 86400 * 7
URL_IMPORT_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
URL_POOL_NOT_FOUND_ERROR = "URL 池不存在"
URL_POOL_EMPTY_ERROR = "URL 池没有可用链接"
URL_POOL_OFFSET_ERROR = "URL 池索引越界，无法分配链接"

_task_url_pool_caches: dict[int, "TaskUrlPoolCache"] = {}
_running_url_import_tasks: dict[int, asyncio.Task] = {}


@dataclass
class NextURLResult:
    url: str
    pool_id: int
    item_id: int
    item_index: int


def normalize_url(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("URL 不能为空")
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", stripped):
        stripped = f"https://{stripped}"
    parsed = urlsplit(stripped)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("URL 仅支持 http 或 https")
    if not parsed.netloc:
        raise ValueError("URL 主机名不能为空")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc, parsed.path, parsed.query, parsed.fragment))


def split_jump_domains(value: str) -> list[str]:
    candidates = re.split(r"[\n,;]+", value or "")
    domains = []
    seen = set()
    for candidate in candidates:
        stripped = candidate.strip()
        if not stripped:
            continue
        normalized = normalize_url(stripped)
        if normalized not in seen:
            seen.add(normalized)
            domains.append(normalized)
    return domains


def build_random_value(length: int, chars: str) -> str:
    available_chars = chars or "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    safe_length = max(1, min(int(length), 128))
    return "".join(choice(available_chars) for _ in range(safe_length))


def normalize_random_param_name(value: str) -> str:
    name = (value or "uid").strip().strip("?&=") or "uid"
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name) or "uid"


def choose_jump_domain(pool: URLPool, fallback_url: str) -> str:
    domains = split_jump_domains(pool.jump_domain)
    if not domains:
        return normalize_url(fallback_url)
    counter = choice(range(len(domains)))
    return domains[counter]


def append_jump_params(original_url: str, jump_domain: str, random_param_name: str, random_value: str) -> str:
    parsed = urlsplit(normalize_url(original_url))
    jump_url = normalize_url(jump_domain)
    param_name = normalize_random_param_name(random_param_name)
    if param_name == "r":
        param_name = "uid"
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["r"] = jump_url
    query[param_name] = random_value
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def _counter_key(pool_id: int, task_id: int | None) -> str:
    """Redis 轮询计数器键：任务级与预览/全局级分离，避免互相干扰。"""
    if task_id is not None:
        return f"url_pool:{pool_id}:task:{task_id}:index"
    return f"url_pool:{pool_id}:index"


async def _reserve_pool_offsets(pool_id: int, total: int, task_id: int | None, count: int) -> list[int]:
    """通过 Redis INCRBY 原子递增索引，按 total 取模实现循环轮询（链接不会被弹出或删除）。"""
    if total <= 0:
        logger.warning("URL 池为空，无法分配链接 pool_id=%s task_id=%s", pool_id, task_id)
        return []
    normalized_count = max(1, min(int(count), URL_PREFETCH_MAX_BATCH_SIZE))
    counter_key = _counter_key(pool_id, task_id)
    try:
        pipe = redis_client.pipeline(transaction=True)
        pipe.incrby(counter_key, normalized_count)
        pipe.expire(counter_key, 60 * 60 * 24 * 365)
        end_counter, _expire_result = await pipe.execute()
        end_counter = int(end_counter)
        start_counter = end_counter - normalized_count + 1
        return [(start_counter + index - 1) % total for index in range(normalized_count)]
    except Exception as exc:
        logger.warning(
            "URL 池 Redis 分配失败，降级为数据库 next_index pool_id=%s task_id=%s count=%s error=%s",
            pool_id,
            task_id,
            normalized_count,
            exc,
        )
        async with UrlPoolSessionLocal() as db:
            pool_result = await db.execute(select(URLPool).where(URLPool.id == pool_id))
            pool = pool_result.scalar_one_or_none()
            if pool is None:
                raise ValueError(URL_POOL_NOT_FOUND_ERROR)
            current_index = int(pool.next_index or 0)
            offsets = [(current_index + index) % total for index in range(normalized_count)]
            pool.next_index = (current_index + normalized_count) % total
            await commit_session_with_retry(db)
            return offsets


def _build_next_url_result(pool: URLPool, item: URLItem) -> NextURLResult:
    source_url = (item.original_url or item.url or "").strip()
    final_url = normalize_url(source_url)
    if pool.jump_enabled:
        random_value = build_random_value(pool.random_length, pool.random_chars)
        jump_domain = choose_jump_domain(pool, final_url)
        final_url = append_jump_params(final_url, jump_domain, pool.random_prefix, random_value)
    return NextURLResult(url=final_url, pool_id=pool.id, item_id=item.id, item_index=item.item_index)


async def _count_active_url_items(pool_id: int) -> int:
    async with UrlPoolSessionLocal() as db:
        total = await db.scalar(
            select(func.count())
            .select_from(URLItem)
            .where(URLItem.pool_id == pool_id, URLItem.is_active.is_(True))
        )
    return max(0, int(total or 0))


async def _fetch_active_items_at_offsets(pool_id: int, offsets: list[int]) -> dict[int, URLItem]:
    normalized_offsets = sorted({max(0, int(offset)) for offset in offsets})
    if not normalized_offsets:
        return {}
    async with UrlPoolSessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    """
                    WITH ranked AS (
                        SELECT
                            ui.id,
                            ui.pool_id,
                            ui.url,
                            ui.original_url,
                            ui.item_index,
                            ui.is_active,
                            ROW_NUMBER() OVER (ORDER BY ui.item_index ASC, ui.id ASC) - 1 AS rn
                        FROM url_items ui
                        WHERE ui.pool_id = :pool_id AND ui.is_active IS TRUE
                    )
                    SELECT id, pool_id, url, original_url, item_index, is_active, rn
                    FROM ranked
                    WHERE rn = ANY(:offsets)
                    """
                ),
                {"pool_id": int(pool_id), "offsets": normalized_offsets},
            )
        ).mappings().all()
    items_by_offset: dict[int, URLItem] = {}
    for row in rows:
        item = URLItem(
            id=int(row["id"]),
            pool_id=int(row["pool_id"]),
            url=row["url"],
            original_url=row["original_url"],
            item_index=int(row["item_index"] or 0),
            is_active=bool(row["is_active"]),
        )
        items_by_offset[int(row["rn"])] = item
    return items_by_offset


async def _build_url_results_from_offsets(pool: URLPool, offsets: list[int]) -> list[NextURLResult]:
    items_by_offset = await _fetch_active_items_at_offsets(int(pool.id), offsets)
    results: list[NextURLResult] = []
    for offset in offsets:
        item = items_by_offset.get(int(offset))
        if item is None:
            raise ValueError(URL_POOL_OFFSET_ERROR)
        results.append(_build_next_url_result(pool, item))
    return results


async def _fetch_url_batch_with_meta(
    pool: URLPool,
    *,
    active_total: int,
    task_id: int | None,
    count: int,
) -> list[NextURLResult]:
    normalized_count = max(1, min(int(count), URL_PREFETCH_MAX_BATCH_SIZE))
    total = max(0, int(active_total or 0))
    if total <= 0:
        raise ValueError(URL_POOL_EMPTY_ERROR)
    offsets = await _reserve_pool_offsets(int(pool.id), total, task_id, normalized_count)
    return await _build_url_results_from_offsets(pool, offsets)


async def _fetch_url_batch(pool_id: int, task_id: int | None, count: int) -> list[NextURLResult]:
    """按循环轮询为 count 个收件人分配 URL；池非空时保证返回等长结果。"""
    normalized_count = max(1, min(int(count), URL_PREFETCH_MAX_BATCH_SIZE))
    async with UrlPoolSessionLocal() as db:
        pool_result = await db.execute(select(URLPool).where(URLPool.id == pool_id))
        pool = pool_result.scalar_one_or_none()
        if pool is None:
            raise ValueError(URL_POOL_NOT_FOUND_ERROR)
        total = await db.scalar(
            select(func.count())
            .select_from(URLItem)
            .where(URLItem.pool_id == pool_id, URLItem.is_active.is_(True))
        )
    return await _fetch_url_batch_with_meta(
        pool,
        active_total=max(0, int(total or 0)),
        task_id=task_id,
        count=normalized_count,
    )


async def _fetch_url_batch_chunked(pool_id: int, task_id: int | None, count: int) -> list[NextURLResult]:
    normalized_count = max(0, int(count))
    if normalized_count <= 0:
        return []
    results: list[NextURLResult] = []
    remaining = normalized_count
    while remaining > 0:
        chunk_size = min(remaining, URL_PREFETCH_MAX_BATCH_SIZE)
        results.extend(await _fetch_url_batch(pool_id, task_id, chunk_size))
        remaining -= chunk_size
    return results


class TaskUrlPoolCache:
    """任务级 URL 预取缓存：底层统一走 Redis 索引轮询 + PostgreSQL 按偏移取链，池耗尽后自动从头循环。"""

    def __init__(self, task_id: int, pool_id: int) -> None:
        self.task_id = task_id
        self.pool_id = pool_id
        self._buffer: asyncio.Queue[NextURLResult] = asyncio.Queue()
        self._refill_lock = asyncio.Lock()
        self._refill_task: asyncio.Task | None = None
        self._closed = False
        self._pool: URLPool | None = None
        self._active_total: int = 0

    async def _ensure_pool_meta(self) -> URLPool:
        if self._pool is not None:
            return self._pool
        async with UrlPoolSessionLocal() as db:
            pool_result = await db.execute(select(URLPool).where(URLPool.id == self.pool_id))
            pool = pool_result.scalar_one_or_none()
            if pool is None:
                raise ValueError(URL_POOL_NOT_FOUND_ERROR)
            self._pool = pool
            self._active_total = await _count_active_url_items(self.pool_id)
            if self._active_total <= 0:
                logger.warning(
                    "URL 池无可用链接 task_id=%s pool_id=%s",
                    self.task_id,
                    self.pool_id,
                )
                raise ValueError(URL_POOL_EMPTY_ERROR)
        return self._pool

    async def _refill_buffer(self, count: int) -> None:
        pool = await self._ensure_pool_meta()
        normalized_count = max(1, min(int(count), URL_PREFETCH_MAX_BATCH_SIZE))
        batch = await _fetch_url_batch_with_meta(
            pool,
            active_total=self._active_total,
            task_id=self.task_id,
            count=normalized_count,
        )
        if len(batch) < normalized_count:
            raise ValueError(URL_POOL_EMPTY_ERROR)
        for item in batch:
            await self._buffer.put(item)
        logger.debug(
            "任务 %s URL 池 %s 预取 %s 条链接（循环轮询）缓冲=%s",
            self.task_id,
            self.pool_id,
            len(batch),
            self._buffer.qsize(),
        )

    async def warm_up(self) -> None:
        await self._refill_buffer(URL_PREFETCH_BATCH_SIZE)

    async def get_next(self) -> NextURLResult:
        if self._closed:
            raise RuntimeError("URL 池缓存已关闭")
        if self._buffer.qsize() <= URL_PREFETCH_LOW_WATERMARK:
            self._schedule_background_refill()
        try:
            return self._buffer.get_nowait()
        except asyncio.QueueEmpty:
            pass
        if self._buffer.empty():
            async with self._refill_lock:
                if self._buffer.empty():
                    await self._refill_buffer(URL_PREFETCH_BATCH_SIZE)
        return await self._buffer.get()

    async def get_next_batch(self, count: int) -> list[NextURLResult]:
        """批量领取 URL：优先消费任务级内存预取缓冲，避免发送热路径每批都访问数据库。"""
        if self._closed:
            raise RuntimeError("URL 池缓存已关闭")
        normalized_count = max(0, int(count))
        if normalized_count <= 0:
            return []
        await self._ensure_pool_meta()
        results: list[NextURLResult] = []
        while len(results) < normalized_count:
            try:
                results.append(self._buffer.get_nowait())
                continue
            except asyncio.QueueEmpty:
                pass
            async with self._refill_lock:
                if self._buffer.empty():
                    refill_count = max(URL_PREFETCH_BATCH_SIZE, normalized_count - len(results))
                    await self._refill_buffer(refill_count)
        if self._buffer.qsize() <= URL_PREFETCH_LOW_WATERMARK:
            self._schedule_background_refill()
        return results

    def _schedule_background_refill(self) -> None:
        if self._closed:
            return
        if self._refill_task is not None and not self._refill_task.done():
            return
        self._refill_task = asyncio.create_task(
            self._background_refill(),
            name=f"url-pool-refill-{self.task_id}-{self.pool_id}",
        )

    async def _background_refill(self) -> None:
        try:
            async with self._refill_lock:
                if self._closed or self._buffer.qsize() > URL_PREFETCH_LOW_WATERMARK:
                    return
                await self._refill_buffer(URL_PREFETCH_BATCH_SIZE)
        except Exception as exc:
            logger.warning(
                "task %s url pool %s background refill failed error_type=%s error=%s",
                self.task_id,
                self.pool_id,
                type(exc).__name__,
                exc,
            )

    async def close(self) -> None:
        self._closed = True
        if self._refill_task is not None and not self._refill_task.done():
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass


async def start_task_url_pool_cache(task_id: int, pool_id: int) -> TaskUrlPoolCache:
    existing = _task_url_pool_caches.get(task_id)
    if existing is not None:
        if existing.pool_id == pool_id and not existing._closed:
            return existing
        await existing.close()
    cache = TaskUrlPoolCache(task_id, pool_id)
    _task_url_pool_caches[task_id] = cache
    await cache.warm_up()
    return cache


async def get_next_urls_batch(
    cache: TaskUrlPoolCache | None,
    pool_id: int,
    task_id: int | None,
    count: int,
) -> list[NextURLResult]:
    """一次为多个收件人分配 URL；池非空时保证返回 count 条，索引超出后自动从头循环。"""
    normalized_count = max(0, int(count))
    if normalized_count <= 0:
        return []
    if cache is not None and not cache._closed and cache.pool_id == pool_id:
        return await cache.get_next_batch(normalized_count)
    results = await _fetch_url_batch_chunked(pool_id, task_id, normalized_count)
    if len(results) < normalized_count:
        raise ValueError(URL_POOL_EMPTY_ERROR)
    return results


async def get_next_url_from_cache(cache: TaskUrlPoolCache | None, pool_id: int, task_id: int | None = None) -> NextURLResult:
    if cache is not None and not cache._closed and cache.pool_id == pool_id:
        return await cache.get_next()
    return await get_next_url(pool_id, task_id=task_id)


async def close_task_url_pool_cache(task_id: int) -> None:
    cache = _task_url_pool_caches.pop(task_id, None)
    if cache is not None:
        await cache.close()


async def next_pool_offset(pool: URLPool, pool_id: int, total: int, task_id: int | None = None) -> int:
    offsets = await _reserve_pool_offsets(pool_id, total, task_id, 1)
    return offsets[0] if offsets else 0


async def get_next_url(pool_id: int, task_id: int | None = None) -> NextURLResult:
    batch = await _fetch_url_batch(pool_id, task_id, 1)
    if not batch:
        raise ValueError(URL_POOL_EMPTY_ERROR)
    return batch[0]


def url_import_progress_key(task_id: int) -> str:
    return f"url_import_progress:{task_id}"


def count_nonempty_lines(file_path: Path) -> int:
    total = 0
    with file_path.open("rb") as source:
        for raw_line in source:
            if decode_url_import_line(raw_line):
                total += 1
    return total


def decode_url_import_line(raw_line: bytes) -> str:
    if raw_line.endswith(b"\r\n"):
        raw_line = raw_line[:-2]
    elif raw_line.endswith(b"\n") or raw_line.endswith(b"\r"):
        raw_line = raw_line[:-1]
    if raw_line.startswith(b"\xef\xbb\xbf"):
        raw_line = raw_line[3:]
    return raw_line.decode("utf-8", errors="ignore").strip()


def append_url_import_error(errors: list[str], line_number: int, value: str, detail: str) -> None:
    if len(errors) >= MAX_URL_IMPORT_ERROR_DETAILS:
        return
    preview = str(value or "").strip()
    if len(preview) > 120:
        preview = f"{preview[:117]}..."
    errors.append(f"第{line_number}行 URL 无效：{preview}（{detail}）")


def load_pool_urls_json_value(raw_value: str | None) -> list[str]:
    try:
        parsed = json.loads(raw_value or "[]")
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except (TypeError, json.JSONDecodeError):
        return []
    return []


async def _get_import_task_status(task_id: int) -> str | None:
    async with AsyncSessionLocal() as db:
        status_value = (
            await db.execute(select(UrlImportTask.status).where(UrlImportTask.id == task_id))
        ).scalar_one_or_none()
        return str(status_value or "").strip() or None


async def _sync_url_import_progress_redis(
    task_id: int,
    *,
    pool_id: int,
    status: str,
    processed_rows: int,
    total_rows: int,
    accepted_rows: int,
    error_message: str = "",
) -> None:
    mapping = {
        "pool_id": pool_id,
        "status": status,
        "processed": processed_rows,
        "total": total_rows,
        "accepted": accepted_rows,
        "error": error_message[:500],
    }
    try:
        await redis_client.hset(url_import_progress_key(task_id), mapping=mapping)
        await redis_client.expire(url_import_progress_key(task_id), URL_IMPORT_REDIS_TTL_SECONDS)
    except Exception as exc:
        logger.warning("URL 导入任务 %s Redis 进度同步失败：%s", task_id, exc)


async def _update_url_import_task_progress(
    task_id: int,
    *,
    pool_id: int,
    status: str | None = None,
    processed_rows: int | None = None,
    total_rows: int | None = None,
    accepted_rows: int,
    error_message: str | None = None,
) -> None:
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(UrlImportTask).where(UrlImportTask.id == task_id))).scalar_one_or_none()
        if task is None:
            return
        if status is not None:
            task.status = status
        if processed_rows is not None:
            task.processed_rows = int(processed_rows)
        if total_rows is not None:
            task.total_rows = int(total_rows)
        if error_message is not None:
            task.error_message = error_message[:500]
        task.updated_at = datetime.utcnow()
        await commit_session_with_retry(db)
        current_status = task.status or status or "running"
        current_processed = int(task.processed_rows or 0)
        current_total = int(task.total_rows or 0)
        current_error = task.error_message or ""
    await _sync_url_import_progress_redis(
        task_id,
        pool_id=pool_id,
        status=current_status,
        processed_rows=current_processed,
        total_rows=current_total,
        accepted_rows=accepted_rows,
        error_message=current_error,
    )


async def _is_url_import_cancelled(task_id: int) -> bool:
    status_value = await _get_import_task_status(task_id)
    return status_value == "cancelled"


async def _flush_url_import_batch(
    db,
    pool_id: int,
    batch_rows: list[URLItem],
    merged_urls: list[str],
    pool_urls_json: list[str],
) -> int:
    if not batch_rows:
        return 0
    rows = [
        {
            "pool_id": row.pool_id,
            "url": row.url,
            "original_url": row.original_url,
            "item_index": row.item_index,
            "is_active": row.is_active,
            "created_at": row.created_at or datetime.utcnow(),
            "updated_at": row.updated_at or datetime.utcnow(),
        }
        for row in batch_rows
    ]
    await db.execute(
        text(
            """
            INSERT INTO url_items (pool_id, url, original_url, item_index, is_active, created_at, updated_at)
            VALUES (:pool_id, :url, :original_url, :item_index, :is_active, :created_at, :updated_at)
            """
        ),
        rows,
    )
    for row in batch_rows:
        if row.url not in pool_urls_json:
            pool_urls_json.append(row.url)
            merged_urls.append(row.url)
    await commit_session_with_retry(db)
    return len(batch_rows)


async def run_url_import_task(task_id: int, pool_id: int, file_path: str, total_rows: int) -> None:
    path = Path(file_path)
    processed_rows = 0
    accepted_rows = 0
    invalid_rows = 0
    duplicate_rows = 0
    pending_items: list[URLItem] = []
    merged_urls: list[str] = []
    pool_urls_json: list[str] = []
    import_errors: list[str] = []
    next_item_index = 0
    try:
        file_size = path.stat().st_size if path.exists() else 0
        logger.info("开始 URL 导入 task_id=%s pool_id=%s file=%s file_size=%s total_rows=%s batch_size=%s", task_id, pool_id, path.name, file_size, total_rows, URL_IMPORT_DB_BATCH_SIZE)
        await _update_url_import_task_progress(
            task_id,
            pool_id=pool_id,
            status="running",
            processed_rows=0,
            total_rows=total_rows,
            accepted_rows=0,
            error_message="",
        )
        async with AsyncSessionLocal() as db:
            pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
            if pool is None:
                raise ValueError("URL 池不存在")
            pool_urls_json = load_pool_urls_json_value(pool.urls)
            merged_urls = list(pool_urls_json)
            existing_urls = set(pool_urls_json)
            existing_result = await db.execute(select(URLItem.url).where(URLItem.pool_id == pool_id))
            for (existing_url,) in existing_result.all():
                normalized_existing = str(existing_url or "").strip()
                if normalized_existing:
                    existing_urls.add(normalized_existing)
            next_item_index = int(
                (await db.execute(select(func.count()).select_from(URLItem).where(URLItem.pool_id == pool_id))).scalar_one()
                or 0
            )

        with path.open("rb") as source:
            while True:
                if await _is_url_import_cancelled(task_id):
                    await _update_url_import_task_progress(
                        task_id,
                        pool_id=pool_id,
                        status="cancelled",
                        processed_rows=processed_rows,
                        total_rows=total_rows,
                        accepted_rows=accepted_rows,
                        error_message="导入任务已取消",
                    )
                    return
                raw_lines = await anyio.to_thread.run_sync(
                    lambda: [line for _, line in zip(range(URL_IMPORT_DB_BATCH_SIZE), source)]
                )
                if not raw_lines:
                    break
                async with AsyncSessionLocal() as db:
                    for raw_line in raw_lines:
                        if await _is_url_import_cancelled(task_id):
                            await _update_url_import_task_progress(
                                task_id,
                                pool_id=pool_id,
                                status="cancelled",
                                processed_rows=processed_rows,
                                total_rows=total_rows,
                                accepted_rows=accepted_rows,
                                error_message="导入任务已取消",
                            )
                            return
                        stripped = decode_url_import_line(raw_line)
                        if not stripped:
                            continue
                        processed_rows += 1
                        try:
                            normalized_url = normalize_url(stripped)
                        except ValueError as exc:
                            invalid_rows += 1
                            append_url_import_error(import_errors, processed_rows, stripped, str(exc))
                            if invalid_rows <= MAX_URL_IMPORT_ERROR_DETAILS:
                                logger.warning("URL 导入无效行 task_id=%s pool_id=%s line=%s value=%s error=%s", task_id, pool_id, processed_rows, stripped[:160], exc)
                            if processed_rows % URL_IMPORT_PROGRESS_FLUSH_EVERY == 0:
                                await _update_url_import_task_progress(
                                    task_id,
                                    pool_id=pool_id,
                                    processed_rows=processed_rows,
                                    total_rows=total_rows,
                                    accepted_rows=accepted_rows,
                                    error_message="；".join(import_errors),
                                )
                            continue
                        if normalized_url in existing_urls:
                            duplicate_rows += 1
                        else:
                            existing_urls.add(normalized_url)
                            next_item_index += 1
                            pending_items.append(
                                URLItem(
                                    pool_id=pool_id,
                                    url=normalized_url,
                                    original_url=normalized_url,
                                    item_index=next_item_index,
                                    is_active=True,
                                )
                            )
                        if processed_rows % URL_IMPORT_PROGRESS_FLUSH_EVERY == 0:
                            await _update_url_import_task_progress(
                                task_id,
                                pool_id=pool_id,
                                processed_rows=processed_rows,
                                total_rows=total_rows,
                                accepted_rows=accepted_rows,
                                error_message="；".join(import_errors),
                            )
                    if pending_items:
                        accepted_rows += await _flush_url_import_batch(
                            db,
                            pool_id,
                            pending_items,
                            merged_urls,
                            pool_urls_json,
                        )
                        pending_items = []

        async with AsyncSessionLocal() as db:
            if pending_items:
                accepted_rows += await _flush_url_import_batch(db, pool_id, pending_items, merged_urls, pool_urls_json)
            pool = (await db.execute(select(URLPool).where(URLPool.id == pool_id))).scalar_one_or_none()
            if pool is not None:
                pool.urls = json.dumps(merged_urls, ensure_ascii=False)
                pool.updated_at = datetime.utcnow()
                await commit_session_with_retry(db)

        summary = f"成功导入 {accepted_rows} 条"
        if duplicate_rows > 0:
            summary += f"，跳过重复 {duplicate_rows} 条"
        if invalid_rows > 0:
            summary += f"，无效行 {invalid_rows} 条"
        await _update_url_import_task_progress(
            task_id,
            pool_id=pool_id,
            status="completed",
            processed_rows=max(processed_rows, total_rows),
            total_rows=max(total_rows, processed_rows),
            accepted_rows=accepted_rows,
            error_message=summary,
        )
        logger.warning(
            "url import task %s completed pool_id=%s accepted=%s processed=%s invalid=%s duplicate=%s",
            task_id,
            pool_id,
            accepted_rows,
            processed_rows,
            invalid_rows,
            duplicate_rows,
        )
    except asyncio.CancelledError:
        await _update_url_import_task_progress(
            task_id,
            pool_id=pool_id,
            status="cancelled",
            processed_rows=processed_rows,
            total_rows=total_rows,
            accepted_rows=accepted_rows,
            error_message="导入任务已取消",
        )
        raise
    except Exception as exc:
        logger.exception(
            "URL 导入任务失败 task_id=%s pool_id=%s file=%s total_rows=%s processed=%s accepted=%s invalid=%s duplicate=%s error=%s",
            task_id,
            pool_id,
            path.name,
            total_rows,
            processed_rows,
            accepted_rows,
            invalid_rows,
            duplicate_rows,
            exc,
        )
        await _update_url_import_task_progress(
            task_id,
            pool_id=pool_id,
            status="failed",
            processed_rows=processed_rows,
            total_rows=total_rows,
            accepted_rows=accepted_rows,
            error_message=f"导入失败：{exc}"[:500],
        )
    finally:
        _running_url_import_tasks.pop(task_id, None)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def launch_url_import_task(task_id: int, pool_id: int, file_path: str, total_rows: int) -> asyncio.Task:
    current = _running_url_import_tasks.get(task_id)
    if current is not None and not current.done():
        return current
    worker = asyncio.create_task(
        run_url_import_task(task_id, pool_id, file_path, total_rows),
        name=f"url-import-task-{task_id}",
    )
    _running_url_import_tasks[task_id] = worker
    return worker


async def cancel_url_import_task(task_id: int) -> bool:
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(UrlImportTask).where(UrlImportTask.id == task_id))).scalar_one_or_none()
        if task is None:
            return False
        if task.status in URL_IMPORT_TERMINAL_STATUSES:
            return task.status == "cancelled"
        task.status = "cancelled"
        task.error_message = "导入任务已取消"
        task.updated_at = datetime.utcnow()
        await commit_session_with_retry(db)
        pool_id = int(task.pool_id)
        processed_rows = int(task.processed_rows or 0)
        total_rows = int(task.total_rows or 0)
    await _sync_url_import_progress_redis(
        task_id,
        pool_id=pool_id,
        status="cancelled",
        processed_rows=processed_rows,
        total_rows=total_rows,
        accepted_rows=0,
        error_message="导入任务已取消",
    )
    running = _running_url_import_tasks.get(task_id)
    if running is not None and not running.done():
        running.cancel()
        try:
            await running
        except asyncio.CancelledError:
            pass
    return True


async def get_url_import_progress_snapshot(task_id: int) -> dict[str, str]:
    try:
        progress = await redis_client.hgetall(url_import_progress_key(task_id))
        if progress:
            return progress
    except Exception as exc:
        logger.warning("URL 导入任务 %s Redis 进度读取失败：%s", task_id, exc)
    async with AsyncSessionLocal() as db:
        task = (await db.execute(select(UrlImportTask).where(UrlImportTask.id == task_id))).scalar_one_or_none()
        if task is None:
            return {}
        return {
            "pool_id": str(task.pool_id),
            "status": task.status or "pending",
            "processed": str(task.processed_rows or 0),
            "total": str(task.total_rows or 0),
            "accepted": "0",
            "error": task.error_message or "",
        }
