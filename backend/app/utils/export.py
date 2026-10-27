"""PostgreSQL COPY 流式导出工具。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg

from app.core.config import settings

logger = logging.getLogger(__name__)

COPY_QUEUE_MAXSIZE = 16
EXPORT_COPY_COMMAND_TIMEOUT_SECONDS = 300.0
EXPORT_ASYNC_COPY_COMMAND_TIMEOUT_SECONDS = 7200.0
# 导出 DISTINCT 哈希聚合内存上限（仅当前事务/连接有效）
EXPORT_COPY_WORK_MEM = "256MB"

_export_pool: asyncpg.Pool | None = None
_export_pool_lock = asyncio.Lock()


class ExportCopyError(RuntimeError):
    """COPY 导出失败。"""


def format_export_error_message(exc: BaseException) -> str:
    """将导出异常转为面向用户的中文说明。"""
    if isinstance(exc, ExportCopyError):
        message = str(exc).strip()
        return message or "导出数据失败，请稍后重试"
    if isinstance(exc, asyncpg.PostgresError):
        detail = str(exc).strip()
        return f"数据库导出错误：{detail}" if detail else "数据库导出错误，请稍后重试"
    message = str(exc).strip()
    if message:
        return f"导出失败：{message}"
    return "导出失败，请稍后重试"


def describe_export_pool_state() -> dict[str, object]:
    return {
        "pool_initialized": _export_pool is not None,
        "pool_closed": bool(getattr(_export_pool, "_closed", False)) if _export_pool is not None else True,
    }


async def probe_export_copy() -> dict[str, object]:
    """执行最小 COPY 探测，帮助排查权限、连接池与 work_mem 问题。"""
    probe_sql = "SELECT 'export_copy_probe'::text"
    result: dict[str, object] = {
        "probe_sql": probe_sql,
        "pool_state": describe_export_pool_state(),
        "ok": False,
        "error": "",
    }
    queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=1)

    async def output_writer(data: bytes) -> None:
        if data:
            await queue.put(data)

    try:
        pool = await _get_export_pool()
        async with pool.acquire() as conn:
            await _copy_from_query_with_tuning(conn, probe_sql, output_writer)
        drain_count = 0
        while not queue.empty():
            item = await queue.get()
            if item is not None:
                drain_count += 1
        result["ok"] = True
        result["rows_emitted"] = drain_count
    except Exception as exc:
        result["error"] = str(exc) if str(exc) else exc.__class__.__name__
    return result


def _build_asyncpg_dsn() -> str:
    raw_url = str(settings.database_url or "").strip()
    if raw_url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + raw_url[len("postgresql+asyncpg://") :]
    return raw_url


async def _get_export_pool() -> asyncpg.Pool:
    global _export_pool
    if _export_pool is not None:
        return _export_pool
    async with _export_pool_lock:
        if _export_pool is None:
            dsn = _build_asyncpg_dsn()
            logger.warning("初始化 PostgreSQL COPY 导出专用连接池 dsn=%s", dsn)
            _export_pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=8,
                command_timeout=EXPORT_COPY_COMMAND_TIMEOUT_SECONDS,
            )
            logger.warning("PostgreSQL COPY 导出专用连接池已创建 min_size=1 max_size=8 command_timeout=%s", EXPORT_COPY_COMMAND_TIMEOUT_SECONDS)
    return _export_pool


async def close_export_pool() -> None:
    global _export_pool
    if _export_pool is None:
        return
    await _export_pool.close()
    _export_pool = None
    logger.info("PostgreSQL COPY 导出专用连接池已关闭")


def _normalize_copy_query(query: str) -> str:
    return str(query or "").strip().rstrip(";")


async def _copy_from_query_with_tuning(conn, normalized_query: str, output_writer) -> None:
    """在事务内提升 work_mem 后执行 COPY，加速 DISTINCT 哈希去重。"""
    logger.info("开始执行 PostgreSQL COPY 导出查询 sql=%s", normalized_query[:500])
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        logger.info("设置导出会话 work_mem=%s", EXPORT_COPY_WORK_MEM)
        await conn.execute(f"SET LOCAL work_mem = '{EXPORT_COPY_WORK_MEM}'")
        await conn.copy_from_query(
            normalized_query,
            output=output_writer,
            format="text",
        )
    logger.info("PostgreSQL COPY 导出查询执行完成 sql=%s", normalized_query[:500])


async def _fetch_rows_as_text(conn, normalized_query: str, output_writer) -> None:
    """COPY 不可用时的兜底方案：使用游标分批读取逐行写出。"""
    logger.warning("开始执行导出兜底查询（非 COPY） sql=%s", normalized_query[:500])
    row_count = 0
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        logger.info("兜底导出同样设置 work_mem=%s", EXPORT_COPY_WORK_MEM)
        await conn.execute(f"SET LOCAL work_mem = '{EXPORT_COPY_WORK_MEM}'")
        statement = await conn.prepare(normalized_query)
        cursor = statement.cursor()
        while True:
            rows = await cursor.fetch(2000)
            if not rows:
                break
            for row in rows:
                value = row[0] if row else None
                if value is None:
                    continue
                text_line = f"{str(value).strip()}\n".encode("utf-8")
                await output_writer(text_line)
                row_count += 1
    logger.warning("导出兜底查询完成 row_count=%s sql=%s", row_count, normalized_query[:500])


async def copy_query_to_file(query: str, destination: Path) -> int:
    """将 COPY 查询结果写入本地文件，返回字节数。"""
    normalized_query = _normalize_copy_query(query)
    if not normalized_query:
        raise ExportCopyError("导出 SQL 为空")
    destination.parent.mkdir(parents=True, exist_ok=True)
    logger.info("准备写入异步导出文件 path=%s", destination)
    total_bytes = 0
    conn = None
    try:
        conn = await asyncpg.connect(
            _build_asyncpg_dsn(),
            command_timeout=EXPORT_ASYNC_COPY_COMMAND_TIMEOUT_SECONDS,
        )
        with destination.open("wb") as file_handle:
            async def output_writer(data: bytes) -> None:
                nonlocal total_bytes
                if not data:
                    return
                file_handle.write(data)
                total_bytes += len(data)

            try:
                await _copy_from_query_with_tuning(conn, normalized_query, output_writer)
            except asyncpg.PostgresError as exc:
                logger.warning("PostgreSQL COPY 失败，尝试使用普通查询兜底 path=%s error=%s", destination, exc)
                await _fetch_rows_as_text(conn, normalized_query, output_writer)
    except asyncpg.PostgresError as exc:
        logger.exception("PostgreSQL 文件导出数据库错误 path=%s error=%s", destination, exc)
        if destination.exists():
            try:
                destination.unlink()
            except OSError:
                logger.warning("删除失败的导出文件失败 path=%s", destination, exc_info=True)
        raise ExportCopyError(format_export_error_message(exc)) from exc
    except Exception as exc:
        logger.exception("PostgreSQL 文件导出失败 path=%s error=%s", destination, exc)
        if destination.exists():
            try:
                destination.unlink()
            except OSError:
                logger.warning("删除失败的导出文件失败 path=%s", destination, exc_info=True)
        raise ExportCopyError(format_export_error_message(exc)) from exc
    finally:
        if conn is not None:
            await conn.close()
    logger.info("异步导出文件写入完成 path=%s bytes=%s", destination, total_bytes)
    return total_bytes


async def stream_copy_txt(query: str) -> AsyncIterator[bytes]:
    """通过 COPY (query) TO STDOUT (FORMAT text) 流式输出 UTF-8 文本（无 BOM）。"""
    normalized_query = _normalize_copy_query(query)
    if not normalized_query:
        raise ExportCopyError("导出 SQL 为空，无法生成文件")

    logger.info("开始流式 COPY 导出 sql=%s", normalized_query[:500])
    queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=COPY_QUEUE_MAXSIZE)
    copy_error: list[BaseException] = []

    async def output_writer(data: bytes) -> None:
        if data:
            await queue.put(data)

    async def run_copy() -> None:
        conn = None
        try:
            conn = await asyncpg.connect(
                _build_asyncpg_dsn(),
                command_timeout=EXPORT_COPY_COMMAND_TIMEOUT_SECONDS,
            )
            try:
                await _copy_from_query_with_tuning(conn, normalized_query, output_writer)
            except asyncpg.PostgresError as exc:
                logger.warning("PostgreSQL COPY 文本导出失败，将改用普通查询兜底 error=%s", exc)
                await _fetch_rows_as_text(conn, normalized_query, output_writer)
        except Exception as exc:
            logger.exception("PostgreSQL COPY 文本导出失败：%s", exc)
            copy_error.append(exc)
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    logger.warning("关闭流式导出数据库连接失败", exc_info=True)
            await queue.put(None)

    copy_task = asyncio.create_task(run_copy())
    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk
        if copy_error:
            raise ExportCopyError(format_export_error_message(copy_error[0])) from copy_error[0]
    except asyncio.CancelledError:
        logger.info("PostgreSQL COPY 导出流已取消")
        raise
    finally:
        if not copy_task.done():
            copy_task.cancel()
            try:
                await copy_task
            except asyncio.CancelledError:
                pass
