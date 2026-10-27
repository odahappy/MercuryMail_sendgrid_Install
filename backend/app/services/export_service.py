"""TXT 流式导出（PostgreSQL COPY）。

数据边界：
- 任务详情导出：仅查 send_log 热表，依据 SDK 写入的 status（sent/failed），不使用 final_status。
- 数据中心有效收件人（热/归档）：按所选任务 + final_status 投递结果筛选，排除退订与硬弹。
- 数据中心全局有效：收件人池未退订且未被硬弹。
- 按分组导出：仅读 recipients 表当前分组内实际存在记录（硬删除，无 deleted_at 字段），
  与 GET /api/recipients/lists/{list_id}/recipients 相同数据源；按 row_number 升序还原导入行序。

去重约定：任务/有效收件人导出在 SQL 层通过 DISTINCT 去重；按分组导出保留池内全部行，
与 recipients 表一一对应，按 row_number 升序输出。
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import AsyncIterator
from datetime import datetime

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select, text

from app.core.config import settings
from app.core.database import AsyncSessionLocal, ExportSessionLocal
from app.models.models import RecipientList, Task
from app.utils.task_status import resolve_task_status_label
from app.services.async_export_service import AsyncExportBusyError, _count_processing_exports, start_async_export
from app.utils.export import ExportCopyError, format_export_error_message, stream_copy_txt

logger = logging.getLogger(__name__)

EXPORT_TIMEOUT_SECONDS = 7200.0
VALID_RECIPIENT_SOURCES = frozenset({"all"})
VALID_DELIVERY_EXPORT_TYPES = frozenset({"success", "soft", "all"})
EXPORT_TASK_WINDOWS = frozenset({"hot", "archive"})
MAX_EXPORT_TASK_IDS = 500


class ExportTaskNotFoundError(Exception):
    def __init__(self, task_id: int):
        self.task_id = int(task_id)
        super().__init__(f"任务 {task_id} 不存在")


class ExportScopeError(ValueError):
    """导出范围无效。"""


def _normalize_export_task_id(task_id: int) -> int:
    normalized = int(task_id or 0)
    if normalized <= 0:
        raise ExportScopeError("任务 ID 无效")
    return normalized


def _normalize_valid_source(source: str | None) -> str:
    normalized = str(source or "").strip().lower()
    if normalized not in VALID_RECIPIENT_SOURCES:
        raise ExportScopeError("source 参数必须为 all")
    return normalized


def _normalize_delivery_export_type(export_type: str | None) -> str:
    normalized = str(export_type or "").strip().lower()
    if normalized not in VALID_DELIVERY_EXPORT_TYPES:
        raise ExportScopeError("type 参数必须为 success、soft 或 all")
    return normalized


def _normalize_export_task_ids(task_ids: list[int]) -> list[int]:
    if not task_ids:
        raise ExportScopeError("请至少选择一个任务")
    normalized: list[int] = []
    seen: set[int] = set()
    for raw_id in task_ids:
        task_id = int(raw_id or 0)
        if task_id <= 0 or task_id in seen:
            continue
        seen.add(task_id)
        normalized.append(task_id)
    if not normalized:
        raise ExportScopeError("任务 ID 无效")
    if len(normalized) > MAX_EXPORT_TASK_IDS:
        raise ExportScopeError(f"最多选择 {MAX_EXPORT_TASK_IDS} 个任务")
    return normalized


def _normalize_export_window(window: str | None) -> str:
    normalized = str(window or "").strip().lower()
    if normalized not in EXPORT_TASK_WINDOWS:
        raise ExportScopeError("window 参数必须为 hot 或 archive")
    return normalized


def export_timestamp_filename(prefix: str) -> str:
    safe_prefix = re.sub(r"[^\w\-]+", "_", str(prefix or "export").strip()) or "export"
    return f"{safe_prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"


def sanitize_group_filename(name: str) -> str:
    safe = re.sub(r"[^\w\-\u4e00-\u9fff]+", "_", str(name or "").strip())
    safe = safe.strip("_")
    return (safe[:80] if safe else "分组")


def _hot_retention_days() -> int:
    return max(1, int(settings.hot_retention_days or 3))


def _archive_retention_days() -> int:
    return max(_hot_retention_days() + 1, int(settings.archive_retention_days or 30))


def _hard_bounce_exists_sql(*, email_expr: str, recipient_id_expr: str) -> str:
    return f"""
      EXISTS (
        SELECT 1 FROM send_logs AS hb
        WHERE COALESCE(hb.log_type, 'regular') = 'regular'
          AND (
            ({recipient_id_expr} IS NOT NULL AND hb.recipient_id = {recipient_id_expr})
            OR (
              hb.email IS NOT NULL AND BTRIM(hb.email) <> ''
              AND LOWER(BTRIM(hb.email)) = LOWER(BTRIM({email_expr}))
            )
          )
          AND (LOWER(BTRIM(COALESCE(hb.bounce_type, ''))) = 'hard' OR LOWER(BTRIM(COALESCE(hb.final_status, ''))) IN ('bounced', 'bounce', 'blocked', 'dropped', 'spamreported', 'spamreport', 'unsubscribed'))
      )
      OR EXISTS (
        SELECT 1 FROM send_log_archive AS hba
        WHERE COALESCE(hba.log_type, 'regular') = 'regular'
          AND (
            ({recipient_id_expr} IS NOT NULL AND hba.recipient_id = {recipient_id_expr})
            OR (
              hba.email IS NOT NULL AND BTRIM(hba.email) <> ''
              AND LOWER(BTRIM(hba.email)) = LOWER(BTRIM({email_expr}))
            )
          )
          AND (LOWER(BTRIM(COALESCE(hba.bounce_type, ''))) = 'hard' OR LOWER(BTRIM(COALESCE(hba.final_status, ''))) IN ('bounced', 'bounce', 'blocked', 'dropped', 'spamreported', 'spamreport', 'unsubscribed'))
      )
    """


def _unsubscribed_exists_sql(email_expr: str) -> str:
    return f"""
      EXISTS (
        SELECT 1 FROM unsubscribes AS u
        WHERE COALESCE(NULLIF(BTRIM(u.normalized_email), ''), LOWER(BTRIM(u.email))) = LOWER(BTRIM({email_expr}))
      )
    """


def _recipient_validity_sql(*, email_expr: str, recipient_id_expr: str, is_unsubscribed_expr: str) -> str:
    return f"""
      {email_expr} IS NOT NULL
      AND BTRIM({email_expr}) <> ''
      AND COALESCE({is_unsubscribed_expr}, FALSE) = FALSE
      AND NOT ({_unsubscribed_exists_sql(email_expr)})
      AND NOT ({_hard_bounce_exists_sql(email_expr=email_expr, recipient_id_expr=recipient_id_expr)})
    """


def build_valid_recipients_sql(source: str) -> str:
    _normalize_valid_source(source)
    return """
SELECT export_rows.email
FROM (
  SELECT DISTINCT LOWER(BTRIM(candidate.email)) AS email
  FROM (
    SELECT sl.email
    FROM send_logs AS sl
    WHERE COALESCE(sl.log_type, 'regular') = 'regular'
    UNION ALL
    SELECT sla.email
    FROM send_log_archive AS sla
    WHERE COALESCE(sla.log_type, 'regular') = 'regular'
  ) AS candidate
  WHERE candidate.email IS NOT NULL
    AND BTRIM(candidate.email) <> ''
    AND NOT EXISTS (
      SELECT 1 FROM unsubscribes AS u
      WHERE COALESCE(NULLIF(BTRIM(u.normalized_email), ''), LOWER(BTRIM(u.email))) = LOWER(BTRIM(candidate.email))
    )
    AND NOT EXISTS (
      SELECT 1 FROM send_logs AS hb
      WHERE COALESCE(hb.log_type, 'regular') = 'regular'
        AND hb.email IS NOT NULL
        AND BTRIM(hb.email) <> ''
        AND LOWER(BTRIM(hb.email)) = LOWER(BTRIM(candidate.email))
        AND (LOWER(BTRIM(COALESCE(hb.bounce_type, ''))) = 'hard' OR LOWER(BTRIM(COALESCE(hb.final_status, ''))) IN ('bounced', 'bounce', 'blocked', 'dropped', 'spamreported', 'spamreport', 'spam_report', 'unsubscribed'))
    )
    AND NOT EXISTS (
      SELECT 1 FROM send_log_archive AS hba
      WHERE COALESCE(hba.log_type, 'regular') = 'regular'
        AND hba.email IS NOT NULL
        AND BTRIM(hba.email) <> ''
        AND LOWER(BTRIM(hba.email)) = LOWER(BTRIM(candidate.email))
        AND (LOWER(BTRIM(COALESCE(hba.bounce_type, ''))) = 'hard' OR LOWER(BTRIM(COALESCE(hba.final_status, ''))) IN ('bounced', 'bounce', 'blocked', 'dropped', 'spamreported', 'spamreport', 'spam_report', 'unsubscribed'))
    )
) AS export_rows
ORDER BY export_rows.email ASC
"""


def _normalized_final_status_sql(alias: str = "sl") -> str:
    return f"LOWER(BTRIM(COALESCE({alias}.final_status, '')))"


def _delivery_type_filter_sql(export_type: str) -> str:
    normalized = _normalize_delivery_export_type(export_type)
    final_status = _normalized_final_status_sql("sl")
    status = "LOWER(BTRIM(COALESCE(sl.status, '')))"
    bounce_type = "LOWER(BTRIM(COALESCE(sl.bounce_type, '')))"
    if normalized == "success":
        return f"{final_status} = 'delivered'"
    if normalized == "soft":
        return f"""(
          {bounce_type} = 'soft'
          OR {final_status} IN ('processed', 'deferred')
          OR (
            {final_status} <> ''
            AND {final_status} NOT IN ('delivered', 'bounced', 'blocked', 'dropped', 'spamreported', 'unsubscribed')
            AND {bounce_type} <> 'hard'
          )
        )"""
    return f"""(
          {final_status} IN ('processed', 'deferred', 'delivered')
          OR (
            {status} IN ('sent', 'success')
            AND ({final_status} = '' OR {final_status} IN ('processed', 'deferred', 'delivered'))
            AND {bounce_type} <> 'hard'
          )
        )"""


def build_task_delivery_export_sql(*, log_table: str, task_ids: list[int], export_type: str) -> str:
    normalized_ids = _normalize_export_task_ids(task_ids)
    normalized_table = str(log_table or "").strip()
    if normalized_table not in {"send_logs", "send_log_archive"}:
        raise ExportScopeError("导出日志表无效")
    ids_sql = ", ".join(str(item) for item in normalized_ids)
    type_filter = _delivery_type_filter_sql(export_type)
    email_expr = "COALESCE(r.email, sl.email)"
    recipient_id_expr = "COALESCE(r.id, sl.recipient_id)"
    validity = _recipient_validity_sql(
        email_expr=email_expr,
        recipient_id_expr=recipient_id_expr,
        is_unsubscribed_expr="COALESCE(r.is_unsubscribed, FALSE)",
    )
    negative_final_status_guard = f"""
    NOT EXISTS (
      SELECT 1 FROM (
        SELECT neg_hot.task_id, neg_hot.email, neg_hot.final_status, neg_hot.log_type
        FROM send_logs AS neg_hot
        WHERE neg_hot.task_id IN ({ids_sql})
        UNION ALL
        SELECT neg_archive.task_id, neg_archive.email, neg_archive.final_status, neg_archive.log_type
        FROM send_log_archive AS neg_archive
        WHERE neg_archive.task_id IN ({ids_sql})
      ) AS neg
      WHERE COALESCE(neg.log_type, 'regular') = 'regular'
        AND neg.email IS NOT NULL
        AND BTRIM(neg.email) <> ''
        AND LOWER(BTRIM(neg.email)) = LOWER(BTRIM({email_expr}))
        AND LOWER(BTRIM(COALESCE(neg.final_status, ''))) IN ('bounced', 'bounce', 'blocked', 'dropped', 'spamreported', 'spamreport', 'spam_report', 'unsubscribed')
    )
    """
    return f"""
SELECT export_rows.email
FROM (
  SELECT DISTINCT LOWER(BTRIM(COALESCE(r.email, sl.email))) AS email
  FROM {normalized_table} AS sl
  LEFT JOIN recipients AS r ON r.id = sl.recipient_id
  WHERE sl.task_id IN ({ids_sql})
    AND COALESCE(sl.log_type, 'regular') = 'regular'
    AND ({type_filter})
    AND {email_expr} IS NOT NULL
    AND BTRIM({email_expr}) <> ''
    AND {validity}
    AND {negative_final_status_guard}
) AS export_rows
ORDER BY export_rows.email ASC
"""


def build_group_export_sql(group_id: int) -> str:
    normalized_group_id = int(group_id or 0)
    if normalized_group_id <= 0:
        raise ExportScopeError("分组 ID 无效")
    # Recipient 无软删除字段；删除分组/单条收件人时物理删除 recipients 记录。
    # WHERE 条件与 list_recipients 一致：仅按 list_id 过滤当前池内实际存在的收件人。
    # 导出原始分组数据时按 row_number 保留同邮箱首条，避免历史异常重复行造成导出错乱。
    return f"""
SELECT export_rows.email
FROM (
  SELECT DISTINCT ON (LOWER(BTRIM(r.email)))
    BTRIM(r.email) AS email,
    r.row_number AS sort_row_number,
    r.id AS sort_id
  FROM recipients AS r
  INNER JOIN recipient_lists AS rl ON rl.id = r.list_id
  WHERE r.list_id = {normalized_group_id}
    AND r.email IS NOT NULL
    AND BTRIM(r.email) <> ''
  ORDER BY LOWER(BTRIM(r.email)) ASC, r.row_number ASC, r.id ASC
) AS export_rows
ORDER BY export_rows.sort_row_number ASC, export_rows.sort_id ASC
"""


async def list_export_tasks(window: str) -> list[dict[str, object]]:
    normalized_window = _normalize_export_window(window)
    hot_days = _hot_retention_days()
    archive_days = _archive_retention_days()

    if normalized_window == "hot":
        query_sql = text(
            f"""
            SELECT DISTINCT
              t.id,
              t.name,
              t.status,
              COALESCE(
                (
                  SELECT MIN(log_time)
                  FROM (
                    SELECT sl.created_at AS log_time
                    FROM send_logs AS sl
                    WHERE sl.task_id = t.id
                    UNION ALL
                    SELECT sla.created_at AS log_time
                    FROM send_log_archive AS sla
                    WHERE sla.task_id = t.id
                  ) AS combined_logs
                ),
                t.scheduled_at
              ) AS started_at,
              COALESCE(NULLIF(BTRIM(t.timezone), ''), 'Asia/Shanghai') AS timezone
            FROM tasks AS t
            WHERE EXISTS (
              SELECT 1 FROM send_logs AS sl
              WHERE sl.task_id = t.id
                AND sl.created_at > NOW() - INTERVAL '{hot_days} days'
                AND COALESCE(sl.log_type, 'regular') = 'regular'
                AND sl.email IS NOT NULL
                AND BTRIM(sl.email) <> ''
            )
            ORDER BY started_at DESC NULLS LAST, t.id DESC
            """
        )
    else:
        query_sql = text(
            f"""
            SELECT DISTINCT
              t.id,
              t.name,
              t.status,
              COALESCE(
                (
                  SELECT MIN(log_time)
                  FROM (
                    SELECT sl.created_at AS log_time
                    FROM send_logs AS sl
                    WHERE sl.task_id = t.id
                    UNION ALL
                    SELECT sla.created_at AS log_time
                    FROM send_log_archive AS sla
                    WHERE sla.task_id = t.id
                  ) AS combined_logs
                ),
                t.scheduled_at
              ) AS started_at,
              COALESCE(NULLIF(BTRIM(t.timezone), ''), 'Asia/Shanghai') AS timezone
            FROM tasks AS t
            WHERE EXISTS (
              SELECT 1 FROM send_log_archive AS sl
              WHERE sl.task_id = t.id
                AND sl.created_at > NOW() - INTERVAL '{archive_days} days'
                AND sl.created_at <= NOW() - INTERVAL '{hot_days} days'
                AND COALESCE(sl.log_type, 'regular') = 'regular'
                AND sl.email IS NOT NULL
                AND BTRIM(sl.email) <> ''
            )
            ORDER BY started_at DESC NULLS LAST, t.id DESC
            """
        )

    async with ExportSessionLocal() as db:
        rows = (await db.execute(query_sql)).mappings().all()

    return [
        {
            "id": int(row["id"]),
            "name": str(row["name"] or ""),
            "status": str(row["status"] or ""),
            "status_label": resolve_task_status_label(row["status"]),
            "started_at": row["started_at"],
            "timezone": str(row["timezone"] or "Asia/Shanghai"),
        }
        for row in rows
    ]


def build_task_success_sql(task_id: int) -> str:
    normalized_task_id = _normalize_export_task_id(task_id)
    return f"""
SELECT export_rows.email
FROM (
  SELECT DISTINCT LOWER(BTRIM(COALESCE(r.email, sl.email))) AS email
  FROM send_logs AS sl
  LEFT JOIN recipients AS r ON r.id = sl.recipient_id
  WHERE sl.task_id = {normalized_task_id}
    AND COALESCE(sl.log_type, 'regular') = 'regular'
    AND sl.status IN ('sent', 'success')
    AND COALESCE(r.email, sl.email) IS NOT NULL
    AND BTRIM(COALESCE(r.email, sl.email)) <> ''
) AS export_rows
ORDER BY export_rows.email ASC
"""


def build_task_failed_sql(task_id: int) -> str:
    normalized_task_id = _normalize_export_task_id(task_id)
    return f"""
SELECT export_rows.email
FROM (
  SELECT DISTINCT LOWER(BTRIM(COALESCE(r.email, sl.email))) AS email
  FROM send_logs AS sl
  LEFT JOIN recipients AS r ON r.id = sl.recipient_id
  WHERE sl.task_id = {normalized_task_id}
    AND COALESCE(sl.log_type, 'regular') = 'regular'
    AND sl.status IN ('failed', 'bounced', 'blocked', 'dropped')
    AND COALESCE(r.email, sl.email) IS NOT NULL
    AND BTRIM(COALESCE(r.email, sl.email)) <> ''
) AS export_rows
ORDER BY export_rows.email ASC
"""


def build_task_unset_sql(task_id: int, recipient_list_id: int) -> str:
    normalized_task_id = _normalize_export_task_id(task_id)
    normalized_list_id = int(recipient_list_id or 0)
    if normalized_list_id <= 0:
        raise ExportScopeError("任务未绑定收件人池")
    return f"""
SELECT export_rows.email
FROM (
  SELECT DISTINCT LOWER(BTRIM(r.email)) AS email
  FROM recipients AS r
  WHERE r.list_id = {normalized_list_id}
    AND r.email IS NOT NULL
    AND BTRIM(r.email) <> ''
    AND NOT EXISTS (
      SELECT 1
      FROM send_logs AS sl
      WHERE sl.task_id = {normalized_task_id}
        AND COALESCE(sl.log_type, 'regular') = 'regular'
        AND (
          (sl.recipient_id IS NOT NULL AND sl.recipient_id = r.id)
          OR (
            sl.email IS NOT NULL
            AND BTRIM(sl.email) <> ''
            AND LOWER(BTRIM(sl.email)) = LOWER(BTRIM(r.email))
          )
        )
    )
) AS export_rows
ORDER BY export_rows.email ASC
"""


def create_export_streaming_response(
    request: Request,
    byte_stream: AsyncIterator[bytes],
    *,
    media_type: str,
    filename: str,
    extra_headers: dict[str, str] | None = None,
) -> StreamingResponse:
    async def guarded() -> AsyncIterator[bytes]:
        started = time.monotonic()
        try:
            async for chunk in byte_stream:
                if time.monotonic() - started > EXPORT_TIMEOUT_SECONDS:
                    logger.warning("导出流超时已中止 filename=%s", filename)
                    return
                if await request.is_disconnected():
                    logger.info("客户端断开连接，停止导出 filename=%s", filename)
                    return
                yield chunk
        except ExportCopyError as exc:
            logger.error("导出流 COPY 失败 filename=%s：%s", filename, exc)
            raise
        except asyncio.CancelledError:
            logger.info("导出流任务已取消 filename=%s", filename)
            return

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    if extra_headers:
        headers.update(extra_headers)
    return StreamingResponse(guarded(), media_type=media_type, headers=headers)


async def _load_task_for_export(db, task_id: int) -> Task:
    normalized_task_id = _normalize_export_task_id(task_id)
    task = (await db.execute(select(Task).where(Task.id == normalized_task_id))).scalar_one_or_none()
    if task is None:
        raise ExportTaskNotFoundError(normalized_task_id)
    return task


def _should_skip_precise_count(sql: str, filename: str) -> bool:
    normalized_sql = str(sql or "").strip().lower()
    normalized_filename = str(filename or "").strip().lower()
    if not normalized_sql:
        return True
    # 仅对大导出路径应用降压策略，避免 COUNT(*) 对数据库造成瞬时压力。
    if not (
        "valid_recipients_all" in normalized_filename
        or "valid_recipients_archive" in normalized_filename
        or "valid_recipients_hot" in normalized_filename
    ):
        return False
    complexity_score = 0
    for marker in (" join ", " exists ", " union ", "select 1 from", " from ("):
        if marker in normalized_sql:
            complexity_score += 1
    return complexity_score >= 2


async def estimate_export_row_count(sql: str, params: dict | None = None) -> int | None:
    """预估导出行数；复杂查询返回 None 以降低数据库压力。"""
    normalized = str(sql or "").strip().rstrip(";")
    if not normalized:
        return None
    count_sql = f"SELECT COUNT(*) FROM ({normalized}) AS count_subq"
    try:
        async with AsyncSessionLocal() as db:
            if params:
                result = await db.execute(text(count_sql), params)
            else:
                result = await db.execute(text(count_sql))
            return int(result.scalar_one() or 0)
    except Exception as exc:
        logger.exception("导出行数预估失败，将使用流式导出 error=%s", exc)
        return None


async def dispatch_smart_export(
    request: Request,
    sql: str,
    filename: str,
    *,
    params: dict | None = None,
    user_id: int | None = None,
) -> StreamingResponse | JSONResponse:
    """统一采用异步后台导出，避免长连接被代理超时切断。"""
    logger.info("开始导出任务（异步模式） filename=%s", filename)
    try:
        row_count: int | None = None
        processing_count = await _count_processing_exports()
        if processing_count > 0:
            logger.info(
                "已有 %s 个导出任务进行中，跳过行数预估以降低数据库压力 filename=%s",
                processing_count,
                filename,
            )
        elif _should_skip_precise_count(sql, filename):
            logger.info("导出行数预估已跳过（复杂查询降压） filename=%s", filename)
        else:
            # 仍保留一次性行数预估，仅作为日志与后续调优依据，不阻塞导出主流程。
            row_count = await asyncio.wait_for(estimate_export_row_count(sql, params), timeout=5.0)
            logger.info("导出行数预估完成 filename=%s row_count=%s", filename, row_count)
    except asyncio.TimeoutError:
        row_count = None
        logger.warning("导出行数预估超时，直接进入异步导出 filename=%s", filename)
    except Exception as exc:
        row_count = None
        logger.warning("导出行数预估失败，直接进入异步导出 filename=%s error=%s", filename, exc)

    try:
        export_id = await start_async_export(
            sql,
            filename,
            estimated_rows=int(row_count or 0),
            user_id=user_id,
        )
        logger.info(
            "导出已创建异步任务 filename=%s export_id=%s estimated_rows=%s",
            filename,
            export_id,
            "未知" if row_count is None else row_count,
        )
    except AsyncExportBusyError as exc:
        logger.warning("异步导出任务繁忙 filename=%s error=%s", filename, exc)
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("创建异步导出任务失败 filename=%s", filename)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=format_export_error_message(exc),
        ) from exc
    return JSONResponse(
        content={"export_id": export_id, "status": "processing"},
        media_type="application/json",
    )


async def stream_copy_sql(sql: str) -> AsyncIterator[bytes]:
    logger.info("开始执行 COPY 导出流")
    try:
        async for chunk in stream_copy_txt(sql):
            yield chunk
    except Exception as exc:
        logger.exception("COPY 导出流执行失败 error=%s", exc)
        raise


async def stream_valid_recipients_txt(source: str) -> AsyncIterator[bytes]:
    logger.info("开始导出全局有效收件人 source=%s", source)
    sql = build_valid_recipients_sql(source)
    async for chunk in stream_copy_sql(sql):
        yield chunk


async def stream_task_delivery_export_txt(
    *,
    log_table: str,
    task_ids: list[int],
    export_type: str,
) -> AsyncIterator[bytes]:
    logger.info("开始导出任务投递结果 log_table=%s task_ids=%s export_type=%s", log_table, task_ids, export_type)
    try:
        sql = build_task_delivery_export_sql(log_table=log_table, task_ids=task_ids, export_type=export_type)
        logger.info("任务投递导出 SQL 已构建 log_table=%s task_ids=%s export_type=%s sql=%s", log_table, task_ids, export_type, sql[:500])
        async for chunk in stream_copy_sql(sql):
            yield chunk
    except Exception:
        logger.exception("任务投递结果导出失败 log_table=%s task_ids=%s export_type=%s", log_table, task_ids, export_type)
        raise


async def stream_group_export_txt(group_id: int) -> AsyncIterator[bytes]:
    logger.info("开始导出分组邮箱 group_id=%s", group_id)
    try:
        async with AsyncSessionLocal() as db:
            group = (
                await db.execute(select(RecipientList).where(RecipientList.id == int(group_id)))
            ).scalar_one_or_none()
            if group is None:
                raise ExportScopeError("收件人分组不存在")
        sql = build_group_export_sql(group_id)
        logger.info("分组导出 SQL 已构建 group_id=%s sql=%s", group_id, sql[:500])
        async for chunk in stream_copy_sql(sql):
            yield chunk
    except Exception:
        logger.exception("分组邮箱导出失败 group_id=%s", group_id)
        raise


async def resolve_group_export_filename(group_id: int) -> str:
    async with AsyncSessionLocal() as db:
        group = (
            await db.execute(select(RecipientList).where(RecipientList.id == int(group_id)))
        ).scalar_one_or_none()
        if group is None:
            raise ExportScopeError("收件人分组不存在")
        safe_name = sanitize_group_filename(str(group.name or ""))
    return f"group_{safe_name}.txt"


async def stream_task_success_txt(task_id: int) -> AsyncIterator[bytes]:
    logger.info("开始导出任务成功收件人 task_id=%s", task_id)
    try:
        async with AsyncSessionLocal() as db:
            await _load_task_for_export(db, task_id)
        sql = build_task_success_sql(task_id)
        logger.info("任务成功导出 SQL 已构建 task_id=%s sql=%s", task_id, sql[:500])
        async for chunk in stream_copy_sql(sql):
            yield chunk
    except Exception:
        logger.exception("任务成功收件人导出失败 task_id=%s", task_id)
        raise


async def stream_task_failed_txt(task_id: int) -> AsyncIterator[bytes]:
    logger.info("开始导出任务失败收件人 task_id=%s", task_id)
    try:
        async with AsyncSessionLocal() as db:
            await _load_task_for_export(db, task_id)
        sql = build_task_failed_sql(task_id)
        logger.info("任务失败导出 SQL 已构建 task_id=%s sql=%s", task_id, sql[:500])
        async for chunk in stream_copy_sql(sql):
            yield chunk
    except Exception:
        logger.exception("任务失败收件人导出失败 task_id=%s", task_id)
        raise


async def stream_task_unset_txt(task_id: int) -> AsyncIterator[bytes]:
    logger.info("开始导出任务未发送收件人 task_id=%s", task_id)
    try:
        async with AsyncSessionLocal() as db:
            task = await _load_task_for_export(db, task_id)
            sql = build_task_unset_sql(task_id, int(task.recipient_list_id))
        logger.info("任务未发送导出 SQL 已构建 task_id=%s sql=%s", task_id, sql[:500])
        async for chunk in stream_copy_sql(sql):
            yield chunk
    except Exception:
        logger.exception("任务未发送收件人导出失败 task_id=%s", task_id)
        raise
