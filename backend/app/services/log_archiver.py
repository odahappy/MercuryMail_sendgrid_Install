"""SendLog 热表归档：将已确定最终状态的旧记录迁移至 send_log_archive。"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from time import monotonic
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text

from app.core.config import settings
from app.core.database import AsyncSessionLocal, engine, maintain_send_logs_indexes
from app.models.models import SendLog
from app.services.send_log_constants import FINAL_SEND_STATUS_SQL, SEND_LOG_ARCHIVE_COLUMNS

logger = logging.getLogger(__name__)

ARCHIVE_TIMEZONE = ZoneInfo("Asia/Shanghai")
ARCHIVE_BATCH_SIZE = 10000
HOT_TABLE_WARN_ROWS = 50_000_000
HOT_TABLE_MONITOR_INTERVAL_SECONDS = 3600
DAILY_ARCHIVE_INTERVAL_SECONDS = 24 * 60 * 60


def _archive_min_age_days() -> int:
    return int(getattr(settings, "hot_retention_days", 3) or 3)


def _stale_delete_days() -> int:
    return int(getattr(settings, "lost_event_retention_days", 14) or 14)


def _archive_retention_days() -> int:
    return int(getattr(settings, "archive_retention_days", 30) or 30)

_ARCHIVE_COLUMNS_SQL = ", ".join(SEND_LOG_ARCHIVE_COLUMNS)


def seconds_until_next_midnight(now: datetime | None = None) -> float:
    current = now or datetime.now(ARCHIVE_TIMEZONE)
    if current.tzinfo is None:
        current = current.replace(tzinfo=ARCHIVE_TIMEZONE)
    else:
        current = current.astimezone(ARCHIVE_TIMEZONE)
    today_midnight = datetime.combine(current.date(), time(0, 0, 0), tzinfo=ARCHIVE_TIMEZONE)
    if current <= today_midnight:
        return 0.0
    next_midnight = today_midnight + timedelta(days=1)
    return max(0.0, (next_midnight - current).total_seconds())


@dataclass
class ArchiveJobStats:
    archived_rows: int = 0
    deleted_stale_rows: int = 0
    deleted_archive_rows: int = 0
    hot_table_rows: int = 0
    elapsed_ms: float = 0.0


async def count_hot_send_logs() -> int:
    async with AsyncSessionLocal() as db:
        return int((await db.execute(select(func.count()).select_from(SendLog))).scalar_one() or 0)


async def monitor_hot_table_size() -> int:
    hot_count = await count_hot_send_logs()
    logger.info("SendLog 热表当前行数：%s", hot_count)
    if hot_count >= HOT_TABLE_WARN_ROWS:
        logger.warning(
            "SendLog 热表行数已达 %s，超过阈值 %s，请检查归档策略或缩短归档窗口",
            hot_count,
            HOT_TABLE_WARN_ROWS,
        )
    return hot_count


async def migrate_send_logs_batch() -> int:
    archive_days = _archive_min_age_days()
    migrate_sql = text(
        f"""
        WITH moved AS (
            DELETE FROM send_logs
            WHERE id IN (
                SELECT id FROM send_logs
                WHERE final_status IN ({FINAL_SEND_STATUS_SQL})
                  AND created_at < NOW() - INTERVAL '{archive_days} days'
                LIMIT {ARCHIVE_BATCH_SIZE}
            )
            RETURNING *
        )
        INSERT INTO send_log_archive ({_ARCHIVE_COLUMNS_SQL})
        SELECT {_ARCHIVE_COLUMNS_SQL} FROM moved
        RETURNING id
        """
    )
    async with engine.begin() as conn:
        result = await conn.execute(migrate_sql)
        moved_ids = result.fetchall()
        return len(moved_ids)


async def delete_stale_send_logs_batch() -> int:
    stale_days = _stale_delete_days()
    delete_sql = text(
        f"""
        DELETE FROM send_logs
        WHERE id IN (
            SELECT id FROM send_logs
            WHERE (final_status IS NULL OR final_status = '')
              AND created_at < NOW() - INTERVAL '{stale_days} days'
            LIMIT {ARCHIVE_BATCH_SIZE}
        )
        """
    )
    async with engine.begin() as conn:
        result = await conn.execute(delete_sql)
        return int(result.rowcount or 0)


async def delete_expired_archive_batch() -> int:
    retention_days = _archive_retention_days()
    delete_sql = text(
        f"""
        DELETE FROM send_log_archive
        WHERE id IN (
            SELECT id FROM send_log_archive
            WHERE created_at < NOW() - INTERVAL '{retention_days} days'
            LIMIT {ARCHIVE_BATCH_SIZE}
        )
        """
    )
    async with engine.begin() as conn:
        result = await conn.execute(delete_sql)
        return int(result.rowcount or 0)


async def delete_expired_archive_rows() -> int:
    total_deleted = 0
    while True:
        deleted = await delete_expired_archive_batch()
        if deleted <= 0:
            break
        total_deleted += deleted
        logger.info("SendLog 归档表过期清理 deleted=%s total=%s", deleted, total_deleted)
    return total_deleted


async def archive_eligible_send_logs() -> int:
    total_moved = 0
    while True:
        moved = await migrate_send_logs_batch()
        if moved <= 0:
            break
        total_moved += moved
        logger.info("SendLog 归档批次完成 moved=%s total=%s", moved, total_moved)
    return total_moved


async def delete_stale_send_logs() -> int:
    total_deleted = 0
    while True:
        deleted = await delete_stale_send_logs_batch()
        if deleted <= 0:
            break
        total_deleted += deleted
        logger.info("SendLog 清理无最终状态旧记录 deleted=%s total=%s", deleted, total_deleted)
    return total_deleted


async def run_nightly_archive_job() -> ArchiveJobStats:
    started_at = monotonic()
    stats = ArchiveJobStats()
    logger.warning(
        "SendLog 夜间归档任务开始 timezone=%s archive_age_days=%s stale_delete_days=%s archive_retention_days=%s batch_size=%s",
        ARCHIVE_TIMEZONE.key,
        _archive_min_age_days(),
        _stale_delete_days(),
        _archive_retention_days(),
        ARCHIVE_BATCH_SIZE,
    )
    stats.archived_rows = await archive_eligible_send_logs()
    stats.deleted_stale_rows = await delete_stale_send_logs()
    stats.deleted_archive_rows = await delete_expired_archive_rows()
    stats.hot_table_rows = await monitor_hot_table_size()
    stats.elapsed_ms = (monotonic() - started_at) * 1000.0
    logger.warning(
        "SendLog 夜间归档任务完成 archived=%s deleted_stale=%s deleted_archive=%s hot_rows=%s elapsed_ms=%.1f timezone=%s",
        stats.archived_rows,
        stats.deleted_stale_rows,
        stats.deleted_archive_rows,
        stats.hot_table_rows,
        stats.elapsed_ms,
        ARCHIVE_TIMEZONE.key,
    )
    try:
        await maintain_send_logs_indexes()
    except Exception:
        logger.exception("SendLog 热表索引维护失败")
    return stats


async def hot_table_monitor_loop() -> None:
    logger.info("SendLog 热表行数监控已启动 interval_seconds=%s", HOT_TABLE_MONITOR_INTERVAL_SECONDS)
    while True:
        try:
            await monitor_hot_table_size()
        except Exception:
            logger.exception("SendLog 热表行数监控失败")
        await asyncio.sleep(HOT_TABLE_MONITOR_INTERVAL_SECONDS)


async def send_log_daily_archive_loop() -> None:
    logger.warning(
        "SendLog 热表归档调度已启动 timezone=%s interval_hours=24 monitor_interval_hours=%s",
        ARCHIVE_TIMEZONE.key,
        HOT_TABLE_MONITOR_INTERVAL_SECONDS / 3600.0,
    )
    monitor_task = asyncio.create_task(hot_table_monitor_loop(), name="send-log-hot-table-monitor")
    try:
        while True:
            wait_seconds = seconds_until_next_midnight()
            if wait_seconds > 0:
                logger.info(
                    "SendLog 归档任务等待至下个午夜 sleep_seconds=%.0f timezone=%s",
                    wait_seconds,
                    ARCHIVE_TIMEZONE.key,
                )
                await asyncio.sleep(wait_seconds)
            await run_nightly_archive_job()
            await asyncio.sleep(DAILY_ARCHIVE_INTERVAL_SECONDS)
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
