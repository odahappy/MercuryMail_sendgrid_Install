"""兼容入口：SendLog 每日任务已改为热表归档。"""

from app.services.log_archiver import send_log_daily_archive_loop as send_log_daily_cleanup_loop

__all__ = ["send_log_daily_cleanup_loop"]
