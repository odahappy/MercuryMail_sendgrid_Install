CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_regular_email_created_status
ON send_logs (task_id, LOWER(BTRIM(email)), created_at DESC, id DESC)
INCLUDE (status, final_status, bounce_type)
WHERE COALESCE(log_type, 'regular') = 'regular'
  AND email IS NOT NULL
  AND BTRIM(email) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_task_status_log_type_created
ON send_logs (task_id, status, log_type, created_at DESC, id DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_task_stats_list_email_valid
ON recipients (list_id, LOWER(BTRIM(email)))
INCLUDE (is_unsubscribed)
WHERE COALESCE(is_valid, TRUE) IS TRUE
  AND email IS NOT NULL
  AND BTRIM(email) <> '';
