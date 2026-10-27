CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_recipient_id
    ON send_logs (recipient_id);

-- 验证索引：
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE schemaname = current_schema()
--   AND tablename = 'send_logs'
--   AND indexdef ILIKE '%(recipient_id)%';
