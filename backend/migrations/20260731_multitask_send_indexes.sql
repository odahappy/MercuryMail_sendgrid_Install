-- MercuryMail 多任务发送稳定性索引迁移
-- 注意：CREATE INDEX CONCURRENTLY 不可在事务中执行，请由迁移工具以 autocommit 执行。

ALTER TABLE recipients ADD COLUMN IF NOT EXISTS normalized_email VARCHAR(255) NOT NULL DEFAULT '';

UPDATE recipients
SET normalized_email = lower(btrim(email))
WHERE normalized_email = '' AND email IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_normalized_email
    ON recipients(normalized_email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_row_id_v17
    ON recipients(list_id, row_number, id)
    WHERE is_valid = TRUE AND is_unsubscribed = FALSE AND email <> '' AND row_number > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_covering_v18
    ON recipients(list_id, row_number, id, normalized_email)
    INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at)
    WHERE is_valid = TRUE AND is_unsubscribed = FALSE AND email <> '' AND row_number > 0 AND normalized_email <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_recipients_list_row
    ON recipients(list_id, row_number, is_valid, is_unsubscribed);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_unsub_norm
    ON unsubscribes(normalized_email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_unsub_email
    ON unsubscribes(email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sendlogs_task_email_logtype
    ON send_logs(task_id, email, log_type);
