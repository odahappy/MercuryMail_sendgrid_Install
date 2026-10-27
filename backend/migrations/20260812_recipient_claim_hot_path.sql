-- 收件人预取热路径：让 keyset claim 与实际过滤条件完全匹配，避免大表过滤后排序。
-- CREATE INDEX CONCURRENTLY 必须由迁移工具以 autocommit 执行。

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_recipients_claim_hot_path_v19
    ON recipients (list_id, row_number, id, normalized_email)
    INCLUDE (email, name, company, phone, tags, custom_fields_json, is_valid, is_unsubscribed, created_at)
    WHERE is_valid IS TRUE
      AND COALESCE(is_unsubscribed, FALSE) IS FALSE
      AND normalized_email <> ''
      AND row_number > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_unsubscribes_normalized_email_claim
    ON unsubscribes (normalized_email)
    WHERE normalized_email IS NOT NULL AND normalized_email <> '';
