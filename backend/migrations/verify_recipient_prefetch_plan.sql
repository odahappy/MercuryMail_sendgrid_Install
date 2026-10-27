-- 运维手动验证 MercuryMail 收件人预取索引。
-- 将 list_id、游标值替换为生产任务的真实值后执行。

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT id, email, normalized_email, row_number
FROM recipients
WHERE list_id = 1
  AND row_number > 0
  AND (row_number > 0 OR (row_number = 0 AND id > 0))
  AND is_valid IS TRUE
  AND is_unsubscribed IS FALSE
ORDER BY row_number ASC, id ASC
LIMIT 2000;

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = current_schema()
  AND tablename = 'recipients'
  AND indexname IN (
    'ix_recipients_claim_covering_v18',
    'ix_recipients_claim_row_id_v17'
  )
ORDER BY indexname;

-- 期望执行计划出现 Index Scan 或 Index Only Scan，索引名至少包含上述任意一个。
