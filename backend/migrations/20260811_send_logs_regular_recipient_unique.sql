-- regular 发送结果按任务和收件人唯一，作为恢复与 CompletionAuditor 的最终防重约束。
-- 创建索引前先删除历史重复记录，只保留最早一条。
DELETE FROM send_logs AS duplicate
USING send_logs AS keeper
WHERE duplicate.task_id = keeper.task_id
  AND duplicate.recipient_id = keeper.recipient_id
  AND duplicate.log_type = 'regular'
  AND keeper.log_type = 'regular'
  AND duplicate.recipient_id IS NOT NULL
  AND duplicate.id > keeper.id;

CREATE UNIQUE INDEX IF NOT EXISTS ux_send_logs_regular_task_recipient
ON send_logs (task_id, recipient_id)
WHERE log_type = 'regular' AND recipient_id IS NOT NULL;
