-- 监控邮件触发里程碑迁移。
-- 语义约束：
-- 1. 普通发送日志的 monitor_trigger_count 必须为 NULL，不能用 0 冒充监控里程碑。
-- 2. 新监控日志必须具有大于 0 的任务级触发里程碑；无法恢复里程碑的历史记录保留 NULL。
-- 3. 去重身份为 task_id + monitor_trigger_count + 规范化邮箱。
-- 4. 只有发送成功的监控日志参与唯一去重；失败日志允许同一里程碑重试。
--
-- 注意：CREATE INDEX CONCURRENTLY 不能在事务块中执行。本迁移需要以非事务模式运行。

ALTER TABLE send_logs
    ADD COLUMN IF NOT EXISTS monitor_trigger_count INTEGER;

-- 修正旧列定义中的 DEFAULT 0。新写入的普通日志应自然保持 NULL。
ALTER TABLE send_logs
    ALTER COLUMN monitor_trigger_count DROP DEFAULT;

-- 清理普通日志被旧默认值污染的数据。
UPDATE send_logs
SET monitor_trigger_count = NULL
WHERE COALESCE(log_type, 'regular') <> 'monitor'
  AND monitor_trigger_count IS NOT NULL;

-- 无法确认真实触发里程碑的旧监控记录不能伪装成第 0 个里程碑。
UPDATE send_logs
SET monitor_trigger_count = NULL
WHERE log_type = 'monitor'
  AND COALESCE(monitor_trigger_count, 0) <= 0;

-- 删除旧索引，避免其继续把失败监控日志纳入唯一约束。
DROP INDEX CONCURRENTLY IF EXISTS ux_send_logs_monitor_task_trigger_email;
DROP INDEX CONCURRENTLY IF EXISTS ix_send_logs_monitor_trigger_count;

-- 任务级监控历史和恢复查询索引。
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_send_logs_monitor_task_trigger
    ON send_logs (task_id, monitor_trigger_count DESC)
    WHERE log_type = 'monitor'
      AND monitor_trigger_count > 0;

-- 只对成功监控邮件去重。失败记录必须允许重试，且不同任务永不互相去重。
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ux_send_logs_monitor_success_task_trigger_email
    ON send_logs (
        task_id,
        monitor_trigger_count,
        LOWER(BTRIM(email))
    )
    WHERE log_type = 'monitor'
      AND LOWER(COALESCE(status, '')) IN ('monitor_sent', 'sent', 'success', 'delivered')
      AND monitor_trigger_count > 0
      AND email IS NOT NULL
      AND BTRIM(email) <> '';

-- 在不锁表扫描的情况下约束列的行级语义；NOT VALID 允许先部署再单独校验历史数据。
ALTER TABLE send_logs
    DROP CONSTRAINT IF EXISTS ck_send_logs_monitor_trigger_count_semantics;

ALTER TABLE send_logs
    ADD CONSTRAINT ck_send_logs_monitor_trigger_count_semantics
    CHECK (
        (
            COALESCE(log_type, 'regular') = 'monitor'
            AND (
                monitor_trigger_count IS NULL
                OR monitor_trigger_count > 0
            )
        )
        OR
        (
            COALESCE(log_type, 'regular') <> 'monitor'
            AND monitor_trigger_count IS NULL
        )
    ) NOT VALID;

ALTER TABLE send_logs
    VALIDATE CONSTRAINT ck_send_logs_monitor_trigger_count_semantics;
