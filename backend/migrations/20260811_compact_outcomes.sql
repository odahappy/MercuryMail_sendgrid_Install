CREATE TABLE IF NOT EXISTS compact_outcomes (
    id BIGSERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    recipient_id INTEGER REFERENCES recipients(id) ON DELETE SET NULL,
    status VARCHAR(20) NOT NULL,
    provider_message_id VARCHAR(255) NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    log_type VARCHAR(40) NOT NULL DEFAULT 'regular',
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_compact_outcomes_task_recipient
ON compact_outcomes (task_id, recipient_id)
WHERE recipient_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_compact_outcomes_monitor_event
ON compact_outcomes (task_id, provider_message_id)
WHERE log_type = 'monitor' AND provider_message_id <> '';

CREATE INDEX IF NOT EXISTS ix_compact_outcomes_task_status
ON compact_outcomes (task_id, status);

INSERT INTO compact_outcomes (
    task_id, recipient_id, status, provider_message_id, attempt_count, log_type, created_at
)
SELECT
    task_id,
    recipient_id,
    CASE
        WHEN status IN ('sent', 'success', 'delivered', 'monitor_sent') THEN 'success'
        WHEN status = 'bounced' THEN 'bounced'
        WHEN status IN ('unknown', 'delivery_unknown') THEN 'unknown'
        ELSE 'failed'
    END,
    COALESCE(provider_message_id, ''),
    COALESCE(attempt_count, 0),
    CASE WHEN log_type = 'monitor' THEN 'monitor' ELSE 'regular' END,
    COALESCE(created_at, NOW())
FROM send_logs
WHERE (log_type = 'regular' AND recipient_id IS NOT NULL)
   OR (log_type = 'monitor' AND COALESCE(provider_message_id, '') <> '')
ON CONFLICT DO NOTHING;
