CREATE TABLE IF NOT EXISTS task_stats_snapshots (
    id BIGSERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status VARCHAR(40) NOT NULL DEFAULT 'draft',
    current_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    target_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    delivered_count INTEGER NOT NULL DEFAULT 0,
    hard_bounced_count INTEGER NOT NULL DEFAULT 0,
    soft_bounced_count INTEGER NOT NULL DEFAULT 0,
    total_count INTEGER NOT NULL DEFAULT 0,
    pending_count INTEGER NOT NULL DEFAULT 0,
    recent_429_count INTEGER NOT NULL DEFAULT 0,
    admitted_workers INTEGER NOT NULL DEFAULT 0,
    active_http_inflight INTEGER NOT NULL DEFAULT 0,
    proxy_rtt_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT ux_task_stats_snapshots_task_id UNIQUE (task_id)
);

CREATE INDEX IF NOT EXISTS ix_task_stats_snapshots_updated_at
ON task_stats_snapshots (updated_at DESC);
