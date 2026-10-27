ALTER TABLE warmup_plans
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMP NULL;

ALTER TABLE warmup_plans
    ADD COLUMN IF NOT EXISTS total_days INTEGER NOT NULL DEFAULT 0;

ALTER TABLE warmup_plans
    ADD CONSTRAINT ck_warmup_plans_total_days_nonnegative
    CHECK (total_days >= 0) NOT VALID;

ALTER TABLE warmup_plans
    VALIDATE CONSTRAINT ck_warmup_plans_total_days_nonnegative;
