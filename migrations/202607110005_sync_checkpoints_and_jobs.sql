CREATE TABLE rebate_sync_checkpoints (
    sync_type VARCHAR(64) NOT NULL,
    source_namespace VARCHAR(64) NOT NULL,
    cursor_time BIGINT NOT NULL,
    cursor_id BIGINT NOT NULL,
    lookback_seconds BIGINT NOT NULL DEFAULT 300,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (sync_type, source_namespace),
    CONSTRAINT ck_rebate_sync_checkpoints_cursor_time CHECK (cursor_time >= 0),
    CONSTRAINT ck_rebate_sync_checkpoints_cursor_id CHECK (cursor_id >= 0),
    CONSTRAINT ck_rebate_sync_checkpoints_lookback CHECK (lookback_seconds >= 0)
);

INSERT INTO rebate_sync_checkpoints (
    sync_type, source_namespace, cursor_time, cursor_id, updated_at
)
SELECT DISTINCT ON (sync_type)
       sync_type, 'legacy', GREATEST(last_sync_time, 0), GREATEST(last_sync_id, 0), created_at
FROM sync_log
ORDER BY sync_type, last_sync_time DESC, last_sync_id DESC, id DESC
ON CONFLICT (sync_type, source_namespace) DO NOTHING;

CREATE TABLE rebate_background_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type VARCHAR(64) NOT NULL,
    partition_key VARCHAR(128) NOT NULL DEFAULT 'default',
    job_state VARCHAR(24) NOT NULL,
    cursor_time BIGINT NOT NULL DEFAULT 0,
    cursor_id BIGINT NOT NULL DEFAULT 0,
    run_after BIGINT NOT NULL,
    lease_owner VARCHAR(128),
    lease_token VARCHAR(128),
    lease_expires_at BIGINT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_code VARCHAR(128),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    completed_at BIGINT,
    CONSTRAINT uq_rebate_background_jobs_identity UNIQUE (job_type, partition_key),
    CONSTRAINT ck_rebate_background_jobs_state CHECK (job_state IN (
        'queued', 'running', 'retry_wait', 'completed', 'blocked'
    )),
    CONSTRAINT ck_rebate_background_jobs_cursor_time CHECK (cursor_time >= 0),
    CONSTRAINT ck_rebate_background_jobs_cursor_id CHECK (cursor_id >= 0),
    CONSTRAINT ck_rebate_background_jobs_attempt_count CHECK (attempt_count >= 0)
);

CREATE INDEX idx_rebate_background_jobs_runnable
    ON rebate_background_jobs(job_state, run_after, id)
    WHERE job_state IN ('queued', 'retry_wait');
