-- Core Jobs Table
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key VARCHAR(128) NULL,
    type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    retries_attempted INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_lease_id UUID NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    locked_by VARCHAR(128) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ NULL,
    completed_at TIMESTAMPTZ NULL,

    CONSTRAINT chk_job_state CHECK (
        state IN ('pending', 'running', 'completed', 'failed', 'dead_letter')
    ),
    CONSTRAINT chk_priority_range CHECK (priority >= 0 AND priority <= 100),
    CONSTRAINT chk_retries CHECK (retries_attempted >= 0 AND retries_attempted <= max_retries + 1)
);

-- Transactional Outbox Table
CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL,

    CONSTRAINT chk_outbox_status CHECK (
        status IN ('pending', 'published', 'failed')
    )
);

-- Client Idempotency Cache Table
CREATE TABLE IF NOT EXISTS idempotency_records (
    key VARCHAR(128) PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    response_code INT NOT NULL,
    response_body JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

-- Job Execution Audit Log Table
CREATE TABLE IF NOT EXISTS job_executions (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INT NOT NULL,
    execution_lease_id UUID NOT NULL,
    worker_id VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    error TEXT NULL,
    duration_ms INT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,

    CONSTRAINT chk_execution_status CHECK (
        status IN ('running', 'completed', 'failed', 'timed_out')
    )
);

-- High-throughput Outbox querying with SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_outbox_pending_scheduled 
ON outbox_messages (scheduled_at ASC, id ASC) 
WHERE status = 'pending';

-- Worker state polling and reaper scans
CREATE INDEX IF NOT EXISTS idx_jobs_state_next_attempt 
ON jobs (state, next_attempt_at ASC) 
WHERE state IN ('pending', 'retrying');

CREATE INDEX IF NOT EXISTS idx_jobs_stale_reaper 
ON jobs (lease_expires_at ASC) 
WHERE state = 'running';

-- API listing and filtering
CREATE INDEX IF NOT EXISTS idx_jobs_type_state_created 
ON jobs (type, state, created_at DESC);

-- Idempotency key fast lookup
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_key 
ON jobs (idempotency_key) 
WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_idempotency_expires 
ON idempotency_records (expires_at);
