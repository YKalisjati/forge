-- Performance index for job executions lookups and foreign key cascades
CREATE INDEX IF NOT EXISTS idx_job_executions_job_id_attempt ON job_executions(job_id, attempt ASC);

-- Performance index for outbox backlog metrics calculation
CREATE INDEX IF NOT EXISTS idx_outbox_pending_created ON outbox_messages(created_at) WHERE status = 'pending';

