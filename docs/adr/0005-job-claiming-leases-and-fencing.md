# ADR 0005: Job Claiming, Leases, and Fencing Tokens for Stale Job Recovery

## Status
Accepted

## Context
When background jobs take a long time to run, or when a worker process stalls (e.g. stop-the-world garbage collection pauses, host memory paging, or network partition), the system must handle two opposing risks:
1. **The Liveness Problem**: If a worker crashes or hangs indefinitely while holding a job, the job must not remain in `running` forever. Another worker must recover and execute it.
2. **The Safety Problem (Zombie Worker)**: If Worker A is presumed dead due to a temporary partition or pause, and Worker B reclaims and completes the job, Worker A may subsequently wake up and attempt to commit its stale results. Without concurrency controls, Worker A could overwrite newer state or execute duplicate mutations.

## Decision
Forge implements **Optimistic Leases with Fencing Tokens (`execution_lease_id`)**:

### 1. Atomic Lease Claiming
When a worker claims a job, it generates a fresh UUIDv4 `execution_lease_id` (the fencing token) and establishes an expiration time `lease_expires_at = NOW() + INTERVAL '30 seconds'`:
```sql
UPDATE jobs 
SET state = 'running',
    execution_lease_id = $new_lease_id,
    lease_expires_at = NOW() + $lease_duration,
    locked_by = $worker_id,
    retries_attempted = retries_attempted + 1,
    updated_at = NOW()
WHERE id = $job_id 
  AND (
    state IN ('pending', 'retrying') 
    OR (state = 'running' AND lease_expires_at < NOW())
  )
RETURNING id, type, payload, execution_lease_id;
```

### 2. Lease Heartbeating
For jobs executing longer than half the lease interval (e.g., > 15 seconds), the worker maintains an internal heartbeat goroutine:
```sql
UPDATE jobs 
SET lease_expires_at = NOW() + $lease_duration,
    updated_at = NOW()
WHERE id = $job_id 
  AND execution_lease_id = $lease_id;
```
If the heartbeat query returns `0` rows affected, the worker knows its lease was revoked and cancels its execution `context.Context` immediately.

### 3. Fenced Completion and Terminal Updates
When execution finishes, the worker commits the terminal state strictly conditional on its fencing token:
```sql
UPDATE jobs 
SET state = 'completed',
    completed_at = NOW(),
    execution_lease_id = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE id = $job_id 
  AND execution_lease_id = $lease_id;
```
- If `RowsAffected() == 1`: The commit succeeded legitimately. The worker then acknowledges (`basic.ack`) the message in RabbitMQ.
- If `RowsAffected() == 0`: A timeout occurred and the job was reclaimed by another worker. The zombie worker discards its result, emits a warning log with metric `forge_fencing_violations_total`, and ACKs the RabbitMQ message.

### 4. Stale Job Reaper
A lightweight background loop (running inside Outbox Relay or Worker daemon) runs periodically:
```sql
SELECT id, retries_attempted, max_retries 
FROM jobs 
WHERE state = 'running' AND lease_expires_at < NOW()
FOR UPDATE SKIP LOCKED;
```
For expired jobs:
- If `retries_attempted < max_retries`: resets `state = 'pending'`, clears `execution_lease_id`, and emits an outbox dispatch event.
- If `retries_attempted >= max_retries`: sets `state = 'dead_letter'`, records `last_error = 'lease expired and retries exhausted'`.

## Consequences

### Positive
- **Guaranteed Mutual Exclusion**: Only one worker can hold a valid fencing token for a given execution period.
- **Immunity to Zombie Workers**: Stale workers cannot overwrite modern data even during severe network partitions or extreme GC pauses.
- **Autonomous Recovery**: Crashed workers release their work automatically without manual administrative intervention.

### Negative & Mitigations
- **Heartbeat Overhead**: Long-running jobs issue lightweight updates every 15s.
  *Mitigation*: Heartbeat queries are primary-key point updates and consume negligible database I/O.
- **External Side-Effect Window**: If a stale worker calls an external payment gateway before being fenced out, the external call might have occurred before the check.
  *Mitigation*: As established in ADR 0003, pass `execution_lease_id` or `job_id + attempt` as external idempotency keys.
