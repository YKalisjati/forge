# Forge Failure Scenarios & Distributed Resilience Matrix

This document provides an exhaustive technical analysis of failure modes in Forge, detailing the exact failure trigger, vulnerability window, system behavior, recovery mechanics, and formal data integrity guarantees.

---

## 1. Matrix Overview

| ID | Failure Scenario | Subsystem Affected | Recovery Mechanism | Integrity Guarantee |
|---|---|---|---|---|
| **FS-01** | DB commit succeeds; RabbitMQ publish fails | Outbox Relay / Broker | Transactional Outbox retry loop | Zero message loss; eventual publish |
| **FS-02** | Publish succeeds; Relay crashes before confirmation | Outbox Relay | Re-publish on restart + Worker deduplication | At-least-once delivery; idempotent consumption |
| **FS-03** | Worker crashes before processing job | Worker | RabbitMQ unacked requeue | Automatic failover to available worker |
| **FS-04** | Worker crashes after side effect but before ACK | Worker | Fencing token / Lease verification + Handler Idempotency | At-least-once execution; zombie worker fenced out |
| **FS-05** | Duplicate delivery and concurrent workers | Broker / Workers | Optimistic Lease Claiming (`execution_lease_id`) | Exactly-one active worker executes; losers safely ACK |
| **FS-06** | Poison pills & Retry exhaustion | Handler / Worker | Immediate DLQ classification + Max retry threshold | System does not loop infinitely; audited in DLQ |
| **FS-07** | Stale processing jobs (Hang / Partition) | Worker / Reaper | Lease timeout + Stale Job Reaper + Fencing tokens | Stale job reclaimed; zombie worker prevented from committing |
| **FS-08** | PostgreSQL outage | Source of Truth | Circuit break across all binaries | Strict fail-fast; zero silent corruption |
| **FS-09** | RabbitMQ broker outage | Message Transport | API buffers into PostgreSQL outbox; Relay backs off | Zero ingestion loss; backlog drains automatically on recovery |
| **FS-10** | Redis outage | Rate Limiter | Fail-open / degraded ingestion logging | System continues functioning; DB protected by bounded connection pools |

---

## 2. Detailed Scenario Breakdowns

### FS-01: Database Commit Succeeds but RabbitMQ Publish Fails
- **Trigger**: Client calls `POST /v1/jobs`. PostgreSQL transaction commits `jobs` and `outbox_messages` rows. At that exact moment, RabbitMQ is down, network partition occurs, or Outbox Relay process cannot reach the broker.
- **Vulnerability Window**: From PostgreSQL commit until RabbitMQ reconnects.
- **System Behavior**:
  1. API returns `202 Accepted` to client because the job is durably persisted in PostgreSQL.
  2. Outbox Relay attempts to publish to RabbitMQ exchange `forge.jobs.direct`.
  3. Connection timeout or socket error occurs; publisher confirm is not received.
  4. Outbox Relay rolls back the outbox update, leaving the message in `status = 'pending'`.
  5. Relay increments `retry_count` and applies exponential backoff with jitter before the next poll.
- **Recovery**: Once RabbitMQ recovers, the relay polling loop (`SELECT ... FOR UPDATE SKIP LOCKED`) retrieves the pending outbox row and successfully publishes it.
- **Guarantee**: **Zero data loss**. Ingestion never depends on synchronous broker availability.

---

### FS-02: Publish Succeeds but Relay Crashes Before Recording Confirmation
- **Trigger**: Outbox Relay successfully publishes a message to RabbitMQ and broker sends an ACK confirm, but the Relay process crashes (OOM, SIGKILL, power loss) before updating `outbox_messages.status = 'published'` in PostgreSQL.
- **Vulnerability Window**: Microseconds between RabbitMQ ACK receipt and PostgreSQL `status = 'published'` commit.
- **System Behavior**:
  1. RabbitMQ holds the published message in queue `forge.jobs`.
  2. The crashed Relay process restarts.
  3. The Relay queries `outbox_messages WHERE status = 'pending'` and finds the same message.
  4. Relay publishes the message to RabbitMQ a second time.
  5. RabbitMQ now holds duplicate messages for the same `job_id`.
- **Recovery & Deduplication**:
  1. Worker A receives the first delivery and executes:
     ```sql
     UPDATE jobs 
     SET state = 'running', execution_lease_id = $new_lease, lease_expires_at = NOW() + '30s'
     WHERE id = $job_id AND state IN ('pending', 'retrying')
     RETURNING ...;
     ```
  2. Worker A acquires the lease and processes the job.
  3. Worker B (or Worker A later) receives the duplicate delivery.
  4. Worker attempts the same query: because `state` is now `'running'` (with valid unexpired lease) or `'completed'`, the query returns `0` rows.
  5. Worker detects that lease acquisition returned no rows, immediately acknowledges (`basic.ack`) the duplicate message to RabbitMQ, and terminates processing safely.
- **Guarantee**: **At-least-once delivery handled idempotently**. No duplicate side-effects.

---

### FS-03: Worker Crashes Before Processing
- **Trigger**: A message is delivered to a worker instance over an open AMQP channel. The worker process crashes immediately (panic, host crash, SIGKILL) before running business logic.
- **Vulnerability Window**: Time between `basic.deliver` and job execution.
- **System Behavior**:
  1. The worker operates with `autoAck = false`.
  2. The TCP socket between RabbitMQ and the worker abruptly terminates.
  3. RabbitMQ detects socket closure and notices the delivered message was unacknowledged.
  4. RabbitMQ sets the message header `redelivered = true` and re-queues it at the head of the queue.
  5. Another available worker consumes the message.
- **Guarantee**: **Guaranteed delivery**. Messages are never lost due to worker node failure.

---

### FS-04: Worker Crashes After a Side Effect but Before ACK
- **Trigger**: Worker claims lease, performs an external side effect (e.g., sends an email via SendGrid, charges Stripe, calls webhook), updates PostgreSQL to `state = 'completed'`, but crashes right before calling `channel.Ack()`.
- **Vulnerability Window**: Milliseconds between PostgreSQL commit and RabbitMQ ACK.
- **System Behavior**:
  1. Worker crash triggers channel closure; RabbitMQ re-queues message with `redelivered = true`.
  2. A second worker consumes the redelivered message.
  3. Second worker attempts to acquire lease:
     ```sql
     UPDATE jobs SET state = 'running' ... WHERE id = $id AND state IN ('pending', 'retrying');
     ```
  4. Query returns `0` rows because `jobs.state` is already `'completed'`!
  5. The second worker immediately issues `basic.ack` to clean up the queue.
- **Special Case (Crash BEFORE PostgreSQL update but AFTER external side effect)**:
  - If worker crashes after calling an external API but *before* committing `completed` to PostgreSQL, the lease eventually expires.
  - The next worker re-executes the handler.
  - **Mitigation Requirement**: Handlers making non-idempotent external calls must pass deterministic idempotency keys (e.g., `job_id + "-" + attempt`) to external services so the external service deduplicates.

---

### FS-05: Duplicate Delivery and Concurrent Processing
- **Trigger**: Two workers receive the same job ID concurrently (e.g., network hiccup caused RabbitMQ redelivery, or two outbox entries fired).
- **Vulnerability Window**: Simultaneous message consumption.
- **System Behavior**:
  1. Worker 1 and Worker 2 both issue atomic conditional updates against PostgreSQL.
  2. PostgreSQL row-level locks on `jobs.id` serialize the two `UPDATE` queries.
  3. The first transaction wins, updates `state = 'running'`, sets `execution_lease_id = UUID_1`, and commits.
  4. The second transaction inspects the row: the `WHERE state IN ('pending', 'retrying')` predicate now evaluates to FALSE (since state is `'running'` with active lease).
  5. Worker 2 receives `0` updated rows.
  6. Worker 2 emits `basic.ack` and safely exits without doing any work.
- **Guarantee**: **Mutual exclusion**. Under no circumstances do two workers execute the same job concurrently.

---

### FS-06: Retry Scheduling, Retry Exhaustion & Poison Pills

#### 1. Transient Failures & Exponential Backoff with Jitter
- **Trigger**: Handlers encounter transient errors (e.g. network timeout, rate limiting, downstream 503).
- **Evaluation**: The error is checked with `job.IsPermanent(err)`. If false, and `claimedJob.RetriesAttempted <= claimedJob.MaxRetries`, the job is eligible for retry.
- **Backoff Formula**: Full Jitter Exponential Backoff:
  $$\text{Backoff} = \min\left(\text{MaxInterval}, \text{BaseInterval} \times 2^{\text{attempt}-1} + \text{random\_jitter}\right)$$
  Where `BaseInterval` = 2s, `MaxInterval` = 300s, and `random_jitter` is uniform in $[0, \text{BaseInterval}]$.
- **Atomic Database Transition**:
  In a single atomic PostgreSQL transaction:
  1. `jobs`: Updates `state = 'pending'`, `next_attempt_at = NOW() + delay`, `last_error = $err`, `execution_lease_id = NULL`, `lease_expires_at = NULL`.
  2. `job_executions`: Updates current attempt to `status = 'failed'`, recording error and duration.
  3. `outbox_messages`: Inserts new row with `event_type = 'job.scheduled'`, `scheduled_at = next_attempt_at`.
- **Broker Acknowledgment**: Only after the PostgreSQL transaction commits is the current RabbitMQ message acknowledged (`basic.ack`).
- **Re-Dispatch**: When `scheduled_at <= NOW()`, Outbox Relay claims the outbox record with `FOR UPDATE SKIP LOCKED` and publishes the next attempt delivery to RabbitMQ.

#### 2. Permanent Failures
- **Trigger**: Handler returns an error marked permanent via `job.MarkPermanent(err)` or `job.ErrPermanent`.
- **System Behavior**:
  1. Worker skips retry scheduling.
  2. In an atomic transaction, `jobs.state` is set to `'dead_letter'`, `job_executions` is updated with `status = 'failed'`, and no outbox message is generated.
  3. The current RabbitMQ message is positively acknowledged (`basic.ack`) to release broker queue resources.
  4. Metric `forge_jobs_dead_letter_total` is incremented.

#### 3. Retry Exhaustion
- **Trigger**: A job fails and `retries_attempted > max_retries`.
- **System Behavior**:
  1. The job transitions to terminal state `state = 'dead_letter'`.
  2. `last_error` records `retries exhausted (N/max): <error>`.
  3. Attempt audit is finalized in `job_executions`.
  4. Delivery is acknowledged (`basic.ack`) to remove it from the broker queue.
  5. Metrics `forge_jobs_exhausted_total` and `forge_jobs_dead_letter_total` are incremented.

#### 4. RabbitMQ Dead-Letter Handling for Poison or Malformed Messages
- **Trigger**:
  - Malformed JSON envelope body that cannot be parsed.
  - Unsupported `schema_version` (e.g. `!= 1`).
  - Handler returns `job.ErrPoisonMessage`.
- **Dead-Letter Routing Mechanics**:
  1. Worker calls `channel.Nack(multiple=false, requeue=false)`.
  2. Because the `forge.jobs` queue is declared with:
     ```
     x-dead-letter-exchange: forge.jobs.direct
     x-dead-letter-routing-key: forge.jobs.dlq
     ```
  3. RabbitMQ automatically intercepts the un-requeued negative acknowledgment and routes the message directly into `forge.jobs.dlq`.
  4. The poison message is permanently preserved in the DLQ for operator inspection without stalling or blocking worker consumption.
  5. Metrics `forge_dlq_events_total` and `forge_malformed_messages_total` are incremented.

---

### FS-07: Stale Processing Jobs (Worker Hang or Partition)
- **Trigger**: A worker process encounters a GC pause, network partition from PostgreSQL, or infinite loop in custom job handler code.
- **System Behavior**:
  1. The worker’s heartbeat goroutine stops extending `lease_expires_at`.
  2. `jobs.lease_expires_at` lapses (`lease_expires_at < NOW()`).
  3. The **Stale Job Reaper** runs:
     ```sql
     SELECT id, retries_attempted, max_retries, execution_lease_id
     FROM jobs 
     WHERE state = 'running' AND lease_expires_at < NOW()
     ORDER BY lease_expires_at ASC
     LIMIT $batch_size
     FOR UPDATE SKIP LOCKED;
     ```
  4. If `retries_attempted <= max_retries`:
     - Resets `state = 'pending'`, `execution_lease_id = NULL`, `lease_expires_at = NULL`, `next_attempt_at = NOW()`.
     - Updates `job_executions` row for the timed-out attempt to `status = 'timed_out'`.
     - Inserts a new `outbox_messages` event for immediate dispatch.
     - Increments metric `forge_stale_jobs_recovered_total`.
  5. If `retries_attempted > max_retries`:
     - Sets `state = 'dead_letter'`, `last_error = 'execution lease expired and max retries exhausted'`.
     - Updates `job_executions` to `status = 'timed_out'`.
     - Increments metric `forge_stale_jobs_dead_lettered_total`.
- **Fencing Protection**: If the zombie worker wakes up later and attempts to commit completion (`CompleteJob`), schedule retry (`ScheduleRetry`), or extend lease (`ExtendLease`):
  ```sql
  UPDATE jobs 
  SET ...
  WHERE id = $id AND execution_lease_id = $stale_lease;
  ```
  Because the Reaper cleared `execution_lease_id`, this query matches `0` rows.
  The zombie worker receives `ErrFencingViolation`, discards its work, increments `forge_fencing_violations_total`, and safely ACKs the RabbitMQ delivery.
- **Guarantee**: **Fencing safety**. Stale workers cannot overwrite modern state.

---

### 3. Distributed Guarantees & Formal Limitations

#### At-Least-Once Delivery vs Exactly-Once Execution
- **Forge guarantees at-least-once message delivery and atomic database state transitions.**
- **Forge does NOT claim magic "exactly-once execution" across distributed network boundaries.**
  If a worker claims a job, performs an external side effect (e.g. calls Stripe API, sends an email, or issues an HTTP webhook), and crashes *before* committing `state = 'completed'` to PostgreSQL:
  - The lease will expire and the Stale Job Reaper (or RabbitMQ requeue) will re-deliver the job to another worker.
  - The second worker will execute the handler again.
  - **Mitigation Requirement**: Any handler performing non-idempotent external mutations must pass deterministic idempotency tokens (such as `job.ID.String() + "-" + strconv.Itoa(job.RetriesAttempted)`) to downstream services so the downstream provider deduplicates the action.

#### PostgreSQL Availability Dependency
- PostgreSQL is the authoritative single source of truth. If PostgreSQL becomes partitioned or unavailable, workers cannot claim leases or persist transitions.
- Deliveries received during a database outage are NACKed with requeue (`requeue=true`) or held unacknowledged until connection recovery.

#### Broker Requeue Behavior
- Messages NACKed with `requeue=true` return to the head of the RabbitMQ queue. To prevent tight spinning when PostgreSQL is degraded, workers close channels or back off polling before re-attempting consumption.

---

### FS-08: Database, RabbitMQ, and Redis Outages

#### 1. PostgreSQL Outage
- **Impact**: Source of truth is unreachable.
- **API**: Fails readiness checks (`/readyz` returns 503); returns 503 to new incoming requests.
- **Outbox Relay**: Pauses polling, enters exponential reconnection retry.
- **Workers**: Unable to claim leases or commit results; messages remain unacknowledged in RabbitMQ or are NACKed with requeue.
- **Resolution**: Once PostgreSQL recovers, system resumes exactly where it stopped. No data lost.

#### 2. RabbitMQ Outage
- **Impact**: Asynchronous transport disrupted.
- **API**: **Unaffected!** Ingests jobs normally to PostgreSQL.
- **Outbox Relay**: Catches connection error, halts publishing, buffers messages safely inside `outbox_messages` table.
- **Workers**: Disconnected from broker, idle waiting for reconnect.
- **Resolution**: When RabbitMQ recovers, Outbox Relay drains the backlog at controlled rate.

#### 3. Redis Outage
- **Impact**: Rate limiter disrupted.
- **System Behavior**: Redis is strictly a non-authoritative cache/limiter.
- The API rate-limiting middleware catches Redis dial error, increments a degraded-mode metric, and **fails open** to allow valid requests through without failing job submission.
