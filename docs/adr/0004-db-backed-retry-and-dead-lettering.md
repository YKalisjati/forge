# ADR 0004: Database-Backed Retry Scheduling and Dead-Letter Handling

## Status
Accepted

## Context
When background jobs fail due to transient errors (e.g., temporary network glitches, rate limiting, downstream 503s), they must be retried with exponential backoff and jitter.

Common scheduling approaches:
1. **RabbitMQ Delayed Message Plugin (`rabbitmq_delayed_message_exchange`)**: Uses an Erlang Mnesia table inside RabbitMQ to hold delayed messages.
   - *Drawback*: Requires installing and maintaining a non-standard community plugin on RabbitMQ nodes; can suffer from memory pressure and cluster partition quirks under heavy delayed load.
2. **RabbitMQ Dead-Letter Exchange (DLX) with Message TTL**: Chains queues with message TTLs that dead-letter into the main queue when expired.
   - *Drawback*: RabbitMQ queue TTL only expires messages at the head of the queue. If a message with a 10-minute delay is behind a message with a 1-second delay, head-of-line blocking can occur unless per-delay dedicated queues are provisioned.
3. **Database-Backed Retry Scheduler with `next_attempt_at` and Outbox Flow**: Failed jobs update their `next_attempt_at` in PostgreSQL and write an outbox record with `scheduled_at = next_attempt_at`. The Outbox Relay queries for `scheduled_at <= NOW()`.

## Decision
We select **Approach 3: Database-Backed Retry Scheduler with Outbox Flow**.

### Retry Mechanics
1. **Backoff Calculation**:
   Full Jitter Exponential Backoff formula:
   $$\text{Backoff} = \min\left(\text{MaxInterval}, \text{BaseInterval} \times 2^{\text{retries\_attempted}} + \text{random\_jitter}\right)$$
   Where default `BaseInterval = 2s`, `MaxInterval = 300s`, and `random_jitter` is uniform in $[0, \text{BaseInterval}]$.
2. **State Transition**:
   When a handler returns a retryable error and `retries_attempted < max_retries`:
   - Job is set to `state = 'pending'` with `next_attempt_at = NOW() + Backoff`.
   - A new row is inserted into `outbox_messages` with `scheduled_at = next_attempt_at`.
   - The current RabbitMQ message is positively acknowledged (`basic.ack`) to release worker resources and prevent queue congestion.
3. **Outbox Polling**:
   Outbox Relay queries:
   ```sql
   SELECT id, job_id, payload 
   FROM outbox_messages 
   WHERE status = 'pending' AND scheduled_at <= NOW()
   ORDER BY scheduled_at ASC
   LIMIT $batch_size
   FOR UPDATE SKIP LOCKED;
   ```
4. **Dead-Letter Handling (Terminal State)**:
   A job transitions to `state = 'dead_letter'` when:
   - `retries_attempted >= max_retries`, OR
   - The handler returns an explicit `ErrNonRetryable` / `ErrFatal` (poison pill).
   In this case, the job is not re-enqueued. Its terminal state and error message are persisted in PostgreSQL, an audit entry is recorded in `job_executions`, and a Prometheus counter is incremented.

## Consequences

### Positive
- **Zero Extra Dependencies**: Runs on standard RabbitMQ without custom Erlang plugins.
- **Accurate Scheduling**: Timers are durable, persistent across restarts, and cannot be lost during broker crashes.
- **No Head-of-Line Blocking**: PostgreSQL indexes on `(scheduled_at ASC)` allow instant discovery of runnable jobs regardless of schedule order.

### Negative & Mitigations
- **Database Polling Load**: Frequent polling for ready retries could generate database load.
  *Mitigation*: The composite partial index on `(scheduled_at ASC, id ASC) WHERE status = 'pending'` makes the index scan extremely cheap (sub-millisecond). Polling occurs on a modest interval (e.g. 500ms–1s).
