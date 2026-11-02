# ADR 0003: At-Least-Once Delivery and Idempotent Processing

## Status
Accepted

## Context
In distributed systems, physical "exactly-once delivery" across network boundaries is impossible according to the Two Generals' Problem and the FLP Impossibility Theorem. Network packets, acknowledgments, and process heartbeats can be delayed, dropped, or duplicated.

Claiming "exactly-once processing" without explicit semantic qualification leads to severe architectural flaws and hidden data corruption.

## Decision
Forge explicitly adopts **At-Least-Once Delivery** combined with **Idempotent Execution** across two distinct system boundaries:

### 1. Client-to-API Idempotency (Submission Boundary)
- Clients can provide an `Idempotency-Key` HTTP header on `POST /v1/jobs`.
- The API computes a SHA-256 hash of the request body and attempts to insert an entry into `idempotency_records`.
- If an entry exists:
  - If the hash matches, the API replays the original response with header `Idempotent-Replayed: true`.
  - If the hash differs, the API rejects the request with `409 Conflict` (`IDEMPOTENCY_PAYLOAD_MISMATCH`).
- Cache retention is bounded to 24 hours.

### 2. Relay-to-Worker Idempotency (Execution Boundary)
- Because RabbitMQ delivery and Outbox publishing guarantee at-least-once delivery, workers must tolerate duplicate message consumption.
- A worker never blindly executes a received job. Instead, it must first execute an atomic conditional state transition in PostgreSQL:
  ```sql
  UPDATE jobs 
  SET state = 'running', 
      execution_lease_id = $new_lease_id, 
      lease_expires_at = NOW() + $lease_duration,
      retries_attempted = retries_attempted + 1
  WHERE id = $job_id 
    AND (
      state IN ('pending', 'retrying') 
      OR (state = 'running' AND lease_expires_at < NOW())
    );
  ```
- If this update affects `0` rows (because another worker is currently processing it or it already completed), the receiving worker immediately acknowledges (`basic.ack`) the duplicate message to RabbitMQ and exits cleanly.

### 3. External Side-Effect Idempotency (Handler Boundary)
- For handlers that call external downstream APIs (payment gateways, emails, webhooks), the handler is provided with a unique execution token (`job_id + "-" + attempt`) to pass downstream as an external idempotency key.

## Consequences

### Positive
- **Provable Semantics**: System guarantees zero job drops while guaranteeing no duplicate executions under normal operations or duplicate message deliveries.
- **Fail-Safe Deduplication**: Database-level optimistic locking acts as an unforgeable arbiter for execution rights.

### Negative & Mitigations
- **Non-Idempotent Downstream Services**: If an external service does not support idempotency keys and the worker crashes mid-call, duplicate external requests could occur.
  *Mitigation*: Document external side-effect limitations in the job development guide; recommend staging external effects in two-phase states where feasible.
