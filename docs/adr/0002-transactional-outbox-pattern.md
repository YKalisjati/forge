# ADR 0002: Transactional Outbox Pattern for Asynchronous Message Dispatch

## Status
Accepted

## Context
When an API client submits a job, Forge must both persist the job record in PostgreSQL and publish an event to RabbitMQ for worker consumption.

Directly publishing to RabbitMQ within the HTTP request handler introduces the **Dual-Write Anti-Pattern**:
- If the database commit succeeds but the RabbitMQ publish fails (network drop, broker crash), the job exists in the database but will never be processed.
- If the RabbitMQ publish succeeds but the database transaction rolls back, the worker receives a notification for a nonexistent job.
- Distributed transactions (Two-Phase Commit / XA) between PostgreSQL and RabbitMQ are complex, introduce high latency, and lack robust support across modern cloud environments.

## Decision
We implement the **Transactional Outbox Pattern**:
1. When a job is submitted, the API writes both the `jobs` row and an `outbox_messages` row in the **same PostgreSQL transaction**.
2. A separate autonomous background service—the **Forge Outbox Relay**—asynchronously polls the `outbox_messages` table using `SELECT ... FOR UPDATE SKIP LOCKED`.
3. The relay publishes each message to RabbitMQ using **Publisher Confirms**.
4. Once the broker acknowledges receipt, the relay marks the outbox message `status = 'published'` (or removes it).

## Consequences

### Positive
- **Guaranteed Consistency**: A message is guaranteed to be published if and only if the job was durably committed to PostgreSQL.
- **Broker Decoupling**: If RabbitMQ is down, the API continues accepting jobs without failing client requests. Backlogs buffer safely in PostgreSQL.
- **Controlled Ingestion Throughput**: Outbox Relay acts as a natural rate governor and backpressure buffer between database ingestion and message queuing.

### Negative & Mitigations
- **At-Least-Once Delivery / Duplicate Messages**: If the relay process crashes after publishing to RabbitMQ but before marking the outbox row as published, the message will be published again upon restart.
  *Mitigation*: Workers enforce idempotent processing and atomic lease acquisition (see ADR 0003 and ADR 0005).
- **Relay Latency**: Introducing an asynchronous relay adds a slight latency overhead (poll interval, typically 10–50ms).
  *Mitigation*: Tune polling frequency and optionally support `LISTEN/NOTIFY` for sub-millisecond wakeups if sub-10ms latency is needed.
