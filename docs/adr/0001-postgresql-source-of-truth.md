# ADR 0001: PostgreSQL as the Authoritative Source of Truth

## Status
Accepted

## Context
A background job processing engine requires a persistent datastore to record job definitions, payloads, lifecycle states, idempotency records, and execution audit history.

Options considered:
1. **Message Broker as Source of Truth (RabbitMQ / Redis Streams alone)**: Relying purely on broker queues to store job state until completion.
2. **Document / NoSQL Store (MongoDB / DynamoDB)**: Storing job state in document collections.
3. **Relational Database (PostgreSQL) with ACID transactions**: Maintaining all state, transitions, and outbox records in relational tables.

## Decision
We choose **PostgreSQL** as the single authoritative source of truth for Forge.

RabbitMQ will serve solely as a transient, low-latency asynchronous delivery mechanism, and Redis will serve purely as an optional rate-limiting layer.

## Consequences

### Positive
- **Strict ACID Transactions**: Guarantees that job creation and transactional outbox entries commit atomically in one database transaction, eliminating dual-write anomalies.
- **Rich Querying & Filtering**: Supports flexible filtering by state, job type, timestamp ranges, and execution metrics via standard indexed SQL without requiring specialized search engines.
- **Concurrent Row-Level Locking (`FOR UPDATE SKIP LOCKED`)**: Enables high-concurrency polling of outbox records and safe reaper sweeps without deadlock or thread contention.
- **Schema Validation & Constraints**: Database-level constraints (`CHECK`, `FOREIGN KEY`, `NOT NULL`) prevent corrupted job states at the data tier.

### Negative & Mitigations
- **Database Load Under High Throughput**: Unbounded polling could saturate database connections.
  *Mitigation*: Use index-backed `SKIP LOCKED` queries, batch polling, and RabbitMQ for worker dispatch so workers do not constantly poll PostgreSQL.
- **Storage Growth**: Retaining execution history indefinitely can lead to large table sizes.
  *Mitigation*: Implement data retention and archiving policies for terminal jobs (`completed`, `dead_letter`).
