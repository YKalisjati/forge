# Forge End-to-End Reliability Verification Report (Phase 4B)

## 1. Executive Summary

This report documents the automated verification and audit of the Forge distributed background job processing platform against the failure scenarios defined in [`docs/failure-scenarios.md`](file:///Users/yohaneskalisjatiw.k./Documents/private_Project/forge/docs/failure-scenarios.md).

All tests were executed against live containerized dependencies (PostgreSQL 16, RabbitMQ 3.13 with management plugin, and Redis 7) using an automated end-to-end integration test harness located in [`tests/e2e/reliability_test.go`](file:///Users/yohaneskalisjatiw.k./Documents/private_Project/forge/tests/e2e/reliability_test.go).

### Verification Summary
- **Total Critical Scenarios Tested**: 14
- **Scenarios Passed**: 14
- **Scenarios Failed**: 0
- **Pass Rate**: 100%
- **Data Races Detected**: 0 (verified with `go test -race`)
- **Static Analysis Issues**: 0 (verified with `golangci-lint` and `go vet`)

---

## 2. Test Execution Environment

| Component | Specification | Configuration |
|---|---|---|
| **Operating System** | macOS (Darwin 24.6.0 arm64) | Local development workstation |
| **Go Runtime** | Go 1.26.0 darwin/arm64 | Multi-stage distroless build compatible |
| **PostgreSQL** | v16.8 (Alpine) | Port `5433`, connection pool min=2, max=10 |
| **RabbitMQ** | v3.13.7 (Alpine Management) | Port `5672`, durable direct exchange `forge.jobs.direct`, DLQ topology |
| **Redis** | v7.2 (Alpine) | Port `6379`, sliding-window Lua rate limiter |
| **Test Suite Execution** | `go test -v -race -p 1 ./tests/e2e/...` | Full race detection enabled, sequential package execution |

---

## 3. Scenario Audit & Verification Results

The table below outlines each of the 14 critical reliability scenarios tested, the expected distributed behavior, observed results, and final pass/fail status.

| Scenario ID | Test Name | Expected Distributed Behavior | Observed Behavior | Status |
|---|---|---|---|---|
| **E2E-01** | `TestE2E_01_SubmitAndObserveSuccessfulCompletion` | API returns 202 Accepted; outbox relay claims event with `SKIP LOCKED` and publishes to RabbitMQ; worker consumes message, claims execution lease, generates report, commits `completed`, and ACKs message. | End-to-end pipeline completed in 220ms. `jobs.state = 'completed'`, `job_executions` recorded attempt with `status = 'completed'`, outbox record marked `published`. | **PASS** |
| **E2E-02** | `TestE2E_02_SubmitIdenticalIdempotentRequestsConcurrently` | 10 concurrent requests with the identical `Idempotency-Key` and payload are serialized by PostgreSQL unique constraint; exactly 1 creates the job, 9 receive `Idempotent-Replayed: true`; all return identical `job_id`. | Exactly 1 creator response (202), 9 replayed responses (202 with `Idempotent-Replayed: true`). Exactly 1 job and 1 outbox record inserted in PostgreSQL. | **PASS** |
| **E2E-03** | `TestE2E_03_ReuseIdempotencyKeyWithDifferentPayloads` | Reusing an existing `Idempotency-Key` with a differing payload triggers conflict detection and rejects the second request with HTTP 409 Conflict without modifying database state. | Request 1 returned 202 Accepted; request 2 returned 409 Conflict. Database contains only the original job with payload 1; no duplicate or corrupted rows. | **PASS** |
| **E2E-04** | `TestE2E_04_RestartAPIDuringNormalOperation` | API process can be terminated and restarted cleanly without losing state or disrupting job ingestion; newly started instance reads existing jobs from PostgreSQL. | API instance 1 ingested job 1 and shut down; API instance 2 started on the same database, ingested job 2, and successfully served details for both jobs via `GET /v1/jobs/{id}`. | **PASS** |
| **E2E-05** | `TestE2E_05_StopRabbitMQWhileOutboxPendingThenRestore` | If RabbitMQ becomes unavailable, Outbox Relay safely buffers unpublished events in PostgreSQL, increments `retry_count`, and applies exponential backoff; when broker recovers, relay drains the backlog without data loss. | Relay logged broker dial failure, rolled back publish transaction, and left outbox record in `pending` (`retry_count = 1`). Upon broker restoration, relay published event with publisher confirm, marked outbox `published`, and worker executed job. | **PASS** |
| **E2E-06** | `TestE2E_06_SimulateDuplicateEventPublication` | Duplicate broker deliveries (e.g. from relay crash before database status update) are deduplicated by the worker: first delivery executes; second delivery detects `completed` state, ACKs immediately, and skips execution. | Delivery 1 claimed lease, executed handler, and committed `completed`. Delivery 2 arrived, detected `state = 'completed'`, issued `basic.ack`, and exited. Handler counter = 1, `job_executions` count = 1. | **PASS** |
| **E2E-07** | `TestE2E_07_TerminateWorkerAfterReceivingMessage` | If a worker process terminates abruptly while holding an uncommitted delivery, RabbitMQ requeues the unacknowledged message; after lease expiration/reset, a replacement worker claims and completes the job. | Worker 1 simulated crash before completion commit; message was NACKed with requeue. Worker 2 started, claimed the job lease, executed the handler to completion, and positively acknowledged the delivery. | **PASS** |
| **E2E-08** | `TestE2E_08_SimulateRetryableJobFailureFollowedBySuccess` | Transient handler failure triggers scheduled retry backoff; worker sets `next_attempt_at`, records failed attempt in `job_executions`, and ACKs delivery; next attempt executes and succeeds. | Attempt 1 failed with transient error; worker scheduled retry (`retries_attempted = 1`). On attempt 2, handler succeeded, job reached `completed`, and `job_executions` recorded exactly 2 audit entries (1 failed, 1 completed). | **PASS** |
| **E2E-09** | `TestE2E_09_ExhaustRetryBudget` | When a job exceeds `max_retries`, worker terminates retry scheduling, transitions `jobs.state` to `'dead_letter'`, writes audit log, and positively acknowledges the message to prevent infinite broker loops. | Job with `max_retries = 1` failed attempt 1 (retried) and attempt 2 (exhausted). Worker transitioned job to `dead_letter` with `last_error` stating retries exhausted, and cleaned message from queue. | **PASS** |
| **E2E-10** | `TestE2E_10_RecoverStaleProcessingJob` | If a worker hangs or crashes while processing, its lease expires; the Stale Job Reaper detects `lease_expires_at < NOW()`, resets state to `pending`, and re-emits an outbox event. Any late commit by the stale worker is rejected via fencing tokens (`ErrFencingViolation`). | Reaper detected expired lease, marked attempt `timed_out`, and reset job to `pending`. Zombie worker attempted `CompleteJob` and received `ErrFencingViolation`. Replacement worker claimed renewed lease and completed job. | **PASS** |
| **E2E-11** | `TestE2E_11_RunMultipleWorkersConcurrently` | 15 jobs distributed across 3 concurrent worker instances with pool concurrency = 5. Workers use PostgreSQL row-level locks (`SKIP LOCKED`) to claim leases without double execution or race conditions. | All 15 jobs claimed and executed concurrently across the 3 worker instances. All 15 finished in `completed` state with zero fencing violations and zero duplicate executions. | **PASS** |
| **E2E-12** | `TestE2E_12_VerifyTerminalJobsNotAccidentallyReExecuted` | Redelivering AMQP messages for jobs already in terminal states (`completed` or `dead_letter`) results in immediate broker acknowledgment without re-executing business logic. | Envelopes for completed and dead-lettered jobs were redelivered. Worker inspected PostgreSQL, identified terminal states, ACKed deliveries immediately. Execution counter remained 0; states unmodified. | **PASS** |
| **E2E-13** | `TestE2E_13_ShutDownServicesDuringActiveProcessing` | During active job execution, triggering service shutdown (`SIGTERM` / context cancellation) stops consumer ingestion, allows in-flight tasks to complete within `ShutdownTimeout`, and commits terminal state before process exit. | Context cancelled while job was executing. Worker stopped accepting new deliveries, waited for active goroutine to complete within 80ms, committed `completed` to PostgreSQL via dedicated commit context, and exited cleanly with nil error. | **PASS** |
| **E2E-14** | `TestE2E_14_VerifyDatabaseUnavailabilityNoFalseSuccessNoLoops` | When PostgreSQL is unreachable, worker cannot claim leases or commit state. Worker rejects/requeues delivery without issuing false positive ACKs, and does not enter an uncontrolled panic loop. | Worker with unreachable database attempted `ClaimJob`, caught connection error, and NACKed message with requeue. Job was never falsely marked completed. Process handled disconnection safely. | **PASS** |

---

## 4. Confirmed Defects & Resolutions

During the Phase 4B reliability verification audit, three issues were identified and resolved:

### Defect 1: Context Cancellation During Graceful Shutdown Aborted Database Commits
- **Severity**: High (Data Reliability)
- **Component**: [`internal/app/worker/worker.go`](file:///Users/yohaneskalisjatiw.k./Documents/private_Project/forge/internal/app/worker/worker.go)
- **Root Cause**: `ProcessDelivery` passed the worker's root `ctx` into `CompleteJob`, `DeadLetterJob`, and `ScheduleRetry`. When the worker received a shutdown signal (`SIGTERM` / `cancel()`), the root context was cancelled immediately to halt ingestion. As a result, in-flight jobs that successfully finished their execution during the shutdown drain window (`ShutdownTimeout`) attempted to commit to PostgreSQL with a cancelled context, resulting in `context.Canceled` errors and forced message requeues.
- **Resolution**: Updated `worker.go` to use a dedicated bounded-timeout context (`context.WithTimeout(context.Background(), 5*time.Second)`) for finalizing terminal state transitions (`CompleteJob`, `ScheduleRetry`, `DeadLetterJob`). This ensures that in-flight jobs completing within `ShutdownTimeout` persist their results cleanly before the worker process terminates.

### Defect 2: Expired Lease Reset Left Jobs in Unclaimable State
- **Severity**: Medium (Test Harness Edge Case)
- **Component**: [`tests/e2e/reliability_test.go`](file:///Users/yohaneskalisjatiw.k./Documents/private_Project/forge/tests/e2e/reliability_test.go)
- **Root Cause**: In the worker termination test, clearing `execution_lease_id = NULL` and `lease_expires_at = NULL` while keeping `state = 'running'` caused the atomic lease query `(state = 'running' AND lease_expires_at < NOW())` to evaluate to `NULL < NOW()`, which SQL evaluates to `NULL` (false), causing `ClaimJob` to return `ErrJobClaimFailed`.
- **Resolution**: Ensured that stale lease recovery resets `state = 'pending'` or explicitly sets `lease_expires_at < NOW()`, consistent with how the Stale Job Reaper operates in production.

### Defect 3: Ad-hoc Job Type Allowlist Enforcement
- **Severity**: Low (Validation Consistency)
- **Component**: [`tests/e2e/reliability_test.go`](file:///Users/yohaneskalisjatiw.k./Documents/private_Project/forge/tests/e2e/reliability_test.go)
- **Root Cause**: Initial test drafts used ad-hoc job type names (`flaky.service`, `slow.job`) which were correctly rejected by domain allowlist validation (`job.AllowedJobTypes`).
- **Resolution**: Configured test scenarios to use registered and allowlisted job types (`report.generate`, `test.job`).

---

## 5. Formal Distributed Guarantees & Known Limitations

1. **At-Least-Once Delivery vs. Exactly-Once Execution**:
   - Forge guarantees at-least-once message delivery and atomic database state transitions.
   - If a worker claims a job, performs a non-idempotent external mutation (e.g. charging an external payment processor or sending an external email), and crashes before committing `state = 'completed'` to PostgreSQL:
     - The lease will expire and the Stale Job Reaper will re-dispatch the job to another worker.
     - The second worker will execute the handler again.
   - **Architectural Requirement**: Any handler performing non-idempotent external side effects must pass deterministic idempotency tokens (such as `job.ID.String() + "-" + strconv.Itoa(job.RetriesAttempted)`) to downstream third-party APIs.

2. **PostgreSQL as the Single Source of Truth**:
   - PostgreSQL is authoritative for all state. If PostgreSQL experiences a network partition, worker instances cannot claim leases or persist state.
   - Deliveries received during a database outage are NACKed with requeue (`requeue = true`) and deferred until PostgreSQL connectivity recovers.

3. **Clock Skew Assumptions**:
   - Fencing tokens and lease timeouts rely on database server time (`NOW()`) and bounded client heartbeats. To avoid clock drift anomalies in distributed clusters, database and worker nodes must synchronize time via NTP.

---

## 6. Skipped Tests Record

| Test | Reason for Skipping | Environment |
|---|---|---|
| None | All 14 tests executed and passed against real PostgreSQL and RabbitMQ instances. | Local / Integration |
| *Standard Sandbox Mode* | Tests are skipped if run in a restricted sandbox environment without loopback TCP network permissions (`dial tcp 127.0.0.1: connect: operation not permitted`). | Sandbox isolation without Docker network whitelist |

