# Forge v1.0.0 Production Release Checklist

This checklist defines the operational verification, hygiene checks, and release gates required prior to tagging and publishing `v1.0.0` of Forge.

---

## 1. Codebase & Repository Hygiene

- [x] **No Hardcoded Secrets**: Scanned repository for credentials, private keys, authorization tokens, or internal IP addresses.
- [x] **Environment Sanitization**: `.env` is excluded in `.gitignore`; `.env.example` contains only synthetic placeholders and development defaults.
- [x] **Build Artifacts Excluded**: `bin/`, `dist/`, `tmp/`, `coverage.out`, and `.DS_Store` are ignored by `.gitignore`.
- [x] **Permissive Open-Source License**: `LICENSE` file contains the complete MIT License with current copyright.
- [x] **Dependency Version Pinning**: `go.mod` and `go.sum` have all dependencies locked to compatible, vetted versions.
- [x] **Documentation Accuracy**: All documented commands, URLs, endpoints, and status codes reflect actual implementation without aspirational claims.

---

## 2. Distributed Correctness & Test Verification

- [x] **Race Detector Cleanliness**: `go test -race ./pkg/... ./internal/domain/...` passes with zero data race warnings.
- [x] **Static Analysis Compliance**: `make lint` passes cleanly with `golangci-lint`, `go vet`, and `gofmt`.
- [x] **All Platform Binaries Build**: `make build` compiles `api`, `worker`, `outbox-relay`, `migrate`, and `bench` without compiler warnings.
- [x] **14/14 E2E Resilience Scenarios Passed**: `tests/e2e/reliability_test.go` verifies all failure scenarios in `docs/failure-scenarios.md`:
  - `TestE2E_01_SubmitAndObserveSuccessfulCompletion` (Happy path pipeline)
  - `TestE2E_02_SubmitIdenticalIdempotentRequestsConcurrently` (PostgreSQL lock serialization)
  - `TestE2E_03_ReuseIdempotencyKeyWithDifferentPayloads` (409 Conflict rejection)
  - `TestE2E_04_RestartAPIDuringNormalOperation` (Stateless restart)
  - `TestE2E_05_StopRabbitMQWhileOutboxPendingThenRestore` (Broker crash buffering)
  - `TestE2E_06_SimulateDuplicateEventPublication` (At-least-once deduplication)
  - `TestE2E_07_TerminateWorkerAfterReceivingMessage` (AMQP requeue & replacement worker)
  - `TestE2E_08_SimulateRetryableJobFailureFollowedBySuccess` (Exponential backoff)
  - `TestE2E_09_ExhaustRetryBudget` (Terminal dead-letter transition)
  - `TestE2E_10_RecoverStaleProcessingJob` (Reaper & fencing token rejection)
  - `TestE2E_11_RunMultipleWorkersConcurrently` (SKIP LOCKED partition-free claims)
  - `TestE2E_12_VerifyTerminalJobsNotAccidentallyReExecuted` (Idempotent terminal state ACKs)
  - `TestE2E_13_ShutDownServicesDuringActiveProcessing` (Graceful drain & commit)
  - `TestE2E_14_VerifyDatabaseUnavailabilityNoFalseSuccessNoLoops` (Clean fail-fast)

---

## 3. Database Schema & Migration Verifications

- [x] **Initial Schema Migration**: `000001_init_schema.up.sql` applies tables, foreign keys, check constraints, and partial indexes.
- [x] **Schema Aliases Migration**: `000002_add_aliases.up.sql` defines operational views (`idempotency_keys`, `outbox_events`).
- [x] **Performance Indexes Migration**: `000003_add_performance_indexes.up.sql` adds B-tree index on `job_executions(job_id, attempt)` and outbox metrics index.
- [x] **Migration Rollbacks Tested**: All migrations provide matching `.down.sql` files for automated rollbacks.

---

## 4. Performance & SLO Validation

- [x] **Ingestion Throughput Target**: Measured **2,267.94 RPS** (exceeds $\ge 1,000\text{ RPS}$ target by 126.8%).
- [x] **Status Polling Target**: Measured **8,590.65 RPS** with p95 latency of **9.52 ms** (exceeds $\le 20\text{ ms}$ SLO).
- [x] **Zero Errors Under Load**: 100% successful HTTP 200/202 responses under sustained 50 concurrent virtual users.
- [x] **Performance Report Published**: Detailed baseline vs. optimized comparisons documented in `docs/benchmark.md`.

---

## 5. Deployment & Container Artifacts

- [x] **Multi-Stage Dockerfile**: Builds non-root, lightweight production container images with separate runtime targets (`api`, `worker`, `outbox-relay`).
- [x] **Docker Compose Stack**: `docker-compose.yml` configures PostgreSQL 16, RabbitMQ 3.13, and Redis 7 with container healthchecks.
- [x] **GitHub Actions CI**: `.github/workflows/ci.yml` validates formatting, vet, linting, tests, and multi-stage container builds on every commit.

---

## 6. Pre-Publish Release Actions (Upon User Approval)

- [ ] Initialize git tag: `git tag -a v1.0.0 -m "Forge v1.0.0: High-Reliability Distributed Background Job Processing Platform"`
- [ ] Push main branch and tags: `git push origin main --tags`
- [ ] Publish GitHub Release with changelog referencing `docs/test-report.md` and `docs/benchmark.md`.

