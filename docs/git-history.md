# Forge Git History & Release Lineage

This document records the curated, chronological Git history of the **Forge** platform. Each commit represents an atomic, bisect-friendly milestone in the platform's architectural evolution, adhering to Conventional Commits standards.

---

## Commit Lineage Summary

| # | Commit Hash | Type | Subject | Verification Gate |
|---|-------------|------|---------|-------------------|
| 1 | `805a639` | `docs` | define initial Forge architecture, ADRs, and requirements | Markdown validation & ADR consistency |
| 2 | `4b59718` | `chore` | bootstrap Go workspace, local infrastructure, and initial migrations | `go test ./pkg/...`, Docker Compose config check |
| 3 | `f2c22e0` | `feat` | implement job domain model, persistence repositories, and REST API | `go test ./internal/domain/... ./internal/app/job/...` |
| 4 | `a7a81d7` | `feat` | implement transactional outbox relay and RabbitMQ publisher | `go test ./internal/app/outbox/...` |
| 5 | `deb1587` | `feat` | implement concurrent job worker engine and lease execution | `go test ./internal/app/worker/...` |
| 6 | `f4bed9b` | `feat` | add retry scheduling, dead-letter routing, and stale job reaper | `go build ./cmd/worker` |
| 7 | `930f01c` | `feat` | add Prometheus metrics, Redis rate limiting, and HTTP router server | `go test ./pkg/metrics/... ./internal/adapter/http/...` |
| 8 | `77de96b` | `test` | add 14-scenario end-to-end distributed reliability verification | `docs/test-report.md` (14/14 automated scenarios pass) |
| 9 | `5144003` | `perf` | add load benchmarking suite and query plan optimizations | k6 load tests, EXPLAIN ANALYZE query verification |
| 10 | `7cdb443` | `docs` | finalize portfolio documentation, release checklist, and MIT license | Documentation review & license verification |
| 11 | `2fcd2ed` | `security` | harden repository configuration, CI workflow, and audit report | Gitleaks secret scan (0 leaks), CI workflow validation |
| 12 | `deb77c6` | `fix` | resolve CI test failure via sequential execution and linter install | Local reproduction & verification |
| 13 | `acaba87` | `fix` | pin Go toolchain version in CI via go.mod to prevent export data mismatch | GitHub Actions CI green (Run 41473678530) |

---

## Detailed Commit Specifications

### Commit 1: Architecture & Design Decisions
- **Hash**: `805a639`
- **Subject**: `docs: define initial Forge architecture, ADRs, and requirements`
- **Scope**: Requirements specification, distributed systems architectural decisions (ADRs 0001–0006), failure scenario catalog, and API contract.
- **Rationale**: Establishes unambiguous technical contracts before any implementation code is introduced, ensuring alignment on transactional boundaries, lease tokens, and idempotency keys.

### Commit 2: Infrastructure Bootstrap & Migration Framework
- **Hash**: `4b59718`
- **Subject**: `chore: bootstrap Go workspace, local infrastructure, and initial migrations`
- **Scope**: Go module initialization, `pkg/config`, `pkg/logger`, `pkg/health`, `pkg/migrator`, `docker-compose.yml`, multi-stage `Dockerfile`, `Makefile`, and initial SQL migrations (`000001_init_schema`, `000002_add_aliases`).
- **Rationale**: Lays down developer ergonomics, local multi-container dependencies (Postgres, RabbitMQ, Redis), structured logging with secret redaction, and an embedded migration runner.

### Commit 3: Core Domain, PostgreSQL Persistence & API Handlers
- **Hash**: `f2c22e0`
- **Subject**: `feat: implement job domain model, persistence repositories, and REST API`
- **Scope**: `internal/domain/`, `internal/app/job/`, `internal/adapter/postgres/`, and `internal/adapter/http/` (job handler and RFC 7807 problem details).
- **Rationale**: Implements atomic submission transaction (job + idempotency record + outbox event), SHA-256 idempotency validation, strict state machine transitions, and cursor-based pagination.

### Commit 4: Transactional Outbox Relay & RabbitMQ Topology
- **Hash**: `a7a81d7`
- **Subject**: `feat: implement transactional outbox relay and RabbitMQ publisher`
- **Scope**: `internal/adapter/rabbitmq/`, `internal/app/outbox/`, `cmd/outbox-relay/`.
- **Rationale**: Bridges the database outbox table to the message broker using publisher confirms, bounded batch polling, `FOR UPDATE SKIP LOCKED` claims, and exponential backoff on nacks.

### Commit 5: Concurrent Worker Engine & Handlers
- **Hash**: `deb1587`
- **Subject**: `feat: implement concurrent job worker engine and lease execution`
- **Scope**: `internal/app/worker/`, `internal/app/worker/handlers/` (deterministic report summary handler).
- **Rationale**: Delivers bounded worker pools, optimistic lease claims (`attempt` fencing token), allowlist handler routing, and idempotent duplicate message discarding.

### Commit 6: Fault Recovery, Dead-Letter Routing & Reaper
- **Hash**: `f4bed9b`
- **Subject**: `feat: add retry scheduling, dead-letter routing, and stale job reaper`
- **Scope**: `internal/app/reaper/`, `cmd/worker/`.
- **Rationale**: Implements full-jitter exponential backoff, dead-letter queue routing for poison messages and exhausted retries, and an automated background lease reaper to recover orphaned in-flight jobs.

### Commit 7: Observability & Redis Rate Limiting
- **Hash**: `930f01c`
- **Subject**: `feat: add Prometheus metrics, Redis rate limiting, and HTTP router server`
- **Scope**: `pkg/metrics/`, `internal/adapter/redis/`, `internal/adapter/http/router.go`, `cmd/api/`.
- **Rationale**: Adds production observability via Prometheus metrics, correlation ID propagation across HTTP headers and logs, and sliding-window rate limiting with fail-open / fail-closed resiliency policies.

### Commit 8: End-to-End Reliability Verification
- **Hash**: `77de96b`
- **Subject**: `test: add 14-scenario end-to-end distributed reliability verification`
- **Scope**: `tests/e2e/reliability_test.go`, `docs/test-report.md`.
- **Rationale**: Proves platform resilience against network cuts, worker crashes, duplicate deliveries, lease expirations, and broker outages across 14 automated integration scenarios.

### Commit 9: Benchmarking & Query Optimization
- **Hash**: `5144003`
- **Subject**: `perf: add load benchmarking suite and query plan optimizations`
- **Scope**: `benchmarks/`, `cmd/bench/`, `migrations/000003_add_performance_indexes.*`, `docs/benchmark.md`.
- **Rationale**: Establishes reproducible baseline performance with k6 and synthetic workloads, optimizes PostgreSQL execution plans via composite and partial indexes, and records performance gains (2.4x throughput increase).

### Commit 10: Portfolio Documentation & Release Hygiene
- **Hash**: `7cdb443`
- **Subject**: `docs: finalize portfolio documentation, release checklist, and MIT license`
- **Scope**: `README.md`, `LICENSE`, `docs/release-checklist.md`.
- **Rationale**: Synthesizes the architecture, failure handling, benchmarks, and API usage into an industry-grade README with complete architectural diagrams and release readiness gates.

### Commit 11: Security Hardening & CI Workflow
- **Hash**: `2fcd2ed`
- **Subject**: `security: harden repository configuration, CI workflow, and audit report`
- **Scope**: `.gitleaks.toml`, `.github/workflows/ci.yml`, `docs/security-audit.md`, `docs/git-history.md`.
- **Rationale**: Integrates automated multi-service GitHub Actions CI, configures secret detection rules, and seals the security audit ensuring zero exposed secrets or sensitive metadata.

### Commit 12 & 13: CI Reliability & Toolchain Hardening
- **Hashes**: `deb77c6`, `acaba87`
- **Subjects**: `fix: resolve CI test failure`, `fix: pin Go toolchain version in CI via go.mod to prevent export data mismatch`
- **Scope**: `.github/workflows/ci.yml`.
- **Rationale**: Enforces sequential package test execution (`-p 1`) to eliminate race conditions across shared test database tables and message queues; pins runner Go toolchain to `go.mod` (`go-version-file: 'go.mod'`) to ensure compiler export data format compatibility in static analysis tools. Verified passing on GitHub Actions run `41473678530`.
