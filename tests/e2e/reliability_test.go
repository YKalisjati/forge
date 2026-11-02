package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	httpAdapter "github.com/YKalisjati/forge/internal/adapter/http"
	"github.com/YKalisjati/forge/internal/adapter/postgres"
	"github.com/YKalisjati/forge/internal/adapter/rabbitmq"
	appJob "github.com/YKalisjati/forge/internal/app/job"
	appOutbox "github.com/YKalisjati/forge/internal/app/outbox"
	"github.com/YKalisjati/forge/internal/app/reaper"
	appWorker "github.com/YKalisjati/forge/internal/app/worker"
	"github.com/YKalisjati/forge/internal/app/worker/handlers"
	domainJob "github.com/YKalisjati/forge/internal/domain/job"
	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/health"
	"github.com/YKalisjati/forge/pkg/metrics"
)

// testEnv encapsulates the shared test infrastructure for E2E scenarios.
type testEnv struct {
	cfg         *config.Config
	pgClient    *postgres.Client
	rmqClient   *rabbitmq.Client
	pool        *pgxpool.Pool
	uow         *postgres.UnitOfWork
	jobRepo     *postgres.JobRepository
	outboxRepo  *postgres.OutboxRepository
	idempRepo   *postgres.IdempotencyRepository
	jobService  *appJob.Service
	jobHandler  *httpAdapter.JobHandler
	router      http.Handler
	healthReg   *health.Registry
	promMetrics *metrics.Metrics
	queueName   string
}

// setupE2E initializes PostgreSQL, RabbitMQ, and API components with isolated queues.
func setupE2E(t *testing.T) (*testEnv, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pgClient, err := postgres.NewClient(initCtx, &cfg.Postgres)
	if err != nil {
		t.Skipf("skipping E2E test: postgresql unreachable: %v", err)
	}

	rmqClient, err := rabbitmq.NewClient(initCtx, &cfg.RabbitMQ)
	if err != nil {
		pgClient.Close()
		t.Skipf("skipping E2E test: rabbitmq unreachable: %v", err)
	}

	if err := rabbitmq.DeclareTopology(initCtx, rmqClient, &cfg.RabbitMQ); err != nil {
		pgClient.Close()
		_ = rmqClient.Close()
		t.Fatalf("failed to declare rabbitmq topology: %v", err)
	}

	// Purge main and DLQ queues and clean database tables to ensure clean state
	ch, err := rmqClient.Connection().Channel()
	if err == nil {
		_, _ = ch.QueuePurge(cfg.RabbitMQ.Queue, false)
		_, _ = ch.QueuePurge(cfg.RabbitMQ.DLQ, false)
		_ = ch.Close()
	}

	pool := pgClient.Pool()
	_, _ = pool.Exec(initCtx, `
		DELETE FROM outbox_messages;
		DELETE FROM job_executions;
		DELETE FROM jobs;
		DELETE FROM idempotency_keys;
	`)

	uow := postgres.NewUnitOfWork(pool)
	jobRepo := postgres.NewJobRepository(pool)
	outboxRepo := postgres.NewOutboxRepository(pool)
	idempRepo := postgres.NewIdempotencyRepository(pool)

	jobService := appJob.NewService(uow, jobRepo, outboxRepo, idempRepo, cfg.API.MaxPayloadBytes)
	jobHandler := httpAdapter.NewJobHandler(jobService, cfg.API.MaxPayloadBytes)
	healthReg := health.NewRegistry("forge-e2e")
	promMetrics := metrics.New(nil)

	router := httpAdapter.NewRouter(httpAdapter.ServerOptions{
		Registry:   healthReg,
		JobHandler: jobHandler,
		Metrics:    promMetrics,
	})

	env := &testEnv{
		cfg:         cfg,
		pgClient:    pgClient,
		rmqClient:   rmqClient,
		pool:        pool,
		uow:         uow,
		jobRepo:     jobRepo,
		outboxRepo:  outboxRepo,
		idempRepo:   idempRepo,
		jobService:  jobService,
		jobHandler:  jobHandler,
		router:      router,
		healthReg:   healthReg,
		promMetrics: promMetrics,
		queueName:   cfg.RabbitMQ.Queue,
	}

	cleanup := func() {
		pgClient.Close()
		_ = rmqClient.Close()
	}

	return env, cleanup
}

// pollJobState deterministically polls the database until job reaches targetState or times out.
func (e *testEnv) pollJobState(t *testing.T, jobID uuid.UUID, targetState domainJob.State, timeout time.Duration) *domainJob.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		j, err := e.jobRepo.GetByID(ctx, jobID)
		cancel()
		if err == nil && j.State == targetState {
			return &j.Job
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Final fetch for failure diagnosis
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	j, err := e.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("timeout waiting for job %s state %q: failed to fetch: %v", jobID, targetState, err)
	}
	lastErrStr := "<nil>"
	if j.LastError != nil {
		lastErrStr = *j.LastError
	}
	t.Fatalf("timeout waiting for job %s state %q; current state: %q (retries: %d, last_error: %s)",
		jobID, targetState, j.State, j.RetriesAttempted, lastErrStr)
	return nil
}

// pollOutboxStatus deterministically polls the outbox until event reaches targetStatus or times out.
func (e *testEnv) pollOutboxStatus(t *testing.T, jobID uuid.UUID, targetStatus domainOutbox.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		var status string
		err := e.pool.QueryRow(ctx, "SELECT status FROM outbox_messages WHERE job_id = $1 ORDER BY id DESC LIMIT 1;", jobID).Scan(&status)
		cancel()
		if err == nil && domainOutbox.Status(status) == targetStatus {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for outbox event for job %s to reach status %q", jobID, targetStatus)
}

// submitJobHelper issues an HTTP POST /v1/jobs request against the test router.
func (e *testEnv) submitJobHelper(t *testing.T, idempotencyKey string, body string) (int, *httpAdapter.SubmitJobResponse, http.Header) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)

	var res httpAdapter.SubmitJobResponse
	if rec.Code == http.StatusAccepted {
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
	}
	return rec.Code, &res, rec.Header()
}

type mockPublisher struct {
	publishFunc func(ctx context.Context, envelope *domainOutbox.Envelope) error
	closeFunc   func() error
}

func (m *mockPublisher) Publish(ctx context.Context, envelope *domainOutbox.Envelope) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, envelope)
	}
	return nil
}

func (m *mockPublisher) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// Scenario 1: Submit a job and observe successful completion end-to-end.
func TestE2E_01_SubmitAndObserveSuccessfulCompletion(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start Outbox Relay
	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	relay := appOutbox.NewRelay(env.outboxRepo, publisher, nil, nil, appOutbox.Config{
		PollInterval:   20 * time.Millisecond,
		BatchSize:      10,
		PublishTimeout: 2 * time.Second,
	})
	go func() { _ = relay.Run(ctx) }()

	// 2. Start Worker
	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(nil))
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue:         env.queueName,
		Concurrency:   2,
		LeaseDuration: 10 * time.Second,
	})
	go func() { _ = worker.Run(ctx) }()

	// 3. Submit Job via API
	idempKey := "e2e-01-" + uuid.NewString()
	body := `{"type": "report.generate", "payload": {"dataset": "q4_financials", "format": "pdf"}}`
	code, submitRes, _ := env.submitJobHelper(t, idempKey, body)

	if code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", code)
	}

	// 4. Poll until state is completed
	completedJob := env.pollJobState(t, submitRes.ID, domainJob.StateCompleted, 5*time.Second)
	if completedJob.State != domainJob.StateCompleted {
		t.Errorf("expected state completed, got %s", completedJob.State)
	}

	// 5. Verify outbox message is marked published
	env.pollOutboxStatus(t, submitRes.ID, domainOutbox.StatusPublished, 3*time.Second)

	// 6. Verify job_executions audit log
	var execStatus string
	var execDuration int
	err := env.pool.QueryRow(ctx, "SELECT status, duration_ms FROM job_executions WHERE job_id = $1;", submitRes.ID).Scan(&execStatus, &execDuration)
	if err != nil {
		t.Fatalf("failed to query job_executions: %v", err)
	}
	if execStatus != "completed" {
		t.Errorf("expected execution audit status 'completed', got %q", execStatus)
	}
}

// Scenario 2: Submit identical idempotent requests concurrently.
func TestE2E_02_SubmitIdenticalIdempotentRequestsConcurrently(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	idempKey := "e2e-02-concurrent-" + uuid.NewString()
	body := `{"type": "report.generate", "payload": {"report_id": 999}}`

	concurrency := 10
	var wg sync.WaitGroup
	statusCodes := make([]int, concurrency)
	jobIDs := make([]uuid.UUID, concurrency)
	replayedHeaders := make([]string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			code, res, header := env.submitJobHelper(t, idempKey, body)
			statusCodes[idx] = code
			if res != nil {
				jobIDs[idx] = res.ID
			}
			replayedHeaders[idx] = header.Get("Idempotent-Replayed")
		}(i)
	}
	wg.Wait()

	creatorCount := 0
	replayedCount := 0
	var canonicalJobID uuid.UUID

	for i := 0; i < concurrency; i++ {
		if statusCodes[i] != http.StatusAccepted {
			t.Errorf("goroutine %d: expected 202 Accepted, got %d", i, statusCodes[i])
		}
		if i == 0 {
			canonicalJobID = jobIDs[i]
		} else if jobIDs[i] != canonicalJobID {
			t.Errorf("goroutine %d: returned different job ID %s (expected %s)", i, jobIDs[i], canonicalJobID)
		}

		if replayedHeaders[i] == "true" {
			replayedCount++
		} else {
			creatorCount++
		}
	}

	if creatorCount != 1 {
		t.Errorf("expected exactly 1 creator response, got %d", creatorCount)
	}
	if replayedCount != concurrency-1 {
		t.Errorf("expected %d replayed responses, got %d", concurrency-1, replayedCount)
	}

	// Verify database uniqueness
	ctx := context.Background()
	var jobCount int
	_ = env.pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE id = $1;", canonicalJobID).Scan(&jobCount)
	if jobCount != 1 {
		t.Errorf("expected exactly 1 job in database, found %d", jobCount)
	}

	var outboxCount int
	_ = env.pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_messages WHERE job_id = $1;", canonicalJobID).Scan(&outboxCount)
	if outboxCount != 1 {
		t.Errorf("expected exactly 1 outbox event in database, found %d", outboxCount)
	}
}

// Scenario 3: Reuse an idempotency key with different payloads.
func TestE2E_03_ReuseIdempotencyKeyWithDifferentPayloads(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	idempKey := "e2e-03-conflict-" + uuid.NewString()
	body1 := `{"type": "report.generate", "payload": {"version": 1}}`
	body2 := `{"type": "report.generate", "payload": {"version": 2}}`

	// First submission succeeds
	code1, res1, _ := env.submitJobHelper(t, idempKey, body1)
	if code1 != http.StatusAccepted {
		t.Fatalf("first request expected 202, got %d", code1)
	}

	// Second submission with differing payload must return 409 Conflict
	code2, _, _ := env.submitJobHelper(t, idempKey, body2)
	if code2 != http.StatusConflict {
		t.Fatalf("reused key with differing payload expected 409 Conflict, got %d", code2)
	}

	// Verify database only has original job
	ctx := context.Background()
	var count int
	_ = env.pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1;", idempKey).Scan(&count)
	if count != 1 {
		t.Errorf("expected exactly 1 job in database, got %d", count)
	}

	var storedPayload []byte
	_ = env.pool.QueryRow(ctx, "SELECT payload FROM jobs WHERE id = $1;", res1.ID).Scan(&storedPayload)
	if !bytes.Contains(storedPayload, []byte(`"version": 1`)) {
		t.Errorf("expected payload with version 1, got %s", string(storedPayload))
	}
}

// Scenario 4: Restart the API during normal operation.
func TestE2E_04_RestartAPIDuringNormalOperation(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	// 1. Ingest job 1 via API instance 1
	idempKey1 := "e2e-04-job1-" + uuid.NewString()
	code1, res1, _ := env.submitJobHelper(t, idempKey1, `{"type": "report.generate", "payload": {"batch": 1}}`)
	if code1 != http.StatusAccepted {
		t.Fatalf("job 1 submission failed: %d", code1)
	}

	// 2. Simulate API instance 1 shutdown and restart instance 2
	apiInstance2 := httpAdapter.NewRouter(httpAdapter.ServerOptions{
		Registry:   env.healthReg,
		JobHandler: env.jobHandler,
		Metrics:    env.promMetrics,
	})

	// 3. Ingest job 2 via restarted API instance 2
	idempKey2 := "e2e-04-job2-" + uuid.NewString()
	req2 := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(`{"type": "report.generate", "payload": {"batch": 2}}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", idempKey2)
	rec2 := httptest.NewRecorder()
	apiInstance2.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("job 2 submission on restarted API failed: %d", rec2.Code)
	}
	var res2 httpAdapter.SubmitJobResponse
	_ = json.Unmarshal(rec2.Body.Bytes(), &res2)

	// 4. Query job 1 and job 2 from restarted API
	getReq1 := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+res1.ID.String(), nil)
	getRec1 := httptest.NewRecorder()
	apiInstance2.ServeHTTP(getRec1, getReq1)
	if getRec1.Code != http.StatusOK {
		t.Errorf("failed to fetch job 1 from restarted API: %d", getRec1.Code)
	}

	getReq2 := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+res2.ID.String(), nil)
	getRec2 := httptest.NewRecorder()
	apiInstance2.ServeHTTP(getRec2, getReq2)
	if getRec2.Code != http.StatusOK {
		t.Errorf("failed to fetch job 2 from restarted API: %d", getRec2.Code)
	}
}

// Scenario 5: Stop RabbitMQ while outbox records are pending, then restore it.
func TestE2E_05_StopRabbitMQWhileOutboxPendingThenRestore(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Ingest job via API into PostgreSQL
	idempKey := "e2e-05-outbox-" + uuid.NewString()
	code, submitRes, _ := env.submitJobHelper(t, idempKey, `{"type": "report.generate", "payload": {"data": "buffered"}}`)
	if code != http.StatusAccepted {
		t.Fatalf("job submission failed: %d", code)
	}

	// 2. Initialize Relay with failing publisher simulating RabbitMQ broker outage
	failingPublisher := &mockPublisher{
		publishFunc: func(_ context.Context, _ *domainOutbox.Envelope) error {
			return errors.New("dial tcp: connection refused (broker down)")
		},
	}
	relayFailing := appOutbox.NewRelay(env.outboxRepo, failingPublisher, nil, nil, appOutbox.Config{
		BatchSize:      10,
		PublishTimeout: 500 * time.Millisecond,
	})

	// Process batch with broker down
	count, err := relayFailing.ProcessBatch(ctx)
	if err != nil {
		t.Fatalf("process batch should not return fatal error on message publish failure: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 published messages during outage, got %d", count)
	}

	// Verify outbox message is STILL pending in PostgreSQL with retry_count incremented
	var status string
	var retries int
	err = env.pool.QueryRow(ctx, "SELECT status, retry_count FROM outbox_messages WHERE job_id = $1;", submitRes.ID).Scan(&status, &retries)
	if err != nil {
		t.Fatalf("failed to query outbox record: %v", err)
	}
	if domainOutbox.Status(status) != domainOutbox.StatusPending {
		t.Errorf("expected outbox message status 'pending', got %q", status)
	}
	if retries < 1 {
		t.Errorf("expected retry_count >= 1, got %d", retries)
	}

	// 3. Broker Restored: Initialize Relay with live RabbitMQ publisher
	livePublisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = livePublisher.Close() }()

	relayRestored := appOutbox.NewRelay(env.outboxRepo, livePublisher, nil, nil, appOutbox.Config{
		BatchSize:      10,
		PublishTimeout: 2 * time.Second,
	})

	// Reset scheduled_at to NOW() so relay claims it immediately
	_, _ = env.pool.Exec(ctx, "UPDATE outbox_messages SET scheduled_at = NOW() WHERE job_id = $1;", submitRes.ID)

	publishedCount, pubErr := relayRestored.ProcessBatch(ctx)
	if pubErr != nil {
		t.Fatalf("process batch failed after broker recovery: %v", pubErr)
	}
	if publishedCount != 1 {
		t.Fatalf("expected 1 message published upon broker recovery, got %d", publishedCount)
	}

	// 4. Verify outbox message is marked published
	env.pollOutboxStatus(t, submitRes.ID, domainOutbox.StatusPublished, 3*time.Second)

	// 5. Worker consumes and completes the job
	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(nil))
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() { _ = worker.Run(workerCtx) }()

	env.pollJobState(t, submitRes.ID, domainJob.StateCompleted, 5*time.Second)
}

// Scenario 6: Simulate duplicate event publication.
func TestE2E_06_SimulateDuplicateEventPublication(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	ctx := context.Background()

	// Ingest job directly
	jobQuery := `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'report.generate', '{"dup": true}', 'pending', 10, 3, 0, NOW(), NOW());
	`
	if _, err := env.pool.Exec(ctx, jobQuery, jobID); err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	// Publish duplicate messages to RabbitMQ
	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	envelope := &domainOutbox.Envelope{
		EventID:       1001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"dup": true}`),
		Timestamp:     time.Now().UTC(),
	}

	// Publish delivery 1 and delivery 2
	if err := publisher.Publish(ctx, envelope); err != nil {
		t.Fatalf("failed to publish delivery 1: %v", err)
	}
	if err := publisher.Publish(ctx, envelope); err != nil {
		t.Fatalf("failed to publish delivery 2: %v", err)
	}

	// Track handler execution count
	var execCount int64
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", appWorker.HandlerFunc(func(_ context.Context, _ *domainJob.Job) error {
		atomic.AddInt64(&execCount, 1)
		return nil
	}))

	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() { _ = worker.Run(workerCtx) }()

	// Wait for job completion
	env.pollJobState(t, jobID, domainJob.StateCompleted, 5*time.Second)

	// Allow short window for second delivery to be safely ACKed
	time.Sleep(100 * time.Millisecond)

	// Assert handler executed EXACTLY once
	finalExecs := atomic.LoadInt64(&execCount)
	if finalExecs != 1 {
		t.Errorf("expected handler to execute exactly 1 time, executed %d times", finalExecs)
	}

	// Verify only 1 row exists in job_executions
	var rowCount int
	_ = env.pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_executions WHERE job_id = $1;", jobID).Scan(&rowCount)
	if rowCount != 1 {
		t.Errorf("expected 1 execution record, got %d", rowCount)
	}
}

// Scenario 7: Terminate a worker after receiving a message.
func TestE2E_07_TerminateWorkerAfterReceivingMessage(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	ctx := context.Background()

	// Ingest job
	_, err := env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'report.generate', '{"worker_crash": true}', 'pending', 10, 3, 0, NOW(), NOW());
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	envelope := &domainOutbox.Envelope{
		EventID:       2001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"worker_crash": true}`),
		Timestamp:     time.Now().UTC(),
	}
	if err := publisher.Publish(ctx, envelope); err != nil {
		t.Fatalf("failed to publish envelope: %v", err)
	}

	// 1. Worker 1 simulates crash before completion commit via OnBeforeComplete hook
	consumer1 := rabbitmq.NewConsumer(env.rmqClient)
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(nil))

	worker1 := appWorker.NewWorker(env.jobRepo, consumer1, registry, nil, nil, appWorker.Config{
		Queue:         env.queueName,
		Concurrency:   1,
		LeaseDuration: 50 * time.Millisecond,
	})

	w1Ctx, cancelW1 := context.WithCancel(context.Background())
	var crashed int32
	worker1.OnBeforeComplete = func(_ *domainJob.Job, _ uuid.UUID) error {
		if atomic.CompareAndSwapInt32(&crashed, 0, 1) {
			_ = consumer1.Close()
			cancelW1()
			return errors.New("simulated fatal worker crash before DB commit")
		}
		return nil
	}

	w1Done := make(chan struct{})
	go func() {
		_ = worker1.Run(w1Ctx)
		close(w1Done)
	}()

	// Wait for worker 1 to crash and exit
	select {
	case <-w1Done:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker 1 did not exit in time")
	}

	// 2. Clear lease and reset to pending so worker 2 can claim requeued message
	_, _ = env.pool.Exec(ctx, "UPDATE jobs SET state = 'pending', execution_lease_id = NULL, lease_expires_at = NULL WHERE id = $1;", jobID)

	// 3. Worker 2 starts clean and processes requeued message
	consumer2 := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer2.Close() }()
	worker2 := appWorker.NewWorker(env.jobRepo, consumer2, registry, nil, nil, appWorker.Config{
		Queue:         env.queueName,
		Concurrency:   1,
		LeaseDuration: 10 * time.Second,
	})

	w2Ctx, cancelW2 := context.WithCancel(context.Background())
	defer cancelW2()
	go func() { _ = worker2.Run(w2Ctx) }()

	// Poll until completed
	completed := env.pollJobState(t, jobID, domainJob.StateCompleted, 5*time.Second)
	if completed.State != domainJob.StateCompleted {
		t.Errorf("expected job to complete under worker 2, got %s", completed.State)
	}
}

// Scenario 8: Simulate a retryable job failure followed by success.
func TestE2E_08_SimulateRetryableJobFailureFollowedBySuccess(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	ctx := context.Background()

	// Ingest job with max_retries = 3
	_, err := env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'test.job', '{"test": 8}', 'pending', 10, 3, 0, NOW(), NOW());
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	envelope := &domainOutbox.Envelope{
		EventID:       3001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		Payload:       []byte(`{"test": 8}`),
		Timestamp:     time.Now().UTC(),
	}
	if err := publisher.Publish(ctx, envelope); err != nil {
		t.Fatalf("failed to publish envelope: %v", err)
	}

	// Flaky handler fails on attempt 1, succeeds on attempt 2
	var attemptCount int64
	registry := appWorker.NewRegistry()
	_ = registry.Register("test.job", appWorker.HandlerFunc(func(_ context.Context, _ *domainJob.Job) error {
		attempt := atomic.AddInt64(&attemptCount, 1)
		if attempt == 1 {
			return errors.New("transient downstream timeout")
		}
		return nil
	}))

	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() { _ = worker.Run(workerCtx) }()

	// Wait for attempt 1 failure and retry scheduling
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		var retries int
		_ = env.pool.QueryRow(ctx, "SELECT state, retries_attempted FROM jobs WHERE id = $1;", jobID).Scan(&state, &retries)
		if state == "pending" && retries == 1 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	// Force next attempt immediately
	_, _ = env.pool.Exec(ctx, "UPDATE jobs SET next_attempt_at = NOW() WHERE id = $1;", jobID)

	// Publish retry attempt delivery
	retryEnv := &domainOutbox.Envelope{
		EventID:       3002,
		JobID:         jobID,
		EventType:     "job.scheduled",
		SchemaVersion: 1,
		Payload:       []byte(`{"test": 8}`),
		Timestamp:     time.Now().UTC(),
	}
	_ = publisher.Publish(ctx, retryEnv)

	// Poll until completed on attempt 2
	completed := env.pollJobState(t, jobID, domainJob.StateCompleted, 5*time.Second)
	if completed.RetriesAttempted < 1 {
		t.Errorf("expected retries_attempted >= 1, got %d", completed.RetriesAttempted)
	}

	// Verify job_executions audit log has exactly 2 records
	var execCount int
	_ = env.pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_executions WHERE job_id = $1;", jobID).Scan(&execCount)
	if execCount != 2 {
		t.Errorf("expected 2 execution records in job_executions, got %d", execCount)
	}
}

// Scenario 9: Exhaust the retry budget.
func TestE2E_09_ExhaustRetryBudget(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	ctx := context.Background()

	// Ingest job with max_retries = 1
	_, err := env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'test.job', '{"fail": true}', 'pending', 10, 1, 0, NOW(), NOW());
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	envelope := &domainOutbox.Envelope{
		EventID:       4001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		Payload:       []byte(`{"fail": true}`),
		Timestamp:     time.Now().UTC(),
	}
	_ = publisher.Publish(ctx, envelope)

	// Always failing handler
	registry := appWorker.NewRegistry()
	_ = registry.Register("test.job", appWorker.HandlerFunc(func(_ context.Context, _ *domainJob.Job) error {
		return errors.New("permanent transient failure")
	}))

	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() { _ = worker.Run(workerCtx) }()

	// Wait for attempt 1 failure
	time.Sleep(100 * time.Millisecond)

	// Trigger second attempt
	_, _ = env.pool.Exec(ctx, "UPDATE jobs SET next_attempt_at = NOW() WHERE id = $1;", jobID)
	_ = publisher.Publish(ctx, &domainOutbox.Envelope{
		EventID:       4002,
		JobID:         jobID,
		EventType:     "job.scheduled",
		SchemaVersion: 1,
		Payload:       []byte(`{"fail": true}`),
		Timestamp:     time.Now().UTC(),
	})

	// Poll until dead_letter
	dlJob := env.pollJobState(t, jobID, domainJob.StateDeadLetter, 5*time.Second)
	if dlJob.State != domainJob.StateDeadLetter {
		t.Errorf("expected state dead_letter, got %s", dlJob.State)
	}
}

// Scenario 10: Recover a stale processing job.
func TestE2E_10_RecoverStaleProcessingJob(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	staleLease := uuid.New()
	ctx := context.Background()

	// Ingest job directly in running state with an expired lease
	pastTime := time.Now().Add(-5 * time.Second)
	_, err := env.pool.Exec(ctx, `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted,
			execution_lease_id, lease_expires_at, created_at, updated_at
		) VALUES (
			$1, 'report.generate', '{"stale": true}', 'running', 10, 3, 0,
			$2, $3, NOW(), NOW()
		);
	`, jobID, staleLease, pastTime)
	if err != nil {
		t.Fatalf("failed to insert stale job: %v", err)
	}

	// Insert running execution log for the stale lease
	_, _ = env.pool.Exec(ctx, `
		INSERT INTO job_executions (job_id, attempt, execution_lease_id, worker_id, status, started_at)
		VALUES ($1, 0, $2, 'stale-worker', 'running', NOW() - INTERVAL '10 seconds');
	`, jobID, staleLease)

	// 1. Run Reaper: must detect expired lease and recover job to pending
	r := reaper.NewReaper(env.jobRepo, nil, nil, reaper.Config{BatchSize: 10})
	recovered, _, _, reapErr := r.RunOnce(ctx)
	if reapErr != nil {
		t.Fatalf("reaper failed: %v", reapErr)
	}
	if recovered != 1 {
		t.Errorf("expected 1 recovered stale job, got %d", recovered)
	}

	// 2. Fencing Check: If zombie worker attempts to CompleteJob with stale lease, it must be rejected!
	fencingErr := env.jobRepo.CompleteJob(ctx, jobID, staleLease, 100*time.Millisecond)
	if !errors.Is(fencingErr, domainJob.ErrFencingViolation) {
		t.Fatalf("expected ErrFencingViolation for stale worker, got %v", fencingErr)
	}

	// 3. New Worker claims and completes the job
	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(nil))
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	_ = publisher.Publish(ctx, &domainOutbox.Envelope{
		EventID:       5001,
		JobID:         jobID,
		EventType:     "job.scheduled",
		SchemaVersion: 1,
		Payload:       []byte(`{"stale": true}`),
		Timestamp:     time.Now().UTC(),
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() { _ = worker.Run(workerCtx) }()

	completed := env.pollJobState(t, jobID, domainJob.StateCompleted, 5*time.Second)
	if completed.State != domainJob.StateCompleted {
		t.Errorf("expected recovered job to complete, got %s", completed.State)
	}
}

// Scenario 11: Run multiple workers concurrently.
func TestE2E_11_RunMultipleWorkersConcurrently(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	totalJobs := 15
	jobIDs := make([]uuid.UUID, totalJobs)

	// 1. Ingest 15 jobs via API
	for i := 0; i < totalJobs; i++ {
		idempKey := fmt.Sprintf("e2e-11-multi-%d-%s", i, uuid.NewString())
		body := fmt.Sprintf(`{"type": "report.generate", "payload": {"index": %d}}`, i)
		code, res, _ := env.submitJobHelper(t, idempKey, body)
		if code != http.StatusAccepted {
			t.Fatalf("failed to submit job %d: %d", i, code)
		}
		jobIDs[i] = res.ID
	}

	// 2. Start Outbox Relay to publish all 15 events
	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	relay := appOutbox.NewRelay(env.outboxRepo, publisher, nil, nil, appOutbox.Config{
		PollInterval:   20 * time.Millisecond,
		BatchSize:      20,
		PublishTimeout: 2 * time.Second,
	})
	go func() { _ = relay.Run(ctx) }()

	// 3. Start 3 Worker instances concurrently
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(nil))

	for w := 0; w < 3; w++ {
		c := rabbitmq.NewConsumer(env.rmqClient)
		defer func() { _ = c.Close() }()
		worker := appWorker.NewWorker(env.jobRepo, c, registry, nil, nil, appWorker.Config{
			Queue:       env.queueName,
			Concurrency: 5,
		})
		worker.SetWorkerID(fmt.Sprintf("worker-pool-%d", w))
		go func(wInst *appWorker.Worker) {
			_ = wInst.Run(ctx)
		}(worker)
	}

	// 4. Assert all 15 jobs complete
	for i, id := range jobIDs {
		j := env.pollJobState(t, id, domainJob.StateCompleted, 8*time.Second)
		if j.State != domainJob.StateCompleted {
			t.Errorf("job %d (%s) failed to reach completed state", i, id)
		}
	}
}

// Scenario 12: Verify terminal jobs are not accidentally re-executed.
func TestE2E_12_VerifyTerminalJobsNotAccidentallyReExecuted(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	ctx := context.Background()

	// Ingest terminal job 1 (completed) and terminal job 2 (dead_letter)
	job1 := uuid.New()
	job2 := uuid.New()

	_, _ = env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'report.generate', '{}', 'completed', 10, 3, 0, NOW(), NOW());
	`, job1)

	_, _ = env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'report.generate', '{}', 'dead_letter', 10, 3, 3, NOW(), NOW());
	`, job2)

	// Publish deliveries for both terminal jobs
	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	_ = publisher.Publish(ctx, &domainOutbox.Envelope{
		EventID: 6001, JobID: job1, EventType: "job.created", SchemaVersion: 1, Payload: []byte(`{}`), Timestamp: time.Now().UTC(),
	})
	_ = publisher.Publish(ctx, &domainOutbox.Envelope{
		EventID: 6002, JobID: job2, EventType: "job.created", SchemaVersion: 1, Payload: []byte(`{}`), Timestamp: time.Now().UTC(),
	})

	var executionCounter int64
	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", appWorker.HandlerFunc(func(_ context.Context, _ *domainJob.Job) error {
		atomic.AddInt64(&executionCounter, 1)
		return nil
	}))

	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() { _ = worker.Run(workerCtx) }()

	// Wait briefly for worker to consume both messages
	time.Sleep(200 * time.Millisecond)

	// Assert handler was NEVER invoked
	count := atomic.LoadInt64(&executionCounter)
	if count != 0 {
		t.Errorf("expected 0 handler executions for terminal jobs, got %d", count)
	}

	// Verify database states remain terminal
	var s1, s2 string
	_ = env.pool.QueryRow(ctx, "SELECT state FROM jobs WHERE id = $1;", job1).Scan(&s1)
	_ = env.pool.QueryRow(ctx, "SELECT state FROM jobs WHERE id = $1;", job2).Scan(&s2)
	if s1 != "completed" {
		t.Errorf("job 1 state modified: %s", s1)
	}
	if s2 != "dead_letter" {
		t.Errorf("job 2 state modified: %s", s2)
	}
}

// Scenario 13: Shut down services during active processing.
func TestE2E_13_ShutDownServicesDuringActiveProcessing(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	ctx := context.Background()

	_, err := env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'test.job', '{"duration": 100}', 'pending', 10, 3, 0, NOW(), NOW());
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	_ = publisher.Publish(ctx, &domainOutbox.Envelope{
		EventID:       7001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		Payload:       []byte(`{}`),
		Timestamp:     time.Now().UTC(),
	})

	// Handler takes 80ms and finishes cleanly during shutdown drain window
	registry := appWorker.NewRegistry()
	_ = registry.Register("test.job", appWorker.HandlerFunc(func(_ context.Context, _ *domainJob.Job) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	}))

	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()
	worker := appWorker.NewWorker(env.jobRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue:           env.queueName,
		ShutdownTimeout: 2 * time.Second,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)

	go func() {
		workerDone <- worker.Run(workerCtx)
	}()

	// Wait 30ms for job execution to start, then trigger shutdown
	time.Sleep(30 * time.Millisecond)
	cancelWorker()

	// Worker should drain cleanly within shutdown timeout
	select {
	case err := <-workerDone:
		if err != nil {
			t.Errorf("worker exited with unexpected error on shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("worker failed to shut down within timeout")
	}

	// Verify the in-flight job completed successfully
	env.pollJobState(t, jobID, domainJob.StateCompleted, 2*time.Second)
}

// Scenario 14: Verify database unavailability does not cause false success or uncontrolled message loops.
func TestE2E_14_VerifyDatabaseUnavailabilityNoFalseSuccessNoLoops(t *testing.T) {
	env, cleanup := setupE2E(t)
	defer cleanup()

	jobID := uuid.New()
	ctx := context.Background()

	// Ingest job
	_, err := env.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at)
		VALUES ($1, 'report.generate', '{}', 'pending', 10, 3, 0, NOW(), NOW());
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	publisher := rabbitmq.NewPublisher(env.rmqClient, env.cfg.RabbitMQ.Exchange, env.cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()
	_ = publisher.Publish(ctx, &domainOutbox.Envelope{
		EventID:       8001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		Payload:       []byte(`{}`),
		Timestamp:     time.Now().UTC(),
	})

	// Create worker with closed/broken DB client to simulate DB outage
	brokenPool, _ := pgxpool.New(ctx, "postgres://invalid:invalid@127.0.0.1:59999/invalid?sslmode=disable")
	defer brokenPool.Close()

	brokenRepo := postgres.NewJobRepository(brokenPool)

	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(nil))

	consumer := rabbitmq.NewConsumer(env.rmqClient)
	defer func() { _ = consumer.Close() }()

	worker := appWorker.NewWorker(brokenRepo, consumer, registry, nil, nil, appWorker.Config{
		Queue: env.queueName,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	go func() { _ = worker.Run(workerCtx) }()

	// Allow delivery to hit worker with broken DB
	time.Sleep(150 * time.Millisecond)
	cancelWorker()

	// Verify that the job was NEVER marked completed (no false success!)
	var state string
	_ = env.pool.QueryRow(ctx, "SELECT state FROM jobs WHERE id = $1;", jobID).Scan(&state)
	if state == "completed" {
		t.Fatalf("CRITICAL: job was marked completed despite database outage!")
	}
	if state != "pending" && state != "running" {
		t.Errorf("unexpected job state: %s", state)
	}
}
