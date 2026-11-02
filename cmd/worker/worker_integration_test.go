package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/internal/adapter/postgres"
	"github.com/YKalisjati/forge/internal/adapter/rabbitmq"
	appWorker "github.com/YKalisjati/forge/internal/app/worker"
	"github.com/YKalisjati/forge/internal/app/worker/handlers"
	"github.com/YKalisjati/forge/internal/domain/job"
	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
	"github.com/YKalisjati/forge/pkg/config"
)

func setupTestEnvironment(t *testing.T) (*postgres.Client, *rabbitmq.Client, *config.Config, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pgClient, err := postgres.NewClient(initCtx, &cfg.Postgres)
	if err != nil {
		t.Skipf("skipping test: postgresql unreachable: %v", err)
	}

	rmqClient, err := rabbitmq.NewClient(initCtx, &cfg.RabbitMQ)
	if err != nil {
		pgClient.Close()
		t.Skipf("skipping test: rabbitmq unreachable: %v", err)
	}

	// Ensure durable topology
	if err := rabbitmq.DeclareTopology(initCtx, rmqClient, &cfg.RabbitMQ); err != nil {
		pgClient.Close()
		_ = rmqClient.Close()
		t.Fatalf("failed to declare topology: %v", err)
	}

	// Clean database tables and purge queue before test run
	_, _ = pgClient.Pool().Exec(initCtx, "DELETE FROM jobs;")

	ch, err := rmqClient.Connection().Channel()
	if err == nil {
		_, _ = ch.QueuePurge(cfg.RabbitMQ.Queue, false)
		_, _ = ch.QueuePurge(cfg.RabbitMQ.DLQ, false)
		_ = ch.Close()
	}

	cleanup := func() {
		pgClient.Close()
		_ = rmqClient.Close()
	}

	return pgClient, rmqClient, cfg, cleanup
}

func insertTestJob(t *testing.T, pgClient *postgres.Client, jobID uuid.UUID, jobType string, payload []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobQuery := `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at
		) VALUES (
			$1, $2, $3, 'pending', 10, 5, 0, NOW(), NOW()
		);
	`
	_, err := pgClient.Pool().Exec(ctx, jobQuery, jobID, jobType, payload)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}
}

func publishEnvelopeToQueue(t *testing.T, rmqClient *rabbitmq.Client, cfg *config.Config, env *domainOutbox.Envelope) {
	publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := publisher.Publish(ctx, env); err != nil {
		t.Fatalf("failed to publish envelope to queue: %v", err)
	}
}

// 1. Valid job succeeds end-to-end
func TestIntegration_ValidJobSucceeds(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	payload := []byte(`{"report_id":"rep-valid-1","title":"Q3 Summary","data_points":[10.0,20.0,30.0,40.0]}`)
	insertTestJob(t, pgClient, jobID, "report.generate", payload)

	env := &domainOutbox.Envelope{
		EventID:       2001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       payload,
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	reg := appWorker.NewRegistry()
	_ = reg.Register("report.generate", handlers.NewReportHandler(nil))
	metrics := appWorker.NewMetrics()

	workerCfg := appWorker.Config{
		Queue:             cfg.RabbitMQ.Queue,
		Concurrency:       2,
		PrefetchCount:     5,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}

	w := appWorker.NewWorker(jobRepo, consumer, reg, metrics, nil, workerCfg)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// Wait for job to be completed in PostgreSQL
	deadline := time.Now().Add(5 * time.Second)
	var finalJob *job.WithExecutions
	var queryErr error
	for time.Now().Before(deadline) {
		finalJob, queryErr = jobRepo.GetByID(context.Background(), jobID)
		if queryErr == nil && finalJob.State == job.StateCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelWorker()
	<-done

	if finalJob == nil || finalJob.State != job.StateCompleted {
		t.Fatalf("expected job to be completed, got: %+v (queryErr: %v)", finalJob, queryErr)
	}
	if finalJob.CompletedAt == nil {
		t.Errorf("expected completed_at to be populated")
	}
	if finalJob.ExecutionLeaseID != nil {
		t.Errorf("expected execution_lease_id to be cleared upon completion")
	}
	if len(finalJob.Executions) != 1 {
		t.Fatalf("expected 1 execution audit record, got %d", len(finalJob.Executions))
	}
	exec := finalJob.Executions[0]
	if exec.Status != "completed" {
		t.Errorf("expected execution status 'completed', got '%s'", exec.Status)
	}
	if exec.FinishedAt == nil {
		t.Errorf("expected execution finished_at to be populated")
	}
	if exec.DurationMS == nil || *exec.DurationMS < 0 {
		t.Errorf("expected valid duration_ms, got %v", exec.DurationMS)
	}

	snap := metrics.Snapshot()
	if snap.JobsSucceededTotal != 1 {
		t.Errorf("expected 1 job succeeded in metrics, got %d", snap.JobsSucceededTotal)
	}

	// Verify RabbitMQ queue is empty (message was acknowledged)
	ch, err := rmqClient.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	qInfo, err := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to inspect queue: %v", err)
	}
	if qInfo.Messages != 0 {
		t.Errorf("expected 0 messages in queue after ack, got %d", qInfo.Messages)
	}
}

// 2. Unknown job ID is safely acknowledged without blocking queue
func TestIntegration_UnknownJobID(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	unknownID := uuid.New()
	env := &domainOutbox.Envelope{
		EventID:       2002,
		JobID:         unknownID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: unknownID.String(),
		Payload:       []byte(`{}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	metrics := appWorker.NewMetrics()
	w := appWorker.NewWorker(jobRepo, consumer, nil, metrics, nil, appWorker.DefaultConfig())

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// Wait for metrics to record unknown job
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if metrics.Snapshot().UnknownJobsTotal >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelWorker()
	<-done

	if metrics.Snapshot().UnknownJobsTotal != 1 {
		t.Errorf("expected 1 unknown job recorded, got %d", metrics.Snapshot().UnknownJobsTotal)
	}

	// Queue should be empty
	ch, _ := rmqClient.Connection().Channel()
	defer func() { _ = ch.Close() }()
	qInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if qInfo.Messages != 0 {
		t.Errorf("expected queue empty, got %d messages", qInfo.Messages)
	}
}

// 3. Unsupported job type is failed gracefully and acknowledged
func TestIntegration_UnsupportedJobType(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "data.cleanup", []byte(`{"target":"temp_files"}`))

	env := &domainOutbox.Envelope{
		EventID:       2003,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"target":"temp_files"}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	// Registry only has report.generate, NOT data.cleanup
	reg := appWorker.NewRegistry()
	_ = reg.Register("report.generate", handlers.NewReportHandler(nil))
	metrics := appWorker.NewMetrics()

	w := appWorker.NewWorker(jobRepo, consumer, reg, metrics, nil, appWorker.DefaultConfig())

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// Wait for job to transition to dead_letter
	deadline := time.Now().Add(5 * time.Second)
	var finalJob *job.WithExecutions
	for time.Now().Before(deadline) {
		finalJob, _ = jobRepo.GetByID(context.Background(), jobID)
		if finalJob != nil && finalJob.State == job.StateDeadLetter {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelWorker()
	<-done

	if finalJob == nil || finalJob.State != job.StateDeadLetter {
		t.Fatalf("expected job to be in dead_letter state, got: %+v", finalJob)
	}
	if finalJob.LastError == nil || *finalJob.LastError == "" {
		t.Errorf("expected last_error to be recorded")
	}

	snap := metrics.Snapshot()
	if snap.UnsupportedTypeTotal != 1 || snap.JobsFailedTotal != 1 || snap.DeadLetteredTotal != 1 {
		t.Errorf("unexpected metrics for unsupported type: %+v", snap)
	}
}

// 4. Malformed message envelope is safely discarded
func TestIntegration_MalformedMessage(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ch, err := rmqClient.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	// 1. Publish malformed non-JSON body
	err = ch.PublishWithContext(
		context.Background(),
		cfg.RabbitMQ.Exchange,
		cfg.RabbitMQ.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(`{not-json-at-all`),
		},
	)
	if err != nil {
		t.Fatalf("failed to publish malformed message: %v", err)
	}

	// 2. Publish message with unsupported schema version
	unsupportedSchemaMsg, _ := json.Marshal(domainOutbox.Envelope{
		EventID:       2004,
		JobID:         uuid.New(),
		SchemaVersion: 99,
	})
	_ = ch.PublishWithContext(
		context.Background(),
		cfg.RabbitMQ.Exchange,
		cfg.RabbitMQ.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         unsupportedSchemaMsg,
		},
	)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	metrics := appWorker.NewMetrics()
	w := appWorker.NewWorker(jobRepo, consumer, nil, metrics, nil, appWorker.DefaultConfig())

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// Wait for 2 malformed messages to be processed
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if metrics.Snapshot().MalformedMessagesTotal >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelWorker()
	<-done

	if metrics.Snapshot().MalformedMessagesTotal != 2 {
		t.Errorf("expected 2 malformed messages, got %d", metrics.Snapshot().MalformedMessagesTotal)
	}
	if metrics.Snapshot().DLQEventsTotal != 2 {
		t.Errorf("expected 2 DLQ events recorded, got %d", metrics.Snapshot().DLQEventsTotal)
	}

	// Queue should be drained
	qInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if qInfo.Messages != 0 {
		t.Errorf("expected queue drained of malformed messages, got %d", qInfo.Messages)
	}

	// Dead letter queue should receive the 2 rejected messages
	dlqInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.DLQ, true, false, false, false, nil)
	if dlqInfo.Messages != 2 {
		t.Errorf("expected 2 messages in DLQ, got %d", dlqInfo.Messages)
	}
}

// 5. Duplicate message for an already completed job is safely acknowledged
func TestIntegration_DuplicateMessageForCompletedJob(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	// Insert job that is ALREADY completed
	completedQuery := `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted, completed_at, created_at, updated_at
		) VALUES (
			$1, 'report.generate', '{}', 'completed', 5, 3, 1, NOW(), NOW(), NOW()
		);
	`
	_, err := pgClient.Pool().Exec(context.Background(), completedQuery, jobID)
	if err != nil {
		t.Fatalf("failed to insert completed job: %v", err)
	}

	env := &domainOutbox.Envelope{
		EventID:       2005,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	metrics := appWorker.NewMetrics()
	w := appWorker.NewWorker(jobRepo, consumer, nil, metrics, nil, appWorker.DefaultConfig())

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if metrics.Snapshot().DuplicateDeliveriesTotal >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancelWorker()
	<-done

	if metrics.Snapshot().DuplicateDeliveriesTotal != 1 {
		t.Errorf("expected 1 duplicate delivery recorded, got %d", metrics.Snapshot().DuplicateDeliveriesTotal)
	}

	// Queue should be empty
	ch, _ := rmqClient.Connection().Channel()
	defer func() { _ = ch.Close() }()
	qInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if qInfo.Messages != 0 {
		t.Errorf("expected queue empty, got %d", qInfo.Messages)
	}
}

// 6. Two workers competing for the same job (Mutual exclusion test)
func TestIntegration_TwoWorkersCompetingSameJob(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	payload := []byte(`{"title":"Competitive Report","data_points":[5.0,15.0]}`)
	insertTestJob(t, pgClient, jobID, "report.generate", payload)

	// Publish TWO identical messages for the same job ID
	for eventID := int64(2006); eventID <= 2007; eventID++ {
		env := &domainOutbox.Envelope{
			EventID:       eventID,
			JobID:         jobID,
			EventType:     "job.created",
			SchemaVersion: 1,
			CorrelationID: jobID.String(),
			Payload:       payload,
			Timestamp:     time.Now().UTC(),
		}
		publishEnvelopeToQueue(t, rmqClient, cfg, env)
	}

	jobRepo := postgres.NewJobRepository(pgClient.Pool())

	reg := appWorker.NewRegistry()
	_ = reg.Register("report.generate", handlers.NewReportHandler(nil))

	consumerA := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumerA.Close() }()
	metricsA := appWorker.NewMetrics()
	workerA := appWorker.NewWorker(jobRepo, consumerA, reg, metricsA, nil, appWorker.DefaultConfig())
	workerA.SetWorkerID("worker-compete-A")

	consumerB := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumerB.Close() }()
	metricsB := appWorker.NewMetrics()
	workerB := appWorker.NewWorker(jobRepo, consumerB, reg, metricsB, nil, appWorker.DefaultConfig())
	workerB.SetWorkerID("worker-compete-B")

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = workerA.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = workerB.Run(ctx)
	}()

	// Wait for job to be completed in PostgreSQL
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := jobRepo.GetByID(context.Background(), jobID)
		if err == nil && j.State == job.StateCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Give time for duplicate message to be processed and acknowledged
	time.Sleep(500 * time.Millisecond)

	cancel()
	wg.Wait()

	// Verify database state: Job completed, exactly ONE execution record
	finalJob, err := jobRepo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get job details: %v", err)
	}
	if finalJob.State != job.StateCompleted {
		t.Errorf("expected job state 'completed', got '%s'", finalJob.State)
	}
	if len(finalJob.Executions) != 1 {
		t.Fatalf("expected exactly 1 execution record, got %d", len(finalJob.Executions))
	}

	// Verify that one worker succeeded and the other handled duplicate
	totalSucceeded := metricsA.Snapshot().JobsSucceededTotal + metricsB.Snapshot().JobsSucceededTotal
	totalDuplicates := metricsA.Snapshot().DuplicateDeliveriesTotal + metricsB.Snapshot().DuplicateDeliveriesTotal

	if totalSucceeded != 1 {
		t.Errorf("expected exactly 1 worker to succeed, got %d", totalSucceeded)
	}
	if totalDuplicates != 1 {
		t.Errorf("expected exactly 1 duplicate delivery detected by loser, got %d", totalDuplicates)
	}

	// Queue should be empty
	ch, _ := rmqClient.Connection().Channel()
	defer func() { _ = ch.Close() }()
	qInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if qInfo.Messages != 0 {
		t.Errorf("expected queue empty after mutual exclusion handling, got %d", qInfo.Messages)
	}
}

// 7. Worker cancellation and graceful shutdown
func TestIntegration_WorkerCancellationAndShutdown(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "test.job", []byte(`{"duration_ms":300}`))

	env := &domainOutbox.Envelope{
		EventID:       2008,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"duration_ms":300}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	reg := appWorker.NewRegistry()
	_ = reg.Register("test.job", handlers.NewTestJobHandler())

	workerCfg := appWorker.Config{
		Queue:             cfg.RabbitMQ.Queue,
		Concurrency:       2,
		PrefetchCount:     5,
		LeaseDuration:     10 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		ShutdownTimeout:   2 * time.Second,
	}

	w := appWorker.NewWorker(jobRepo, consumer, reg, nil, nil, workerCfg)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// Let worker pick up the job and begin executing
	time.Sleep(50 * time.Millisecond)

	// Cancel context during active execution
	cancelWorker()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("worker returned error on cancellation: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("worker failed to shut down within timeout")
	}
}

// 8. Database error before success is persisted (RabbitMQ message is NOT acknowledged)
func TestIntegration_DatabaseErrorBeforeSuccessPersisted(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "report.generate", []byte(`{"title":"Simulated DB Failure"}`))

	env := &domainOutbox.Envelope{
		EventID:       2009,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"title":"Simulated DB Failure"}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	reg := appWorker.NewRegistry()
	_ = reg.Register("report.generate", handlers.NewReportHandler(nil))

	metrics := appWorker.NewMetrics()
	w := appWorker.NewWorker(jobRepo, consumer, reg, metrics, nil, appWorker.DefaultConfig())

	// Simulate database failure before success can be persisted
	w.OnBeforeComplete = func(_ *job.Job, _ uuid.UUID) error {
		return errors.New("simulated fatal database error before commit")
	}

	// Consume message directly
	deliveries, err := consumer.Consume(context.Background(), cfg.RabbitMQ.Queue, "test-err-worker", 1)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	d := <-deliveries
	w.ProcessDelivery(context.Background(), d)

	// Close consumer channel so requeued unacknowledged message returns to ready state in queue
	_ = consumer.Close()

	// Verify job in PostgreSQL is NOT marked completed
	j, err := jobRepo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if j.State == job.StateCompleted {
		t.Fatalf("job must NOT be completed when DB error occurred before success was persisted")
	}

	// Verify RabbitMQ message was NACKed with requeue=true (message remains in queue)
	ch, _ := rmqClient.Connection().Channel()
	defer func() { _ = ch.Close() }()
	qInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if qInfo.Messages != 1 {
		t.Errorf("expected 1 message preserved in queue, got %d", qInfo.Messages)
	}
}

// 9. Stale worker fencing violation test
func TestIntegration_FencingViolationStaleWorker(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "report.generate", []byte(`{"title":"Fencing Test"}`))

	jobRepo := postgres.NewJobRepository(pgClient.Pool())

	// 1. Worker A claims the job with a short lease (50ms)
	claimedJob, leaseA, _, err := jobRepo.ClaimJob(context.Background(), jobID, 50*time.Millisecond, "worker-A")
	if err != nil {
		t.Fatalf("failed to claim job as worker A: %v", err)
	}

	// 2. Worker A stalls and its lease expires
	time.Sleep(100 * time.Millisecond)

	// 3. Worker B reclaims the job with a new lease
	_, leaseB, _, err := jobRepo.ClaimJob(context.Background(), jobID, 10*time.Second, "worker-B")
	if err != nil {
		t.Fatalf("failed to reclaim job as worker B: %v", err)
	}
	if leaseA == leaseB {
		t.Fatalf("expected different lease IDs for Worker A and Worker B")
	}

	// 4. Stale Worker A wakes up and attempts to commit completion using stale leaseA
	err = jobRepo.CompleteJob(context.Background(), claimedJob.ID, leaseA, 100*time.Millisecond)
	if err == nil || !errors.Is(err, job.ErrFencingViolation) {
		t.Fatalf("expected ErrFencingViolation for stale Worker A, got: %v", err)
	}

	// 5. Worker B successfully commits completion with active leaseB
	err = jobRepo.CompleteJob(context.Background(), claimedJob.ID, leaseB, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("expected Worker B completion to succeed, got: %v", err)
	}

	// Verify job state in DB is completed by Worker B
	finalJob, err := jobRepo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if finalJob.State != job.StateCompleted {
		t.Errorf("expected job state 'completed', got '%s'", finalJob.State)
	}
}
