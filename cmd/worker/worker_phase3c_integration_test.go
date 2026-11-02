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
	"github.com/YKalisjati/forge/internal/app/reaper"
	appWorker "github.com/YKalisjati/forge/internal/app/worker"
	"github.com/YKalisjati/forge/internal/app/worker/handlers"
	"github.com/YKalisjati/forge/internal/domain/job"
	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
)

// 1. Retryable error followed by success:
// First attempt fails with transient error, schedules retry in outbox with backoff.
// Second attempt succeeds, transitioning job to completed.
func TestIntegration_RetryableErrorFollowedBySuccess(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	// Fail attempt 1, succeed on attempt 2
	insertTestJob(t, pgClient, jobID, "test.job", []byte(`{"fail_retryable_until_attempt": 2}`))

	env := &domainOutbox.Envelope{
		EventID:       3001,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"fail_retryable_until_attempt": 2}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	reg := appWorker.NewRegistry()
	_ = reg.Register("test.job", handlers.NewTestJobHandler())
	metrics := appWorker.NewMetrics()

	workerCfg := appWorker.DefaultConfig()
	workerCfg.Backoff = job.BackoffPolicy{
		BaseInterval: 50 * time.Millisecond,
		MaxInterval:  200 * time.Millisecond,
	}

	w := appWorker.NewWorker(jobRepo, consumer, reg, metrics, nil, workerCfg)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// 1. Wait for attempt 1 to fail and schedule retry
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if metrics.Snapshot().RetriesScheduledTotal >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if metrics.Snapshot().RetriesScheduledTotal != 1 {
		t.Fatalf("expected 1 retry scheduled, got %d", metrics.Snapshot().RetriesScheduledTotal)
	}

	// Verify job is back in pending state with next_attempt_at recorded
	j1, err := jobRepo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get job after attempt 1: %v", err)
	}
	if j1.State != job.StatePending {
		t.Fatalf("expected job in pending state after retryable failure, got: %s", j1.State)
	}
	if j1.RetriesAttempted != 1 {
		t.Fatalf("expected retries_attempted 1, got %d", j1.RetriesAttempted)
	}

	// Verify outbox has a pending event scheduled for the retry
	var outboxID int64
	var scheduledAt time.Time
	err = pgClient.Pool().QueryRow(context.Background(),
		`SELECT id, scheduled_at FROM outbox_messages WHERE job_id = $1 AND status = 'pending';`,
		jobID,
	).Scan(&outboxID, &scheduledAt)
	if err != nil {
		t.Fatalf("expected pending outbox event for retry: %v", err)
	}

	// 2. Wait until scheduled_at arrives and publish attempt 2
	if time.Now().Before(scheduledAt) {
		time.Sleep(time.Until(scheduledAt) + 20*time.Millisecond)
	}

	// Mark outbox published and publish attempt 2 to RabbitMQ
	_, _ = pgClient.Pool().Exec(context.Background(), `UPDATE outbox_messages SET status = 'published' WHERE id = $1;`, outboxID)
	env2 := &domainOutbox.Envelope{
		EventID:       outboxID,
		JobID:         jobID,
		EventType:     "job.scheduled",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"fail_retryable_until_attempt": 2}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env2)

	// Wait for attempt 2 to complete successfully
	deadline2 := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline2) {
		if metrics.Snapshot().JobsSucceededTotal >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancelWorker()
	<-done

	if metrics.Snapshot().JobsSucceededTotal != 1 {
		t.Fatalf("expected 1 job succeeded, got %d", metrics.Snapshot().JobsSucceededTotal)
	}

	// Verify final job state in DB
	finalJob, err := jobRepo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get final job: %v", err)
	}
	if finalJob.State != job.StateCompleted {
		t.Fatalf("expected job completed, got: %s", finalJob.State)
	}
	if finalJob.RetriesAttempted != 2 {
		t.Fatalf("expected retries_attempted 2, got %d", finalJob.RetriesAttempted)
	}
	if len(finalJob.Executions) != 2 {
		t.Fatalf("expected 2 execution audit entries, got %d", len(finalJob.Executions))
	}
	if finalJob.Executions[0].Status != "failed" || finalJob.Executions[1].Status != "completed" {
		t.Fatalf("unexpected execution history: %+v", finalJob.Executions)
	}
}

// 2. Permanent error: Non-retryable error immediately transitions to dead_letter without retrying.
func TestIntegration_PermanentError(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "test.job", []byte(`{"fail_permanent": true}`))

	env := &domainOutbox.Envelope{
		EventID:       3002,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"fail_permanent": true}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	reg := appWorker.NewRegistry()
	_ = reg.Register("test.job", handlers.NewTestJobHandler())
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

	// Verify NO retry outbox was scheduled
	var outboxCount int64
	_ = pgClient.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM outbox_messages WHERE job_id = $1 AND status = 'pending';`,
		jobID,
	).Scan(&outboxCount)
	if outboxCount != 0 {
		t.Fatalf("expected 0 pending outbox retries for permanent error, got %d", outboxCount)
	}

	snap := metrics.Snapshot()
	if snap.DeadLetteredTotal != 1 || snap.RetriesScheduledTotal != 0 {
		t.Errorf("unexpected metrics for permanent error: %+v", snap)
	}
}

// 3. Retry exhaustion: Job exceeding max_retries transitions to dead_letter.
func TestIntegration_RetryExhaustion(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	// Set max_retries = 1, retries_attempted = 1. Next execution will be attempt 2 (> 1), causing exhaustion.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pgClient.Pool().Exec(ctx, `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at
		) VALUES (
			$1, 'test.job', '{"fail": true}', 'pending', 10, 1, 1, NOW(), NOW()
		);
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert job: %v", err)
	}

	env := &domainOutbox.Envelope{
		EventID:       3003,
		JobID:         jobID,
		EventType:     "job.scheduled",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"fail": true}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer.Close() }()

	reg := appWorker.NewRegistry()
	_ = reg.Register("test.job", handlers.NewTestJobHandler())
	metrics := appWorker.NewMetrics()

	w := appWorker.NewWorker(jobRepo, consumer, reg, metrics, nil, appWorker.DefaultConfig())

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- w.Run(workerCtx)
	}()

	// Wait for job to transition to dead_letter due to retry exhaustion
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
		t.Fatalf("expected job to be in dead_letter state after exhaustion, got: %+v", finalJob)
	}
	if finalJob.RetriesAttempted != 2 {
		t.Fatalf("expected retries_attempted 2, got %d", finalJob.RetriesAttempted)
	}

	snap := metrics.Snapshot()
	if snap.ExhaustedAttemptsTotal != 1 || snap.DeadLetteredTotal != 1 {
		t.Errorf("unexpected metrics for retry exhaustion: %+v", snap)
	}
}

// 4. Worker crash during execution: Unacked delivery is requeued by RabbitMQ and recovered by another worker.
func TestIntegration_WorkerCrashDuringExecution(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "test.job", []byte(`{"duration_ms": 300}`))

	env := &domainOutbox.Envelope{
		EventID:       3004,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"duration_ms": 300}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())

	// Start Worker 1 with a very short lease (100ms)
	consumer1 := rabbitmq.NewConsumer(rmqClient)
	reg := appWorker.NewRegistry()
	_ = reg.Register("test.job", handlers.NewTestJobHandler())
	wCfg1 := appWorker.DefaultConfig()
	wCfg1.LeaseDuration = 100 * time.Millisecond
	wCfg1.HeartbeatInterval = 0 // disabled so lease will expire
	w1 := appWorker.NewWorker(jobRepo, consumer1, reg, appWorker.NewMetrics(), nil, wCfg1)

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() {
		done1 <- w1.Run(ctx1)
	}()

	// Wait until Worker 1 claims the job
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, _ := jobRepo.GetByID(context.Background(), jobID)
		if j != nil && j.State == job.StateRunning {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Abruptly crash Worker 1 (cancelling context and closing consumer channel before completion)
	cancel1()
	_ = consumer1.Close()
	<-done1

	// Wait for Worker 1's lease to expire
	time.Sleep(200 * time.Millisecond)

	// Start Worker 2: Consumes the requeued message and executes successfully
	consumer2 := rabbitmq.NewConsumer(rmqClient)
	defer func() { _ = consumer2.Close() }()
	wCfg2 := appWorker.DefaultConfig()
	w2 := appWorker.NewWorker(jobRepo, consumer2, reg, appWorker.NewMetrics(), nil, wCfg2)

	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		done2 <- w2.Run(ctx2)
	}()

	// Wait for Worker 2 to complete the job
	deadline2 := time.Now().Add(5 * time.Second)
	var finalJob *job.WithExecutions
	for time.Now().Before(deadline2) {
		finalJob, _ = jobRepo.GetByID(context.Background(), jobID)
		if finalJob != nil && finalJob.State == job.StateCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel2()
	<-done2

	if finalJob == nil || finalJob.State != job.StateCompleted {
		t.Fatalf("expected job to be completed by recovered Worker 2, got: %+v", finalJob)
	}
}

// 5. Expired lease & Stale Job Recovery:
// A job stuck in running with an expired lease is reclaimed by the Reaper and scheduled for retry.
func TestIntegration_ExpiredLeaseAndStaleJobRecovery(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	leaseID := uuid.New()
	workerID := "crashed-worker-99"

	// Insert running job with lease expired 10 seconds ago
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobQuery := `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted,
			execution_lease_id, lease_expires_at, locked_by, created_at, updated_at
		) VALUES (
			$1, 'test.job', '{"title":"Stale Job"}', 'running', 10, 3, 1,
			$2, NOW() - INTERVAL '10 seconds', $3, NOW(), NOW()
		);
	`
	_, err := pgClient.Pool().Exec(ctx, jobQuery, jobID, leaseID, workerID)
	if err != nil {
		t.Fatalf("failed to insert stale job: %v", err)
	}

	// Insert audit record
	execQuery := `
		INSERT INTO job_executions (
			job_id, attempt, execution_lease_id, worker_id, status, started_at
		) VALUES (
			$1, 1, $2, $3, 'running', NOW() - INTERVAL '10 seconds'
		);
	`
	_, err = pgClient.Pool().Exec(ctx, execQuery, jobID, leaseID, workerID)
	if err != nil {
		t.Fatalf("failed to insert execution log: %v", err)
	}

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	metrics := appWorker.NewMetrics()
	reaperDaemon := reaper.NewReaper(jobRepo, metrics, nil, reaper.Config{BatchSize: 10})

	// Run single reaper pass
	recovered, dl, _, err := reaperDaemon.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("reaper RunOnce failed: %v", err)
	}
	if recovered != 1 || dl != 0 {
		t.Fatalf("expected 1 job recovered and 0 dead lettered, got %d recovered and %d dl", recovered, dl)
	}

	// Verify job in DB: state reset to pending, lease cleared
	recoveredJob, err := jobRepo.GetByID(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get recovered job: %v", err)
	}
	if recoveredJob.State != job.StatePending {
		t.Fatalf("expected state pending, got '%s'", recoveredJob.State)
	}
	if recoveredJob.ExecutionLeaseID != nil {
		t.Errorf("expected execution_lease_id nil after recovery")
	}
	if recoveredJob.LeaseExpiresAt != nil {
		t.Errorf("expected lease_expires_at nil after recovery")
	}

	// Verify previous execution log updated to timed_out
	if len(recoveredJob.Executions) != 1 || recoveredJob.Executions[0].Status != "timed_out" {
		t.Fatalf("expected execution status 'timed_out', got: %+v", recoveredJob.Executions)
	}

	// Verify outbox message created for retry dispatch
	var outboxCount int64
	_ = pgClient.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM outbox_messages WHERE job_id = $1 AND status = 'pending';`,
		jobID,
	).Scan(&outboxCount)
	if outboxCount != 1 {
		t.Fatalf("expected 1 pending outbox event for recovered job, got %d", outboxCount)
	}

	if metrics.Snapshot().StaleJobsRecoveredTotal != 1 {
		t.Errorf("expected 1 stale job recovered metric, got %d", metrics.Snapshot().StaleJobsRecoveredTotal)
	}
}

// 6. Stale worker attempts to commit: Fencing token violation prevents zombie worker from committing.
func TestIntegration_StaleWorkerAttemptsToCommit(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "test.job", []byte(`{"title":"Fencing Test"}`))

	jobRepo := postgres.NewJobRepository(pgClient.Pool())

	// 1. Worker A claims job
	claimedJob, leaseA, _, err := jobRepo.ClaimJob(context.Background(), jobID, 50*time.Millisecond, "worker-A")
	if err != nil {
		t.Fatalf("failed to claim job as worker A: %v", err)
	}

	// 2. Worker A stalls, lease expires
	time.Sleep(100 * time.Millisecond)

	// 3. Worker B reclaims job with fresh lease
	_, leaseB, _, err := jobRepo.ClaimJob(context.Background(), jobID, 10*time.Second, "worker-B")
	if err != nil {
		t.Fatalf("failed to reclaim job as worker B: %v", err)
	}

	// 4. Stale Worker A wakes up and attempts ScheduleRetry with stale leaseA
	err = jobRepo.ScheduleRetry(context.Background(), claimedJob.ID, leaseA, "stale error", time.Now().Add(5*time.Second), 100*time.Millisecond)
	if err == nil || !errors.Is(err, job.ErrFencingViolation) {
		t.Fatalf("expected ErrFencingViolation for stale Worker A ScheduleRetry, got: %v", err)
	}

	// 5. Stale Worker A attempts DeadLetterJob with stale leaseA
	err = jobRepo.DeadLetterJob(context.Background(), claimedJob.ID, leaseA, "stale error", 100*time.Millisecond)
	if err == nil || !errors.Is(err, job.ErrFencingViolation) {
		t.Fatalf("expected ErrFencingViolation for stale Worker A DeadLetterJob, got: %v", err)
	}

	// 6. Active Worker B successfully completes job
	err = jobRepo.CompleteJob(context.Background(), claimedJob.ID, leaseB, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("expected Worker B CompleteJob to succeed, got: %v", err)
	}

	finalJob, _ := jobRepo.GetByID(context.Background(), jobID)
	if finalJob.State != job.StateCompleted {
		t.Errorf("expected final job state 'completed', got '%s'", finalJob.State)
	}
}

// 7. Database outage during acknowledgment workflow: Message remains preserved in RabbitMQ.
func TestIntegration_DatabaseOutageDuringAcknowledgmentWorkflow(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	insertTestJob(t, pgClient, jobID, "test.job", []byte(`{"fail": true}`))

	env := &domainOutbox.Envelope{
		EventID:       3007,
		JobID:         jobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: jobID.String(),
		Payload:       []byte(`{"fail": true}`),
		Timestamp:     time.Now().UTC(),
	}
	publishEnvelopeToQueue(t, rmqClient, cfg, env)

	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	consumer := rabbitmq.NewConsumer(rmqClient)

	reg := appWorker.NewRegistry()
	_ = reg.Register("test.job", handlers.NewTestJobHandler())
	metrics := appWorker.NewMetrics()

	w := appWorker.NewWorker(jobRepo, consumer, reg, metrics, nil, appWorker.DefaultConfig())

	// Inject database failure before retry schedule can be saved
	w.OnBeforeRetry = func(_ *job.Job, _ uuid.UUID) error {
		return errors.New("simulated fatal database disconnect during retry persistence")
	}

	// Consume single message directly so worker does not re-consume its own requeue in loop
	deliveries, err := consumer.Consume(context.Background(), cfg.RabbitMQ.Queue, "test-outage-worker", 1)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	d := <-deliveries
	w.ProcessDelivery(context.Background(), d)

	// Close consumer immediately before inspecting queue so RabbitMQ leaves message in ready state
	_ = consumer.Close()

	// Verify message was NOT acknowledged (preserved in queue)
	ch, _ := rmqClient.Connection().Channel()
	defer func() { _ = ch.Close() }()
	qInfo, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if qInfo.Messages != 1 {
		t.Errorf("expected 1 message preserved in queue after DB outage, got %d", qInfo.Messages)
	}
}

// 8. Duplicate retry scheduling prevention: Concurrent schedulers do not create duplicate outbox records.
func TestIntegration_DuplicateRetrySchedulingPrevention(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Insert pending job ready for retry
	_, err := pgClient.Pool().Exec(ctx, `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted,
			next_attempt_at, created_at, updated_at
		) VALUES (
			$1, 'test.job', '{"title":"Scheduled Retry"}', 'pending', 10, 3, 1,
			NOW() - INTERVAL '5 seconds', NOW(), NOW()
		);
	`, jobID)
	if err != nil {
		t.Fatalf("failed to insert pending job: %v", err)
	}

	jobRepo := postgres.NewJobRepository(pgClient.Pool())

	// Run two concurrent scheduler passes
	var wg sync.WaitGroup
	var scheduledA, scheduledB int
	var errA, errB error

	wg.Add(2)
	go func() {
		defer wg.Done()
		scheduledA, errA = jobRepo.SchedulePendingRetries(context.Background(), 10)
	}()
	go func() {
		defer wg.Done()
		scheduledB, errB = jobRepo.SchedulePendingRetries(context.Background(), 10)
	}()
	wg.Wait()

	if errA != nil || errB != nil {
		t.Fatalf("scheduler execution error: %v, %v", errA, errB)
	}

	totalScheduled := scheduledA + scheduledB
	if totalScheduled != 1 {
		t.Fatalf("expected exactly 1 scheduler to claim the job, got total %d (A=%d, B=%d)", totalScheduled, scheduledA, scheduledB)
	}

	// Verify exactly 1 pending outbox row exists
	var count int64
	_ = pgClient.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM outbox_messages WHERE job_id = $1 AND status = 'pending';`,
		jobID,
	).Scan(&count)
	if count != 1 {
		t.Fatalf("expected strictly 1 pending outbox event, got %d", count)
	}
}

// 9. Poison message routing: Malformed / poison messages are rejected to RabbitMQ Dead Letter Queue.
func TestIntegration_PoisonMessageRouting(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ch, err := rmqClient.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	// 1. Publish malformed non-JSON payload
	err = ch.PublishWithContext(context.Background(),
		cfg.RabbitMQ.Exchange,
		cfg.RabbitMQ.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(`{poison:true,malformed`),
		},
	)
	if err != nil {
		t.Fatalf("failed to publish malformed message: %v", err)
	}

	// 2. Publish poison message with invalid schema version
	invalidSchema, _ := json.Marshal(domainOutbox.Envelope{
		EventID:       9999,
		JobID:         uuid.New(),
		SchemaVersion: 999,
		Payload:       []byte(`{"data":"poison"}`),
	})
	_ = ch.PublishWithContext(context.Background(),
		cfg.RabbitMQ.Exchange,
		cfg.RabbitMQ.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         invalidSchema,
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

	// Wait until both poison messages are rejected and routed to DLQ
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if metrics.Snapshot().DLQEventsTotal >= 2 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	cancelWorker()
	<-done

	snap := metrics.Snapshot()
	if snap.DLQEventsTotal != 2 {
		t.Fatalf("expected 2 DLQ events recorded in metrics, got %d", snap.DLQEventsTotal)
	}
	if snap.MalformedMessagesTotal != 2 {
		t.Fatalf("expected 2 malformed messages recorded, got %d", snap.MalformedMessagesTotal)
	}

	// Main queue should be completely empty
	mainQ, _ := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if mainQ.Messages != 0 {
		t.Errorf("expected main queue empty, got %d messages", mainQ.Messages)
	}

	// Dead Letter Queue should hold both poison messages
	dlq, err := ch.QueueDeclarePassive(cfg.RabbitMQ.DLQ, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to inspect DLQ: %v", err)
	}
	if dlq.Messages != 2 {
		t.Fatalf("expected 2 messages in DLQ (%s), got %d", cfg.RabbitMQ.DLQ, dlq.Messages)
	}

	// Consume and verify the first message from the DLQ
	msg1, ok, err := ch.Get(cfg.RabbitMQ.DLQ, true)
	if err != nil || !ok {
		t.Fatalf("failed to get first message from DLQ: %v", err)
	}
	if string(msg1.Body) != `{poison:true,malformed` {
		t.Errorf("unexpected body in DLQ message 1: %s", string(msg1.Body))
	}

	// Consume and verify the second message from the DLQ
	msg2, ok, err := ch.Get(cfg.RabbitMQ.DLQ, true)
	if err != nil || !ok {
		t.Fatalf("failed to get second message from DLQ: %v", err)
	}
	var env2 domainOutbox.Envelope
	if err := json.Unmarshal(msg2.Body, &env2); err != nil {
		t.Fatalf("failed to unmarshal DLQ envelope: %v", err)
	}
	if env2.SchemaVersion != 999 {
		t.Errorf("expected schema version 999 in DLQ message 2, got %d", env2.SchemaVersion)
	}
}
