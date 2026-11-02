package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/internal/adapter/postgres"
	"github.com/YKalisjati/forge/internal/adapter/rabbitmq"
	appOutbox "github.com/YKalisjati/forge/internal/app/outbox"
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

	// Clean pending jobs and outbox messages and purge queue before test run
	_, _ = pgClient.Pool().Exec(initCtx, "DELETE FROM jobs;")

	ch, err := rmqClient.Connection().Channel()
	if err == nil {
		_, _ = ch.QueuePurge(cfg.RabbitMQ.Queue, false)
		_ = ch.Close()
	}

	cleanup := func() {
		pgClient.Close()
		_ = rmqClient.Close()
	}

	return pgClient, rmqClient, cfg, cleanup
}

func insertTestJobAndOutbox(t *testing.T, pgClient *postgres.Client, jobID uuid.UUID, eventType string, payload []byte) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobQuery := `
		INSERT INTO jobs (
			id, type, payload, state, priority, max_retries, retries_attempted, created_at, updated_at
		) VALUES (
			$1, 'email.transactional', $2, 'pending', 10, 5, 0, NOW(), NOW()
		);
	`
	_, err := pgClient.Pool().Exec(ctx, jobQuery, jobID, payload)
	if err != nil {
		t.Fatalf("failed to insert test job: %v", err)
	}

	outboxQuery := `
		INSERT INTO outbox_messages (
			job_id, event_type, payload, status, scheduled_at, retry_count, created_at
		) VALUES (
			$1, $2, $3, 'pending', NOW(), 0, NOW()
		) RETURNING id;
	`
	var eventID int64
	err = pgClient.Pool().QueryRow(ctx, outboxQuery, jobID, eventType, payload).Scan(&eventID)
	if err != nil {
		t.Fatalf("failed to insert test outbox event: %v", err)
	}

	return eventID
}

// 1. Successful submission and publication flow
func TestIntegration_SuccessfulPublication(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	eventID := insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"to":"user@example.com"}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())
	publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	metrics := appOutbox.NewMetrics()
	relayCfg := appOutbox.Config{
		PollInterval:       50 * time.Millisecond,
		BatchSize:          10,
		MaxRetries:         5,
		ClaimLeaseDuration: 5 * time.Second,
		PublishTimeout:     3 * time.Second,
	}

	relay := appOutbox.NewRelay(outboxRepo, publisher, metrics, nil, relayCfg)

	// Process batch
	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch failed: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected at least 1 message processed, got %d", n)
	}

	// Verify PostgreSQL record state
	msg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to get outbox message: %v", err)
	}
	if msg.Status != domainOutbox.StatusPublished {
		t.Errorf("expected status 'published', got '%s'", msg.Status)
	}
	if msg.PublishedAt == nil {
		t.Errorf("expected published_at to be non-nil")
	}

	// Verify RabbitMQ message delivery
	ch, err := rmqClient.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open consume channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	delivery, ok, err := ch.Get(cfg.RabbitMQ.Queue, true)
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}
	if !ok {
		t.Fatalf("expected message in queue, got none")
	}

	if delivery.Headers["event_id"] != eventID {
		t.Errorf("expected event_id header %d, got %v", eventID, delivery.Headers["event_id"])
	}
	if delivery.Headers["job_id"] != jobID.String() {
		t.Errorf("expected job_id header %s, got %v", jobID.String(), delivery.Headers["job_id"])
	}
	if delivery.DeliveryMode != amqp.Persistent {
		t.Errorf("expected DeliveryMode Persistent (2), got %d", delivery.DeliveryMode)
	}
}

// 2. RabbitMQ unavailable failure scenario
func TestIntegration_RabbitMQUnavailable(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	eventID := insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"attempt":"offline"}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())

	// Use custom publisher that returns network error
	failingPublisher := &mockPublisher{
		publishFunc: func(_ context.Context, _ *domainOutbox.Envelope) error {
			return errors.New("dial tcp 127.0.0.1:59999: connect: connection refused")
		},
	}

	metrics := appOutbox.NewMetrics()
	relayCfg := appOutbox.Config{
		PollInterval:       50 * time.Millisecond,
		BatchSize:          10,
		MaxRetries:         5,
		ClaimLeaseDuration: 5 * time.Second,
		PublishTimeout:     1 * time.Second,
	}

	relay := appOutbox.NewRelay(outboxRepo, failingPublisher, metrics, nil, relayCfg)

	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected ProcessBatch error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 published messages, got %d", n)
	}

	// Verify database state: message MUST remain pending and NOT be deleted
	msg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to retrieve outbox message: %v", err)
	}
	if msg.Status != domainOutbox.StatusPending {
		t.Errorf("expected status 'pending', got '%s'", msg.Status)
	}
	if msg.RetryCount != 1 {
		t.Errorf("expected retry_count 1, got %d", msg.RetryCount)
	}
	if msg.LastError == nil || *msg.LastError == "" {
		t.Errorf("expected last_error to be recorded")
	}

	snap := metrics.Snapshot()
	if snap.ConfirmFailures != 1 || snap.PublishAttempts != 1 {
		t.Errorf("expected 1 attempt and 1 failure, got %+v", snap)
	}
}

// 3. Publish confirmation failure (Negative confirmation / NACK)
func TestIntegration_PublishConfirmationFailure(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	eventID := insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"test":"nack"}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())

	nackPublisher := &mockPublisher{
		publishFunc: func(_ context.Context, _ *domainOutbox.Envelope) error {
			return rabbitmq.ErrNegativeConfirmation
		},
	}

	metrics := appOutbox.NewMetrics()
	relayCfg := appOutbox.Config{
		PollInterval:       50 * time.Millisecond,
		BatchSize:          10,
		MaxRetries:         5,
		ClaimLeaseDuration: 5 * time.Second,
		PublishTimeout:     1 * time.Second,
	}

	relay := appOutbox.NewRelay(outboxRepo, nackPublisher, metrics, nil, relayCfg)

	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch returned error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 published, got %d", n)
	}

	// Verify state in PostgreSQL
	msg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to get outbox message: %v", err)
	}
	if msg.Status != domainOutbox.StatusPending {
		t.Errorf("expected status 'pending', got '%s'", msg.Status)
	}
	if msg.RetryCount != 1 {
		t.Errorf("expected retry_count 1, got %d", msg.RetryCount)
	}
	if msg.LastError == nil || *msg.LastError != rabbitmq.ErrNegativeConfirmation.Error() {
		t.Errorf("expected recorded last_error '%v', got '%v'", rabbitmq.ErrNegativeConfirmation, msg.LastError)
	}
}

// 4. Relay crash before publishing (recovery after claim lease expires)
func TestIntegration_RelayCrashBeforePublishing(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	eventID := insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"crash":"before"}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())
	publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	relayCfg := appOutbox.Config{
		PollInterval:       50 * time.Millisecond,
		BatchSize:          10,
		MaxRetries:         5,
		ClaimLeaseDuration: 1 * time.Second, // short lease for fast recovery
		PublishTimeout:     1 * time.Second,
	}

	relay := appOutbox.NewRelay(outboxRepo, publisher, nil, nil, relayCfg)

	// Simulate crash right before publish
	relay.OnBeforePublish = func(envelope *domainOutbox.Envelope) error {
		if envelope.EventID == eventID {
			return errors.New("simulated fatal process crash before publish")
		}
		return nil
	}

	// Run relay - should halt on simulated crash
	_, _ = relay.ProcessBatch(context.Background())

	// Verify PostgreSQL: message claimed (scheduled in future), still pending
	msg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to get outbox message: %v", err)
	}
	if msg.Status != domainOutbox.StatusPending {
		t.Errorf("expected status 'pending', got '%s'", msg.Status)
	}

	// Wait for claim lease to expire (1 second)
	time.Sleep(1100 * time.Millisecond)

	// Remove hook (simulate relay restart)
	relay.OnBeforePublish = nil

	// Re-run batch - relay recovers and publishes the unpublished record
	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("recovery ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 message published upon recovery, got %d", n)
	}

	// Verify PostgreSQL is now marked published
	recoveredMsg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to get recovered outbox message: %v", err)
	}
	if recoveredMsg.Status != domainOutbox.StatusPublished {
		t.Errorf("expected status 'published', got '%s'", recoveredMsg.Status)
	}
}

// 5. Relay crash after publish but before updating PostgreSQL (Duplicate delivery scenario)
func TestIntegration_RelayCrashAfterPublishBeforeDBUpdate(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	eventID := insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"crash":"after"}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())
	publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	relayCfg := appOutbox.Config{
		PollInterval:       50 * time.Millisecond,
		BatchSize:          10,
		MaxRetries:         5,
		ClaimLeaseDuration: 1 * time.Second, // short lease for fast recovery
		PublishTimeout:     2 * time.Second,
	}

	relay := appOutbox.NewRelay(outboxRepo, publisher, nil, nil, relayCfg)

	// Simulate crash AFTER publish confirm but BEFORE DB update
	relay.OnAfterPublish = func(envelope *domainOutbox.Envelope) error {
		if envelope.EventID == eventID {
			return errors.New("simulated crash after broker confirmation")
		}
		return nil
	}

	// First execution: publishes to RabbitMQ then crashes
	_, _ = relay.ProcessBatch(context.Background())

	// Verify PostgreSQL still has status 'pending' because DB update never committed
	msg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to retrieve outbox message: %v", err)
	}
	if msg.Status != domainOutbox.StatusPending {
		t.Errorf("expected status 'pending' due to crash before DB commit, got '%s'", msg.Status)
	}

	// Verify RabbitMQ received 1 message
	ch, err := rmqClient.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	qInfo, err := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to inspect queue: %v", err)
	}
	if qInfo.Messages != 1 {
		t.Fatalf("expected 1 message in queue after initial publish, got %d", qInfo.Messages)
	}

	// Wait for claim lease to expire (1 second)
	time.Sleep(1100 * time.Millisecond)

	// Simulate relay restart: remove crash hook
	relay.OnAfterPublish = nil

	// Second execution: relay re-claims pending event and publishes AGAIN
	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("restart ProcessBatch failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 message processed on restart, got %d", n)
	}

	// Verify PostgreSQL is now marked published
	finalMsg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to get final outbox message: %v", err)
	}
	if finalMsg.Status != domainOutbox.StatusPublished {
		t.Errorf("expected final status 'published', got '%s'", finalMsg.Status)
	}

	// Verify RabbitMQ queue now has 2 messages (duplicate delivery preserved and documented)
	qInfoAfter, err := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to inspect queue: %v", err)
	}
	if qInfoAfter.Messages != 2 {
		t.Errorf("expected 2 messages in queue due to at-least-once re-delivery, got %d", qInfoAfter.Messages)
	}

	// Consume and verify both messages share identical event_id and job_id
	del1, ok1, _ := ch.Get(cfg.RabbitMQ.Queue, true)
	del2, ok2, _ := ch.Get(cfg.RabbitMQ.Queue, true)
	if !ok1 || !ok2 {
		t.Fatalf("expected two deliveries from queue, got ok1=%v, ok2=%v", ok1, ok2)
	}

	if del1.Headers["event_id"] != del2.Headers["event_id"] || del1.Headers["job_id"] != del2.Headers["job_id"] {
		t.Errorf("expected duplicate messages to have identical IDs, got %v and %v", del1.Headers, del2.Headers)
	}
}

// 6. Concurrent relay instances (verifying FOR UPDATE SKIP LOCKED prevents double-claiming)
func TestIntegration_ConcurrentRelayInstances(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	const totalMessages = 20
	const numRelays = 4

	eventIDs := make([]int64, totalMessages)
	for i := 0; i < totalMessages; i++ {
		jobID := uuid.New()
		payload := []byte(`{"concurrency_test": true}`)
		eventIDs[i] = insertTestJobAndOutbox(t, pgClient, jobID, "job.created", payload)
	}

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())

	relayCfg := appOutbox.Config{
		PollInterval:       10 * time.Millisecond,
		BatchSize:          5, // small batch size to force multiple rounds across relays
		MaxRetries:         5,
		ClaimLeaseDuration: 30 * time.Second,
		PublishTimeout:     3 * time.Second,
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Launch concurrent relay workers
	for i := 0; i < numRelays; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
			defer func() { _ = pub.Close() }()

			relay := appOutbox.NewRelay(outboxRepo, pub, nil, nil, relayCfg)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					n, err := relay.ProcessBatch(ctx)
					if err != nil || n == 0 {
						time.Sleep(20 * time.Millisecond)
						// Check if all messages are published
						pending, _, _ := outboxRepo.GetBacklogMetrics(ctx)
						if pending == 0 {
							return
						}
					}
				}
			}
		}()
	}

	wg.Wait()

	// Verify all 20 outbox records are published
	for _, id := range eventIDs {
		msg, err := outboxRepo.GetByID(context.Background(), id)
		if err != nil {
			t.Fatalf("failed to get outbox message %d: %v", id, err)
		}
		if msg.Status != domainOutbox.StatusPublished {
			t.Errorf("expected outbox message %d to be 'published', got '%s'", id, msg.Status)
		}
	}

	// Verify all 20 messages are in RabbitMQ queue
	ch, err := rmqClient.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	qInfo, err := ch.QueueDeclarePassive(cfg.RabbitMQ.Queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to inspect queue: %v", err)
	}
	if qInfo.Messages != totalMessages {
		t.Errorf("expected %d messages in queue, got %d", totalMessages, qInfo.Messages)
	}
}

// 7. Recovery after broker restart (reconnect on connection drop)
func TestIntegration_RecoveryAfterBrokerRestart(t *testing.T) {
	pgClient, rmqClient, cfg, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	eventID := insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"reconnect":true}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())
	publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() { _ = publisher.Close() }()

	// Force-close underlying AMQP connection to simulate network drop / broker restart
	_ = rmqClient.Connection().Close()

	relayCfg := appOutbox.Config{
		PollInterval:       50 * time.Millisecond,
		BatchSize:          10,
		MaxRetries:         5,
		ClaimLeaseDuration: 5 * time.Second,
		PublishTimeout:     3 * time.Second,
	}

	relay := appOutbox.NewRelay(outboxRepo, publisher, nil, nil, relayCfg)

	// EnsureConnection should automatically re-dial on next ProcessBatch
	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("ProcessBatch failed after simulated broker restart: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 message published upon recovery, got %d", n)
	}

	msg, err := outboxRepo.GetByID(context.Background(), eventID)
	if err != nil {
		t.Fatalf("failed to retrieve outbox message: %v", err)
	}
	if msg.Status != domainOutbox.StatusPublished {
		t.Errorf("expected message to be 'published', got '%s'", msg.Status)
	}
}

// 8. Backlog and Oldest Event Age Metrics
func TestIntegration_OutboxBacklogMetrics(t *testing.T) {
	pgClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	jobID := uuid.New()
	_ = insertTestJobAndOutbox(t, pgClient, jobID, "job.created", []byte(`{"metric":true}`))

	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())

	pendingCount, oldestAge, err := outboxRepo.GetBacklogMetrics(context.Background())
	if err != nil {
		t.Fatalf("GetBacklogMetrics failed: %v", err)
	}

	if pendingCount < 1 {
		t.Errorf("expected pendingCount >= 1, got %d", pendingCount)
	}
	if oldestAge < 0 {
		t.Errorf("expected oldestAge >= 0, got %f", oldestAge)
	}
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
