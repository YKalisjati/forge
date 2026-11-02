package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	appJob "github.com/YKalisjati/forge/internal/app/job"
	domainIdempotency "github.com/YKalisjati/forge/internal/domain/idempotency"
	domainJob "github.com/YKalisjati/forge/internal/domain/job"
	"github.com/YKalisjati/forge/pkg/config"
)

func setupTestService(t *testing.T) (*Client, *appJob.Service, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load test configuration: %v", err)
	}

	client, err := NewClient(context.Background(), &cfg.Postgres)
	if err != nil {
		t.Skipf("skipping PostgreSQL integration test (database unreachable): %v", err)
	}

	uow := NewUnitOfWork(client.Pool())
	jobRepo := NewJobRepository(client.Pool())
	outboxRepo := NewOutboxRepository(client.Pool())
	idempotencyRepo := NewIdempotencyRepository(client.Pool())

	svc := appJob.NewService(uow, jobRepo, outboxRepo, idempotencyRepo, 1048576)

	cleanup := func() {
		client.Close()
	}

	return client, svc, cleanup
}

func TestIntegration_SuccessfulSubmission(t *testing.T) {
	_, svc, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()
	key := fmt.Sprintf("test-key-success-%s", uuid.NewString())
	input := appJob.SubmitJobInput{
		Type:         "email.transactional",
		Payload:      json.RawMessage(`{"to": "user@example.com", "template": "welcome"}`),
		Priority:     10,
		MaxRetries:   3,
		DelaySeconds: 0,
	}

	res, err := svc.SubmitJob(ctx, &key, input)
	if err != nil {
		t.Fatalf("unexpected submission error: %v", err)
	}

	if res.Job.ID == uuid.Nil {
		t.Fatal("expected non-nil job ID")
	}
	if res.Replayed {
		t.Fatal("expected fresh submission to have Replayed=false")
	}
	if res.Job.State != domainJob.StatePending {
		t.Fatalf("expected state pending, got %s", res.Job.State)
	}

	// Verify job retrieval
	retrieved, err := svc.GetJob(ctx, res.Job.ID)
	if err != nil {
		t.Fatalf("failed to get submitted job: %v", err)
	}
	if retrieved.ID != res.Job.ID || retrieved.Priority != 10 {
		t.Fatalf("retrieved job mismatch: %+v", retrieved)
	}
}

func TestIntegration_MissingIdempotencyKey(t *testing.T) {
	_, svc, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()
	input := appJob.SubmitJobInput{
		Type:         "email.transactional",
		Payload:      json.RawMessage(`{"to": "no-key@example.com"}`),
		Priority:     0,
		MaxRetries:   3,
		DelaySeconds: 0,
	}

	// Submit twice without idempotency key
	res1, err := svc.SubmitJob(ctx, nil, input)
	if err != nil {
		t.Fatalf("submission 1 failed: %v", err)
	}

	res2, err := svc.SubmitJob(ctx, nil, input)
	if err != nil {
		t.Fatalf("submission 2 failed: %v", err)
	}

	if res1.Job.ID == res2.Job.ID {
		t.Errorf("jobs submitted without idempotency key must have distinct IDs, got identical: %s", res1.Job.ID)
	}
	if res1.Replayed || res2.Replayed {
		t.Errorf("jobs submitted without key should never be marked replayed")
	}
}

func TestIntegration_SameKeyIdenticalPayload(t *testing.T) {
	client, svc, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()
	key := fmt.Sprintf("test-key-idempotent-%s", uuid.NewString())

	// Different key ordering in identical payload
	payload1 := json.RawMessage(`{"apple": 1, "zebra": 2}`)
	payload2 := json.RawMessage(`{"zebra": 2, "apple": 1}`)

	input1 := appJob.SubmitJobInput{
		Type:       "email.transactional",
		Payload:    payload1,
		Priority:   5,
		MaxRetries: 3,
	}
	input2 := appJob.SubmitJobInput{
		Type:       "email.transactional",
		Payload:    payload2,
		Priority:   5,
		MaxRetries: 3,
	}

	// First submission
	res1, err := svc.SubmitJob(ctx, &key, input1)
	if err != nil {
		t.Fatalf("initial submission failed: %v", err)
	}
	if res1.Replayed {
		t.Errorf("expected initial submission Replayed=false")
	}

	// Second submission with same key & identical normalized payload
	res2, err := svc.SubmitJob(ctx, &key, input2)
	if err != nil {
		t.Fatalf("repeated submission failed: %v", err)
	}
	if !res2.Replayed {
		t.Errorf("expected repeated submission to have Replayed=true")
	}
	if res1.Job.ID != res2.Job.ID {
		t.Errorf("expected identical job ID on replay, got %s vs %s", res1.Job.ID, res2.Job.ID)
	}

	// Verify only 1 row exists in jobs table for this idempotency key
	var count int
	err = client.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1", key).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query jobs count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 job in database, found %d", count)
	}
}

func TestIntegration_SameKeyDifferentPayload(t *testing.T) {
	_, svc, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()
	key := fmt.Sprintf("test-key-mismatch-%s", uuid.NewString())

	input1 := appJob.SubmitJobInput{
		Type:       "email.transactional",
		Payload:    json.RawMessage(`{"action": "first"}`),
		Priority:   5,
		MaxRetries: 3,
	}
	input2 := appJob.SubmitJobInput{
		Type:       "email.transactional",
		Payload:    json.RawMessage(`{"action": "second-different"}`),
		Priority:   5,
		MaxRetries: 3,
	}

	// First submission
	_, err := svc.SubmitJob(ctx, &key, input1)
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}

	// Second submission with different payload
	_, err = svc.SubmitJob(ctx, &key, input2)
	if err == nil {
		t.Fatal("expected error on same key with different payload, got nil")
	}
	if !errors.Is(err, domainIdempotency.ErrPayloadMismatch) {
		t.Errorf("expected ErrPayloadMismatch, got: %v", err)
	}
}

func TestIntegration_ConcurrentSubmissionsSameKey(t *testing.T) {
	client, svc, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()
	key := fmt.Sprintf("test-key-concurrent-%s", uuid.NewString())
	input := appJob.SubmitJobInput{
		Type:       "email.transactional",
		Payload:    json.RawMessage(`{"concurrent": true}`),
		Priority:   10,
		MaxRetries: 3,
	}

	concurrency := 10
	results := make([]*appJob.SubmitResult, concurrency)
	errorsList := make([]error, concurrency)

	var startGate sync.WaitGroup
	startGate.Add(1)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			startGate.Wait() // Synchronize goroutine release

			res, err := svc.SubmitJob(ctx, &key, input)
			results[idx] = res
			errorsList[idx] = err
		}(i)
	}

	startGate.Done() // Unleash all goroutines at once
	wg.Wait()

	var firstID uuid.UUID
	createdCount := 0
	replayedCount := 0

	for i := 0; i < concurrency; i++ {
		if errorsList[i] != nil {
			t.Fatalf("goroutine %d failed with error: %v", i, errorsList[i])
		}
		if results[i] == nil || results[i].Job == nil {
			t.Fatalf("goroutine %d returned nil job", i)
		}

		if firstID == uuid.Nil {
			firstID = results[i].Job.ID
		} else if results[i].Job.ID != firstID {
			t.Errorf("goroutine %d received different job ID %s (expected %s)", i, results[i].Job.ID, firstID)
		}

		if results[i].Replayed {
			replayedCount++
		} else {
			createdCount++
		}
	}

	if createdCount != 1 {
		t.Errorf("expected exactly 1 creator goroutine, got %d", createdCount)
	}
	if replayedCount != concurrency-1 {
		t.Errorf("expected %d replayed goroutines, got %d", concurrency-1, replayedCount)
	}

	// Verify database contains exactly 1 job row
	var count int
	_ = client.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1", key).Scan(&count)
	if count != 1 {
		t.Errorf("database has %d jobs for key, expected exactly 1", count)
	}
}

func TestIntegration_DatabaseRollbackLeavesNoPartialRecords(t *testing.T) {
	client, _, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()
	uow := NewUnitOfWork(client.Pool())
	jobRepo := NewJobRepository(client.Pool())
	outboxRepo := NewOutboxRepository(client.Pool())

	jobID := uuid.New()
	forcedErr := errors.New("simulated error triggering transaction rollback")

	// Execute within transaction and trigger error after inserting job
	err := uow.WithinTransaction(ctx, func(txCtx context.Context) error {
		newJob := &domainJob.Job{
			ID:         jobID,
			Type:       "email.transactional",
			Payload:    json.RawMessage(`{"test": "rollback"}`),
			State:      domainJob.StatePending,
			Priority:   5,
			MaxRetries: 3,
			CreatedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
		}
		if err := jobRepo.Create(txCtx, newJob); err != nil {
			return err
		}

		outboxMsg := &domainJob.Job{} // Not outbox
		_ = outboxMsg
		return forcedErr
	})

	if !errors.Is(err, forcedErr) {
		t.Fatalf("expected forcedErr, got %v", err)
	}

	// Verify that the job was NOT persisted due to rollback
	var exists bool
	_ = client.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)", jobID).Scan(&exists)
	if exists {
		t.Errorf("job record was found in database; transaction rollback failed to clean up")
	}

	// Verify no outbox record exists
	_ = client.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM outbox_messages WHERE job_id = $1)", jobID).Scan(&exists)
	if exists {
		t.Errorf("outbox record was found in database; transaction rollback failed to clean up")
	}

	_ = outboxRepo
}

func TestIntegration_PaginationAndNotFound(t *testing.T) {
	_, svc, cleanup := setupTestService(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Not Found behavior
	nonExistentID := uuid.New()
	_, err := svc.GetJob(ctx, nonExistentID)
	if err == nil {
		t.Fatal("expected ErrJobNotFound for non-existent ID, got nil")
	}
	if !errors.Is(err, domainJob.ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got: %v", err)
	}

	// 2. Seed 5 jobs for pagination
	uniqueType := "webhook.delivery"
	for i := 0; i < 5; i++ {
		input := appJob.SubmitJobInput{
			Type:       uniqueType,
			Payload:    json.RawMessage(fmt.Sprintf(`{"index": %d}`, i)),
			Priority:   i,
			MaxRetries: 3,
		}
		if _, err := svc.SubmitJob(ctx, nil, input); err != nil {
			t.Fatalf("failed to seed job %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // distinct created_at
	}

	// Page 1: Limit 2
	page1Jobs, cursor1, hasMore1, err := svc.ListJobs(ctx, domainJob.Filter{
		Type:  &uniqueType,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("page 1 failed: %v", err)
	}
	if len(page1Jobs) != 2 || !hasMore1 || cursor1 == nil {
		t.Fatalf("page 1 expected 2 items with hasMore=true, got %d items, hasMore=%v", len(page1Jobs), hasMore1)
	}

	// Page 2: Limit 2 with Cursor
	page2Jobs, cursor2, hasMore2, err := svc.ListJobs(ctx, domainJob.Filter{
		Type:   &uniqueType,
		Limit:  2,
		Cursor: cursor1,
	})
	if err != nil {
		t.Fatalf("page 2 failed: %v", err)
	}
	if len(page2Jobs) != 2 || !hasMore2 || cursor2 == nil {
		t.Fatalf("page 2 expected 2 items with hasMore=true, got %d items", len(page2Jobs))
	}
	if page1Jobs[1].ID == page2Jobs[0].ID {
		t.Errorf("cursor pagination returned duplicate item across pages")
	}

	// Page 3: Limit 2 with Cursor
	page3Jobs, _, hasMore3, err := svc.ListJobs(ctx, domainJob.Filter{
		Type:   &uniqueType,
		Limit:  2,
		Cursor: cursor2,
	})
	if err != nil {
		t.Fatalf("page 3 failed: %v", err)
	}
	if len(page3Jobs) < 1 {
		t.Fatalf("page 3 expected at least 1 item, got %d", len(page3Jobs))
	}
	_ = hasMore3
}
