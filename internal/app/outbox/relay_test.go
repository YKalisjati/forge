package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
)

type mockRepository struct {
	claimFunc      func(ctx context.Context, limit int, lease time.Duration) ([]*domainOutbox.Message, error)
	markPubFunc    func(ctx context.Context, ids []int64) error
	recordFailFunc func(ctx context.Context, id int64, errMessage string, nextAttempt time.Time, failedPermanently bool) error
	metricsFunc    func(ctx context.Context) (int64, float64, error)
	getByIDFunc    func(ctx context.Context, id int64) (*domainOutbox.Message, error)
}

func (m *mockRepository) Create(_ context.Context, _ *domainOutbox.Message) error {
	return nil
}

func (m *mockRepository) FetchPendingBatch(_ context.Context, _ int) ([]*domainOutbox.Message, error) {
	return nil, nil
}

func (m *mockRepository) ClaimBatch(ctx context.Context, limit int, lease time.Duration) ([]*domainOutbox.Message, error) {
	if m.claimFunc != nil {
		return m.claimFunc(ctx, limit, lease)
	}
	return nil, nil
}

func (m *mockRepository) MarkPublished(ctx context.Context, ids []int64) error {
	if m.markPubFunc != nil {
		return m.markPubFunc(ctx, ids)
	}
	return nil
}

func (m *mockRepository) RecordFailure(ctx context.Context, id int64, errMessage string, nextAttempt time.Time, failedPermanently bool) error {
	if m.recordFailFunc != nil {
		return m.recordFailFunc(ctx, id, errMessage, nextAttempt, failedPermanently)
	}
	return nil
}

func (m *mockRepository) GetBacklogMetrics(ctx context.Context) (int64, float64, error) {
	if m.metricsFunc != nil {
		return m.metricsFunc(ctx)
	}
	return 0, 0, nil
}

func (m *mockRepository) GetByID(ctx context.Context, id int64) (*domainOutbox.Message, error) {
	if m.getByIDFunc != nil {
		return m.getByIDFunc(ctx, id)
	}
	return nil, nil
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

func TestCalculateBackoff(t *testing.T) {
	relay := NewRelay(&mockRepository{}, &mockPublisher{}, nil, nil, DefaultConfig())

	for attempt := 0; attempt <= 6; attempt++ {
		backoff := relay.CalculateBackoff(attempt)
		expectedMin := 500 * time.Millisecond * (1 << attempt)
		if expectedMin > 30*time.Second {
			expectedMin = 30 * time.Second
		}
		if backoff < expectedMin {
			t.Errorf("attempt %d backoff %v is less than expected base %v", attempt, backoff, expectedMin)
		}
	}
}

func TestRelay_ProcessBatch_Success(t *testing.T) {
	jobID := uuid.New()
	msg := &domainOutbox.Message{
		ID:          1001,
		JobID:       jobID,
		EventType:   "job.created",
		Payload:     json.RawMessage(`{"key":"value"}`),
		Status:      domainOutbox.StatusPending,
		ScheduledAt: time.Now(),
		RetryCount:  0,
		CreatedAt:   time.Now().Add(-5 * time.Minute),
	}

	var publishedEnvelopes []*domainOutbox.Envelope
	pub := &mockPublisher{
		publishFunc: func(_ context.Context, env *domainOutbox.Envelope) error {
			publishedEnvelopes = append(publishedEnvelopes, env)
			return nil
		},
	}

	var markedIDs []int64
	repo := &mockRepository{
		claimFunc: func(_ context.Context, _ int, _ time.Duration) ([]*domainOutbox.Message, error) {
			return []*domainOutbox.Message{msg}, nil
		},
		markPubFunc: func(_ context.Context, ids []int64) error {
			markedIDs = append(markedIDs, ids...)
			return nil
		},
		metricsFunc: func(_ context.Context) (int64, float64, error) {
			return 1, 300.0, nil
		},
	}

	metrics := NewMetrics()
	relay := NewRelay(repo, pub, metrics, nil, DefaultConfig())

	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected process batch error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 processed, got %d", n)
	}

	if len(publishedEnvelopes) != 1 {
		t.Fatalf("expected 1 published envelope, got %d", len(publishedEnvelopes))
	}
	env := publishedEnvelopes[0]
	if env.EventID != 1001 || env.JobID != jobID || env.EventType != "job.created" || env.SchemaVersion != 1 {
		t.Errorf("unexpected envelope fields: %+v", env)
	}

	if len(markedIDs) != 1 || markedIDs[0] != 1001 {
		t.Errorf("expected id 1001 marked published, got: %v", markedIDs)
	}

	snap := metrics.Snapshot()
	if snap.PublishAttempts != 1 || snap.PublishedTotal != 1 || snap.ConfirmFailures != 0 {
		t.Errorf("unexpected metrics snapshot: %+v", snap)
	}
	if snap.BacklogTotal != 1 || snap.OldestEventAgeSeconds != 300.0 {
		t.Errorf("unexpected backlog snapshot: %+v", snap)
	}
}

func TestRelay_ProcessBatch_PublishFailure_Retry(t *testing.T) {
	jobID := uuid.New()
	msg := &domainOutbox.Message{
		ID:          2001,
		JobID:       jobID,
		EventType:   "job.created",
		Payload:     json.RawMessage(`{}`),
		Status:      domainOutbox.StatusPending,
		ScheduledAt: time.Now(),
		RetryCount:  1,
	}

	pub := &mockPublisher{
		publishFunc: func(_ context.Context, _ *domainOutbox.Envelope) error {
			return errors.New("temporary rabbitmq disconnect")
		},
	}

	var recordedFailID int64
	var recordedNextAttempt time.Time
	var recordedPermanent bool
	repo := &mockRepository{
		claimFunc: func(_ context.Context, _ int, _ time.Duration) ([]*domainOutbox.Message, error) {
			return []*domainOutbox.Message{msg}, nil
		},
		recordFailFunc: func(_ context.Context, id int64, _ string, next time.Time, permanent bool) error {
			recordedFailID = id
			recordedNextAttempt = next
			recordedPermanent = permanent
			return nil
		},
	}

	metrics := NewMetrics()
	cfg := DefaultConfig()
	cfg.MaxRetries = 5
	relay := NewRelay(repo, pub, metrics, nil, cfg)

	n, err := relay.ProcessBatch(context.Background())
	if err != nil {
		t.Fatalf("unexpected process batch error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 published, got %d", n)
	}

	if recordedFailID != 2001 {
		t.Errorf("expected failure recorded for id 2001, got %d", recordedFailID)
	}
	if recordedPermanent {
		t.Errorf("expected non-permanent failure since retry 2 < 5")
	}
	if recordedNextAttempt.Before(time.Now()) {
		t.Errorf("expected nextAttempt in future, got %v", recordedNextAttempt)
	}

	snap := metrics.Snapshot()
	if snap.PublishAttempts != 1 || snap.ConfirmFailures != 1 || snap.PublishedTotal != 0 {
		t.Errorf("unexpected metrics snapshot: %+v", snap)
	}
}

func TestRelay_ProcessBatch_ExhaustedRetries(t *testing.T) {
	msg := &domainOutbox.Message{
		ID:          3001,
		JobID:       uuid.New(),
		EventType:   "job.created",
		Payload:     json.RawMessage(`{}`),
		Status:      domainOutbox.StatusPending,
		RetryCount:  4, // 4 + 1 = 5 (max retries)
		ScheduledAt: time.Now(),
	}

	pub := &mockPublisher{
		publishFunc: func(_ context.Context, _ *domainOutbox.Envelope) error {
			return errors.New("negative confirmation nack")
		},
	}

	var recordedPermanent bool
	repo := &mockRepository{
		claimFunc: func(_ context.Context, _ int, _ time.Duration) ([]*domainOutbox.Message, error) {
			return []*domainOutbox.Message{msg}, nil
		},
		recordFailFunc: func(_ context.Context, _ int64, _ string, _ time.Time, permanent bool) error {
			recordedPermanent = permanent
			return nil
		},
	}

	cfg := DefaultConfig()
	cfg.MaxRetries = 5
	relay := NewRelay(repo, pub, nil, nil, cfg)

	_, _ = relay.ProcessBatch(context.Background())

	if !recordedPermanent {
		t.Errorf("expected failure to be marked permanent when attempt reaches max retries")
	}
}

func TestRelay_GracefulShutdown(t *testing.T) {
	repo := &mockRepository{
		claimFunc: func(_ context.Context, _ int, _ time.Duration) ([]*domainOutbox.Message, error) {
			return nil, nil
		},
	}
	pub := &mockPublisher{}

	cfg := DefaultConfig()
	cfg.PollInterval = 10 * time.Millisecond
	relay := NewRelay(repo, pub, nil, nil, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- relay.Run(ctx)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel() // Trigger graceful shutdown

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("relay.Run returned unexpected error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("relay.Run did not terminate cleanly on context cancellation")
	}
}
