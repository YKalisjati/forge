package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
	pkgMetrics "github.com/YKalisjati/forge/pkg/metrics"
)

// Publisher defines the message broker publishing interface.
type Publisher interface {
	Publish(ctx context.Context, envelope *domainOutbox.Envelope) error
	Close() error
}

// Config defines execution parameters for the Outbox Relay.
type Config struct {
	PollInterval       time.Duration
	BatchSize          int
	MaxRetries         int
	ClaimLeaseDuration time.Duration
	PublishTimeout     time.Duration
}

// DefaultConfig returns recommended production defaults.
func DefaultConfig() Config {
	return Config{
		PollInterval:       100 * time.Millisecond,
		BatchSize:          100,
		MaxRetries:         5,
		ClaimLeaseDuration: 30 * time.Second,
		PublishTimeout:     5 * time.Second,
	}
}

// Relay coordinates claiming pending outbox events from PostgreSQL and publishing to RabbitMQ.
type Relay struct {
	repo        domainOutbox.Repository
	publisher   Publisher
	metrics     *Metrics
	promMetrics *pkgMetrics.Metrics
	logger      *slog.Logger
	cfg         Config

	// Test hooks for failure and crash injection
	OnBeforePublish func(envelope *domainOutbox.Envelope) error
	OnAfterPublish  func(envelope *domainOutbox.Envelope) error
}

// NewRelay initializes an Outbox Relay instance.
func NewRelay(
	repo domainOutbox.Repository,
	publisher Publisher,
	metrics *Metrics,
	logger *slog.Logger,
	cfg Config,
) *Relay {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.ClaimLeaseDuration <= 0 {
		cfg.ClaimLeaseDuration = 30 * time.Second
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = NewMetrics()
	}

	return &Relay{
		repo:        repo,
		publisher:   publisher,
		metrics:     metrics,
		promMetrics: pkgMetrics.Default(),
		logger:      logger,
		cfg:         cfg,
	}
}

// SetPromMetrics assigns a custom Prometheus metrics collector.
func (r *Relay) SetPromMetrics(m *pkgMetrics.Metrics) {
	r.promMetrics = m
}

// Metrics returns the active operational metrics tracker.
func (r *Relay) Metrics() *Metrics {
	return r.metrics
}

// Run executes the continuous background polling loop until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	r.logger.Info("outbox relay started",
		slog.Duration("poll_interval", r.cfg.PollInterval),
		slog.Int("batch_size", r.cfg.BatchSize),
		slog.Int("max_retries", r.cfg.MaxRetries),
		slog.Duration("claim_lease", r.cfg.ClaimLeaseDuration),
	)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("outbox relay received shutdown signal, exiting loop")
			return nil
		case <-ticker.C:
			count, err := r.ProcessBatch(ctx)
			if err != nil && ctx.Err() == nil {
				r.logger.Error("error processing outbox batch", slog.String("error", err.Error()))
			} else if count > 0 {
				r.logger.Debug("processed outbox batch", slog.Int("published_count", count))
			}
		}
	}
}

// ProcessBatch claims eligible unpublished outbox events and publishes them to RabbitMQ.
// Does not hold a database transaction open while waiting for broker confirmations.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// 1. Refresh backlog metrics
	pendingCount, oldestAge, err := r.repo.GetBacklogMetrics(ctx)
	if err == nil {
		r.metrics.SetBacklog(pendingCount)
		r.metrics.SetOldestEventAge(oldestAge)
		if r.promMetrics != nil {
			r.promMetrics.SetOutboxBacklog(pendingCount, oldestAge)
		}
	}

	// 2. Claim batch atomically using FOR UPDATE SKIP LOCKED and advance scheduled_at
	messages, err := r.repo.ClaimBatch(ctx, r.cfg.BatchSize, r.cfg.ClaimLeaseDuration)
	if err != nil {
		return 0, fmt.Errorf("failed to claim outbox batch: %w", err)
	}

	if len(messages) == 0 {
		return 0, nil
	}

	r.logger.Debug("claimed outbox batch",
		slog.Int("count", len(messages)),
		slog.Duration("lease_duration", r.cfg.ClaimLeaseDuration),
	)

	processed := 0
	confirmedIDs := make([]int64, 0, len(messages))

	flushConfirmed := func() error {
		if len(confirmedIDs) == 0 {
			return nil
		}
		if markErr := r.repo.MarkPublished(ctx, confirmedIDs); markErr != nil {
			r.logger.Error("failed to mark outbox events published in batch",
				slog.Int("count", len(confirmedIDs)),
				slog.String("error", markErr.Error()),
			)
			return markErr
		}
		processed += len(confirmedIDs)
		confirmedIDs = confirmedIDs[:0]
		return nil
	}

	// 3. Process each message independently
	for _, msg := range messages {
		if ctx.Err() != nil {
			_ = flushConfirmed()
			return processed, ctx.Err()
		}

		envelope := &domainOutbox.Envelope{
			EventID:       msg.ID,
			JobID:         msg.JobID,
			EventType:     msg.EventType,
			SchemaVersion: 1,
			CorrelationID: msg.JobID.String(),
			Payload:       msg.Payload,
			Timestamp:     time.Now().UTC(),
		}

		// Pre-publish hook (e.g. for crash testing)
		if r.OnBeforePublish != nil {
			if hookErr := r.OnBeforePublish(envelope); hookErr != nil {
				r.logger.Warn("pre-publish hook halted processing",
					slog.Int64("event_id", msg.ID),
					slog.String("error", hookErr.Error()),
				)
				_ = flushConfirmed()
				return processed, hookErr
			}
		}

		r.metrics.IncPublishAttempts()

		pubCtx, pubCancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
		pubErr := r.publisher.Publish(pubCtx, envelope)
		pubCancel()

		if pubErr != nil {
			r.metrics.IncConfirmFailures()
			if r.promMetrics != nil {
				r.promMetrics.OutboxPublishErrorsTotal.WithLabelValues("publish_failure").Inc()
			}
			r.logger.Warn("outbox publish or confirm failed",
				slog.Int64("event_id", msg.ID),
				slog.String("job_id", msg.JobID.String()),
				slog.Int("attempt", msg.RetryCount+1),
				slog.String("error", pubErr.Error()),
			)

			failedPermanently := (msg.RetryCount + 1) >= r.cfg.MaxRetries
			var nextAttempt time.Time

			if failedPermanently {
				r.logger.Error("outbox event exhausted max retries, marking failed permanently",
					slog.Int64("event_id", msg.ID),
					slog.Int("max_retries", r.cfg.MaxRetries),
				)
				nextAttempt = time.Now().Add(24 * time.Hour)
			} else {
				delay := r.CalculateBackoff(msg.RetryCount)
				nextAttempt = time.Now().Add(delay)
			}

			if recErr := r.repo.RecordFailure(ctx, msg.ID, pubErr.Error(), nextAttempt, failedPermanently); recErr != nil {
				r.logger.Error("failed to record outbox failure",
					slog.Int64("event_id", msg.ID),
					slog.String("error", recErr.Error()),
				)
			}
			continue
		}

		// Message successfully published and confirmed by broker
		r.metrics.IncPublishedTotal()
		if r.promMetrics != nil {
			r.promMetrics.OutboxPublishedTotal.WithLabelValues(msg.EventType).Inc()
		}
		r.logger.Info("outbox event published and confirmed",
			slog.Int64("event_id", msg.ID),
			slog.String("job_id", msg.JobID.String()),
			slog.String("event_type", msg.EventType),
		)

		// Post-publish hook (for simulating crash after publish but before DB update)
		if r.OnAfterPublish != nil {
			if hookErr := r.OnAfterPublish(envelope); hookErr != nil {
				r.logger.Warn("post-publish hook simulated crash before db update",
					slog.Int64("event_id", msg.ID),
					slog.String("error", hookErr.Error()),
				)
				return processed, hookErr
			}
			// When test hook is active, mark published immediately
			if markErr := r.repo.MarkPublished(ctx, []int64{msg.ID}); markErr != nil {
				r.logger.Error("failed to mark outbox event published",
					slog.Int64("event_id", msg.ID),
					slog.String("error", markErr.Error()),
				)
			} else {
				processed++
			}
		} else {
			confirmedIDs = append(confirmedIDs, msg.ID)
			if len(confirmedIDs) >= 50 {
				_ = flushConfirmed()
			}
		}
	}

	// Flush any remaining confirmed outbox IDs
	_ = flushConfirmed()

	return processed, nil
}

// CalculateBackoff calculates exponential backoff with jitter for a given retry attempt.
func (r *Relay) CalculateBackoff(attempt int) time.Duration {
	base := 500 * time.Millisecond
	maxDelay := 30 * time.Second

	if attempt < 0 {
		attempt = 0
	}
	if attempt > 10 {
		attempt = 10
	}

	delay := base * (1 << attempt)
	if delay > maxDelay {
		delay = maxDelay
	}

	// Add jitter (up to 20% of delay or 50ms)
	jitterMax := int64(float64(delay) * 0.2)
	if jitterMax <= 0 {
		jitterMax = int64(50 * time.Millisecond)
	}
	jitter := time.Duration(rand.Int63n(jitterMax))

	return delay + jitter
}
