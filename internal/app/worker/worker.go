package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/internal/domain/job"
	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
	pkgMetrics "github.com/YKalisjati/forge/pkg/metrics"
)

// Consumer defines the RabbitMQ consumer abstraction.
type Consumer interface {
	Consume(ctx context.Context, queue, consumerTag string, prefetchCount int) (<-chan amqp.Delivery, error)
	Cancel() error
	Close() error
}

// Config defines operational parameters for the worker.
type Config struct {
	Queue             string
	Concurrency       int
	PrefetchCount     int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
	Backoff           job.BackoffPolicy
}

// DefaultConfig provides sensible defaults for worker configuration.
func DefaultConfig() Config {
	return Config{
		Queue:             "forge.jobs",
		Concurrency:       10,
		PrefetchCount:     20,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		ShutdownTimeout:   30 * time.Second,
		Backoff:           job.DefaultBackoffPolicy(),
	}
}

// Worker processes background jobs consumed from RabbitMQ.
type Worker struct {
	repo        job.Repository
	consumer    Consumer
	registry    *Registry
	metrics     *Metrics
	promMetrics *pkgMetrics.Metrics
	logger      *slog.Logger
	cfg         Config
	workerID    string
	backoff     job.BackoffPolicy

	// Test hooks
	OnBeforeComplete func(j *job.Job, leaseID uuid.UUID) error
	OnBeforeRetry    func(j *job.Job, leaseID uuid.UUID) error
}

// NewWorker initializes a new Worker service.
func NewWorker(
	repo job.Repository,
	consumer Consumer,
	registry *Registry,
	metrics *Metrics,
	logger *slog.Logger,
	cfg Config,
) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.PrefetchCount <= 0 {
		cfg.PrefetchCount = 20
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Backoff.BaseInterval <= 0 {
		cfg.Backoff = job.DefaultBackoffPolicy()
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	if registry == nil {
		registry = NewRegistry()
	}

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("worker-%s-%s", hostname, uuid.New().String()[:8])

	return &Worker{
		repo:        repo,
		consumer:    consumer,
		registry:    registry,
		metrics:     metrics,
		promMetrics: pkgMetrics.Default(),
		logger:      logger,
		cfg:         cfg,
		workerID:    workerID,
		backoff:     cfg.Backoff,
	}
}

// SetPromMetrics assigns a custom Prometheus metrics collector.
func (w *Worker) SetPromMetrics(m *pkgMetrics.Metrics) {
	w.promMetrics = m
}

// Metrics returns the active metrics tracker.
func (w *Worker) Metrics() *Metrics {
	return w.metrics
}

// WorkerID returns the unique worker identifier.
func (w *Worker) WorkerID() string {
	return w.workerID
}

// SetWorkerID sets a custom worker ID (useful for multi-worker testing).
func (w *Worker) SetWorkerID(id string) {
	w.workerID = id
}

// Run starts consuming messages and spawns the bounded worker pool.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("starting worker engine",
		slog.String("worker_id", w.workerID),
		slog.String("queue", w.cfg.Queue),
		slog.Int("concurrency", w.cfg.Concurrency),
		slog.Int("prefetch_count", w.cfg.PrefetchCount),
		slog.Duration("lease_duration", w.cfg.LeaseDuration),
	)

	deliveries, err := w.consumer.Consume(ctx, w.cfg.Queue, w.workerID, w.cfg.PrefetchCount)
	if err != nil {
		return fmt.Errorf("worker failed to start consuming: %w", err)
	}

	var wg sync.WaitGroup

	// Spawn worker pool
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					w.ProcessDelivery(ctx, d)
				}
			}
		}(i)
	}

	// Wait for context cancellation
	<-ctx.Done()
	w.logger.Info("worker context cancelled, stopping consumer and draining active executions")

	// Stop accepting new messages
	_ = w.consumer.Cancel()

	// Wait for in-flight tasks with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("all active worker goroutines finished cleanly")
	case <-time.After(w.cfg.ShutdownTimeout):
		w.logger.Warn("worker shutdown timeout exceeded; forcing exit")
	}

	_ = w.consumer.Close()
	return nil
}

// ProcessDelivery handles a single AMQP delivery from RabbitMQ.
func (w *Worker) ProcessDelivery(ctx context.Context, d amqp.Delivery) {
	w.metrics.IncJobsProcessed()

	// 1. Validate message envelope and schema version
	var envelope domainOutbox.Envelope
	if err := json.Unmarshal(d.Body, &envelope); err != nil {
		w.metrics.IncMalformedMessages()
		w.metrics.IncDLQEvents()
		if w.promMetrics != nil {
			w.promMetrics.IncDeadLetter("malformed_json")
		}
		w.logger.Error("discarding malformed message: invalid JSON envelope, routing to DLQ",
			slog.String("error", err.Error()),
			slog.String("body", string(d.Body)),
		)
		_ = d.Nack(false, false)
		return
	}

	if envelope.SchemaVersion != 1 {
		w.metrics.IncMalformedMessages()
		w.metrics.IncDLQEvents()
		if w.promMetrics != nil {
			w.promMetrics.IncDeadLetter("unsupported_schema")
		}
		w.logger.Error("discarding message with unsupported schema version, routing to DLQ",
			slog.Int("schema_version", envelope.SchemaVersion),
			slog.String("job_id", envelope.JobID.String()),
		)
		_ = d.Nack(false, false)
		return
	}

	if envelope.JobID == uuid.Nil {
		w.metrics.IncMalformedMessages()
		w.metrics.IncDLQEvents()
		if w.promMetrics != nil {
			w.promMetrics.IncDeadLetter("nil_job_id")
		}
		w.logger.Error("discarding message with nil job ID, routing to DLQ")
		_ = d.Nack(false, false)
		return
	}

	// 2. Atomic Lease Claiming in PostgreSQL
	claimedJob, leaseID, attempt, err := w.repo.ClaimJob(ctx, envelope.JobID, w.cfg.LeaseDuration, w.workerID)
	if err != nil {
		switch {
		case errors.Is(err, job.ErrJobNotFound):
			w.metrics.IncUnknownJobs()
			w.logger.Warn("job not found in database for delivery, discarding",
				slog.String("job_id", envelope.JobID.String()),
			)
			_ = d.Ack(false)
			return

		case errors.Is(err, job.ErrJobAlreadyCompleted):
			w.metrics.IncDuplicateDeliveries()
			w.logger.Info("duplicate message received for already completed job, acknowledging",
				slog.String("job_id", envelope.JobID.String()),
			)
			_ = d.Ack(false)
			return

		case errors.Is(err, job.ErrJobAlreadyClaimed):
			w.metrics.IncDuplicateDeliveries()
			w.logger.Info("job is currently locked by another active worker lease, acknowledging duplicate delivery",
				slog.String("job_id", envelope.JobID.String()),
			)
			_ = d.Ack(false)
			return

		case errors.Is(err, job.ErrJobDeadLettered):
			w.logger.Warn("job is in dead_letter state, discarding delivery",
				slog.String("job_id", envelope.JobID.String()),
			)
			_ = d.Ack(false)
			return

		default:
			// Transient database error: Nack with requeue=true so it is retried
			w.logger.Error("database error while claiming job, requeuing message",
				slog.String("job_id", envelope.JobID.String()),
				slog.String("error", err.Error()),
			)
			_ = d.Nack(false, true)
			return
		}
	}

	w.logger.Info("claimed job execution lease",
		slog.String("job_id", claimedJob.ID.String()),
		slog.String("type", claimedJob.Type),
		slog.String("lease_id", leaseID.String()),
		slog.Int("attempt", attempt),
	)

	// 3. Handler lookup
	handler, ok := w.registry.Get(claimedJob.Type)
	if !ok {
		w.metrics.IncUnsupportedType()
		w.logger.Error("unsupported job type: no handler registered",
			slog.String("job_id", claimedJob.ID.String()),
			slog.String("type", claimedJob.Type),
		)
		errMsg := fmt.Sprintf("unsupported job type: %s", claimedJob.Type)
		_ = w.repo.DeadLetterJob(ctx, claimedJob.ID, leaseID, errMsg, 0)
		w.metrics.IncJobsFailed()
		w.metrics.IncDeadLettered()
		if w.promMetrics != nil {
			w.promMetrics.ObserveJobExecution(claimedJob.Type, "unsupported_type", 0)
			w.promMetrics.IncDeadLetter("unsupported_type")
		}
		_ = d.Ack(false)
		return
	}

	// 4. Execute handler with context and lease heartbeating
	w.metrics.IncActiveExecutions()
	defer w.metrics.DecActiveExecutions()
	if w.promMetrics != nil {
		w.promMetrics.WorkerActiveExecutions.Inc()
		defer w.promMetrics.WorkerActiveExecutions.Dec()
	}

	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	// Start lease heartbeating goroutine
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)

	if w.cfg.HeartbeatInterval > 0 {
		go func() {
			ticker := time.NewTicker(w.cfg.HeartbeatInterval)
			defer ticker.Stop()

			for {
				select {
				case <-heartbeatDone:
					return
				case <-execCtx.Done():
					return
				case <-ticker.C:
					hbErr := w.repo.ExtendLease(execCtx, claimedJob.ID, leaseID, w.cfg.LeaseDuration)
					if hbErr != nil {
						w.logger.Warn("lease heartbeat extension failed, cancelling in-flight execution",
							slog.String("job_id", claimedJob.ID.String()),
							slog.String("lease_id", leaseID.String()),
							slog.String("error", hbErr.Error()),
						)
						if errors.Is(hbErr, job.ErrFencingViolation) {
							w.metrics.IncFencingViolations()
							if w.promMetrics != nil {
								w.promMetrics.WorkerFencingViolations.Inc()
							}
						}
						cancelExec()
						return
					}
				}
			}
		}()
	}

	startTime := time.Now()
	execErr := handler.Execute(execCtx, claimedJob)
	duration := time.Since(startTime)

	// 5. Completion and Fencing Check
	if execErr != nil {
		if ctx.Err() != nil {
			// Worker is shutting down: requeue message so another worker can process it
			w.logger.Warn("job execution interrupted by worker shutdown, requeuing",
				slog.String("job_id", claimedJob.ID.String()),
			)
			_ = d.Nack(false, true)
			return
		}

		w.metrics.IncJobsFailed()
		w.logger.Warn("job execution failed",
			slog.String("job_id", claimedJob.ID.String()),
			slog.String("error", execErr.Error()),
			slog.Duration("duration", duration),
		)

		isPermanent := job.IsPermanent(execErr)
		isPoison := job.IsPoison(execErr)

		if isPermanent {
			w.logger.Warn("permanent failure detected, moving job to dead_letter",
				slog.String("job_id", claimedJob.ID.String()),
				slog.String("error", execErr.Error()),
			)
			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			dlErr := w.repo.DeadLetterJob(commitCtx, claimedJob.ID, leaseID, execErr.Error(), duration)
			commitCancel()
			if dlErr != nil {
				if errors.Is(dlErr, job.ErrFencingViolation) {
					w.metrics.IncFencingViolations()
					if w.promMetrics != nil {
						w.promMetrics.WorkerFencingViolations.Inc()
					}
					_ = d.Ack(false)
					return
				}
				w.logger.Error("failed to persist dead letter state, requeuing message",
					slog.String("job_id", claimedJob.ID.String()),
					slog.String("error", dlErr.Error()),
				)
				_ = d.Nack(false, true)
				return
			}
			w.metrics.IncDeadLettered()
			if w.promMetrics != nil {
				w.promMetrics.ObserveJobExecution(claimedJob.Type, "failed_permanent", duration)
				w.promMetrics.IncDeadLetter("permanent_failure")
			}
			if isPoison {
				w.metrics.IncDLQEvents()
				_ = d.Nack(false, false)
			} else {
				_ = d.Ack(false)
			}
			return
		}

		// Transient error: evaluate retry eligibility
		if claimedJob.RetriesAttempted <= claimedJob.MaxRetries {
			delay := w.backoff.NextDelay(claimedJob.RetriesAttempted)
			nextAttemptAt := time.Now().Add(delay)

			if w.OnBeforeRetry != nil {
				if hookErr := w.OnBeforeRetry(claimedJob, leaseID); hookErr != nil {
					w.logger.Error("pre-retry hook returned error, message will not be acknowledged",
						slog.String("job_id", claimedJob.ID.String()),
						slog.String("error", hookErr.Error()),
					)
					_ = d.Nack(false, true)
					return
				}
			}

			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			retryErr := w.repo.ScheduleRetry(commitCtx, claimedJob.ID, leaseID, execErr.Error(), nextAttemptAt, duration)
			commitCancel()
			if retryErr != nil {
				if errors.Is(retryErr, job.ErrFencingViolation) {
					w.metrics.IncFencingViolations()
					if w.promMetrics != nil {
						w.promMetrics.WorkerFencingViolations.Inc()
					}
					w.logger.Warn("fencing violation detected on retry schedule: lease expired, discarding update",
						slog.String("job_id", claimedJob.ID.String()),
						slog.String("lease_id", leaseID.String()),
					)
					_ = d.Ack(false)
					return
				}
				w.logger.Error("failed to persist retry schedule to database, requeuing message",
					slog.String("job_id", claimedJob.ID.String()),
					slog.String("error", retryErr.Error()),
				)
				_ = d.Nack(false, true)
				return
			}

			w.metrics.IncRetriesScheduled()
			if w.promMetrics != nil {
				w.promMetrics.IncJobRetry(claimedJob.Type)
				w.promMetrics.ObserveJobExecution(claimedJob.Type, "retry_scheduled", duration)
			}
			w.logger.Info("scheduled job retry",
				slog.String("job_id", claimedJob.ID.String()),
				slog.Int("attempt", claimedJob.RetriesAttempted),
				slog.Int("max_retries", claimedJob.MaxRetries),
				slog.Duration("delay", delay),
				slog.Time("next_attempt_at", nextAttemptAt),
			)
			_ = d.Ack(false)
			return
		}

		// Retries exhausted
		w.logger.Warn("max retries exhausted, moving job to dead_letter",
			slog.String("job_id", claimedJob.ID.String()),
			slog.Int("retries_attempted", claimedJob.RetriesAttempted),
			slog.Int("max_retries", claimedJob.MaxRetries),
		)
		exhaustErrMsg := fmt.Sprintf("retries exhausted (%d/%d): %s", claimedJob.RetriesAttempted, claimedJob.MaxRetries, execErr.Error())
		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		dlErr := w.repo.DeadLetterJob(commitCtx, claimedJob.ID, leaseID, exhaustErrMsg, duration)
		commitCancel()
		if dlErr != nil {
			if errors.Is(dlErr, job.ErrFencingViolation) {
				w.metrics.IncFencingViolations()
				if w.promMetrics != nil {
					w.promMetrics.WorkerFencingViolations.Inc()
				}
				_ = d.Ack(false)
				return
			}
			w.logger.Error("failed to persist retry exhaustion dead letter, requeuing message",
				slog.String("job_id", claimedJob.ID.String()),
				slog.String("error", dlErr.Error()),
			)
			_ = d.Nack(false, true)
			return
		}

		w.metrics.IncExhaustedAttempts()
		w.metrics.IncDeadLettered()
		if w.promMetrics != nil {
			w.promMetrics.ObserveJobExecution(claimedJob.Type, "retries_exhausted", duration)
			w.promMetrics.IncDeadLetter("retries_exhausted")
		}
		_ = d.Ack(false)
		return
	}

	// Test hook (e.g. for simulating database error before success is persisted)
	if w.OnBeforeComplete != nil {
		if hookErr := w.OnBeforeComplete(claimedJob, leaseID); hookErr != nil {
			w.logger.Error("pre-completion hook returned error, message will not be acknowledged",
				slog.String("job_id", claimedJob.ID.String()),
				slog.String("error", hookErr.Error()),
			)
			_ = d.Nack(false, true)
			return
		}
	}

	// Persist success status and result in PostgreSQL BEFORE acknowledging the message
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	compErr := w.repo.CompleteJob(commitCtx, claimedJob.ID, leaseID, duration)
	commitCancel()
	if compErr != nil {
		if errors.Is(compErr, job.ErrFencingViolation) {
			w.metrics.IncFencingViolations()
			if w.promMetrics != nil {
				w.promMetrics.WorkerFencingViolations.Inc()
			}
			w.logger.Warn("fencing violation detected on completion: lease expired and job was reclaimed, discarding result",
				slog.String("job_id", claimedJob.ID.String()),
				slog.String("lease_id", leaseID.String()),
			)
			_ = d.Ack(false)
			return
		}

		// Database error before success is persisted: DO NOT ACK message
		w.metrics.IncJobsFailed()
		w.logger.Error("failed to persist job completion to database, requeuing message",
			slog.String("job_id", claimedJob.ID.String()),
			slog.String("error", compErr.Error()),
		)
		_ = d.Nack(false, true)
		return
	}

	// 6. Acknowledge message only after durable database commit
	w.metrics.IncJobsSucceeded()
	if w.promMetrics != nil {
		w.promMetrics.ObserveJobExecution(claimedJob.Type, "completed", duration)
	}
	w.logger.Info("job execution completed and persisted successfully",
		slog.String("job_id", claimedJob.ID.String()),
		slog.Duration("duration", duration),
	)
	_ = d.Ack(false)
}
