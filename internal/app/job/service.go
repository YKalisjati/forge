package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	domainIdempotency "github.com/YKalisjati/forge/internal/domain/idempotency"
	domainJob "github.com/YKalisjati/forge/internal/domain/job"
	domainOutbox "github.com/YKalisjati/forge/internal/domain/outbox"
)

// Transactor defines transaction execution boundaries.
type Transactor interface {
	WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// Service coordinates job ingestion, retrieval, and idempotency guarantees.
type Service struct {
	transactor      Transactor
	jobRepo         domainJob.Repository
	outboxRepo      domainOutbox.Repository
	idempotencyRepo domainIdempotency.Repository
	maxPayloadBytes int64
}

// NewService constructs an application job service.
func NewService(
	tx Transactor,
	jobRepo domainJob.Repository,
	outboxRepo domainOutbox.Repository,
	idempotencyRepo domainIdempotency.Repository,
	maxPayloadBytes int64,
) *Service {
	return &Service{
		transactor:      tx,
		jobRepo:         jobRepo,
		outboxRepo:      outboxRepo,
		idempotencyRepo: idempotencyRepo,
		maxPayloadBytes: maxPayloadBytes,
	}
}

// SubmitResult contains the resulting Job and whether it was fulfilled from an existing idempotent record.
type SubmitResult struct {
	Job      *domainJob.Job
	Replayed bool
}

// SubmitJob validates the request and atomically creates the job and outbox event.
// Enforces client idempotency when an Idempotency-Key is provided.
func (s *Service) SubmitJob(ctx context.Context, idempotencyKey *string, input SubmitJobInput) (*SubmitResult, error) {
	// 1. Entity constraint validation
	if input.Priority < 0 || input.Priority > 100 {
		return nil, domainJob.ErrInvalidPriority
	}
	if input.MaxRetries < 0 || input.MaxRetries > 20 {
		return nil, domainJob.ErrInvalidRetries
	}
	if input.DelaySeconds < 0 {
		return nil, domainJob.ErrInvalidDelay
	}

	tempJob := &domainJob.Job{
		Type:       input.Type,
		Payload:    input.Payload,
		Priority:   input.Priority,
		MaxRetries: input.MaxRetries,
	}
	if err := tempJob.Validate(s.maxPayloadBytes); err != nil {
		return nil, err
	}

	// 2. Submission with Idempotency Key
	if idempotencyKey != nil && *idempotencyKey != "" {
		requestHash, err := CalculateStableRequestHash(input)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate request hash: %w", err)
		}

		var result *SubmitResult

		err = s.transactor.WithinTransaction(ctx, func(txCtx context.Context) error {
			// Acquire database-level transaction lock on key to serialize concurrent submissions
			if err := s.idempotencyRepo.LockKey(txCtx, *idempotencyKey); err != nil {
				return err
			}

			// Check for an existing idempotency record
			existing, err := s.idempotencyRepo.Get(txCtx, *idempotencyKey)
			if err == nil {
				// Record exists: check payload fingerprint
				if existing.RequestHash != requestHash {
					return domainIdempotency.ErrPayloadMismatch
				}

				// Key matches same payload: replay existing job
				var cachedJob domainJob.Job
				if err := json.Unmarshal(existing.ResponseBody, &cachedJob); err != nil {
					// Fallback to querying job directly if deserialization fails
					jobWithExec, err := s.jobRepo.GetByID(txCtx, existing.JobID)
					if err != nil {
						return err
					}
					cachedJob = jobWithExec.Job
				}

				result = &SubmitResult{
					Job:      &cachedJob,
					Replayed: true,
				}
				return nil
			} else if !errors.Is(err, domainIdempotency.ErrNotFound) {
				return err
			}

			// New request with key: create job, outbox, and idempotency records atomically
			now := time.Now().UTC()
			nextAttempt := now
			if input.DelaySeconds > 0 {
				nextAttempt = now.Add(time.Duration(input.DelaySeconds) * time.Second)
			}

			newJob := &domainJob.Job{
				ID:               uuid.New(),
				IdempotencyKey:   idempotencyKey,
				Type:             input.Type,
				Payload:          input.Payload,
				State:            domainJob.StatePending,
				Priority:         input.Priority,
				MaxRetries:       input.MaxRetries,
				RetriesAttempted: 0,
				NextAttemptAt:    nextAttempt,
				CreatedAt:        now,
				UpdatedAt:        now,
			}

			if err := s.jobRepo.Create(txCtx, newJob); err != nil {
				return fmt.Errorf("failed to create job: %w", err)
			}

			outboxMsg := &domainOutbox.Message{
				JobID:       newJob.ID,
				EventType:   "job.created",
				Payload:     input.Payload,
				Status:      domainOutbox.StatusPending,
				ScheduledAt: nextAttempt,
				RetryCount:  0,
				CreatedAt:   now,
			}

			if err := s.outboxRepo.Create(txCtx, outboxMsg); err != nil {
				return fmt.Errorf("failed to create outbox event: %w", err)
			}

			responseBody, err := json.Marshal(newJob)
			if err != nil {
				return fmt.Errorf("failed to serialize job for idempotency cache: %w", err)
			}

			rec := &domainIdempotency.Record{
				Key:          *idempotencyKey,
				RequestHash:  requestHash,
				JobID:        newJob.ID,
				ResponseCode: 202,
				ResponseBody: responseBody,
				CreatedAt:    now,
				ExpiresAt:    now.Add(24 * time.Hour),
			}

			if err := s.idempotencyRepo.Store(txCtx, rec); err != nil {
				return err
			}

			result = &SubmitResult{
				Job:      newJob,
				Replayed: false,
			}
			return nil
		})

		if err != nil {
			return nil, err
		}

		return result, nil
	}

	// 3. Submission without Idempotency Key
	var newJob *domainJob.Job

	err := s.transactor.WithinTransaction(ctx, func(txCtx context.Context) error {
		now := time.Now().UTC()
		nextAttempt := now
		if input.DelaySeconds > 0 {
			nextAttempt = now.Add(time.Duration(input.DelaySeconds) * time.Second)
		}

		newJob = &domainJob.Job{
			ID:               uuid.New(),
			IdempotencyKey:   nil,
			Type:             input.Type,
			Payload:          input.Payload,
			State:            domainJob.StatePending,
			Priority:         input.Priority,
			MaxRetries:       input.MaxRetries,
			RetriesAttempted: 0,
			NextAttemptAt:    nextAttempt,
			CreatedAt:        now,
			UpdatedAt:        now,
		}

		if err := s.jobRepo.Create(txCtx, newJob); err != nil {
			return fmt.Errorf("failed to create job: %w", err)
		}

		outboxMsg := &domainOutbox.Message{
			JobID:       newJob.ID,
			EventType:   "job.created",
			Payload:     input.Payload,
			Status:      domainOutbox.StatusPending,
			ScheduledAt: nextAttempt,
			RetryCount:  0,
			CreatedAt:   now,
		}

		if err := s.outboxRepo.Create(txCtx, outboxMsg); err != nil {
			return fmt.Errorf("failed to create outbox event: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &SubmitResult{
		Job:      newJob,
		Replayed: false,
	}, nil
}

// GetJob retrieves a job and its execution history by UUID.
func (s *Service) GetJob(ctx context.Context, id uuid.UUID) (*domainJob.WithExecutions, error) {
	return s.jobRepo.GetByID(ctx, id)
}

// ListJobs returns a paginated list of jobs matching the provided filter.
func (s *Service) ListJobs(ctx context.Context, filter domainJob.Filter) ([]*domainJob.Job, *domainJob.Cursor, bool, error) {
	return s.jobRepo.List(ctx, filter)
}
