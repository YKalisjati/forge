package job

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// State represents the lifecycle state of a background job.
type State string

const (
	StatePending    State = "pending"
	StateRunning    State = "running"
	StateCompleted  State = "completed"
	StateFailed     State = "failed"
	StateDeadLetter State = "dead_letter"
)

// Standard domain errors
var (
	ErrJobNotFound             = errors.New("job not found")
	ErrJobAlreadyCompleted     = errors.New("job is already completed")
	ErrJobAlreadyClaimed       = errors.New("job is currently locked by another active worker")
	ErrJobDeadLettered         = errors.New("job is in dead letter state")
	ErrJobClaimFailed          = errors.New("job could not be claimed")
	ErrFencingViolation        = errors.New("fencing violation: execution lease is no longer valid")
	ErrInvalidJobType          = errors.New("unsupported or invalid job type")
	ErrInvalidStateTransition  = errors.New("invalid job state transition")
	ErrTerminalStateTransition = errors.New("cannot transition a job from terminal state")
	ErrInvalidPayload          = errors.New("payload must be a valid non-empty JSON object")
	ErrPayloadTooLarge         = errors.New("payload exceeds maximum allowed size")
	ErrInvalidPriority         = errors.New("priority must be between 0 and 100")
	ErrInvalidRetries          = errors.New("max_retries must be between 0 and 20")
	ErrInvalidDelay            = errors.New("delay_seconds cannot be negative")
	ErrPermanent               = errors.New("permanent failure: non-retryable error")
	ErrPoisonMessage           = errors.New("poison message: rejected to dead letter queue")
)

// PermanentError marks an error as permanent / non-retryable.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

// MarkPermanent marks an error as permanent so workers will not retry it.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent returns true if the error represents a non-retryable permanent failure.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) || errors.Is(err, ErrPoisonMessage) {
		return true
	}
	var pErr *PermanentError
	return errors.As(err, &pErr)
}

// IsPoison returns true if the error indicates a poison message that should be routed to DLQ.
func IsPoison(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrPoisonMessage)
}

// Allowed job types whitelist
var AllowedJobTypes = map[string]struct{}{
	"email.transactional": {},
	"webhook.delivery":    {},
	"report.generate":     {},
	"data.cleanup":        {},
	"test.job":            {},
}

var jobTypeRegex = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)

// IsAllowedJobType checks if a job type is recognized and permitted.
func IsAllowedJobType(t string) bool {
	if !jobTypeRegex.MatchString(t) {
		return false
	}
	_, ok := AllowedJobTypes[t]
	return ok
}

// Job represents a background job unit of work.
type Job struct {
	ID               uuid.UUID       `json:"id"`
	IdempotencyKey   *string         `json:"idempotency_key,omitempty"`
	Type             string          `json:"type"`
	Payload          json.RawMessage `json:"payload"`
	State            State           `json:"state"`
	Priority         int             `json:"priority"`
	MaxRetries       int             `json:"max_retries"`
	RetriesAttempted int             `json:"retries_attempted"`
	LastError        *string         `json:"last_error,omitempty"`
	NextAttemptAt    time.Time       `json:"next_attempt_at"`
	ExecutionLeaseID *uuid.UUID      `json:"execution_lease_id,omitempty"`
	LeaseExpiresAt   *time.Time      `json:"lease_expires_at,omitempty"`
	LockedBy         *string         `json:"locked_by,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	StartedAt        *time.Time      `json:"started_at,omitempty"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
}

// Execution represents an audit log entry for a job execution attempt.
type Execution struct {
	ID               int64      `json:"id,omitempty"`
	JobID            uuid.UUID  `json:"job_id"`
	Attempt          int        `json:"attempt"`
	ExecutionLeaseID uuid.UUID  `json:"execution_lease_id"`
	WorkerID         string     `json:"worker_id"`
	Status           string     `json:"status"`
	Error            *string    `json:"error"`
	DurationMS       *int       `json:"duration_ms"`
	StartedAt        time.Time  `json:"started_at"`
	FinishedAt       *time.Time `json:"finished_at"`
}

// WithExecutions combines a Job with its execution history.
type WithExecutions struct {
	Job
	Executions []Execution `json:"executions"`
}

// IsTerminal returns true if the job is in a final state.
func (j *Job) IsTerminal() bool {
	return j.State == StateCompleted || j.State == StateDeadLetter
}

// CanTransitionTo determines if transitioning to target state is legally allowed.
func (j *Job) CanTransitionTo(target State) bool {
	if j.IsTerminal() {
		return false
	}
	switch j.State {
	case StatePending:
		return target == StateRunning
	case StateRunning:
		return target == StateCompleted || target == StatePending || target == StateFailed || target == StateDeadLetter
	case StateFailed:
		return target == StatePending || target == StateRunning || target == StateDeadLetter
	default:
		return false
	}
}

// TransitionTo updates the job state if the transition is valid.
func (j *Job) TransitionTo(target State) error {
	if j.IsTerminal() {
		return fmt.Errorf("%w: current state is %s", ErrTerminalStateTransition, j.State)
	}
	if !j.CanTransitionTo(target) {
		return fmt.Errorf("%w: from %s to %s", ErrInvalidStateTransition, j.State, target)
	}
	j.State = target
	j.UpdatedAt = time.Now().UTC()
	return nil
}

// Validate validates job entity invariants before persistence.
func (j *Job) Validate(maxPayloadBytes int64) error {
	if !IsAllowedJobType(j.Type) {
		return fmt.Errorf("%w: %q", ErrInvalidJobType, j.Type)
	}
	if len(j.Payload) == 0 {
		return ErrInvalidPayload
	}
	if int64(len(j.Payload)) > maxPayloadBytes {
		return fmt.Errorf("%w: size %d exceeds %d", ErrPayloadTooLarge, len(j.Payload), maxPayloadBytes)
	}
	trimmed := bytes.TrimSpace(j.Payload)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return ErrInvalidPayload
	}
	var js map[string]interface{}
	if err := json.Unmarshal(trimmed, &js); err != nil {
		return ErrInvalidPayload
	}
	if j.Priority < 0 || j.Priority > 100 {
		return ErrInvalidPriority
	}
	if j.MaxRetries < 0 || j.MaxRetries > 20 {
		return ErrInvalidRetries
	}
	return nil
}

// Filter defines criteria for querying and paginating jobs.
type Filter struct {
	State  *State
	Type   *string
	Limit  int
	Cursor *Cursor
}

// Cursor stores Keyset pagination parameters.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// Repository defines data access abstractions for Job entities.
type Repository interface {
	Create(ctx context.Context, job *Job) error
	GetByID(ctx context.Context, id uuid.UUID) (*WithExecutions, error)
	List(ctx context.Context, filter Filter) ([]*Job, *Cursor, bool, error)
	ClaimJob(ctx context.Context, id uuid.UUID, leaseDuration time.Duration, workerID string) (*Job, uuid.UUID, int, error)
	ExtendLease(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, leaseDuration time.Duration) error
	CompleteJob(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, duration time.Duration) error
	FailJob(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, errMsg string, duration time.Duration) error
	ScheduleRetry(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, errMsg string, nextAttemptAt time.Time, duration time.Duration) error
	DeadLetterJob(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, errMsg string, duration time.Duration) error
	RecoverStaleJobs(ctx context.Context, batchSize int) (recovered int, deadLettered int, err error)
	SchedulePendingRetries(ctx context.Context, batchSize int) (scheduled int, err error)
}
