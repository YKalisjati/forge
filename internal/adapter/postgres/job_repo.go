package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YKalisjati/forge/internal/domain/job"
)

// JobRepository implements job.Repository backed by PostgreSQL.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository initializes a new JobRepository.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// Create persists a new Job into the database using parameterized SQL.
// Explicit column lists are used (no SELECT *).
func (r *JobRepository) Create(ctx context.Context, j *job.Job) error {
	db := extractDB(ctx, r.pool)

	query := `
		INSERT INTO jobs (
			id, idempotency_key, type, payload, state, priority,
			max_retries, retries_attempted, last_error, next_attempt_at,
			execution_lease_id, lease_expires_at, locked_by, created_at,
			updated_at, started_at, completed_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12, $13, $14,
			$15, $16, $17
		);
	`

	_, err := db.Exec(ctx, query,
		j.ID,
		j.IdempotencyKey,
		j.Type,
		j.Payload,
		string(j.State),
		j.Priority,
		j.MaxRetries,
		j.RetriesAttempted,
		j.LastError,
		j.NextAttemptAt,
		j.ExecutionLeaseID,
		j.LeaseExpiresAt,
		j.LockedBy,
		j.CreatedAt,
		j.UpdatedAt,
		j.StartedAt,
		j.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	return nil
}

// GetByID retrieves a single job and its execution audit trail by UUID.
// Uses explicit columns and parameterized query.
func (r *JobRepository) GetByID(ctx context.Context, id uuid.UUID) (*job.WithExecutions, error) {
	db := extractDB(ctx, r.pool)

	jobQuery := `
		SELECT 
			id, idempotency_key, type, payload, state, priority,
			max_retries, retries_attempted, last_error, next_attempt_at,
			execution_lease_id, lease_expires_at, locked_by, created_at,
			updated_at, started_at, completed_at
		FROM jobs
		WHERE id = $1;
	`

	var j job.Job
	var stateStr string

	err := db.QueryRow(ctx, jobQuery, id).Scan(
		&j.ID,
		&j.IdempotencyKey,
		&j.Type,
		&j.Payload,
		&stateStr,
		&j.Priority,
		&j.MaxRetries,
		&j.RetriesAttempted,
		&j.LastError,
		&j.NextAttemptAt,
		&j.ExecutionLeaseID,
		&j.LeaseExpiresAt,
		&j.LockedBy,
		&j.CreatedAt,
		&j.UpdatedAt,
		&j.StartedAt,
		&j.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, job.ErrJobNotFound
		}
		return nil, fmt.Errorf("failed to query job: %w", err)
	}
	j.State = job.State(stateStr)

	// Query execution audit logs
	execQuery := `
		SELECT 
			id, job_id, attempt, execution_lease_id, worker_id, status,
			error, duration_ms, started_at, finished_at
		FROM job_executions
		WHERE job_id = $1
		ORDER BY attempt ASC;
	`

	rows, err := db.Query(ctx, execQuery, id)
	if err != nil {
		return nil, fmt.Errorf("failed to query job executions: %w", err)
	}
	defer rows.Close()

	executions := make([]job.Execution, 0)
	for rows.Next() {
		var ex job.Execution
		if err := rows.Scan(
			&ex.ID,
			&ex.JobID,
			&ex.Attempt,
			&ex.ExecutionLeaseID,
			&ex.WorkerID,
			&ex.Status,
			&ex.Error,
			&ex.DurationMS,
			&ex.StartedAt,
			&ex.FinishedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan job execution row: %w", err)
		}
		executions = append(executions, ex)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("job execution rows iteration error: %w", err)
	}

	return &job.WithExecutions{
		Job:        j,
		Executions: executions,
	}, nil
}

// List queries jobs with bounded pagination and optional filtering by state and type.
// Index Justification:
// Query matches composite index `idx_jobs_type_state_created (type, state, created_at DESC)`.
// By ordering by (created_at DESC, id DESC), keyset pagination avoids slow OFFSET scans
// and performs efficient B-Tree index range scans.
func (r *JobRepository) List(ctx context.Context, filter job.Filter) ([]*job.Job, *job.Cursor, bool, error) {
	db := extractDB(ctx, r.pool)

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	var stateParam *string
	if filter.State != nil {
		s := string(*filter.State)
		stateParam = &s
	}

	// Fetch limit + 1 to check for has_more
	fetchLimit := limit + 1

	query := `
		SELECT 
			id, idempotency_key, type, payload, state, priority,
			max_retries, retries_attempted, last_error, next_attempt_at,
			execution_lease_id, lease_expires_at, locked_by, created_at,
			updated_at, started_at, completed_at
		FROM jobs
		WHERE ($1::text IS NULL OR state = $1)
		  AND ($2::text IS NULL OR type = $2)
		  AND (
			$3::timestamptz IS NULL 
			OR (created_at < $3 OR (created_at = $3 AND id < $4))
		  )
		ORDER BY created_at DESC, id DESC
		LIMIT $5;
	`

	var cursorCreatedAt any
	var cursorID any
	if filter.Cursor != nil {
		cursorCreatedAt = filter.Cursor.CreatedAt
		cursorID = filter.Cursor.ID
	}

	rows, err := db.Query(ctx, query, stateParam, filter.Type, cursorCreatedAt, cursorID, fetchLimit)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	items := make([]*job.Job, 0, limit)
	for rows.Next() {
		var j job.Job
		var stateStr string

		if err := rows.Scan(
			&j.ID,
			&j.IdempotencyKey,
			&j.Type,
			&j.Payload,
			&stateStr,
			&j.Priority,
			&j.MaxRetries,
			&j.RetriesAttempted,
			&j.LastError,
			&j.NextAttemptAt,
			&j.ExecutionLeaseID,
			&j.LeaseExpiresAt,
			&j.LockedBy,
			&j.CreatedAt,
			&j.UpdatedAt,
			&j.StartedAt,
			&j.CompletedAt,
		); err != nil {
			return nil, nil, false, fmt.Errorf("failed to scan job row: %w", err)
		}
		j.State = job.State(stateStr)
		items = append(items, &j)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, fmt.Errorf("jobs list iteration error: %w", err)
	}

	hasMore := false
	var nextCursor *job.Cursor

	if len(items) > limit {
		hasMore = true
		items = items[:limit]
		lastItem := items[limit-1]
		nextCursor = &job.Cursor{
			CreatedAt: lastItem.CreatedAt,
			ID:        lastItem.ID,
		}
	}

	return items, nextCursor, hasMore, nil
}

// ClaimJob attempts an atomic lease claim in PostgreSQL for the specified job.
// It sets state='running', assigns execution_lease_id and lease_expires_at, and records a running execution log.
func (r *JobRepository) ClaimJob(ctx context.Context, id uuid.UUID, leaseDuration time.Duration, workerID string) (*job.Job, uuid.UUID, int, error) {
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	leaseID := uuid.New()

	db := extractDB(ctx, r.pool)

	query := `
		UPDATE jobs
		SET state = 'running',
		    execution_lease_id = $1,
		    lease_expires_at = NOW() + ($2 * INTERVAL '1 millisecond'),
		    locked_by = $3,
		    started_at = COALESCE(started_at, NOW()),
		    retries_attempted = retries_attempted + 1,
		    updated_at = NOW()
		WHERE id = $4
		  AND (
		    state IN ('pending', 'retrying')
		    OR (state = 'running' AND lease_expires_at < NOW())
		  )
		RETURNING 
		    id, idempotency_key, type, payload, state, priority,
		    max_retries, retries_attempted, last_error, next_attempt_at,
		    execution_lease_id, lease_expires_at, locked_by, created_at,
		    updated_at, started_at, completed_at;
	`

	var j job.Job
	var stateStr string

	err := db.QueryRow(ctx, query,
		leaseID,
		leaseDuration.Milliseconds(),
		workerID,
		id,
	).Scan(
		&j.ID,
		&j.IdempotencyKey,
		&j.Type,
		&j.Payload,
		&stateStr,
		&j.Priority,
		&j.MaxRetries,
		&j.RetriesAttempted,
		&j.LastError,
		&j.NextAttemptAt,
		&j.ExecutionLeaseID,
		&j.LeaseExpiresAt,
		&j.LockedBy,
		&j.CreatedAt,
		&j.UpdatedAt,
		&j.StartedAt,
		&j.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Inspect why the job was not claimed
			inspectQuery := `SELECT state, execution_lease_id, lease_expires_at FROM jobs WHERE id = $1;`
			var currentState string
			var currentLease *uuid.UUID
			var currentExpires *time.Time

			inspectErr := db.QueryRow(ctx, inspectQuery, id).Scan(&currentState, &currentLease, &currentExpires)
			if inspectErr != nil {
				if errors.Is(inspectErr, pgx.ErrNoRows) {
					return nil, uuid.Nil, 0, job.ErrJobNotFound
				}
				return nil, uuid.Nil, 0, fmt.Errorf("failed to inspect unclaimed job: %w", inspectErr)
			}

			switch job.State(currentState) {
			case job.StateCompleted:
				return nil, uuid.Nil, 0, job.ErrJobAlreadyCompleted
			case job.StateRunning:
				if currentExpires != nil && currentExpires.After(time.Now()) {
					return nil, uuid.Nil, 0, job.ErrJobAlreadyClaimed
				}
				return nil, uuid.Nil, 0, job.ErrJobClaimFailed
			case job.StateDeadLetter:
				return nil, uuid.Nil, 0, job.ErrJobDeadLettered
			default:
				return nil, uuid.Nil, 0, job.ErrJobClaimFailed
			}
		}
		return nil, uuid.Nil, 0, fmt.Errorf("failed to claim job: %w", err)
	}

	j.State = job.State(stateStr)

	// Insert audit record in job_executions
	execQuery := `
		INSERT INTO job_executions (
			job_id, attempt, execution_lease_id, worker_id, status, started_at
		) VALUES (
			$1, $2, $3, $4, 'running', NOW()
		);
	`
	_, err = db.Exec(ctx, execQuery, j.ID, j.RetriesAttempted, leaseID, workerID)
	if err != nil {
		return nil, uuid.Nil, 0, fmt.Errorf("failed to insert job execution audit log: %w", err)
	}

	return &j, leaseID, j.RetriesAttempted, nil
}

// ExtendLease extends lease expiration for a running job if the worker holds the active fencing token.
func (r *JobRepository) ExtendLease(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	db := extractDB(ctx, r.pool)

	query := `
		UPDATE jobs
		SET lease_expires_at = NOW() + ($1 * INTERVAL '1 millisecond'),
		    updated_at = NOW()
		WHERE id = $2
		  AND execution_lease_id = $3;
	`

	tag, err := db.Exec(ctx, query, leaseDuration.Milliseconds(), id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to extend job lease: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return job.ErrFencingViolation
	}

	return nil
}

// CompleteJob transitions a job to completed and finalizes the execution log.
// It requires execution_lease_id to match the active fencing token.
func (r *JobRepository) CompleteJob(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, duration time.Duration) error {
	db := extractDB(ctx, r.pool)

	query := `
		UPDATE jobs
		SET state = 'completed',
		    completed_at = NOW(),
		    execution_lease_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
		  AND execution_lease_id = $2;
	`

	tag, err := db.Exec(ctx, query, id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to mark job completed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return job.ErrFencingViolation
	}

	// Update job execution audit log
	execQuery := `
		UPDATE job_executions
		SET status = 'completed',
		    finished_at = NOW(),
		    duration_ms = $1
		WHERE job_id = $2
		  AND execution_lease_id = $3;
	`
	durationMS := int(duration.Milliseconds())
	_, _ = db.Exec(ctx, execQuery, durationMS, id, leaseID)

	return nil
}

// FailJob transitions a job to failed and records error details in the execution log.
func (r *JobRepository) FailJob(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, errMsg string, duration time.Duration) error {
	db := extractDB(ctx, r.pool)

	query := `
		UPDATE jobs
		SET state = 'failed',
		    last_error = $1,
		    execution_lease_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = NOW()
		WHERE id = $2
		  AND execution_lease_id = $3;
	`

	tag, err := db.Exec(ctx, query, errMsg, id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to mark job failed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return job.ErrFencingViolation
	}

	execQuery := `
		UPDATE job_executions
		SET status = 'failed',
		    error = $1,
		    finished_at = NOW(),
		    duration_ms = $2
		WHERE job_id = $3
		  AND execution_lease_id = $4;
	`
	durationMS := int(duration.Milliseconds())
	_, _ = db.Exec(ctx, execQuery, errMsg, durationMS, id, leaseID)

	return nil
}

// ScheduleRetry resets job to pending with next_attempt_at, updates audit log, and creates outbox event atomically.
func (r *JobRepository) ScheduleRetry(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, errMsg string, nextAttemptAt time.Time, duration time.Duration) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for retry schedule: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `
		UPDATE jobs
		SET state = 'pending',
		    next_attempt_at = $1,
		    last_error = $2,
		    execution_lease_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = NOW()
		WHERE id = $3
		  AND execution_lease_id = $4;
	`
	tag, err := tx.Exec(ctx, query, nextAttemptAt, errMsg, id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to update job for retry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrFencingViolation
	}

	execQuery := `
		UPDATE job_executions
		SET status = 'failed',
		    error = $1,
		    duration_ms = $2,
		    finished_at = NOW()
		WHERE job_id = $3
		  AND execution_lease_id = $4;
	`
	durationMS := int(duration.Milliseconds())
	_, err = tx.Exec(ctx, execQuery, errMsg, durationMS, id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to update job execution for retry: %w", err)
	}

	outboxQuery := `
		INSERT INTO outbox_messages (
			job_id, event_type, payload, status, scheduled_at, created_at
		)
		SELECT id, 'job.scheduled', payload, 'pending', $1, NOW()
		FROM jobs
		WHERE id = $2;
	`
	_, err = tx.Exec(ctx, outboxQuery, nextAttemptAt, id)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event for retry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit retry transaction: %w", err)
	}
	return nil
}

// DeadLetterJob transitions a job to dead_letter state and finalizes the execution log.
func (r *JobRepository) DeadLetterJob(ctx context.Context, id uuid.UUID, leaseID uuid.UUID, errMsg string, duration time.Duration) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for dead letter: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `
		UPDATE jobs
		SET state = 'dead_letter',
		    last_error = $1,
		    execution_lease_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = NOW()
		WHERE id = $2
		  AND execution_lease_id = $3;
	`
	tag, err := tx.Exec(ctx, query, errMsg, id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to update job to dead_letter: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrFencingViolation
	}

	execQuery := `
		UPDATE job_executions
		SET status = 'failed',
		    error = $1,
		    duration_ms = $2,
		    finished_at = NOW()
		WHERE job_id = $3
		  AND execution_lease_id = $4;
	`
	durationMS := int(duration.Milliseconds())
	_, err = tx.Exec(ctx, execQuery, errMsg, durationMS, id, leaseID)
	if err != nil {
		return fmt.Errorf("failed to update job execution for dead letter: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit dead letter transaction: %w", err)
	}
	return nil
}

// RecoverStaleJobs finds running jobs whose leases expired and recovers or dead-letters them.
func (r *JobRepository) RecoverStaleJobs(ctx context.Context, batchSize int) (int, int, error) {
	if batchSize <= 0 {
		batchSize = 50
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to begin tx for stale job recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `
		SELECT id, retries_attempted, max_retries, execution_lease_id
		FROM jobs
		WHERE state = 'running' AND lease_expires_at < NOW()
		ORDER BY lease_expires_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query stale jobs: %w", err)
	}

	type staleJob struct {
		id               uuid.UUID
		retriesAttempted int
		maxRetries       int
		leaseID          *uuid.UUID
	}
	var staleJobs []staleJob
	for rows.Next() {
		var s staleJob
		if err := rows.Scan(&s.id, &s.retriesAttempted, &s.maxRetries, &s.leaseID); err != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("failed to scan stale job row: %w", err)
		}
		staleJobs = append(staleJobs, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("stale jobs rows iteration error: %w", err)
	}

	recovered := 0
	deadLettered := 0

	for _, s := range staleJobs {
		if s.retriesAttempted <= s.maxRetries {
			upQuery := `
				UPDATE jobs
				SET state = 'pending',
				    execution_lease_id = NULL,
				    lease_expires_at = NULL,
				    next_attempt_at = NOW(),
				    last_error = 'execution lease expired: worker heartbeat stopped',
				    updated_at = NOW()
				WHERE id = $1;
			`
			if _, err := tx.Exec(ctx, upQuery, s.id); err != nil {
				return 0, 0, fmt.Errorf("failed to reset stale job %s: %w", s.id, err)
			}

			if s.leaseID != nil {
				execQuery := `
					UPDATE job_executions
					SET status = 'timed_out',
					    error = 'execution lease expired',
					    finished_at = NOW()
					WHERE job_id = $1 AND execution_lease_id = $2;
				`
				_, _ = tx.Exec(ctx, execQuery, s.id, *s.leaseID)
			}

			var exists bool
			checkQuery := `SELECT EXISTS(SELECT 1 FROM outbox_messages WHERE job_id = $1 AND status = 'pending');`
			if err := tx.QueryRow(ctx, checkQuery, s.id).Scan(&exists); err == nil && !exists {
				outboxInsert := `
					INSERT INTO outbox_messages (
						job_id, event_type, payload, status, scheduled_at, created_at
					)
					SELECT id, 'job.scheduled', payload, 'pending', NOW(), NOW()
					FROM jobs
					WHERE id = $1;
				`
				_, _ = tx.Exec(ctx, outboxInsert, s.id)
			}
			recovered++
		} else {
			dlQuery := `
				UPDATE jobs
				SET state = 'dead_letter',
				    execution_lease_id = NULL,
				    lease_expires_at = NULL,
				    last_error = 'execution lease expired and max retries exhausted',
				    updated_at = NOW()
				WHERE id = $1;
			`
			if _, err := tx.Exec(ctx, dlQuery, s.id); err != nil {
				return 0, 0, fmt.Errorf("failed to dead letter stale job %s: %w", s.id, err)
			}

			if s.leaseID != nil {
				execQuery := `
					UPDATE job_executions
					SET status = 'timed_out',
					    error = 'execution lease expired and retries exhausted',
					    finished_at = NOW()
					WHERE job_id = $1 AND execution_lease_id = $2;
				`
				_, _ = tx.Exec(ctx, execQuery, s.id, *s.leaseID)
			}
			deadLettered++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("failed to commit stale job recovery: %w", err)
	}

	return recovered, deadLettered, nil
}

// SchedulePendingRetries finds pending jobs ready for execution that do not have an active pending outbox event.
func (r *JobRepository) SchedulePendingRetries(ctx context.Context, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 50
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx for pending retries scheduler: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `
		SELECT j.id
		FROM jobs j
		WHERE j.state = 'pending'
		  AND j.next_attempt_at <= NOW()
		  AND NOT EXISTS (
		    SELECT 1 FROM outbox_messages o
		    WHERE o.job_id = j.id AND o.status = 'pending'
		  )
		ORDER BY j.next_attempt_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to query pending retries: %w", err)
	}

	var jobIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("failed to scan pending job id: %w", err)
		}
		jobIDs = append(jobIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("pending jobs rows iteration error: %w", err)
	}

	scheduled := 0
	for _, id := range jobIDs {
		insertQuery := `
			INSERT INTO outbox_messages (
				job_id, event_type, payload, status, scheduled_at, created_at
			)
			SELECT id, 'job.scheduled', payload, 'pending', NOW(), NOW()
			FROM jobs
			WHERE id = $1;
		`
		_, err := tx.Exec(ctx, insertQuery, id)
		if err != nil {
			return 0, fmt.Errorf("failed to insert outbox message for job %s: %w", id, err)
		}
		scheduled++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit scheduled retries: %w", err)
	}

	return scheduled, nil
}
