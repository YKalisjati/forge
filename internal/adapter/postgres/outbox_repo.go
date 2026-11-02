package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YKalisjati/forge/internal/domain/outbox"
)

// OutboxRepository implements outbox.Repository backed by PostgreSQL.
type OutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository initializes a new OutboxRepository.
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

// Create inserts a new outbox event record.
func (r *OutboxRepository) Create(ctx context.Context, msg *outbox.Message) error {
	db := extractDB(ctx, r.pool)

	query := `
		INSERT INTO outbox_messages (
			job_id, event_type, payload, status, scheduled_at,
			retry_count, last_error, created_at, published_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9
		) RETURNING id;
	`

	err := db.QueryRow(ctx, query,
		msg.JobID,
		msg.EventType,
		msg.Payload,
		string(msg.Status),
		msg.ScheduledAt,
		msg.RetryCount,
		msg.LastError,
		msg.CreatedAt,
		msg.PublishedAt,
	).Scan(&msg.ID)
	if err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}

	return nil
}

// FetchPendingBatch queries pending outbox messages ordered by scheduled_at using SKIP LOCKED.
func (r *OutboxRepository) FetchPendingBatch(ctx context.Context, limit int) ([]*outbox.Message, error) {
	db := extractDB(ctx, r.pool)

	query := `
		SELECT 
			id, job_id, event_type, payload, status, scheduled_at,
			retry_count, last_error, created_at, published_at
		FROM outbox_messages
		WHERE status = 'pending' AND scheduled_at <= NOW()
		ORDER BY scheduled_at ASC, id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1;
	`

	rows, err := db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending outbox batch: %w", err)
	}
	defer rows.Close()

	messages := make([]*outbox.Message, 0)
	for rows.Next() {
		var m outbox.Message
		var statusStr string

		if err := rows.Scan(
			&m.ID,
			&m.JobID,
			&m.EventType,
			&m.Payload,
			&statusStr,
			&m.ScheduledAt,
			&m.RetryCount,
			&m.LastError,
			&m.CreatedAt,
			&m.PublishedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbox row: %w", err)
		}
		m.Status = outbox.Status(statusStr)
		messages = append(messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox rows iteration error: %w", err)
	}

	return messages, nil
}

// MarkPublished updates status of outbox records to 'published'.
func (r *OutboxRepository) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	db := extractDB(ctx, r.pool)

	query := `
		UPDATE outbox_messages
		SET status = 'published', published_at = NOW()
		WHERE id = ANY($1);
	`

	_, err := db.Exec(ctx, query, ids)
	if err != nil {
		return fmt.Errorf("failed to mark outbox messages published: %w", err)
	}

	return nil
}

// ClaimBatch queries and atomically claims pending outbox messages in a single CTE
// using SELECT ... FOR UPDATE SKIP LOCKED and pushes scheduled_at into the future
// by leaseDuration. This prevents concurrent relay instances from claiming the same
// records and avoids holding a database transaction open while publishing to RabbitMQ.
func (r *OutboxRepository) ClaimBatch(ctx context.Context, limit int, leaseDuration time.Duration) ([]*outbox.Message, error) {
	if limit <= 0 {
		limit = 100
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	db := extractDB(ctx, r.pool)

	query := `
		WITH claimed AS (
			SELECT id
			FROM outbox_messages
			WHERE status = 'pending' AND scheduled_at <= NOW()
			ORDER BY scheduled_at ASC, id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_messages o
		SET scheduled_at = NOW() + ($2 * INTERVAL '1 millisecond')
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING 
			o.id, o.job_id, o.event_type, o.payload, o.status, 
			o.scheduled_at, o.retry_count, o.last_error, o.created_at, o.published_at;
	`

	rows, err := db.Query(ctx, query, limit, leaseDuration.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox batch: %w", err)
	}
	defer rows.Close()

	messages := make([]*outbox.Message, 0)
	for rows.Next() {
		var m outbox.Message
		var statusStr string

		if err := rows.Scan(
			&m.ID,
			&m.JobID,
			&m.EventType,
			&m.Payload,
			&statusStr,
			&m.ScheduledAt,
			&m.RetryCount,
			&m.LastError,
			&m.CreatedAt,
			&m.PublishedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan claimed outbox row: %w", err)
		}
		m.Status = outbox.Status(statusStr)
		messages = append(messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claimed outbox rows iteration error: %w", err)
	}

	return messages, nil
}

// RecordFailure updates retry metadata, sets next attempt timestamp, and optionally marks as permanently failed.
func (r *OutboxRepository) RecordFailure(ctx context.Context, id int64, errMessage string, nextAttempt time.Time, failedPermanently bool) error {
	db := extractDB(ctx, r.pool)

	query := `
		UPDATE outbox_messages
		SET retry_count = retry_count + 1,
		    last_error = $1,
		    scheduled_at = $2,
		    status = CASE WHEN $3::boolean THEN 'failed' ELSE status END
		WHERE id = $4;
	`

	_, err := db.Exec(ctx, query, errMessage, nextAttempt, failedPermanently, id)
	if err != nil {
		return fmt.Errorf("failed to record outbox failure: %w", err)
	}

	return nil
}

// GetBacklogMetrics returns the total pending outbox message count and the age in seconds of the oldest unpublished message.
func (r *OutboxRepository) GetBacklogMetrics(ctx context.Context) (int64, float64, error) {
	db := extractDB(ctx, r.pool)

	query := `
		SELECT 
			COUNT(*) AS pending_count,
			COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0) AS oldest_age_seconds
		FROM outbox_messages
		WHERE status = 'pending';
	`

	var pendingCount int64
	var oldestAgeSeconds float64

	if err := db.QueryRow(ctx, query).Scan(&pendingCount, &oldestAgeSeconds); err != nil {
		return 0, 0, fmt.Errorf("failed to query outbox backlog metrics: %w", err)
	}

	return pendingCount, oldestAgeSeconds, nil
}

// GetByID retrieves a single outbox message by its ID.
func (r *OutboxRepository) GetByID(ctx context.Context, id int64) (*outbox.Message, error) {
	db := extractDB(ctx, r.pool)

	query := `
		SELECT 
			id, job_id, event_type, payload, status, scheduled_at,
			retry_count, last_error, created_at, published_at
		FROM outbox_messages
		WHERE id = $1;
	`

	var m outbox.Message
	var statusStr string

	err := db.QueryRow(ctx, query, id).Scan(
		&m.ID,
		&m.JobID,
		&m.EventType,
		&m.Payload,
		&statusStr,
		&m.ScheduledAt,
		&m.RetryCount,
		&m.LastError,
		&m.CreatedAt,
		&m.PublishedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get outbox message by id: %w", err)
	}
	m.Status = outbox.Status(statusStr)

	return &m, nil
}
