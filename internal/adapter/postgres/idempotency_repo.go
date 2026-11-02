package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YKalisjati/forge/internal/domain/idempotency"
)

// IdempotencyRepository implements idempotency.Repository backed by PostgreSQL.
type IdempotencyRepository struct {
	pool *pgxpool.Pool
}

// NewIdempotencyRepository initializes a new IdempotencyRepository.
func NewIdempotencyRepository(pool *pgxpool.Pool) *IdempotencyRepository {
	return &IdempotencyRepository{pool: pool}
}

// LockKey acquires a PostgreSQL transaction-level advisory lock on the key.
// Ensures that concurrent submissions with the same key serialize at the database level
// without relying on an in-memory mutex.
func (r *IdempotencyRepository) LockKey(ctx context.Context, key string) error {
	db := extractDB(ctx, r.pool)

	h := fnv.New64a()
	_, _ = h.Write([]byte("idempotency:" + key))
	lockID := int64(h.Sum64())

	query := `SELECT pg_advisory_xact_lock($1);`
	_, err := db.Exec(ctx, query, lockID)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory xact lock for key: %w", err)
	}

	return nil
}

// Get retrieves a non-expired cached idempotency record by key.
func (r *IdempotencyRepository) Get(ctx context.Context, key string) (*idempotency.Record, error) {
	db := extractDB(ctx, r.pool)

	query := `
		SELECT key, request_hash, job_id, response_code, response_body, created_at, expires_at
		FROM idempotency_records
		WHERE key = $1 AND expires_at > NOW();
	`

	var rec idempotency.Record
	err := db.QueryRow(ctx, query, key).Scan(
		&rec.Key,
		&rec.RequestHash,
		&rec.JobID,
		&rec.ResponseCode,
		&rec.ResponseBody,
		&rec.CreatedAt,
		&rec.ExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, idempotency.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get idempotency record: %w", err)
	}

	return &rec, nil
}

// Store inserts a new idempotency cache record into PostgreSQL.
// If the key already exists, returns idempotency.ErrConflict.
func (r *IdempotencyRepository) Store(ctx context.Context, record *idempotency.Record) error {
	db := extractDB(ctx, r.pool)

	query := `
		INSERT INTO idempotency_records (
			key, request_hash, job_id, response_code, response_body, created_at, expires_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7
		);
	`

	_, err := db.Exec(ctx, query,
		record.Key,
		record.RequestHash,
		record.JobID,
		record.ResponseCode,
		record.ResponseBody,
		record.CreatedAt,
		record.ExpiresAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return idempotency.ErrConflict
		}
		return fmt.Errorf("failed to store idempotency record: %w", err)
	}

	return nil
}
