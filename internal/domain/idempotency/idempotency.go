package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Standard domain errors
var (
	ErrNotFound        = errors.New("idempotency record not found")
	ErrPayloadMismatch = errors.New("idempotency key reused with different payload")
	ErrConflict        = errors.New("concurrent request in flight with same idempotency key")
)

// Record stores cached API responses for deduplication.
type Record struct {
	Key          string          `json:"key"`
	RequestHash  string          `json:"request_hash"`
	JobID        uuid.UUID       `json:"job_id"`
	ResponseCode int             `json:"response_code"`
	ResponseBody json.RawMessage `json:"response_body"`
	CreatedAt    time.Time       `json:"created_at"`
	ExpiresAt    time.Time       `json:"expires_at"`
}

// Repository defines data access abstractions for Idempotency records.
type Repository interface {
	Get(ctx context.Context, key string) (*Record, error)
	Store(ctx context.Context, record *Record) error
	LockKey(ctx context.Context, key string) error
}
