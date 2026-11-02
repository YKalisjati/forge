package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status represents the state of an outbox message.
type Status string

const (
	StatusPending   Status = "pending"
	StatusPublished Status = "published"
	StatusFailed    Status = "failed"
)

// Message represents an event scheduled for broker dispatch.
type Message struct {
	ID          int64           `json:"id"`
	JobID       uuid.UUID       `json:"job_id"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	Status      Status          `json:"status"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	RetryCount  int             `json:"retry_count"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

// Envelope encapsulates an event published to the RabbitMQ broker.
type Envelope struct {
	EventID       int64           `json:"event_id"`
	JobID         uuid.UUID       `json:"job_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
	Timestamp     time.Time       `json:"timestamp"`
}

// Repository defines data access abstractions for Outbox messages.
type Repository interface {
	Create(ctx context.Context, msg *Message) error
	FetchPendingBatch(ctx context.Context, limit int) ([]*Message, error)
	ClaimBatch(ctx context.Context, limit int, leaseDuration time.Duration) ([]*Message, error)
	MarkPublished(ctx context.Context, ids []int64) error
	RecordFailure(ctx context.Context, id int64, errMessage string, nextAttempt time.Time, failedPermanently bool) error
	GetBacklogMetrics(ctx context.Context) (pendingCount int64, oldestAgeSeconds float64, err error)
	GetByID(ctx context.Context, id int64) (*Message, error)
}
