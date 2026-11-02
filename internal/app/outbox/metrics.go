package outbox

import (
	"math"
	"sync/atomic"
)

// Metrics records operational counters and gauges for the outbox relay.
type Metrics struct {
	publishAttempts atomic.Int64
	confirmFailures atomic.Int64
	publishedTotal  atomic.Int64
	backlogTotal    atomic.Int64
	oldestAgeBits   atomic.Uint64 // float64 stored via math.Float64bits
}

// NewMetrics creates an initialized Metrics tracker.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// IncPublishAttempts increments the total number of publish attempts.
func (m *Metrics) IncPublishAttempts() {
	m.publishAttempts.Add(1)
}

// IncConfirmFailures increments the total number of confirmation failures.
func (m *Metrics) IncConfirmFailures() {
	m.confirmFailures.Add(1)
}

// IncPublishedTotal increments the total number of successfully confirmed publishes.
func (m *Metrics) IncPublishedTotal() {
	m.publishedTotal.Add(1)
}

// SetBacklog sets the current count of pending outbox messages.
func (m *Metrics) SetBacklog(count int64) {
	m.backlogTotal.Store(count)
}

// SetOldestEventAge sets the age in seconds of the oldest pending outbox message.
func (m *Metrics) SetOldestEventAge(seconds float64) {
	m.oldestAgeBits.Store(math.Float64bits(seconds))
}

// Snapshot captures a point-in-time copy of all metrics values.
type Snapshot struct {
	PublishAttempts       int64   `json:"publish_attempts_total"`
	ConfirmFailures       int64   `json:"confirm_failures_total"`
	PublishedTotal        int64   `json:"published_total"`
	BacklogTotal          int64   `json:"outbox_backlog_total"`
	OldestEventAgeSeconds float64 `json:"oldest_event_age_seconds"`
}

// Snapshot returns a copy of current metrics values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		PublishAttempts:       m.publishAttempts.Load(),
		ConfirmFailures:       m.confirmFailures.Load(),
		PublishedTotal:        m.publishedTotal.Load(),
		BacklogTotal:          m.backlogTotal.Load(),
		OldestEventAgeSeconds: math.Float64frombits(m.oldestAgeBits.Load()),
	}
}
