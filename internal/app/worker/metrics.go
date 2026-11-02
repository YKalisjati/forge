package worker

import (
	"sync/atomic"
)

// Metrics records operational counters and gauges for the worker execution engine.
type Metrics struct {
	jobsProcessedTotal       atomic.Int64
	jobsSucceededTotal       atomic.Int64
	jobsFailedTotal          atomic.Int64
	duplicateDeliveriesTotal atomic.Int64
	fencingViolationsTotal   atomic.Int64
	malformedMessagesTotal   atomic.Int64
	unknownJobsTotal         atomic.Int64
	unsupportedTypeTotal     atomic.Int64
	retriesScheduledTotal    atomic.Int64
	exhaustedAttemptsTotal   atomic.Int64
	deadLetteredTotal        atomic.Int64
	dlqEventsTotal           atomic.Int64
	staleJobsRecoveredTotal  atomic.Int64
	activeExecutions         atomic.Int64
}

// NewMetrics initializes a new Metrics tracker.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// IncJobsProcessed increments the total number of deliveries received and processed.
func (m *Metrics) IncJobsProcessed() {
	m.jobsProcessedTotal.Add(1)
}

// IncJobsSucceeded increments the number of successfully executed and completed jobs.
func (m *Metrics) IncJobsSucceeded() {
	m.jobsSucceededTotal.Add(1)
}

// IncJobsFailed increments the number of failed job executions.
func (m *Metrics) IncJobsFailed() {
	m.jobsFailedTotal.Add(1)
}

// IncDuplicateDeliveries increments the count of duplicate deliveries discarded safely.
func (m *Metrics) IncDuplicateDeliveries() {
	m.duplicateDeliveriesTotal.Add(1)
}

// IncFencingViolations increments the count of detected fencing violations (stale leases).
func (m *Metrics) IncFencingViolations() {
	m.fencingViolationsTotal.Add(1)
}

// IncMalformedMessages increments the count of malformed delivery envelopes.
func (m *Metrics) IncMalformedMessages() {
	m.malformedMessagesTotal.Add(1)
}

// IncUnknownJobs increments the count of deliveries with unpersisted job IDs.
func (m *Metrics) IncUnknownJobs() {
	m.unknownJobsTotal.Add(1)
}

// IncUnsupportedType increments the count of deliveries with unregistered job types.
func (m *Metrics) IncUnsupportedType() {
	m.unsupportedTypeTotal.Add(1)
}

// IncActiveExecutions increments the count of currently running jobs.
func (m *Metrics) IncActiveExecutions() {
	m.activeExecutions.Add(1)
}

// DecActiveExecutions decrements the count of currently running jobs.
func (m *Metrics) DecActiveExecutions() {
	m.activeExecutions.Add(-1)
}

// IncRetriesScheduled increments the count of retryable failures scheduled for future execution.
func (m *Metrics) IncRetriesScheduled() {
	m.retriesScheduledTotal.Add(1)
}

// IncExhaustedAttempts increments the count of jobs whose retry limit was reached.
func (m *Metrics) IncExhaustedAttempts() {
	m.exhaustedAttemptsTotal.Add(1)
}

// IncDeadLettered increments the count of jobs moved to dead_letter state.
func (m *Metrics) IncDeadLettered() {
	m.deadLetteredTotal.Add(1)
}

// IncDLQEvents increments the count of poison or malformed messages routed to RabbitMQ DLQ.
func (m *Metrics) IncDLQEvents() {
	m.dlqEventsTotal.Add(1)
}

// IncStaleJobsRecovered increments the count of expired lease jobs reclaimed by the reaper.
func (m *Metrics) IncStaleJobsRecovered() {
	m.staleJobsRecoveredTotal.Add(1)
}

// Snapshot provides a point-in-time copy of worker operational metrics.
type Snapshot struct {
	JobsProcessedTotal       int64 `json:"jobs_processed_total"`
	JobsSucceededTotal       int64 `json:"jobs_succeeded_total"`
	JobsFailedTotal          int64 `json:"jobs_failed_total"`
	DuplicateDeliveriesTotal int64 `json:"duplicate_deliveries_total"`
	FencingViolationsTotal   int64 `json:"fencing_violations_total"`
	MalformedMessagesTotal   int64 `json:"malformed_messages_total"`
	UnknownJobsTotal         int64 `json:"unknown_jobs_total"`
	UnsupportedTypeTotal     int64 `json:"unsupported_type_total"`
	RetriesScheduledTotal    int64 `json:"retries_scheduled_total"`
	ExhaustedAttemptsTotal   int64 `json:"exhausted_attempts_total"`
	DeadLetteredTotal        int64 `json:"dead_lettered_total"`
	DLQEventsTotal           int64 `json:"dlq_events_total"`
	StaleJobsRecoveredTotal  int64 `json:"stale_jobs_recovered_total"`
	ActiveExecutions         int64 `json:"active_executions"`
}

// Snapshot captures the current metrics values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		JobsProcessedTotal:       m.jobsProcessedTotal.Load(),
		JobsSucceededTotal:       m.jobsSucceededTotal.Load(),
		JobsFailedTotal:          m.jobsFailedTotal.Load(),
		DuplicateDeliveriesTotal: m.duplicateDeliveriesTotal.Load(),
		FencingViolationsTotal:   m.fencingViolationsTotal.Load(),
		MalformedMessagesTotal:   m.malformedMessagesTotal.Load(),
		UnknownJobsTotal:         m.unknownJobsTotal.Load(),
		UnsupportedTypeTotal:     m.unsupportedTypeTotal.Load(),
		RetriesScheduledTotal:    m.retriesScheduledTotal.Load(),
		ExhaustedAttemptsTotal:   m.exhaustedAttemptsTotal.Load(),
		DeadLetteredTotal:        m.deadLetteredTotal.Load(),
		DLQEventsTotal:           m.dlqEventsTotal.Load(),
		StaleJobsRecoveredTotal:  m.staleJobsRecoveredTotal.Load(),
		ActiveExecutions:         m.activeExecutions.Load(),
	}
}
