package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics encapsulates all Prometheus metric collectors for Forge.
type Metrics struct {
	Registry *prometheus.Registry

	// API Metrics
	APIRequestsTotal       *prometheus.CounterVec
	APIRequestDuration     *prometheus.HistogramVec
	APIActiveRequests      prometheus.Gauge
	RateLimitExceededTotal prometheus.Counter
	RateLimitErrorsTotal   prometheus.Counter

	// Worker Metrics
	WorkerJobsProcessedTotal *prometheus.CounterVec
	WorkerJobDuration        *prometheus.HistogramVec
	WorkerRetriesTotal       *prometheus.CounterVec
	WorkerActiveExecutions   prometheus.Gauge
	WorkerFencingViolations  prometheus.Counter

	// Outbox Relay Metrics
	OutboxBacklogTotal       prometheus.Gauge
	OutboxOldestAgeSeconds   prometheus.Gauge
	OutboxPublishedTotal     *prometheus.CounterVec
	OutboxPublishErrorsTotal *prometheus.CounterVec

	// Dead-Letter Metrics
	DeadLetterEventsTotal *prometheus.CounterVec
}

var defaultMetrics *Metrics

func init() {
	defaultMetrics = New(prometheus.DefaultRegisterer)
}

// Default returns the default global metrics registry.
func Default() *Metrics {
	return defaultMetrics
}

// New initializes a new Metrics instance registered with the provided prometheus.Registerer.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	m := &Metrics{}

	// If reg is a *prometheus.Registry, assign it
	if r, ok := reg.(*prometheus.Registry); ok {
		m.Registry = r
	} else {
		m.Registry = prometheus.DefaultGatherer.(*prometheus.Registry)
	}

	// 1. API Metrics
	m.APIRequestsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "api",
			Name:      "requests_total",
			Help:      "Total count of HTTP requests processed by endpoint, method, and status code.",
		},
		[]string{"method", "endpoint", "status_code"},
	)

	m.APIRequestDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "forge",
			Subsystem: "api",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency distributions in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"method", "endpoint"},
	)

	m.APIActiveRequests = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "forge",
			Subsystem: "api",
			Name:      "active_requests",
			Help:      "Number of currently in-flight HTTP requests.",
		},
	)

	m.RateLimitExceededTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "api",
			Name:      "rate_limit_exceeded_total",
			Help:      "Total count of HTTP requests rejected due to rate limiting.",
		},
	)

	m.RateLimitErrorsTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "api",
			Name:      "rate_limit_errors_total",
			Help:      "Total count of rate limiter Redis operational or connection errors (failed open).",
		},
	)

	// 2. Worker Metrics
	m.WorkerJobsProcessedTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "worker",
			Name:      "jobs_processed_total",
			Help:      "Total count of background jobs processed by job type and outcome status.",
		},
		[]string{"type", "status"},
	)

	m.WorkerJobDuration = factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "forge",
			Subsystem: "worker",
			Name:      "job_duration_seconds",
			Help:      "Execution duration of background jobs in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
		[]string{"type"},
	)

	m.WorkerRetriesTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "worker",
			Name:      "retries_total",
			Help:      "Total count of background job retries scheduled by job type.",
		},
		[]string{"type"},
	)

	m.WorkerActiveExecutions = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "forge",
			Subsystem: "worker",
			Name:      "active_executions",
			Help:      "Number of background jobs currently running on worker instances.",
		},
	)

	m.WorkerFencingViolations = factory.NewCounter(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "worker",
			Name:      "fencing_violations_total",
			Help:      "Total count of detected fencing lease violations (stale zombie workers).",
		},
	)

	// 3. Outbox Relay Metrics
	m.OutboxBacklogTotal = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "forge",
			Subsystem: "outbox",
			Name:      "backlog_total",
			Help:      "Current count of pending unpublished events in the outbox table.",
		},
	)

	m.OutboxOldestAgeSeconds = factory.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "forge",
			Subsystem: "outbox",
			Name:      "oldest_event_age_seconds",
			Help:      "Age in seconds of the oldest pending unpublished event in the outbox.",
		},
	)

	m.OutboxPublishedTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "outbox",
			Name:      "published_total",
			Help:      "Total count of outbox events published to RabbitMQ broker.",
		},
		[]string{"event_type"},
	)

	m.OutboxPublishErrorsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "outbox",
			Name:      "publish_errors_total",
			Help:      "Total count of outbox publishing or confirmation failures by reason.",
		},
		[]string{"reason"},
	)

	// 4. Dead-Letter Metrics
	m.DeadLetterEventsTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "forge",
			Subsystem: "worker",
			Name:      "dead_letter_events_total",
			Help:      "Total count of jobs moved to dead letter queue by reason.",
		},
		[]string{"reason"},
	)

	return m
}

// ObserveHTTPRequest records HTTP request count and latency metrics.
func (m *Metrics) ObserveHTTPRequest(method, endpoint string, statusCode int, duration time.Duration) {
	statusStr := strconv.Itoa(statusCode)
	m.APIRequestsTotal.WithLabelValues(method, endpoint, statusStr).Inc()
	m.APIRequestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
}

// ObserveJobExecution records worker job execution duration and outcome.
func (m *Metrics) ObserveJobExecution(jobType, status string, duration time.Duration) {
	m.WorkerJobsProcessedTotal.WithLabelValues(jobType, status).Inc()
	m.WorkerJobDuration.WithLabelValues(jobType).Observe(duration.Seconds())
}

// IncJobRetry records a scheduled job retry.
func (m *Metrics) IncJobRetry(jobType string) {
	m.WorkerRetriesTotal.WithLabelValues(jobType).Inc()
}

// IncDeadLetter records a dead-letter event with normalized reason.
func (m *Metrics) IncDeadLetter(reason string) {
	m.DeadLetterEventsTotal.WithLabelValues(reason).Inc()
}

// SetOutboxBacklog updates the outbox pending backlog count and oldest age.
func (m *Metrics) SetOutboxBacklog(count int64, oldestAgeSeconds float64) {
	m.OutboxBacklogTotal.Set(float64(count))
	m.OutboxOldestAgeSeconds.Set(oldestAgeSeconds)
}
