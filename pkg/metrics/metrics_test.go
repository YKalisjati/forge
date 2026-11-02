package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_Observability(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	// Test HTTP request observation
	m.ObserveHTTPRequest("POST", "/v1/jobs", 202, 15*time.Millisecond)
	m.ObserveHTTPRequest("POST", "/v1/jobs", 429, 2*time.Millisecond)
	m.APIActiveRequests.Inc()
	m.RateLimitExceededTotal.Inc()
	m.RateLimitErrorsTotal.Inc()

	// Test Worker job observation
	m.ObserveJobExecution("report.generate", "completed", 50*time.Millisecond)
	m.ObserveJobExecution("report.generate", "failed", 10*time.Millisecond)
	m.IncJobRetry("report.generate")
	m.IncDeadLetter("retries_exhausted")
	m.WorkerActiveExecutions.Set(3)
	m.WorkerFencingViolations.Inc()

	// Test Outbox observation
	m.SetOutboxBacklog(42, 12.5)
	m.OutboxPublishedTotal.WithLabelValues("job.created").Inc()
	m.OutboxPublishErrorsTotal.WithLabelValues("nack").Inc()

	// Gather metrics and verify presence
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather prometheus metrics: %v", err)
	}

	foundMetrics := make(map[string]float64)
	for _, fam := range families {
		name := fam.GetName()
		for _, metric := range fam.GetMetric() {
			if metric.Counter != nil {
				foundMetrics[name] += metric.Counter.GetValue()
			} else if metric.Gauge != nil {
				foundMetrics[name] = metric.Gauge.GetValue()
			} else if metric.Histogram != nil {
				foundMetrics[name] = float64(metric.Histogram.GetSampleCount())
			}
		}
	}

	expectedMetrics := []string{
		"forge_api_requests_total",
		"forge_api_request_duration_seconds",
		"forge_api_active_requests",
		"forge_api_rate_limit_exceeded_total",
		"forge_api_rate_limit_errors_total",
		"forge_worker_jobs_processed_total",
		"forge_worker_job_duration_seconds",
		"forge_worker_retries_total",
		"forge_worker_active_executions",
		"forge_worker_fencing_violations_total",
		"forge_worker_dead_letter_events_total",
		"forge_outbox_backlog_total",
		"forge_outbox_oldest_event_age_seconds",
		"forge_outbox_published_total",
		"forge_outbox_publish_errors_total",
	}

	for _, exp := range expectedMetrics {
		if _, ok := foundMetrics[exp]; !ok {
			t.Errorf("missing expected Prometheus metric: %s", exp)
		}
	}

	if foundMetrics["forge_outbox_backlog_total"] != 42 {
		t.Errorf("expected outbox backlog total 42, got %v", foundMetrics["forge_outbox_backlog_total"])
	}
	if foundMetrics["forge_outbox_oldest_event_age_seconds"] != 12.5 {
		t.Errorf("expected outbox oldest age 12.5, got %v", foundMetrics["forge_outbox_oldest_event_age_seconds"])
	}
}
