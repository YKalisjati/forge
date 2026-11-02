package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/YKalisjati/forge/internal/domain/job"
)

func TestRegistry_AllowlistEnforcement(t *testing.T) {
	reg := NewRegistry()

	dummyHandler := HandlerFunc(func(_ context.Context, _ *job.Job) error {
		return nil
	})

	// 1. Allowed job types should succeed
	err := reg.Register("report.generate", dummyHandler)
	if err != nil {
		t.Fatalf("expected successful registration of report.generate, got: %v", err)
	}

	err = reg.Register("email.transactional", dummyHandler)
	if err != nil {
		t.Fatalf("expected successful registration of email.transactional, got: %v", err)
	}

	// 2. Disallowed job types must fail
	err = reg.Register("malicious.script", dummyHandler)
	if err == nil || !errors.Is(err, job.ErrInvalidJobType) {
		t.Fatalf("expected ErrInvalidJobType for malicious.script, got: %v", err)
	}

	// 3. Retrieval
	h, ok := reg.Get("report.generate")
	if !ok || h == nil {
		t.Errorf("expected to find registered handler for report.generate")
	}

	_, ok = reg.Get("unknown.type")
	if ok {
		t.Errorf("expected not to find unregistered handler")
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := NewMetrics()

	m.IncJobsProcessed()
	m.IncJobsSucceeded()
	m.IncJobsFailed()
	m.IncDuplicateDeliveries()
	m.IncFencingViolations()
	m.IncMalformedMessages()
	m.IncUnknownJobs()
	m.IncUnsupportedType()
	m.IncRetriesScheduled()
	m.IncExhaustedAttempts()
	m.IncDeadLettered()
	m.IncDLQEvents()
	m.IncStaleJobsRecovered()
	m.IncActiveExecutions()

	snap := m.Snapshot()
	if snap.JobsProcessedTotal != 1 ||
		snap.JobsSucceededTotal != 1 ||
		snap.JobsFailedTotal != 1 ||
		snap.DuplicateDeliveriesTotal != 1 ||
		snap.FencingViolationsTotal != 1 ||
		snap.MalformedMessagesTotal != 1 ||
		snap.UnknownJobsTotal != 1 ||
		snap.UnsupportedTypeTotal != 1 ||
		snap.RetriesScheduledTotal != 1 ||
		snap.ExhaustedAttemptsTotal != 1 ||
		snap.DeadLetteredTotal != 1 ||
		snap.DLQEventsTotal != 1 ||
		snap.StaleJobsRecoveredTotal != 1 ||
		snap.ActiveExecutions != 1 {
		t.Fatalf("unexpected snapshot values: %+v", snap)
	}

	m.DecActiveExecutions()
	if m.Snapshot().ActiveExecutions != 0 {
		t.Errorf("expected active executions 0 after decrement")
	}
}
