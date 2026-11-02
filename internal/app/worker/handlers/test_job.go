package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/YKalisjati/forge/internal/domain/job"
)

// TestJobPayload allows configurable testing behavior.
type TestJobPayload struct {
	DurationMS                int  `json:"duration_ms,omitempty"`
	Fail                      bool `json:"fail,omitempty"`
	FailPermanent             bool `json:"fail_permanent,omitempty"`
	FailPoison                bool `json:"fail_poison,omitempty"`
	FailRetryableUntilAttempt int  `json:"fail_retryable_until_attempt,omitempty"`
}

// TestJobHandler implements a configurable handler for test.job.
type TestJobHandler struct{}

// NewTestJobHandler creates a TestJobHandler.
func NewTestJobHandler() *TestJobHandler {
	return &TestJobHandler{}
}

// Execute processes test.job honoring cancellation and simulated failure.
func (h *TestJobHandler) Execute(ctx context.Context, j *job.Job) error {
	var input TestJobPayload
	if len(j.Payload) > 0 {
		_ = json.Unmarshal(j.Payload, &input)
	}

	if input.DurationMS > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(input.DurationMS) * time.Millisecond):
		}
	}

	if input.FailPermanent {
		return job.MarkPermanent(errors.New("simulated permanent test job failure"))
	}

	if input.FailPoison {
		return job.ErrPoisonMessage
	}

	if input.FailRetryableUntilAttempt > 0 && j.RetriesAttempted < input.FailRetryableUntilAttempt {
		return errors.New("simulated transient failure requiring retry")
	}

	if input.Fail {
		return errors.New("simulated test job failure")
	}

	return nil
}
