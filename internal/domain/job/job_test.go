package job

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStateTransitions(t *testing.T) {
	tests := []struct {
		name      string
		initial   State
		target    State
		expectErr error
	}{
		{"pending to running", StatePending, StateRunning, nil},
		{"pending to completed invalid", StatePending, StateCompleted, ErrInvalidStateTransition},
		{"running to completed", StateRunning, StateCompleted, nil},
		{"running to pending", StateRunning, StatePending, nil},
		{"running to dead_letter", StateRunning, StateDeadLetter, nil},
		{"running to failed", StateRunning, StateFailed, nil},
		{"completed is terminal", StateCompleted, StateRunning, ErrTerminalStateTransition},
		{"dead_letter is terminal", StateDeadLetter, StateRunning, ErrTerminalStateTransition},
		{"failed to pending retry", StateFailed, StatePending, nil},
		{"failed to dead_letter", StateFailed, StateDeadLetter, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{State: tt.initial}
			err := j.TransitionTo(tt.target)
			if tt.expectErr != nil {
				if err == nil {
					t.Fatalf("expected error %v, got nil", tt.expectErr)
				}
				if !errors.Is(err, tt.expectErr) {
					t.Errorf("expected error %v, got %v", tt.expectErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if j.State != tt.target {
					t.Errorf("expected state %s, got %s", tt.target, j.State)
				}
			}
		})
	}
}

func TestJobValidation(t *testing.T) {
	validPayload := json.RawMessage(`{"recipient": "user@example.com"}`)

	tests := []struct {
		name        string
		job         *Job
		maxBytes    int64
		expectError error
	}{
		{
			name: "valid job",
			job: &Job{
				Type:       "email.transactional",
				Payload:    validPayload,
				Priority:   10,
				MaxRetries: 3,
			},
			maxBytes:    1048576,
			expectError: nil,
		},
		{
			name: "unsupported job type",
			job: &Job{
				Type:       "invalid.unknown.job",
				Payload:    validPayload,
				Priority:   10,
				MaxRetries: 3,
			},
			maxBytes:    1048576,
			expectError: ErrInvalidJobType,
		},
		{
			name: "empty payload",
			job: &Job{
				Type:       "email.transactional",
				Payload:    nil,
				Priority:   10,
				MaxRetries: 3,
			},
			maxBytes:    1048576,
			expectError: ErrInvalidPayload,
		},
		{
			name: "non-object payload (array)",
			job: &Job{
				Type:       "email.transactional",
				Payload:    json.RawMessage(`["item1", "item2"]`),
				Priority:   10,
				MaxRetries: 3,
			},
			maxBytes:    1048576,
			expectError: ErrInvalidPayload,
		},
		{
			name: "payload exceeds size limit",
			job: &Job{
				Type:       "email.transactional",
				Payload:    validPayload,
				Priority:   10,
				MaxRetries: 3,
			},
			maxBytes:    10, // Payload length is ~34 bytes
			expectError: ErrPayloadTooLarge,
		},
		{
			name: "negative priority",
			job: &Job{
				Type:       "email.transactional",
				Payload:    validPayload,
				Priority:   -1,
				MaxRetries: 3,
			},
			maxBytes:    1048576,
			expectError: ErrInvalidPriority,
		},
		{
			name: "excessive priority",
			job: &Job{
				Type:       "email.transactional",
				Payload:    validPayload,
				Priority:   101,
				MaxRetries: 3,
			},
			maxBytes:    1048576,
			expectError: ErrInvalidPriority,
		},
		{
			name: "excessive max retries",
			job: &Job{
				Type:       "email.transactional",
				Payload:    validPayload,
				Priority:   10,
				MaxRetries: 25,
			},
			maxBytes:    1048576,
			expectError: ErrInvalidRetries,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Validate(tt.maxBytes)
			if tt.expectError != nil {
				if err == nil {
					t.Fatalf("expected error %v, got nil", tt.expectError)
				}
				if !errors.Is(err, tt.expectError) {
					t.Errorf("expected error %v, got %v", tt.expectError, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	completed := &Job{State: StateCompleted}
	deadLetter := &Job{State: StateDeadLetter}
	pending := &Job{State: StatePending}
	running := &Job{State: StateRunning}

	if !completed.IsTerminal() {
		t.Error("expected completed to be terminal")
	}
	if !deadLetter.IsTerminal() {
		t.Error("expected dead_letter to be terminal")
	}
	if pending.IsTerminal() {
		t.Error("expected pending to NOT be terminal")
	}
	if running.IsTerminal() {
		t.Error("expected running to NOT be terminal")
	}
}

func TestCursor(t *testing.T) {
	now := time.Now().UTC()
	id := uuid.New()
	cursor := &Cursor{
		CreatedAt: now,
		ID:        id,
	}

	if cursor.CreatedAt != now || cursor.ID != id {
		t.Errorf("cursor fields mismatch")
	}
}
