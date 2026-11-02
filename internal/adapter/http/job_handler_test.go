package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	appJob "github.com/YKalisjati/forge/internal/app/job"
	domainIdempotency "github.com/YKalisjati/forge/internal/domain/idempotency"
	domainJob "github.com/YKalisjati/forge/internal/domain/job"
	"github.com/YKalisjati/forge/pkg/health"
)

// mockJobService implements JobService for HTTP unit testing.
type mockJobService struct {
	submitFunc func(ctx context.Context, key *string, input appJob.SubmitJobInput) (*appJob.SubmitResult, error)
	getFunc    func(ctx context.Context, id uuid.UUID) (*domainJob.WithExecutions, error)
	listFunc   func(ctx context.Context, filter domainJob.Filter) ([]*domainJob.Job, *domainJob.Cursor, bool, error)
}

func (m *mockJobService) SubmitJob(ctx context.Context, key *string, input appJob.SubmitJobInput) (*appJob.SubmitResult, error) {
	if m.submitFunc != nil {
		return m.submitFunc(ctx, key, input)
	}
	return nil, nil
}

func (m *mockJobService) GetJob(ctx context.Context, id uuid.UUID) (*domainJob.WithExecutions, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, id)
	}
	return nil, nil
}

func (m *mockJobService) ListJobs(ctx context.Context, filter domainJob.Filter) ([]*domainJob.Job, *domainJob.Cursor, bool, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, filter)
	}
	return nil, nil, false, nil
}

func TestSubmitJobValidationFailures(t *testing.T) {
	mockSvc := &mockJobService{}
	handler := NewJobHandler(mockSvc, 1048576)
	router := NewRouter(ServerOptions{
		Registry:   health.NewRegistry("forge-test"),
		Logger:     nil,
		JobHandler: handler,
	})

	tests := []struct {
		name       string
		body       string
		headers    map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "malformed JSON",
			body:       `{ invalid json `,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MALFORMED_JSON",
		},
		{
			name:       "missing type",
			body:       `{"payload": {"foo": "bar"}}`,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
		{
			name:       "unsupported type",
			body:       `{"type": "nonexistent.type", "payload": {"foo": "bar"}}`,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
		{
			name:       "empty payload",
			body:       `{"type": "email.transactional"}`,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
		{
			name:       "non-object payload (array)",
			body:       `{"type": "email.transactional", "payload": [1, 2, 3]}`,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
		{
			name:       "priority exceeds 100",
			body:       `{"type": "email.transactional", "payload": {"foo": "bar"}, "priority": 105}`,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
		{
			name:       "negative max_retries",
			body:       `{"type": "email.transactional", "payload": {"foo": "bar"}, "max_retries": -1}`,
			headers:    map[string]string{"Content-Type": "application/json"},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
		{
			name:       "idempotency key too long",
			body:       `{"type": "email.transactional", "payload": {"foo": "bar"}}`,
			headers:    map[string]string{"Content-Type": "application/json", "Idempotency-Key": string(make([]byte, 150))},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   "VALIDATION_FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(tt.body))
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d, body: %s", tt.wantStatus, rec.Code, rec.Body.String())
			}

			var prob ProblemDetails
			if err := json.Unmarshal(rec.Body.Bytes(), &prob); err != nil {
				t.Fatalf("failed to decode problem details: %v", err)
			}

			if prob.Code != tt.wantCode {
				t.Errorf("expected code %s, got %s", tt.wantCode, prob.Code)
			}
		})
	}
}

func TestSubmitJobSuccessAndReplay(t *testing.T) {
	jobID := uuid.New()
	now := time.Now().UTC()

	mockSvc := &mockJobService{
		submitFunc: func(_ context.Context, key *string, input appJob.SubmitJobInput) (*appJob.SubmitResult, error) {
			replayed := false
			if key != nil && *key == "replayed-key" {
				replayed = true
			}
			return &appJob.SubmitResult{
				Job: &domainJob.Job{
					ID:            jobID,
					Type:          input.Type,
					State:         domainJob.StatePending,
					Priority:      input.Priority,
					MaxRetries:    input.MaxRetries,
					NextAttemptAt: now,
					CreatedAt:     now,
					UpdatedAt:     now,
				},
				Replayed: replayed,
			}, nil
		},
	}

	handler := NewJobHandler(mockSvc, 1048576)
	router := NewRouter(ServerOptions{
		Registry:   health.NewRegistry("forge-test"),
		Logger:     nil,
		JobHandler: handler,
	})

	// 1. Fresh submission
	{
		reqBody := `{"type": "email.transactional", "payload": {"recipient": "alice@example.com"}, "priority": 10}`
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(reqBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "new-key-1")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Location") != "/v1/jobs/"+jobID.String() {
			t.Errorf("expected Location header /v1/jobs/%s, got %s", jobID.String(), rec.Header().Get("Location"))
		}
		if rec.Header().Get("Idempotent-Replayed") == "true" {
			t.Errorf("fresh request should NOT have Idempotent-Replayed: true")
		}

		var res SubmitJobResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if res.ID != jobID || res.State != "pending" || res.Priority != 10 {
			t.Errorf("unexpected response: %+v", res)
		}
	}

	// 2. Replayed submission
	{
		reqBody := `{"type": "email.transactional", "payload": {"recipient": "alice@example.com"}, "priority": 10}`
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(reqBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "replayed-key")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d", rec.Code)
		}
		if rec.Header().Get("Idempotent-Replayed") != "true" {
			t.Errorf("replayed request MUST have Idempotent-Replayed: true header")
		}
	}
}

func TestSubmitJobIdempotencyConflicts(t *testing.T) {
	mockSvc := &mockJobService{
		submitFunc: func(_ context.Context, key *string, _ appJob.SubmitJobInput) (*appJob.SubmitResult, error) {
			if key != nil && *key == "mismatched-key" {
				return nil, domainIdempotency.ErrPayloadMismatch
			}
			if key != nil && *key == "concurrent-key" {
				return nil, domainIdempotency.ErrConflict
			}
			return nil, nil
		},
	}

	handler := NewJobHandler(mockSvc, 1048576)
	router := NewRouter(ServerOptions{
		Registry:   health.NewRegistry("forge-test"),
		Logger:     nil,
		JobHandler: handler,
	})

	// 1. Same key with different payload -> 409 Conflict
	{
		reqBody := `{"type": "email.transactional", "payload": {"foo": "bar"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(reqBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "mismatched-key")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected 409 Conflict for payload mismatch, got %d: %s", rec.Code, rec.Body.String())
		}
		var prob ProblemDetails
		_ = json.Unmarshal(rec.Body.Bytes(), &prob)
		if prob.Code != "IDEMPOTENCY_PAYLOAD_MISMATCH" {
			t.Errorf("expected code IDEMPOTENCY_PAYLOAD_MISMATCH, got %s", prob.Code)
		}
	}

	// 2. Concurrent request in flight -> 409 Conflict
	{
		reqBody := `{"type": "email.transactional", "payload": {"foo": "bar"}}`
		req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(reqBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "concurrent-key")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("expected 409 Conflict for concurrent request, got %d", rec.Code)
		}
		var prob ProblemDetails
		_ = json.Unmarshal(rec.Body.Bytes(), &prob)
		if prob.Code != "CONCURRENT_REQUEST" {
			t.Errorf("expected code CONCURRENT_REQUEST, got %s", prob.Code)
		}
	}
}

func TestGetJobEndpoint(t *testing.T) {
	jobID := uuid.New()
	mockSvc := &mockJobService{
		getFunc: func(_ context.Context, id uuid.UUID) (*domainJob.WithExecutions, error) {
			if id == jobID {
				return &domainJob.WithExecutions{
					Job: domainJob.Job{
						ID:       jobID,
						Type:     "email.transactional",
						State:    domainJob.StateCompleted,
						Priority: 5,
					},
					Executions: []domainJob.Execution{
						{
							JobID:    jobID,
							Attempt:  1,
							WorkerID: "worker-1",
							Status:   "completed",
						},
					},
				}, nil
			}
			return nil, domainJob.ErrJobNotFound
		},
	}

	handler := NewJobHandler(mockSvc, 1048576)
	router := NewRouter(ServerOptions{
		Registry:   health.NewRegistry("forge-test"),
		Logger:     nil,
		JobHandler: handler,
	})

	// 1. Existing job
	{
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+jobID.String(), nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
		var res domainJob.WithExecutions
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res.ID != jobID || len(res.Executions) != 1 {
			t.Errorf("unexpected job details: %+v", res)
		}
	}

	// 2. Not found job
	{
		randomID := uuid.New()
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+randomID.String(), nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got %d", rec.Code)
		}
		var prob ProblemDetails
		_ = json.Unmarshal(rec.Body.Bytes(), &prob)
		if prob.Code != "JOB_NOT_FOUND" {
			t.Errorf("expected JOB_NOT_FOUND, got %s", prob.Code)
		}
	}

	// 3. Malformed UUID
	{
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs/not-a-uuid", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expected 422 for bad UUID, got %d", rec.Code)
		}
	}
}

func TestListJobsEndpoint(t *testing.T) {
	job1 := &domainJob.Job{ID: uuid.New(), Type: "email.transactional", State: domainJob.StatePending, Priority: 10}
	job2 := &domainJob.Job{ID: uuid.New(), Type: "email.transactional", State: domainJob.StatePending, Priority: 5}

	mockSvc := &mockJobService{
		listFunc: func(_ context.Context, filter domainJob.Filter) ([]*domainJob.Job, *domainJob.Cursor, bool, error) {
			if filter.Limit == 2 {
				return []*domainJob.Job{job1, job2}, &domainJob.Cursor{CreatedAt: time.Now(), ID: job2.ID}, true, nil
			}
			return []*domainJob.Job{job1}, nil, false, nil
		},
	}

	handler := NewJobHandler(mockSvc, 1048576)
	router := NewRouter(ServerOptions{
		Registry:   health.NewRegistry("forge-test"),
		Logger:     nil,
		JobHandler: handler,
	})

	// 1. Valid list with pagination
	{
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=2&state=pending&type=email.transactional", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}

		var res ListJobsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if len(res.Items) != 2 || !res.Pagination.HasMore || res.Pagination.NextCursor == "" {
			t.Errorf("unexpected list response: %+v", res)
		}
	}

	// 2. Invalid state filter
	{
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?state=invalid_status", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expected 422 for invalid state, got %d", rec.Code)
		}
	}

	// 3. Invalid limit parameter
	{
		req := httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=500", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expected 422 for limit > 100, got %d", rec.Code)
		}
	}
}
