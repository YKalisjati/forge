package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	appJob "github.com/YKalisjati/forge/internal/app/job"
	domainIdempotency "github.com/YKalisjati/forge/internal/domain/idempotency"
	domainJob "github.com/YKalisjati/forge/internal/domain/job"
)

// JobService defines the application operations consumed by the HTTP adapter.
type JobService interface {
	SubmitJob(ctx context.Context, idempotencyKey *string, input appJob.SubmitJobInput) (*appJob.SubmitResult, error)
	GetJob(ctx context.Context, id uuid.UUID) (*domainJob.WithExecutions, error)
	ListJobs(ctx context.Context, filter domainJob.Filter) ([]*domainJob.Job, *domainJob.Cursor, bool, error)
}

// JobHandler implements HTTP endpoints for background job management.
type JobHandler struct {
	service         JobService
	maxPayloadBytes int64
}

// NewJobHandler constructs a JobHandler.
func NewJobHandler(service JobService, maxPayloadBytes int64) *JobHandler {
	return &JobHandler{
		service:         service,
		maxPayloadBytes: maxPayloadBytes,
	}
}

// SubmitJobRequest defines the JSON schema for job submission.
type SubmitJobRequest struct {
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Priority     *int            `json:"priority"`
	MaxRetries   *int            `json:"max_retries"`
	DelaySeconds *int            `json:"delay_seconds"`
}

// SubmitJobResponse defines the 202 Accepted response body schema.
type SubmitJobResponse struct {
	ID               uuid.UUID `json:"id"`
	Type             string    `json:"type"`
	State            string    `json:"state"`
	Priority         int       `json:"priority"`
	MaxRetries       int       `json:"max_retries"`
	RetriesAttempted int       `json:"retries_attempted"`
	NextAttemptAt    string    `json:"next_attempt_at"`
	CreatedAt        string    `json:"created_at"`
	UpdatedAt        string    `json:"updated_at"`
}

// HandleSubmitJob handles POST /v1/jobs.
func (h *JobHandler) HandleSubmitJob(w http.ResponseWriter, r *http.Request) {
	// 1. Content-Type verification
	contentType := r.Header.Get("Content-Type")
	if contentType != "" && !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/validation-error",
			Title:    "Invalid Content-Type",
			Status:   http.StatusBadRequest,
			Detail:   "Content-Type must be application/json",
			Instance: r.URL.Path,
			Code:     "MALFORMED_JSON",
		})
		return
	}

	// 2. Bound payload size using MaxBytesReader
	limitedReader := http.MaxBytesReader(w, r.Body, h.maxPayloadBytes)
	bodyBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Payload Too Large",
				Status:   http.StatusUnprocessableEntity,
				Detail:   fmt.Sprintf("The payload size exceeds the %d byte limit.", h.maxPayloadBytes),
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}
		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/bad-request",
			Title:    "Malformed JSON Payload",
			Status:   http.StatusBadRequest,
			Detail:   "Request body could not be parsed as valid JSON.",
			Instance: r.URL.Path,
			Code:     "MALFORMED_JSON",
		})
		return
	}

	// 3. Parse JSON
	var req SubmitJobRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/bad-request",
			Title:    "Malformed JSON Payload",
			Status:   http.StatusBadRequest,
			Detail:   "Request body could not be parsed as valid JSON.",
			Instance: r.URL.Path,
			Code:     "MALFORMED_JSON",
		})
		return
	}

	// 4. Validate parameters and collect RFC 7807 invalid_params
	var invalidParams []InvalidParam

	if strings.TrimSpace(req.Type) == "" {
		invalidParams = append(invalidParams, InvalidParam{
			Name:   "type",
			Reason: "job type is required",
		})
	} else if !domainJob.IsAllowedJobType(req.Type) {
		invalidParams = append(invalidParams, InvalidParam{
			Name:   "type",
			Reason: fmt.Sprintf("unsupported job type %q", req.Type),
		})
	}

	if len(req.Payload) == 0 {
		invalidParams = append(invalidParams, InvalidParam{
			Name:   "payload",
			Reason: "payload is required",
		})
	} else {
		var js map[string]interface{}
		if err := json.Unmarshal(req.Payload, &js); err != nil {
			invalidParams = append(invalidParams, InvalidParam{
				Name:   "payload",
				Reason: "must be a valid JSON object",
			})
		}
	}

	priority := 0
	if req.Priority != nil {
		priority = *req.Priority
		if priority < 0 || priority > 100 {
			invalidParams = append(invalidParams, InvalidParam{
				Name:   "priority",
				Reason: "must be an integer between 0 and 100",
			})
		}
	}

	maxRetries := 3
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
		if maxRetries < 0 || maxRetries > 20 {
			invalidParams = append(invalidParams, InvalidParam{
				Name:   "max_retries",
				Reason: "must be an integer between 0 and 20",
			})
		}
	}

	delaySeconds := 0
	if req.DelaySeconds != nil {
		delaySeconds = *req.DelaySeconds
		if delaySeconds < 0 {
			invalidParams = append(invalidParams, InvalidParam{
				Name:   "delay_seconds",
				Reason: "must be greater than or equal to 0",
			})
		}
	}

	if len(invalidParams) > 0 {
		WriteProblem(w, ProblemDetails{
			Type:          "https://forge.dev/errors/validation-error",
			Title:         "Invalid Request Parameters",
			Status:        http.StatusUnprocessableEntity,
			Detail:        "The payload size exceeds the 1MB limit or fields failed validation.",
			Instance:      r.URL.Path,
			Code:          "VALIDATION_FAILED",
			InvalidParams: invalidParams,
		})
		return
	}

	// 5. Idempotency-Key header extraction
	var idempotencyKey *string
	keyHeader := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if keyHeader != "" {
		if len(keyHeader) > 128 {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Invalid Request Parameters",
				Status:   http.StatusUnprocessableEntity,
				Detail:   "Idempotency-Key exceeds maximum length of 128 characters",
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}
		idempotencyKey = &keyHeader
	}

	input := appJob.SubmitJobInput{
		Type:         req.Type,
		Payload:      req.Payload,
		Priority:     priority,
		MaxRetries:   maxRetries,
		DelaySeconds: delaySeconds,
	}

	res, err := h.service.SubmitJob(r.Context(), idempotencyKey, input)
	if err != nil {
		if errors.Is(err, domainIdempotency.ErrPayloadMismatch) {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/conflict",
				Title:    "Idempotency Key Conflict",
				Status:   http.StatusConflict,
				Detail:   "The provided Idempotency-Key was already used with a different request payload.",
				Instance: r.URL.Path,
				Code:     "IDEMPOTENCY_PAYLOAD_MISMATCH",
			})
			return
		}
		if errors.Is(err, domainIdempotency.ErrConflict) {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/conflict",
				Title:    "Concurrent Request in Flight",
				Status:   http.StatusConflict,
				Detail:   "An identical idempotency key is already processing.",
				Instance: r.URL.Path,
				Code:     "CONCURRENT_REQUEST",
			})
			return
		}
		if errors.Is(err, domainJob.ErrInvalidJobType) ||
			errors.Is(err, domainJob.ErrInvalidPriority) ||
			errors.Is(err, domainJob.ErrInvalidRetries) ||
			errors.Is(err, domainJob.ErrInvalidDelay) ||
			errors.Is(err, domainJob.ErrInvalidPayload) {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Validation Failed",
				Status:   http.StatusUnprocessableEntity,
				Detail:   err.Error(),
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}

		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/internal-error",
			Title:    "Internal Server Error",
			Status:   http.StatusInternalServerError,
			Detail:   "An unexpected error occurred while persisting the job.",
			Instance: r.URL.Path,
			Code:     "INTERNAL_SERVER_ERROR",
		})
		return
	}

	// 6. Write Response: 202 Accepted
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", fmt.Sprintf("/v1/jobs/%s", res.Job.ID.String()))
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.WriteHeader(http.StatusAccepted)

	responsePayload := SubmitJobResponse{
		ID:               res.Job.ID,
		Type:             res.Job.Type,
		State:            string(res.Job.State),
		Priority:         res.Job.Priority,
		MaxRetries:       res.Job.MaxRetries,
		RetriesAttempted: res.Job.RetriesAttempted,
		NextAttemptAt:    res.Job.NextAttemptAt.Format(time.RFC3339),
		CreatedAt:        res.Job.CreatedAt.Format(time.RFC3339),
		UpdatedAt:        res.Job.UpdatedAt.Format(time.RFC3339),
	}

	_ = json.NewEncoder(w).Encode(responsePayload)
}

// HandleGetJob handles GET /v1/jobs/{id}.
func (h *JobHandler) HandleGetJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	jobID, err := uuid.Parse(idStr)
	if err != nil {
		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/validation-error",
			Title:    "Invalid Resource Identifier",
			Status:   http.StatusUnprocessableEntity,
			Detail:   "Job ID must be a valid UUIDv4 string.",
			Instance: r.URL.Path,
			Code:     "VALIDATION_FAILED",
		})
		return
	}

	jobWithExec, err := h.service.GetJob(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, domainJob.ErrJobNotFound) {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/not-found",
				Title:    "Job Not Found",
				Status:   http.StatusNotFound,
				Detail:   fmt.Sprintf("Job %s does not exist.", jobID.String()),
				Instance: r.URL.Path,
				Code:     "JOB_NOT_FOUND",
			})
			return
		}

		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/internal-error",
			Title:    "Internal Server Error",
			Status:   http.StatusInternalServerError,
			Detail:   "An unexpected error occurred while retrieving job details.",
			Instance: r.URL.Path,
			Code:     "INTERNAL_SERVER_ERROR",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(jobWithExec)
}

// ListJobsItem defines the projected fields in a job listing.
type ListJobsItem struct {
	ID               uuid.UUID `json:"id"`
	Type             string    `json:"type"`
	State            string    `json:"state"`
	Priority         int       `json:"priority"`
	RetriesAttempted int       `json:"retries_attempted"`
	CreatedAt        string    `json:"created_at"`
}

// ListJobsResponse defines the paginated response schema.
type ListJobsResponse struct {
	Items      []ListJobsItem `json:"items"`
	Pagination PaginationMeta `json:"pagination"`
}

// PaginationMeta encapsulates pagination state.
type PaginationMeta struct {
	Limit      int    `json:"limit"`
	NextCursor string `json:"next_cursor,omitempty"`
	HasMore    bool   `json:"has_more"`
}

type cursorPayload struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

// HandleListJobs handles GET /v1/jobs.
func (h *JobHandler) HandleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Parse state filter
	var stateFilter *domainJob.State
	if stateStr := strings.TrimSpace(q.Get("state")); stateStr != "" {
		st := domainJob.State(stateStr)
		switch st {
		case domainJob.StatePending, domainJob.StateRunning, domainJob.StateCompleted, domainJob.StateFailed, domainJob.StateDeadLetter:
			stateFilter = &st
		default:
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Invalid State Filter",
				Status:   http.StatusUnprocessableEntity,
				Detail:   fmt.Sprintf("Invalid state %q (expected pending, running, completed, failed, dead_letter)", stateStr),
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}
	}

	// Parse type filter
	var typeFilter *string
	if typeStr := strings.TrimSpace(q.Get("type")); typeStr != "" {
		typeFilter = &typeStr
	}

	// Parse limit
	limit := 50
	if limitStr := strings.TrimSpace(q.Get("limit")); limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit < 1 || parsedLimit > 100 {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Invalid Limit Parameter",
				Status:   http.StatusUnprocessableEntity,
				Detail:   "limit must be an integer between 1 and 100",
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}
		limit = parsedLimit
	}

	// Parse cursor
	var cursor *domainJob.Cursor
	if cursorStr := strings.TrimSpace(q.Get("cursor")); cursorStr != "" {
		decoded, err := base64.URLEncoding.DecodeString(cursorStr)
		if err != nil {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Invalid Cursor",
				Status:   http.StatusUnprocessableEntity,
				Detail:   "cursor parameter must be a valid base64-encoded string",
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}
		var cp cursorPayload
		if err := json.Unmarshal(decoded, &cp); err != nil {
			WriteProblem(w, ProblemDetails{
				Type:     "https://forge.dev/errors/validation-error",
				Title:    "Invalid Cursor",
				Status:   http.StatusUnprocessableEntity,
				Detail:   "cursor parameter contents are malformed",
				Instance: r.URL.Path,
				Code:     "VALIDATION_FAILED",
			})
			return
		}
		cursor = &domainJob.Cursor{
			CreatedAt: cp.CreatedAt,
			ID:        cp.ID,
		}
	}

	filter := domainJob.Filter{
		State:  stateFilter,
		Type:   typeFilter,
		Limit:  limit,
		Cursor: cursor,
	}

	jobs, nextCursor, hasMore, err := h.service.ListJobs(r.Context(), filter)
	if err != nil {
		WriteProblem(w, ProblemDetails{
			Type:     "https://forge.dev/errors/internal-error",
			Title:    "Internal Server Error",
			Status:   http.StatusInternalServerError,
			Detail:   "An unexpected error occurred while querying jobs.",
			Instance: r.URL.Path,
			Code:     "INTERNAL_SERVER_ERROR",
		})
		return
	}

	items := make([]ListJobsItem, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, ListJobsItem{
			ID:               j.ID,
			Type:             j.Type,
			State:            string(j.State),
			Priority:         j.Priority,
			RetriesAttempted: j.RetriesAttempted,
			CreatedAt:        j.CreatedAt.Format(time.RFC3339),
		})
	}

	var nextCursorStr string
	if hasMore && nextCursor != nil {
		cp := cursorPayload{
			CreatedAt: nextCursor.CreatedAt,
			ID:        nextCursor.ID,
		}
		marshaled, _ := json.Marshal(cp)
		nextCursorStr = base64.URLEncoding.EncodeToString(marshaled)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(ListJobsResponse{
		Items: items,
		Pagination: PaginationMeta{
			Limit:      limit,
			NextCursor: nextCursorStr,
			HasMore:    hasMore,
		},
	})
}
