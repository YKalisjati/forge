package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	httpAdapter "github.com/YKalisjati/forge/internal/adapter/http"
	"github.com/YKalisjati/forge/internal/adapter/postgres"
	appJob "github.com/YKalisjati/forge/internal/app/job"
	domainJob "github.com/YKalisjati/forge/internal/domain/job"
	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/health"
)

func setupTestServer(t *testing.T) (http.Handler, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	pgClient, err := postgres.NewClient(context.Background(), &cfg.Postgres)
	if err != nil {
		t.Skipf("skipping API integration test (database unreachable): %v", err)
	}

	uow := postgres.NewUnitOfWork(pgClient.Pool())
	jobRepo := postgres.NewJobRepository(pgClient.Pool())
	outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())
	idempotencyRepo := postgres.NewIdempotencyRepository(pgClient.Pool())

	jobService := appJob.NewService(
		uow,
		jobRepo,
		outboxRepo,
		idempotencyRepo,
		cfg.API.MaxPayloadBytes,
	)

	jobHandler := httpAdapter.NewJobHandler(jobService, cfg.API.MaxPayloadBytes)
	healthReg := health.NewRegistry("forge-api-test")

	router := httpAdapter.NewRouter(httpAdapter.ServerOptions{
		Registry:   healthReg,
		Logger:     nil,
		JobHandler: jobHandler,
	})

	cleanup := func() {
		pgClient.Close()
	}

	return router, cleanup
}

func TestAPIIntegration_Lifecycle(t *testing.T) {
	router, cleanup := setupTestServer(t)
	defer cleanup()

	key := fmt.Sprintf("api-integ-%s", uuid.NewString())

	// 1. Submit valid job
	submitBody := `{"type": "email.transactional", "payload": {"recipient": "user@forge.dev"}, "priority": 15}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(submitBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Idempotent-Replayed") == "true" {
		t.Errorf("fresh submission should NOT have Idempotent-Replayed: true")
	}

	var submitRes httpAdapter.SubmitJobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &submitRes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if submitRes.ID == uuid.Nil || submitRes.State != "pending" || submitRes.Priority != 15 {
		t.Fatalf("unexpected submit response fields: %+v", submitRes)
	}

	// 2. Replay with identical payload & same key
	replayReq := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(submitBody))
	replayReq.Header.Set("Content-Type", "application/json")
	replayReq.Header.Set("Idempotency-Key", key)
	replayRec := httptest.NewRecorder()

	router.ServeHTTP(replayRec, replayReq)

	if replayRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on replay, got %d", replayRec.Code)
	}
	if replayRec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("replayed request MUST have Idempotent-Replayed: true")
	}

	var replayRes httpAdapter.SubmitJobResponse
	_ = json.Unmarshal(replayRec.Body.Bytes(), &replayRes)
	if replayRes.ID != submitRes.ID {
		t.Errorf("replayed job ID mismatch: %s vs %s", replayRes.ID, submitRes.ID)
	}

	// 3. Same key with different payload -> 409 Conflict
	conflictBody := `{"type": "email.transactional", "payload": {"recipient": "different@forge.dev"}}`
	conflictReq := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(conflictBody))
	conflictReq.Header.Set("Content-Type", "application/json")
	conflictReq.Header.Set("Idempotency-Key", key)
	conflictRec := httptest.NewRecorder()

	router.ServeHTTP(conflictRec, conflictReq)

	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for mismatched payload, got %d: %s", conflictRec.Code, conflictRec.Body.String())
	}

	// 4. Retrieve job details: GET /v1/jobs/{id}
	getReq := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+submitRes.ID.String(), nil)
	getRec := httptest.NewRecorder()

	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /v1/jobs/{id}, got %d", getRec.Code)
	}
	var getRes domainJob.WithExecutions
	if err := json.Unmarshal(getRec.Body.Bytes(), &getRes); err != nil {
		t.Fatalf("failed to parse get response: %v", err)
	}
	if getRes.ID != submitRes.ID || getRes.Priority != 15 {
		t.Fatalf("get response mismatch: %+v", getRes)
	}

	// 5. Query job list: GET /v1/jobs?type=email.transactional
	listReq := httptest.NewRequest(http.MethodGet, "/v1/jobs?type=email.transactional&limit=5", nil)
	listRec := httptest.NewRecorder()

	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /v1/jobs, got %d", listRec.Code)
	}
	var listRes httpAdapter.ListJobsResponse
	if err := json.Unmarshal(listRec.Body.Bytes(), &listRes); err != nil {
		t.Fatalf("failed to parse list response: %v", err)
	}
	if len(listRes.Items) == 0 {
		t.Fatal("expected at least 1 job in list response")
	}
}
