package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"

	httpAdapter "github.com/YKalisjati/forge/internal/adapter/http"
	"github.com/YKalisjati/forge/internal/adapter/postgres"
	redisAdapter "github.com/YKalisjati/forge/internal/adapter/redis"
	appJob "github.com/YKalisjati/forge/internal/app/job"
	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/health"
	pkgMetrics "github.com/YKalisjati/forge/pkg/metrics"
)

func setupPhase4ATestServer(t *testing.T, customReg *prometheus.Registry, customLimiter redisAdapter.RateLimiter) (http.Handler, *pkgMetrics.Metrics, *health.Registry, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	pgClient, err := postgres.NewClient(context.Background(), &cfg.Postgres)
	if err != nil {
		t.Skipf("skipping Phase 4A integration test (database unreachable): %v", err)
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

	// Register healthy postgres check
	healthReg.Register("postgres", func(ctx context.Context) (float64, error) {
		return pgClient.Ping(ctx)
	})

	var prom *pkgMetrics.Metrics
	if customReg != nil {
		prom = pkgMetrics.New(customReg)
	} else {
		reg := prometheus.NewRegistry()
		prom = pkgMetrics.New(reg)
	}

	router := httpAdapter.NewRouter(httpAdapter.ServerOptions{
		Registry:    healthReg,
		Logger:      nil,
		JobHandler:  jobHandler,
		RateLimiter: customLimiter,
		Metrics:     prom,
	})

	cleanup := func() {
		pgClient.Close()
	}

	return router, prom, healthReg, cleanup
}

// TestPhase4A_MetricsEndpoint verifies Prometheus scraping and metric increments on requests.
func TestPhase4A_MetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	router, _, _, cleanup := setupPhase4ATestServer(t, reg, nil)
	defer cleanup()

	// 1. Submit a valid job -> 202 Accepted
	key := fmt.Sprintf("metric-test-%s", uuid.NewString())
	submitBody := `{"type": "email.transactional", "payload": {"recipient": "metric@forge.dev"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(submitBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Submit an invalid job (bad JSON) -> 400 Bad Request
	badReq := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(`{invalid json`))
	badReq.Header.Set("Content-Type", "application/json")
	badReq.Header.Set("Idempotency-Key", uuid.NewString())
	badRec := httptest.NewRecorder()
	router.ServeHTTP(badRec, badReq)

	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", badRec.Code)
	}

	// 3. Query /metrics endpoint
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	router.ServeHTTP(metricsRec, metricsReq)

	if metricsRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /metrics, got %d", metricsRec.Code)
	}

	metricsOutput := metricsRec.Body.String()

	// Verify standard Forge metrics are present
	expectedMetrics := []string{
		"forge_api_requests_total",
		"forge_api_request_duration_seconds",
		"forge_api_active_requests",
		`forge_api_requests_total{endpoint="/v1/jobs",method="POST",status_code="202"}`,
		`forge_api_requests_total{endpoint="/v1/jobs",method="POST",status_code="400"}`,
	}

	for _, m := range expectedMetrics {
		if !strings.Contains(metricsOutput, m) {
			t.Errorf("expected /metrics to contain %q, but was missing:\n%s", m, metricsOutput)
		}
	}
}

// TestPhase4A_CorrelationIDPropagation verifies X-Request-ID and X-Correlation-ID headers.
func TestPhase4A_CorrelationIDPropagation(t *testing.T) {
	router, _, _, cleanup := setupPhase4ATestServer(t, nil, nil)
	defer cleanup()

	customReqID := "req-" + uuid.NewString()
	customCorrID := "corr-" + uuid.NewString()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", customReqID)
	req.Header.Set("X-Correlation-ID", customCorrID)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") != customReqID {
		t.Errorf("expected X-Request-ID %q, got %q", customReqID, rec.Header().Get("X-Request-ID"))
	}
	if rec.Header().Get("X-Correlation-ID") != customCorrID {
		t.Errorf("expected X-Correlation-ID %q, got %q", customCorrID, rec.Header().Get("X-Correlation-ID"))
	}

	// When headers are omitted, middleware should generate them
	req2 := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)

	if rec2.Header().Get("X-Request-ID") == "" {
		t.Error("expected generated X-Request-ID to be non-empty")
	}
	if rec2.Header().Get("X-Correlation-ID") == "" {
		t.Error("expected generated X-Correlation-ID to be non-empty")
	}
}

// TestPhase4A_ConcurrentRateLimiting verifies Redis sliding window rate limiter under concurrency.
func TestPhase4A_ConcurrentRateLimiting(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	rdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Redis.Addr(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("skipping Redis rate limiting test (Redis unreachable): %v", err)
	}
	defer rdb.Close()

	// Clean up any test keys in redis
	_ = rdb.FlushDB(ctx).Err()

	// Limiter: 5 RPS
	limitRPS := 5
	limiter := redisAdapter.NewRateLimiter(rdb, redisAdapter.LimiterConfig{
		Limit:    limitRPS,
		Window:   1 * time.Second,
		Timeout:  200 * time.Millisecond,
		FailOpen: true,
	})

	reg := prometheus.NewRegistry()
	router, _, _, cleanup := setupPhase4ATestServer(t, reg, limiter)
	defer cleanup()

	totalRequests := 15
	var wg sync.WaitGroup
	statusCodes := make([]int, totalRequests)
	retryAfterHeaders := make([]string, totalRequests)

	// Fire 15 concurrent requests from the same client IP
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-limit-%d-%s", idx, uuid.NewString())
			body := `{"type": "email.transactional", "payload": {"index": ` + fmt.Sprintf("%d", idx) + `}}`
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", key)
			req.RemoteAddr = "192.168.1.100:12345"

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			statusCodes[idx] = rec.Code
			retryAfterHeaders[idx] = rec.Header().Get("Retry-After")
		}(i)
	}

	wg.Wait()

	acceptedCount := 0
	rateLimitedCount := 0

	for i, code := range statusCodes {
		switch code {
		case http.StatusAccepted:
			acceptedCount++
		case http.StatusTooManyRequests:
			rateLimitedCount++
			if retryAfterHeaders[i] == "" {
				t.Errorf("request %d: expected Retry-After header on 429 response", i)
			}
		default:
			t.Errorf("request %d: unexpected status code %d", i, code)
		}
	}

	if acceptedCount != limitRPS {
		t.Errorf("expected exactly %d accepted requests, got %d (rate limited: %d)", limitRPS, acceptedCount, rateLimitedCount)
	}
	if rateLimitedCount != (totalRequests - limitRPS) {
		t.Errorf("expected exactly %d rate-limited requests, got %d", totalRequests-limitRPS, rateLimitedCount)
	}

	// Verify Prometheus rate limit counter was incremented
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	router.ServeHTTP(metricsRec, metricsReq)

	if !strings.Contains(metricsRec.Body.String(), "forge_api_rate_limit_exceeded_total") {
		t.Errorf("expected forge_api_rate_limit_exceeded_total in /metrics")
	}
}

// TestPhase4A_RateLimiterFailOpen verifies fail-open behavior when Redis is unavailable.
func TestPhase4A_RateLimiterFailOpen(t *testing.T) {
	// Connect to non-existent Redis port to simulate complete outage
	badRdb := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:59999",
		DialTimeout: 50 * time.Millisecond,
	})
	defer badRdb.Close()

	limiter := redisAdapter.NewRateLimiter(badRdb, redisAdapter.LimiterConfig{
		Limit:    10,
		Window:   1 * time.Second,
		Timeout:  50 * time.Millisecond,
		FailOpen: true,
	})

	reg := prometheus.NewRegistry()
	router, _, _, cleanup := setupPhase4ATestServer(t, reg, limiter)
	defer cleanup()

	// When Redis is dead, request must STILL SUCCEED (fail-open ensures persistence correctness)
	key := fmt.Sprintf("fail-open-%s", uuid.NewString())
	body := `{"type": "email.transactional", "payload": {"recipient": "failopen@forge.dev"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("fail-open policy failed: expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify RateLimitErrorsTotal counter was incremented
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	router.ServeHTTP(metricsRec, metricsReq)

	if !strings.Contains(metricsRec.Body.String(), "forge_api_rate_limit_errors_total") {
		t.Errorf("expected forge_api_rate_limit_errors_total in /metrics when Redis fails open")
	}
}

// TestPhase4A_ReadinessAndLivenessSemantics verifies the distinction between /healthz and /readyz.
func TestPhase4A_ReadinessAndLivenessSemantics(t *testing.T) {
	router, _, healthReg, cleanup := setupPhase4ATestServer(t, nil, nil)
	defer cleanup()

	// 1. Initial state: everything healthy
	// Liveness: 200 OK
	liveReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	liveRec := httptest.NewRecorder()
	router.ServeHTTP(liveRec, liveReq)
	if liveRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz, got %d", liveRec.Code)
	}

	// Readiness: 200 OK
	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRec := httptest.NewRecorder()
	router.ServeHTTP(readyRec, readyReq)
	if readyRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /readyz, got %d", readyRec.Code)
	}

	// 2. Introduce an unhealthy dependency check
	healthReg.Register("failing_service", func(_ context.Context) (float64, error) {
		return 0, errors.New("connection reset by peer")
	})

	// Liveness must REMAIN 200 OK (process is still alive and running)
	liveRec2 := httptest.NewRecorder()
	router.ServeHTTP(liveRec2, liveReq)
	if liveRec2.Code != http.StatusOK {
		t.Errorf("liveness MUST remain 200 even when downstream checks fail, got %d", liveRec2.Code)
	}

	// Readiness MUST now be 503 Service Unavailable
	readyRec2 := httptest.NewRecorder()
	router.ServeHTTP(readyRec2, readyReq)
	if readyRec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness MUST return 503 when a check fails, got %d: %s", readyRec2.Code, readyRec2.Body.String())
	}

	// Verify response body contains check details
	var body map[string]interface{}
	if err := json.Unmarshal(readyRec2.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse readyz JSON: %v", err)
	}
	if body["status"] != health.StatusFail {
		t.Errorf("expected status %q, got %v", health.StatusFail, body["status"])
	}
	checks, ok := body["checks"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected checks map in readyz response")
	}
	failingCheck, ok := checks["failing_service"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected failing_service in checks map")
	}
	if failingCheck["status"] != health.StatusFail {
		t.Errorf("expected failing_service status to be %q, got %v", health.StatusFail, failingCheck["status"])
	}
}
