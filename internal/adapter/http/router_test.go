package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/YKalisjati/forge/internal/adapter/redis"
	"github.com/YKalisjati/forge/pkg/health"
	"github.com/YKalisjati/forge/pkg/metrics"
)

func TestHealthzEndpoint(t *testing.T) {
	reg := health.NewRegistry("forge-api")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(ServerOptions{Registry: reg, Logger: logger})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("expected X-Request-ID header to be set")
	}

	var res health.LivenessResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.Status != "pass" || res.Service != "forge-api" {
		t.Errorf("unexpected liveness response: %+v", res)
	}
}

func TestReadyzEndpointPassAndFail(t *testing.T) {
	reg := health.NewRegistry("forge-api")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var pgErr error
	reg.Register("postgres", func(_ context.Context) (float64, error) {
		return 1.2, pgErr
	})

	router := NewRouter(ServerOptions{Registry: reg, Logger: logger})

	// 1. Healthy pass
	{
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}

		var res health.ReadinessResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if res.Status != "pass" {
			t.Errorf("expected pass, got %s", res.Status)
		}
	}

	// 2. Degraded failure
	{
		pgErr = errors.New("connection failed")
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d", rec.Code)
		}

		var res health.ReadinessResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if res.Status != "fail" {
			t.Errorf("expected fail, got %s", res.Status)
		}
	}
}

func TestWriteProblemDetails(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteProblem(rec, ProblemDetails{
		Type:   "https://forge.dev/errors/validation-error",
		Title:  "Invalid Request Parameters",
		Status: http.StatusUnprocessableEntity,
		Detail: "Field priority failed validation",
		Code:   "VALIDATION_FAILED",
		InvalidParams: []InvalidParam{
			{Name: "priority", Reason: "must be an integer between 0 and 100"},
		},
	})

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/problem+json" {
		t.Errorf("expected application/problem+json, got %s", rec.Header().Get("Content-Type"))
	}

	var p ProblemDetails
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("failed to decode problem details: %v", err)
	}
	if p.Code != "VALIDATION_FAILED" || len(p.InvalidParams) != 1 {
		t.Errorf("unexpected problem details: %+v", p)
	}
}

type mockRateLimiter struct {
	allowed   bool
	remaining int
	err       error
}

func (m *mockRateLimiter) Allow(_ context.Context, _ string) (redis.Result, error) {
	return redis.Result{
		Allowed:   m.allowed,
		Limit:     10,
		Remaining: m.remaining,
		ResetAt:   time.Now().Add(1 * time.Second),
	}, m.err
}

func TestMetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	router := NewRouter(ServerOptions{Metrics: m})

	// Make an HTTP request to record metric
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	// Fetch /metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	router.ServeHTTP(recMetrics, reqMetrics)

	if recMetrics.Code != http.StatusOK {
		t.Fatalf("expected /metrics to return 200, got %d", recMetrics.Code)
	}

	body := recMetrics.Body.String()
	if !strings.Contains(body, "forge_api_requests_total") {
		t.Errorf("expected /metrics to contain forge_api_requests_total, got: %s", body)
	}
}

func TestCorrelationMiddleware(t *testing.T) {
	router := NewRouter(ServerOptions{})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "custom-req-123")
	req.Header.Set("X-Correlation-ID", "custom-corr-456")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Header().Get("X-Request-ID") != "custom-req-123" {
		t.Errorf("expected custom-req-123, got %s", rec.Header().Get("X-Request-ID"))
	}
	if rec.Header().Get("X-Correlation-ID") != "custom-corr-456" {
		t.Errorf("expected custom-corr-456, got %s", rec.Header().Get("X-Correlation-ID"))
	}
}

func TestRateLimitMiddleware_Exceeded(t *testing.T) {
	limiter := &mockRateLimiter{allowed: false, remaining: 0}
	m := metrics.New(prometheus.NewRegistry())

	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := RateLimitMiddleware(limiter, m, nil)(dummyHandler)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec.Code)
	}
	if rec.Header().Get("X-RateLimit-Limit") != "10" {
		t.Errorf("expected limit header 10, got %s", rec.Header().Get("X-RateLimit-Limit"))
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Errorf("expected Retry-After header to be set")
	}
}

func TestRateLimitMiddleware_FailOpen(t *testing.T) {
	limiter := &mockRateLimiter{allowed: true, remaining: 10, err: errors.New("redis timeout")}
	m := metrics.New(prometheus.NewRegistry())

	reachedHandler := false
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reachedHandler = true
		w.WriteHeader(http.StatusOK)
	})

	handler := RateLimitMiddleware(limiter, m, nil)(dummyHandler)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 under fail-open, got %d", rec.Code)
	}
	if !reachedHandler {
		t.Errorf("expected request to reach handler under fail-open policy")
	}
}
