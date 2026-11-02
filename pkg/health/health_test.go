package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLiveness(t *testing.T) {
	reg := NewRegistry("forge-api")
	res := reg.Liveness()

	if res.Status != StatusPass {
		t.Errorf("expected status pass, got %s", res.Status)
	}
	if res.Service != "forge-api" {
		t.Errorf("expected service forge-api, got %s", res.Service)
	}
	if _, err := time.Parse(time.RFC3339, res.Timestamp); err != nil {
		t.Errorf("expected valid RFC3339 timestamp, got %s", res.Timestamp)
	}
}

func TestReadinessHealthy(t *testing.T) {
	reg := NewRegistry("forge-api")
	reg.Register("postgres", func(_ context.Context) (float64, error) {
		return 1.5, nil
	})
	reg.Register("redis", func(_ context.Context) (float64, error) {
		return 0.8, nil
	})

	res := reg.Readiness(context.Background())
	if res.Status != StatusPass {
		t.Errorf("expected overall status pass, got %s", res.Status)
	}
	if len(res.Checks) != 2 {
		t.Fatalf("expected 2 checks, got %d", len(res.Checks))
	}
	if res.Checks["postgres"].Status != StatusPass || res.Checks["postgres"].LatencyMS == nil {
		t.Errorf("expected postgres pass with latency, got %+v", res.Checks["postgres"])
	}
	if res.Checks["redis"].Status != StatusPass || res.Checks["redis"].LatencyMS == nil {
		t.Errorf("expected redis pass with latency, got %+v", res.Checks["redis"])
	}
}

func TestReadinessDegraded(t *testing.T) {
	reg := NewRegistry("forge-api")
	reg.Register("postgres", func(_ context.Context) (float64, error) {
		return 0, errors.New("connection refused")
	})
	reg.Register("redis", func(_ context.Context) (float64, error) {
		return 0.5, nil
	})

	res := reg.Readiness(context.Background())
	if res.Status != StatusFail {
		t.Errorf("expected overall status fail when dependency is down, got %s", res.Status)
	}
	if res.Checks["postgres"].Status != StatusFail || res.Checks["postgres"].Error != "connection refused" {
		t.Errorf("expected postgres failure detail, got %+v", res.Checks["postgres"])
	}
	if res.Checks["redis"].Status != StatusPass {
		t.Errorf("expected redis pass, got %+v", res.Checks["redis"])
	}
}
