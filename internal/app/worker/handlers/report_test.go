package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/YKalisjati/forge/internal/domain/job"
)

func TestReportHandler_DeterministicAggregation(t *testing.T) {
	handler := NewReportHandler(nil)

	payload := ReportPayload{
		ReportID:   "rep-001",
		Title:      "Q3 Performance",
		DataPoints: []float64{10.0, 20.0, 30.0, 40.0},
		Author:     "tester",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	j := &job.Job{
		ID:      uuid.New(),
		Type:    "report.generate",
		Payload: payloadBytes,
	}

	err = handler.Execute(context.Background(), j)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestReportHandler_EmptyDataPoints(t *testing.T) {
	handler := NewReportHandler(nil)

	j := &job.Job{
		ID:      uuid.New(),
		Type:    "report.generate",
		Payload: []byte(`{"title":"Empty Data"}`),
	}

	err := handler.Execute(context.Background(), j)
	if err != nil {
		t.Fatalf("expected nil error on empty data points, got: %v", err)
	}
}

func TestReportHandler_InvalidJSON(t *testing.T) {
	handler := NewReportHandler(nil)

	j := &job.Job{
		ID:      uuid.New(),
		Type:    "report.generate",
		Payload: []byte(`not-json`),
	}

	err := handler.Execute(context.Background(), j)
	if err == nil {
		t.Fatalf("expected error on invalid JSON, got nil")
	}
}
