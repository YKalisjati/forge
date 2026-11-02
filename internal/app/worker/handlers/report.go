package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/YKalisjati/forge/internal/domain/job"
)

// ReportPayload represents input payload for report.generate jobs.
type ReportPayload struct {
	ReportID   string    `json:"report_id,omitempty"`
	Title      string    `json:"title,omitempty"`
	DataPoints []float64 `json:"data_points,omitempty"`
	Author     string    `json:"author,omitempty"`
}

// ReportSummary stores deterministic aggregation results.
type ReportSummary struct {
	ReportID    string    `json:"report_id"`
	Title       string    `json:"title"`
	Count       int       `json:"count"`
	Sum         float64   `json:"sum"`
	Average     float64   `json:"average"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	ProcessedAt time.Time `json:"processed_at"`
}

// ReportHandler implements a deterministic demo handler for report.generate.
type ReportHandler struct {
	logger *slog.Logger
}

// NewReportHandler initializes a new ReportHandler.
func NewReportHandler(logger *slog.Logger) *ReportHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &ReportHandler{logger: logger}
}

// Execute processes the report generation job deterministically.
func (h *ReportHandler) Execute(_ context.Context, j *job.Job) error {
	var input ReportPayload
	if err := json.Unmarshal(j.Payload, &input); err != nil {
		return fmt.Errorf("failed to unmarshal report payload: %w", err)
	}

	if input.Title == "" {
		input.Title = "Automated Generated Report"
	}
	if input.ReportID == "" {
		input.ReportID = fmt.Sprintf("rep-%s", j.ID.String()[:8])
	}

	count := len(input.DataPoints)
	sum := 0.0
	var minVal, maxVal float64

	if count > 0 {
		minVal = input.DataPoints[0]
		maxVal = input.DataPoints[0]
		for _, v := range input.DataPoints {
			sum += v
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
	}

	avg := 0.0
	if count > 0 {
		avg = sum / float64(count)
	}

	summary := ReportSummary{
		ReportID:    input.ReportID,
		Title:       input.Title,
		Count:       count,
		Sum:         sum,
		Average:     avg,
		Min:         minVal,
		Max:         maxVal,
		ProcessedAt: time.Now().UTC(),
	}

	h.logger.Info("report summary generated successfully",
		slog.String("job_id", j.ID.String()),
		slog.String("report_id", summary.ReportID),
		slog.String("title", summary.Title),
		slog.Int("records_count", summary.Count),
		slog.Float64("sum", summary.Sum),
		slog.Float64("average", summary.Average),
		slog.Float64("min", summary.Min),
		slog.Float64("max", summary.Max),
	)

	return nil
}
