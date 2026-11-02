package reaper

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/YKalisjati/forge/internal/domain/job"
)

// Config defines execution parameters for the Stale Job Reaper.
type Config struct {
	Interval  time.Duration
	BatchSize int
}

// DefaultConfig provides default settings for the reaper.
func DefaultConfig() Config {
	return Config{
		Interval:  30 * time.Second,
		BatchSize: 50,
	}
}

// Metrics captures operational counters for the reaper.
type Metrics interface {
	IncStaleJobsRecovered()
	IncDeadLettered()
	IncRetriesScheduled()
}

// Reaper scans for expired worker leases and un-enqueued pending retries.
type Reaper struct {
	repo    job.Repository
	metrics Metrics
	logger  *slog.Logger
	cfg     Config
}

// NewReaper initializes a new Reaper daemon.
func NewReaper(repo job.Repository, metrics Metrics, logger *slog.Logger, cfg Config) *Reaper {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Reaper{
		repo:    repo,
		metrics: metrics,
		logger:  logger,
		cfg:     cfg,
	}
}

// RunOnce executes a single pass of stale job recovery and pending retry scheduling.
// Returns (staleRecovered, staleDeadLettered, retriesScheduled, error).
func (r *Reaper) RunOnce(ctx context.Context) (int, int, int, error) {
	recovered, dl, err := r.repo.RecoverStaleJobs(ctx, r.cfg.BatchSize)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("reaper failed to recover stale jobs: %w", err)
	}

	if recovered > 0 {
		r.logger.Info("recovered stale jobs with expired leases",
			slog.Int("count", recovered),
		)
		if r.metrics != nil {
			for i := 0; i < recovered; i++ {
				r.metrics.IncStaleJobsRecovered()
			}
		}
	}

	if dl > 0 {
		r.logger.Warn("dead-lettered stale jobs with exhausted retries",
			slog.Int("count", dl),
		)
		if r.metrics != nil {
			for i := 0; i < dl; i++ {
				r.metrics.IncDeadLettered()
			}
		}
	}

	scheduled, err := r.repo.SchedulePendingRetries(ctx, r.cfg.BatchSize)
	if err != nil {
		return recovered, dl, 0, fmt.Errorf("reaper failed to schedule pending retries: %w", err)
	}

	if scheduled > 0 {
		r.logger.Info("scheduled pending retries into outbox",
			slog.Int("count", scheduled),
		)
		if r.metrics != nil {
			for i := 0; i < scheduled; i++ {
				r.metrics.IncRetriesScheduled()
			}
		}
	}

	return recovered, dl, scheduled, nil
}

// Run executes the continuous background reaper loop until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	r.logger.Info("starting stale job reaper daemon",
		slog.Duration("interval", r.cfg.Interval),
		slog.Int("batch_size", r.cfg.BatchSize),
	)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("stale job reaper shutting down")
			return nil
		case <-ticker.C:
			if _, _, _, err := r.RunOnce(ctx); err != nil {
				r.logger.Error("reaper iteration failed", slog.String("error", err.Error()))
			}
		}
	}
}
