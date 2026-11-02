package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/YKalisjati/forge/internal/adapter/postgres"
	"github.com/YKalisjati/forge/internal/adapter/rabbitmq"
	"github.com/YKalisjati/forge/internal/app/reaper"
	appWorker "github.com/YKalisjati/forge/internal/app/worker"
	"github.com/YKalisjati/forge/internal/app/worker/handlers"
	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/health"
	"github.com/YKalisjati/forge/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	log := logger.New("forge-worker", &cfg.App)
	slog.SetDefault(log)

	log.Info("initializing forge-worker service", slog.Any("config", cfg))

	// Health check registry
	healthReg := health.NewRegistry("forge-worker")

	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInit()

	// Initialize PostgreSQL client
	pgClient, err := postgres.NewClient(initCtx, &cfg.Postgres)
	if err != nil {
		log.Warn("postgresql initial connection failed, worker will retry during runtime", slog.String("error", err.Error()))
	} else {
		log.Info("connected to postgresql successfully")
		defer pgClient.Close()
	}
	healthReg.Register("postgres", func(ctx context.Context) (float64, error) {
		if pgClient == nil {
			return 0, errors.New("postgres connection not established")
		}
		return pgClient.Ping(ctx)
	})

	// Initialize RabbitMQ client
	rmqClient, err := rabbitmq.NewClient(initCtx, &cfg.RabbitMQ)
	if err != nil {
		log.Warn("rabbitmq initial connection failed, worker will retry during runtime", slog.String("error", err.Error()))
	} else {
		log.Info("connected to rabbitmq successfully")
		defer func() {
			_ = rmqClient.Close()
		}()

		// Ensure durable topology is declared
		if topErr := rabbitmq.DeclareTopology(initCtx, rmqClient, &cfg.RabbitMQ); topErr != nil {
			log.Warn("failed to declare initial rabbitmq topology, will retry during runtime", slog.String("error", topErr.Error()))
		} else {
			log.Info("rabbitmq durable topology verified and declared")
		}
	}
	healthReg.Register("rabbitmq", func(ctx context.Context) (float64, error) {
		if rmqClient == nil {
			return 0, errors.New("rabbitmq connection not established")
		}
		return rmqClient.Ping(ctx)
	})

	// Run initial readiness evaluation
	readiness := healthReg.Readiness(context.Background())
	log.Info("initial readiness status", slog.String("status", readiness.Status), slog.Any("checks", readiness.Checks))

	// Signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// If dependencies are available, start worker execution engine
	if pgClient != nil && rmqClient != nil {
		jobRepo := postgres.NewJobRepository(pgClient.Pool())
		consumer := rabbitmq.NewConsumer(rmqClient)
		defer func() {
			_ = consumer.Close()
		}()

		registry := appWorker.NewRegistry()
		if regErr := registry.Register("report.generate", handlers.NewReportHandler(log)); regErr != nil {
			log.Error("failed to register report.generate handler", slog.String("error", regErr.Error()))
		}
		if regErr := registry.Register("test.job", handlers.NewTestJobHandler()); regErr != nil {
			log.Error("failed to register test.job handler", slog.String("error", regErr.Error()))
		}

		metrics := appWorker.NewMetrics()
		workerCfg := appWorker.Config{
			Queue:             cfg.RabbitMQ.Queue,
			Concurrency:       cfg.Worker.Concurrency,
			PrefetchCount:     cfg.Worker.PrefetchCount,
			LeaseDuration:     cfg.Worker.LeaseDuration,
			HeartbeatInterval: cfg.Worker.HeartbeatInterval,
			ShutdownTimeout:   cfg.Worker.ShutdownTimeout,
		}

		workerEngine := appWorker.NewWorker(jobRepo, consumer, registry, metrics, log, workerCfg)

		reaperEngine := reaper.NewReaper(jobRepo, metrics, log, reaper.Config{
			Interval:  cfg.Reaper.Interval,
			BatchSize: cfg.Reaper.BatchSize,
		})

		healthReg.Register("worker", func(_ context.Context) (float64, error) {
			return 1.0, nil
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := workerEngine.Run(ctx); runErr != nil {
				log.Error("worker engine stopped with error", slog.String("error", runErr.Error()))
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if rErr := reaperEngine.Run(ctx); rErr != nil {
				log.Error("reaper daemon stopped with error", slog.String("error", rErr.Error()))
			}
		}()

		log.Info("forge-worker initialized and running",
			slog.String("worker_id", workerEngine.WorkerID()),
			slog.Int("concurrency", cfg.Worker.Concurrency),
			slog.Int("prefetch_count", cfg.Worker.PrefetchCount),
		)

		// Wait for termination signal
		<-ctx.Done()

		log.Info("shutdown signal received, awaiting in-flight worker executions")
		wg.Wait()
	} else {
		log.Warn("worker running in degraded mode awaiting dependency initialization")
		<-ctx.Done()
	}

	log.Info("forge-worker process terminated cleanly")
}
