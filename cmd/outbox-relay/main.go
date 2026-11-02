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
	appOutbox "github.com/YKalisjati/forge/internal/app/outbox"
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

	log := logger.New("forge-outbox-relay", &cfg.App)
	slog.SetDefault(log)

	log.Info("initializing forge-outbox-relay service", slog.Any("config", cfg))

	// Health check registry
	healthReg := health.NewRegistry("forge-outbox-relay")

	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInit()

	// Initialize PostgreSQL client
	pgClient, err := postgres.NewClient(initCtx, &cfg.Postgres)
	if err != nil {
		log.Warn("postgresql initial connection failed, relay will retry during runtime", slog.String("error", err.Error()))
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
		log.Warn("rabbitmq initial connection failed, relay will retry during runtime", slog.String("error", err.Error()))
	} else {
		log.Info("connected to rabbitmq successfully")
		defer func() {
			_ = rmqClient.Close()
		}()

		// Declare durable topology
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

	// Wire outbox components if database and rabbitmq are available
	if pgClient != nil && rmqClient != nil {
		outboxRepo := postgres.NewOutboxRepository(pgClient.Pool())
		publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
		defer func() {
			_ = publisher.Close()
		}()

		metrics := appOutbox.NewMetrics()
		relayCfg := appOutbox.Config{
			PollInterval:       cfg.Relay.PollInterval,
			BatchSize:          cfg.Relay.BatchSize,
			MaxRetries:         cfg.Relay.MaxRetries,
			ClaimLeaseDuration: 30 * time.Second,
			PublishTimeout:     5 * time.Second,
		}

		relay := appOutbox.NewRelay(outboxRepo, publisher, metrics, log, relayCfg)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := relay.Run(ctx); runErr != nil {
				log.Error("outbox relay stopped with error", slog.String("error", runErr.Error()))
			}
		}()

		log.Info("forge-outbox-relay initialized and running",
			slog.Duration("poll_interval", cfg.Relay.PollInterval),
			slog.Int("batch_size", cfg.Relay.BatchSize),
			slog.Int("max_retries", cfg.Relay.MaxRetries),
		)

		// Wait for termination signal
		<-ctx.Done()

		log.Info("shutdown signal received, awaiting in-flight relay execution")
		wg.Wait()
	} else {
		log.Warn("relay running in degraded mode awaiting dependency initialization")
		<-ctx.Done()
	}

	log.Info("forge-outbox-relay process terminated cleanly")
}
