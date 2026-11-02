package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpAdapter "github.com/YKalisjati/forge/internal/adapter/http"
	"github.com/YKalisjati/forge/internal/adapter/postgres"
	"github.com/YKalisjati/forge/internal/adapter/redis"
	appJob "github.com/YKalisjati/forge/internal/app/job"
	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/health"
	"github.com/YKalisjati/forge/pkg/logger"
	"github.com/YKalisjati/forge/pkg/metrics"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(1)
	}

	log := logger.New("forge-api", &cfg.App)
	slog.SetDefault(log)

	log.Info("initializing forge-api service", slog.Any("config", cfg))

	// Health check registry
	healthReg := health.NewRegistry("forge-api")

	// Startup context with timeout
	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInit()

	var jobHandler *httpAdapter.JobHandler

	// Initialize PostgreSQL client
	pgClient, err := postgres.NewClient(initCtx, &cfg.Postgres)
	if err != nil {
		log.Warn("postgresql initial connection failed, will retry via readiness probes", slog.String("error", err.Error()))
	} else {
		log.Info("connected to postgresql successfully")
		defer pgClient.Close()

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
		jobHandler = httpAdapter.NewJobHandler(jobService, cfg.API.MaxPayloadBytes)
	}

	healthReg.Register("postgres", func(ctx context.Context) (float64, error) {
		if pgClient == nil {
			return 0, errors.New("postgres connection not established")
		}
		return pgClient.Ping(ctx)
	})

	// Initialize Redis client and Rate Limiter
	var rateLimiter redis.RateLimiter
	redisClient, err := redis.NewClient(initCtx, &cfg.Redis)
	if err != nil {
		log.Warn("redis initial connection failed, rate limiting degraded (failing open)", slog.String("error", err.Error()))
	} else {
		log.Info("connected to redis successfully")
		defer func() {
			_ = redisClient.Close()
		}()
		rateLimiter = redis.NewRateLimiter(redisClient.Client(), redis.LimiterConfig{
			Limit:    cfg.Redis.RateLimitRPS,
			Window:   1 * time.Second,
			Timeout:  200 * time.Millisecond,
			FailOpen: true,
		})
	}
	healthReg.Register("redis", func(ctx context.Context) (float64, error) {
		if redisClient == nil {
			return 0, errors.New("redis connection not established")
		}
		return redisClient.Ping(ctx)
	})

	// Setup HTTP router with metrics, logging, and rate limiting
	router := httpAdapter.NewRouter(httpAdapter.ServerOptions{
		Registry:    healthReg,
		Logger:      log,
		JobHandler:  jobHandler,
		RateLimiter: rateLimiter,
		Metrics:     metrics.Default(),
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.API.Port),
		Handler:      router,
		ReadTimeout:  cfg.API.ReadTimeout,
		WriteTimeout: cfg.API.WriteTimeout,
	}

	// Channel to signal server error
	serverErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", slog.Int("port", cfg.API.Port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Graceful shutdown on SIGINT / SIGTERM
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		log.Error("server listener error", slog.String("error", err.Error()))
		os.Exit(1)
	case sig := <-shutdown:
		log.Info("shutdown signal received, initiating graceful termination", slog.String("signal", sig.String()))

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.API.ShutdownTimeout)
		defer cancelShutdown()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful server shutdown failed", slog.String("error", err.Error()))
			_ = server.Close()
		} else {
			log.Info("http server shut down cleanly")
		}
	}

	log.Info("forge-api process terminated cleanly")
}
