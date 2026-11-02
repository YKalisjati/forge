package config

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Clear any environment overrides that might affect tests
	os.Clearenv()

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("expected Load() to succeed with defaults, got: %v", err)
	}

	if cfg.App.Env != EnvDevelopment {
		t.Errorf("expected default App.Env to be %s, got %s", EnvDevelopment, cfg.App.Env)
	}
	if cfg.API.Port != 8080 {
		t.Errorf("expected default API.Port to be 8080, got %d", cfg.API.Port)
	}
	if cfg.Postgres.Port != 5432 {
		t.Errorf("expected default Postgres.Port to be 5432, got %d", cfg.Postgres.Port)
	}
	if cfg.RabbitMQ.Port != 5672 {
		t.Errorf("expected default RabbitMQ.Port to be 5672, got %d", cfg.RabbitMQ.Port)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("expected default Redis.Port to be 6379, got %d", cfg.Redis.Port)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		modify    func(c *Config)
		errSubstr string
	}{
		{
			name: "invalid log level",
			modify: func(c *Config) {
				c.App.LogLevel = "verbose"
			},
			errSubstr: "invalid LOG_LEVEL",
		},
		{
			name: "invalid log format",
			modify: func(c *Config) {
				c.App.LogFormat = "xml"
			},
			errSubstr: "invalid LOG_FORMAT",
		},
		{
			name: "invalid API port",
			modify: func(c *Config) {
				c.API.Port = 70000
			},
			errSubstr: "invalid API_PORT",
		},
		{
			name: "zero API read timeout",
			modify: func(c *Config) {
				c.API.ReadTimeout = 0
			},
			errSubstr: "API_READ_TIMEOUT must be positive",
		},
		{
			name: "empty DB host",
			modify: func(c *Config) {
				c.Postgres.Host = ""
			},
			errSubstr: "DB_HOST cannot be empty",
		},
		{
			name: "zero worker concurrency",
			modify: func(c *Config) {
				c.Worker.Concurrency = 0
			},
			errSubstr: "WORKER_CONCURRENCY must be positive",
		},
		{
			name: "zero relay batch size",
			modify: func(c *Config) {
				c.Relay.BatchSize = -1
			},
			errSubstr: "RELAY_BATCH_SIZE must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			cfg, err := Load()
			if err != nil {
				t.Fatalf("unexpected Load() error: %v", err)
			}
			tt.modify(cfg)
			err = cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("expected error containing %q, got %q", tt.errSubstr, err.Error())
			}
		})
	}
}

func TestSecretRedaction(t *testing.T) {
	cfg := &Config{
		Postgres: PostgresConfig{
			Password: "super_secret_pg_password",
		},
		RabbitMQ: RabbitMQConfig{
			Password: "super_secret_rmq_password",
		},
		Redis: RedisConfig{
			Password: "super_secret_redis_password",
		},
	}

	val := cfg.LogValue()
	var foundRedacted int
	for _, attr := range val.Group() {
		for _, child := range attr.Value.Group() {
			if child.Key == "password" {
				if child.Value.Kind() == slog.KindString && child.Value.String() == "[REDACTED]" {
					foundRedacted++
				} else {
					t.Errorf("expected [REDACTED] for key %s in group %s, got %v", child.Key, attr.Key, child.Value)
				}
			}
		}
	}

	if foundRedacted != 3 {
		t.Errorf("expected 3 redacted passwords, found %d", foundRedacted)
	}
}

func TestConnectionStrings(t *testing.T) {
	cfg := &Config{
		Postgres: PostgresConfig{
			Host:     "db.internal",
			Port:     5432,
			User:     "user1",
			Password: "pass@word",
			Name:     "testdb",
			SSLMode:  "require",
		},
		RabbitMQ: RabbitMQConfig{
			Host:     "rmq.internal",
			Port:     5672,
			User:     "guest",
			Password: "secret/pass",
			VHost:    "/custom",
		},
		Redis: RedisConfig{
			Host: "redis.internal",
			Port: 6379,
		},
	}

	expectedDSN := "postgres://user1:pass%40word@db.internal:5432/testdb?sslmode=require"
	if cfg.Postgres.DSN() != expectedDSN {
		t.Errorf("expected DSN %s, got %s", expectedDSN, cfg.Postgres.DSN())
	}

	expectedAMQP := "amqp://guest:secret%2Fpass@rmq.internal:5672/custom"
	if cfg.RabbitMQ.URL() != expectedAMQP {
		t.Errorf("expected AMQP URL %s, got %s", expectedAMQP, cfg.RabbitMQ.URL())
	}

	expectedRedisAddr := "redis.internal:6379"
	if cfg.Redis.Addr() != expectedRedisAddr {
		t.Errorf("expected Redis Addr %s, got %s", expectedRedisAddr, cfg.Redis.Addr())
	}
}

func TestEnvOverride(t *testing.T) {
	os.Clearenv()
	t.Setenv("API_PORT", "9090")
	t.Setenv("APP_ENV", "production")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("WORKER_CONCURRENCY", "42")
	t.Setenv("API_SHUTDOWN_TIMEOUT", "45s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected Load() error: %v", err)
	}

	if cfg.API.Port != 9090 {
		t.Errorf("expected API_PORT 9090, got %d", cfg.API.Port)
	}
	if cfg.App.Env != EnvProduction {
		t.Errorf("expected APP_ENV production, got %s", cfg.App.Env)
	}
	if cfg.App.LogLevel != "debug" {
		t.Errorf("expected LOG_LEVEL debug, got %s", cfg.App.LogLevel)
	}
	if cfg.Worker.Concurrency != 42 {
		t.Errorf("expected WORKER_CONCURRENCY 42, got %d", cfg.Worker.Concurrency)
	}
	if cfg.API.ShutdownTimeout != 45*time.Second {
		t.Errorf("expected API_SHUTDOWN_TIMEOUT 45s, got %v", cfg.API.ShutdownTimeout)
	}
}
