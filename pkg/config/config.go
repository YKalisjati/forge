package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Environment constants
const (
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
	EnvTest        = "test"
)

// Config holds the full application configuration.
type Config struct {
	App      AppConfig      `json:"app"`
	API      APIConfig      `json:"api"`
	Postgres PostgresConfig `json:"postgres"`
	RabbitMQ RabbitMQConfig `json:"rabbitmq"`
	Redis    RedisConfig    `json:"redis"`
	Relay    RelayConfig    `json:"relay"`
	Worker   WorkerConfig   `json:"worker"`
	Reaper   ReaperConfig   `json:"reaper"`
}

// AppConfig represents general application settings.
type AppConfig struct {
	Env       string `json:"env"`
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`
}

// APIConfig represents HTTP API settings.
type APIConfig struct {
	Port            int           `json:"port"`
	ShutdownTimeout time.Duration `json:"shutdown_timeout"`
	ReadTimeout     time.Duration `json:"read_timeout"`
	WriteTimeout    time.Duration `json:"write_timeout"`
	MaxPayloadBytes int64         `json:"max_payload_bytes"`
}

// PostgresConfig represents PostgreSQL connection settings.
type PostgresConfig struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	User            string        `json:"user"`
	Password        string        `json:"-"` // Never serialized in JSON
	Name            string        `json:"name"`
	SSLMode         string        `json:"sslmode"`
	MaxOpenConns    int           `json:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`
}

// DSN returns a PostgreSQL connection string.
func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		url.QueryEscape(p.User),
		url.QueryEscape(p.Password),
		p.Host,
		p.Port,
		p.Name,
		p.SSLMode,
	)
}

// RabbitMQConfig represents RabbitMQ connection settings.
type RabbitMQConfig struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	ManagementPort int    `json:"management_port"`
	User           string `json:"user"`
	Password       string `json:"-"` // Never serialized in JSON
	VHost          string `json:"vhost"`
	Exchange       string `json:"exchange"`
	Queue          string `json:"queue"`
	DLQ            string `json:"dlq"`
}

// URL returns an AMQP connection URL.
func (r RabbitMQConfig) URL() string {
	vhost := r.VHost
	if !strings.HasPrefix(vhost, "/") {
		vhost = "/" + vhost
	}
	return fmt.Sprintf("amqp://%s:%s@%s:%d%s",
		url.QueryEscape(r.User),
		url.QueryEscape(r.Password),
		r.Host,
		r.Port,
		vhost,
	)
}

// RedisConfig represents Redis connection and rate-limiting settings.
type RedisConfig struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Password       string `json:"-"` // Never serialized in JSON
	DB             int    `json:"db"`
	RateLimitRPS   int    `json:"rate_limit_rps"`
	RateLimitBurst int    `json:"rate_limit_burst"`
}

// Addr returns host:port for Redis client.
func (r RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// RelayConfig represents Outbox Relay dispatcher settings.
type RelayConfig struct {
	PollInterval time.Duration `json:"poll_interval"`
	BatchSize    int           `json:"batch_size"`
	MaxRetries   int           `json:"max_retries"`
}

// WorkerConfig represents background execution engine settings.
type WorkerConfig struct {
	Concurrency       int           `json:"concurrency"`
	PrefetchCount     int           `json:"prefetch_count"`
	LeaseDuration     time.Duration `json:"lease_duration"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	ShutdownTimeout   time.Duration `json:"shutdown_timeout"`
}

// ReaperConfig represents stale job recovery settings.
type ReaperConfig struct {
	Interval  time.Duration `json:"interval"`
	BatchSize int           `json:"batch_size"`
}

// Load reads configuration from environment variables, optionally loading a .env file if present.
func Load() (*Config, error) {
	return loadInternal(true)
}

// LoadFromEnv reads configuration strictly from environment variables without loading .env files.
func LoadFromEnv() (*Config, error) {
	return loadInternal(false)
}

func loadInternal(loadDotEnv bool) (*Config, error) {
	if loadDotEnv {
		// Attempt loading .env if it exists in current or parent directories
		_ = godotenv.Load()
		_ = godotenv.Load("../.env")
		_ = godotenv.Load("../../.env")
		_ = godotenv.Load("../../../.env")
	}

	cfg := &Config{
		App: AppConfig{
			Env:       getEnv("APP_ENV", EnvDevelopment),
			LogLevel:  strings.ToLower(getEnv("LOG_LEVEL", "info")),
			LogFormat: strings.ToLower(getEnv("LOG_FORMAT", "json")),
		},
		API: APIConfig{
			Port:            getEnvInt("API_PORT", 8080),
			ShutdownTimeout: getEnvDuration("API_SHUTDOWN_TIMEOUT", 10*time.Second),
			ReadTimeout:     getEnvDuration("API_READ_TIMEOUT", 5*time.Second),
			WriteTimeout:    getEnvDuration("API_WRITE_TIMEOUT", 10*time.Second),
			MaxPayloadBytes: getEnvInt64("API_MAX_PAYLOAD_BYTES", 1048576),
		},
		Postgres: PostgresConfig{
			Host:            getEnv("DB_HOST", "localhost"),
			Port:            getEnvInt("DB_PORT", 5432),
			User:            getEnv("DB_USER", "forge_user"),
			Password:        getEnv("DB_PASSWORD", "forge_secret_dev"),
			Name:            getEnv("DB_NAME", "forge_db"),
			SSLMode:         getEnv("DB_SSLMODE", "disable"),
			MaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: getEnvDuration("DB_CONN_MAX_LIFETIME", 5*time.Minute),
		},
		RabbitMQ: RabbitMQConfig{
			Host:           getEnv("RABBITMQ_HOST", "localhost"),
			Port:           getEnvInt("RABBITMQ_PORT", 5672),
			ManagementPort: getEnvInt("RABBITMQ_MANAGEMENT_PORT", 15672),
			User:           getEnv("RABBITMQ_USER", "guest"),
			Password:       getEnv("RABBITMQ_PASSWORD", "guest"),
			VHost:          getEnv("RABBITMQ_VHOST", "/"),
			Exchange:       getEnv("RABBITMQ_EXCHANGE", "forge.jobs.direct"),
			Queue:          getEnv("RABBITMQ_QUEUE", "forge.jobs"),
			DLQ:            getEnv("RABBITMQ_DLQ", "forge.jobs.dlq"),
		},
		Redis: RedisConfig{
			Host:           getEnv("REDIS_HOST", "localhost"),
			Port:           getEnvInt("REDIS_PORT", 6379),
			Password:       getEnv("REDIS_PASSWORD", ""),
			DB:             getEnvInt("REDIS_DB", 0),
			RateLimitRPS:   getEnvInt("RATE_LIMIT_RPS", 100),
			RateLimitBurst: getEnvInt("RATE_LIMIT_BURST", 200),
		},
		Relay: RelayConfig{
			PollInterval: getEnvDuration("RELAY_POLL_INTERVAL", 100*time.Millisecond),
			BatchSize:    getEnvInt("RELAY_BATCH_SIZE", 100),
			MaxRetries:   getEnvInt("RELAY_MAX_RETRIES", 5),
		},
		Worker: WorkerConfig{
			Concurrency:       getEnvInt("WORKER_CONCURRENCY", 10),
			PrefetchCount:     getEnvInt("WORKER_PREFETCH_COUNT", 20),
			LeaseDuration:     getEnvDuration("WORKER_LEASE_DURATION", 30*time.Second),
			HeartbeatInterval: getEnvDuration("WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
			ShutdownTimeout:   getEnvDuration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		Reaper: ReaperConfig{
			Interval:  getEnvDuration("REAPER_INTERVAL", 30*time.Second),
			BatchSize: getEnvInt("REAPER_BATCH_SIZE", 50),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// Validate validates all configuration fields against domain constraints.
func (c *Config) Validate() error {
	var errs []string

	// App validation
	switch c.App.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Sprintf("invalid LOG_LEVEL: %q (expected debug, info, warn, error)", c.App.LogLevel))
	}

	switch c.App.LogFormat {
	case "json", "text":
	default:
		errs = append(errs, fmt.Sprintf("invalid LOG_FORMAT: %q (expected json, text)", c.App.LogFormat))
	}

	// API validation
	if c.API.Port < 1 || c.API.Port > 65535 {
		errs = append(errs, fmt.Sprintf("invalid API_PORT: %d (must be between 1 and 65535)", c.API.Port))
	}
	if c.API.ShutdownTimeout <= 0 {
		errs = append(errs, "API_SHUTDOWN_TIMEOUT must be positive")
	}
	if c.API.ReadTimeout <= 0 {
		errs = append(errs, "API_READ_TIMEOUT must be positive")
	}
	if c.API.WriteTimeout <= 0 {
		errs = append(errs, "API_WRITE_TIMEOUT must be positive")
	}
	if c.API.MaxPayloadBytes <= 0 {
		errs = append(errs, "API_MAX_PAYLOAD_BYTES must be positive")
	}

	// PostgreSQL validation
	if c.Postgres.Host == "" {
		errs = append(errs, "DB_HOST cannot be empty")
	}
	if c.Postgres.Port < 1 || c.Postgres.Port > 65535 {
		errs = append(errs, fmt.Sprintf("invalid DB_PORT: %d (must be between 1 and 65535)", c.Postgres.Port))
	}
	if c.Postgres.User == "" {
		errs = append(errs, "DB_USER cannot be empty")
	}
	if c.Postgres.Name == "" {
		errs = append(errs, "DB_NAME cannot be empty")
	}
	if c.Postgres.MaxOpenConns <= 0 {
		errs = append(errs, "DB_MAX_OPEN_CONNS must be positive")
	}
	if c.Postgres.MaxIdleConns < 0 {
		errs = append(errs, "DB_MAX_IDLE_CONNS cannot be negative")
	}

	// RabbitMQ validation
	if c.RabbitMQ.Host == "" {
		errs = append(errs, "RABBITMQ_HOST cannot be empty")
	}
	if c.RabbitMQ.Port < 1 || c.RabbitMQ.Port > 65535 {
		errs = append(errs, fmt.Sprintf("invalid RABBITMQ_PORT: %d", c.RabbitMQ.Port))
	}
	if c.RabbitMQ.Exchange == "" {
		errs = append(errs, "RABBITMQ_EXCHANGE cannot be empty")
	}
	if c.RabbitMQ.Queue == "" {
		errs = append(errs, "RABBITMQ_QUEUE cannot be empty")
	}

	// Redis validation
	if c.Redis.Host == "" {
		errs = append(errs, "REDIS_HOST cannot be empty")
	}
	if c.Redis.Port < 1 || c.Redis.Port > 65535 {
		errs = append(errs, fmt.Sprintf("invalid REDIS_PORT: %d", c.Redis.Port))
	}
	if c.Redis.RateLimitRPS <= 0 {
		errs = append(errs, "RATE_LIMIT_RPS must be positive")
	}
	if c.Redis.RateLimitBurst <= 0 {
		errs = append(errs, "RATE_LIMIT_BURST must be positive")
	}

	// Relay validation
	if c.Relay.PollInterval <= 0 {
		errs = append(errs, "RELAY_POLL_INTERVAL must be positive")
	}
	if c.Relay.BatchSize <= 0 {
		errs = append(errs, "RELAY_BATCH_SIZE must be positive")
	}

	// Worker validation
	if c.Worker.Concurrency <= 0 {
		errs = append(errs, "WORKER_CONCURRENCY must be positive")
	}
	if c.Worker.PrefetchCount <= 0 {
		errs = append(errs, "WORKER_PREFETCH_COUNT must be positive")
	}
	if c.Worker.LeaseDuration <= 0 {
		errs = append(errs, "WORKER_LEASE_DURATION must be positive")
	}
	if c.Worker.HeartbeatInterval <= 0 {
		errs = append(errs, "WORKER_HEARTBEAT_INTERVAL must be positive")
	}

	// Reaper validation
	if c.Reaper.Interval <= 0 {
		errs = append(errs, "REAPER_INTERVAL must be positive")
	}
	if c.Reaper.BatchSize <= 0 {
		errs = append(errs, "REAPER_BATCH_SIZE must be positive")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

// LogValue implements slog.LogValuer to ensure passwords and credentials are REDACTED in structured logs.
func (c *Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Group("app",
			slog.String("env", c.App.Env),
			slog.String("log_level", c.App.LogLevel),
			slog.String("log_format", c.App.LogFormat),
		),
		slog.Group("api",
			slog.Int("port", c.API.Port),
			slog.Duration("shutdown_timeout", c.API.ShutdownTimeout),
			slog.Duration("read_timeout", c.API.ReadTimeout),
			slog.Duration("write_timeout", c.API.WriteTimeout),
			slog.Int64("max_payload_bytes", c.API.MaxPayloadBytes),
		),
		slog.Group("postgres",
			slog.String("host", c.Postgres.Host),
			slog.Int("port", c.Postgres.Port),
			slog.String("user", c.Postgres.User),
			slog.String("password", "[REDACTED]"),
			slog.String("name", c.Postgres.Name),
			slog.String("sslmode", c.Postgres.SSLMode),
			slog.Int("max_open_conns", c.Postgres.MaxOpenConns),
			slog.Int("max_idle_conns", c.Postgres.MaxIdleConns),
			slog.Duration("conn_max_lifetime", c.Postgres.ConnMaxLifetime),
		),
		slog.Group("rabbitmq",
			slog.String("host", c.RabbitMQ.Host),
			slog.Int("port", c.RabbitMQ.Port),
			slog.String("user", c.RabbitMQ.User),
			slog.String("password", "[REDACTED]"),
			slog.String("vhost", c.RabbitMQ.VHost),
			slog.String("exchange", c.RabbitMQ.Exchange),
			slog.String("queue", c.RabbitMQ.Queue),
			slog.String("dlq", c.RabbitMQ.DLQ),
		),
		slog.Group("redis",
			slog.String("host", c.Redis.Host),
			slog.Int("port", c.Redis.Port),
			slog.String("password", "[REDACTED]"),
			slog.Int("db", c.Redis.DB),
			slog.Int("rate_limit_rps", c.Redis.RateLimitRPS),
			slog.Int("rate_limit_burst", c.Redis.RateLimitBurst),
		),
		slog.Group("relay",
			slog.Duration("poll_interval", c.Relay.PollInterval),
			slog.Int("batch_size", c.Relay.BatchSize),
			slog.Int("max_retries", c.Relay.MaxRetries),
		),
		slog.Group("worker",
			slog.Int("concurrency", c.Worker.Concurrency),
			slog.Int("prefetch_count", c.Worker.PrefetchCount),
			slog.Duration("lease_duration", c.Worker.LeaseDuration),
			slog.Duration("heartbeat_interval", c.Worker.HeartbeatInterval),
			slog.Duration("shutdown_timeout", c.Worker.ShutdownTimeout),
		),
		slog.Group("reaper",
			slog.Duration("interval", c.Reaper.Interval),
			slog.Int("batch_size", c.Reaper.BatchSize),
		),
	)
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		if intVal, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		if intVal, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		if dur, err := time.ParseDuration(strings.TrimSpace(val)); err == nil {
			return dur
		}
	}
	return defaultVal
}
