package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/YKalisjati/forge/pkg/config"
)

// Client wraps a pgxpool.Pool connection.
type Client struct {
	pool *pgxpool.Pool
}

// NewClient connects to PostgreSQL and verifies connectivity.
func NewClient(ctx context.Context, cfg *config.PostgresConfig) (*Client, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgres config: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxOpenConns)
	poolConfig.MinConns = int32(cfg.MaxIdleConns)
	poolConfig.MaxConnLifetime = cfg.ConnMaxLifetime

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(connectCtx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres pool: %w", err)
	}

	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &Client{pool: pool}, nil
}

// Pool returns the underlying pgxpool.Pool.
func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}

// Close gracefully closes the connection pool.
func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

// Ping checks database responsiveness and reports roundtrip latency in milliseconds.
func (c *Client) Ping(ctx context.Context) (float64, error) {
	start := time.Now()
	err := c.pool.Ping(ctx)
	latency := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		return 0, err
	}
	return latency, nil
}
