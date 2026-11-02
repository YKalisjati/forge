package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/YKalisjati/forge/pkg/config"
)

// Client wraps a Redis client.
type Client struct {
	client *redis.Client
}

// NewClient initializes a connection to Redis.
func NewClient(ctx context.Context, cfg *config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	return &Client{client: rdb}, nil
}

// Client returns the underlying *redis.Client.
func (c *Client) Client() *redis.Client {
	return c.client
}

// Ping checks Redis responsiveness and reports roundtrip latency in milliseconds.
func (c *Client) Ping(ctx context.Context) (float64, error) {
	start := time.Now()
	err := c.client.Ping(ctx).Err()
	latency := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		return 0, err
	}
	return latency, nil
}

// Close gracefully closes the Redis connection.
func (c *Client) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
