package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/pkg/config"
)

// Client wraps an AMQP connection to RabbitMQ.
type Client struct {
	cfg  *config.RabbitMQConfig
	conn *amqp.Connection
	mu   sync.RWMutex
}

// NewClient establishes an AMQP connection to RabbitMQ.
func NewClient(_ context.Context, cfg *config.RabbitMQConfig) (*Client, error) {
	conn, err := amqp.Dial(cfg.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	return &Client{
		cfg:  cfg,
		conn: conn,
	}, nil
}

// Connection returns the active amqp.Connection.
func (c *Client) Connection() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// EnsureConnection verifies that the AMQP connection is open, or re-establishes it if closed.
func (c *Client) EnsureConnection(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		return nil
	}

	conn, err := amqp.Dial(c.cfg.URL())
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	c.conn = conn
	return nil
}

// Ping verifies RabbitMQ connectivity by opening and closing a lightweight channel.
func (c *Client) Ping(_ context.Context) (float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil || c.conn.IsClosed() {
		return 0, fmt.Errorf("rabbitmq connection is closed")
	}

	start := time.Now()
	ch, err := c.conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("failed to open test channel: %w", err)
	}
	_ = ch.Close()
	latency := float64(time.Since(start).Microseconds()) / 1000.0

	return latency, nil
}

// Close gracefully closes the AMQP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn.Close()
	}
	return nil
}
