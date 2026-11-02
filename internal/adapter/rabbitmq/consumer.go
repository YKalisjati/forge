package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer defines the interface for consuming AMQP deliveries.
type Consumer interface {
	Consume(ctx context.Context, queue, consumerTag string, prefetchCount int) (<-chan amqp.Delivery, error)
	Cancel() error
	Close() error
}

// ChannelConsumer implements Consumer using an amqp.Channel.
type ChannelConsumer struct {
	client      *Client
	mu          sync.Mutex
	channel     *amqp.Channel
	consumerTag string
	closed      bool
}

// NewConsumer creates a new ChannelConsumer.
func NewConsumer(client *Client) *ChannelConsumer {
	return &ChannelConsumer{
		client: client,
	}
}

// Consume sets up channel QoS and starts consuming from the target queue with manual acknowledgments.
func (c *ChannelConsumer) Consume(ctx context.Context, queue, consumerTag string, prefetchCount int) (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.client.EnsureConnection(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure rabbitmq connection for consumer: %w", err)
	}

	ch, err := c.client.Connection().Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open consumer channel: %w", err)
	}

	if prefetchCount <= 0 {
		prefetchCount = 1
	}

	// Set QoS prefetch count
	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to set channel QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		queue,
		consumerTag,
		false, // autoAck = false (manual ack required)
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to start consuming queue '%s': %w", queue, err)
	}

	c.channel = ch
	c.consumerTag = consumerTag
	c.closed = false

	return deliveries, nil
}

// Cancel cancels the active consumer tag, stopping new deliveries from RabbitMQ.
func (c *ChannelConsumer) Cancel() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.channel != nil && !c.channel.IsClosed() && c.consumerTag != "" {
		return c.channel.Cancel(c.consumerTag, false)
	}
	return nil
}

// Close gracefully closes the consumer channel.
func (c *ChannelConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.channel != nil && !c.channel.IsClosed() {
		return c.channel.Close()
	}
	return nil
}
