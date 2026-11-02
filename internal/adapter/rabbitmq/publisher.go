package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/internal/domain/outbox"
)

// Standard publisher errors
var (
	ErrNegativeConfirmation = errors.New("rabbitmq: publisher negative confirmation (nack) received from broker")
	ErrPublisherClosed      = errors.New("rabbitmq: publisher is closed")
)

// Publisher defines message publishing with publisher confirms.
type Publisher interface {
	Publish(ctx context.Context, envelope *outbox.Envelope) error
	Close() error
}

// ChannelPublisher publishes persistent messages to RabbitMQ with publisher confirms.
type ChannelPublisher struct {
	client     *Client
	exchange   string
	routingKey string
	mu         sync.Mutex
	channel    *amqp.Channel
	closed     bool
}

// NewPublisher initializes a ChannelPublisher for the target exchange and routing key.
func NewPublisher(client *Client, exchange, routingKey string) *ChannelPublisher {
	return &ChannelPublisher{
		client:     client,
		exchange:   exchange,
		routingKey: routingKey,
	}
}

// getChannel returns an active channel in confirm mode, opening a new one if necessary.
func (p *ChannelPublisher) getChannel(ctx context.Context) (*amqp.Channel, error) {
	if p.channel != nil && !p.channel.IsClosed() {
		return p.channel, nil
	}

	if err := p.client.EnsureConnection(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure rabbitmq connection: %w", err)
	}

	ch, err := p.client.Connection().Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open rabbitmq channel: %w", err)
	}

	// Put channel into confirm mode
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("failed to put channel into confirm mode: %w", err)
	}

	p.channel = ch
	return p.channel, nil
}

// Publish publishes a persistent message envelope and waits for broker confirmation.
func (p *ChannelPublisher) Publish(ctx context.Context, envelope *outbox.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPublisherClosed
	}

	ch, err := p.getChannel(ctx)
	if err != nil {
		return err
	}

	bodyBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to serialize envelope: %w", err)
	}

	headers := amqp.Table{
		"event_id":       envelope.EventID,
		"job_id":         envelope.JobID.String(),
		"event_type":     envelope.EventType,
		"schema_version": envelope.SchemaVersion,
		"correlation_id": envelope.CorrelationID,
	}

	msg := amqp.Publishing{
		Headers:         headers,
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		DeliveryMode:    amqp.Persistent, // Durable message
		MessageId:       fmt.Sprintf("%d", envelope.EventID),
		CorrelationId:   envelope.CorrelationID,
		Type:            envelope.EventType,
		Timestamp:       envelope.Timestamp,
		Body:            bodyBytes,
	}

	// Use deferred confirmation with context
	deferred, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		p.exchange,
		p.routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)
	if err != nil {
		// Connection or channel error: discard channel so next call reconnects
		_ = ch.Close()
		p.channel = nil
		return fmt.Errorf("failed to publish message to rabbitmq: %w", err)
	}

	// Wait for publisher confirm from RabbitMQ
	acked, err := deferred.WaitContext(ctx)
	if err != nil {
		// Timeout or context cancellation:
		// Important: A timeout does not prove the broker didn't accept the message.
		// Close channel to discard in-flight state safely.
		_ = ch.Close()
		p.channel = nil
		return fmt.Errorf("publish confirmation timeout/cancelled: %w", err)
	}

	if !acked {
		return ErrNegativeConfirmation
	}

	return nil
}

// Close gracefully closes the publisher's channel.
func (p *ChannelPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	if p.channel != nil && !p.channel.IsClosed() {
		return p.channel.Close()
	}
	return nil
}
