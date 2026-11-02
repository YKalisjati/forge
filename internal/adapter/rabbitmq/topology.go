package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/pkg/config"
)

// DeclareTopology sets up durable direct exchanges, queues, DLQs, and bindings in RabbitMQ.
func DeclareTopology(ctx context.Context, client *Client, cfg *config.RabbitMQConfig) error {
	if err := client.EnsureConnection(ctx); err != nil {
		return fmt.Errorf("failed to ensure rabbitmq connection for topology declaration: %w", err)
	}

	ch, err := client.Connection().Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel for topology declaration: %w", err)
	}
	defer func() {
		_ = ch.Close()
	}()

	// 1. Declare durable direct exchange
	err = ch.ExchangeDeclare(
		cfg.Exchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange '%s': %w", cfg.Exchange, err)
	}

	// 2. Declare durable Dead Letter Queue (DLQ)
	_, err = ch.QueueDeclare(
		cfg.DLQ,
		true,  // durable
		false, // auto-deleted
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare dlq '%s': %w", cfg.DLQ, err)
	}

	// 3. Bind DLQ to direct exchange
	err = ch.QueueBind(
		cfg.DLQ,
		cfg.DLQ, // routing key matches DLQ name
		cfg.Exchange,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind dlq '%s' to exchange '%s': %w", cfg.DLQ, cfg.Exchange, err)
	}

	// 4. Declare durable main job queue with dead-letter configuration
	queueArgs := amqp.Table{
		"x-dead-letter-exchange":    cfg.Exchange,
		"x-dead-letter-routing-key": cfg.DLQ,
	}

	_, err = ch.QueueDeclare(
		cfg.Queue,
		true,  // durable
		false, // auto-deleted
		false, // exclusive
		false, // no-wait
		queueArgs,
	)
	if err != nil {
		return fmt.Errorf("failed to declare job queue '%s': %w", cfg.Queue, err)
	}

	// 5. Bind main job queue to exchange with default routing key
	err = ch.QueueBind(
		cfg.Queue,
		cfg.Queue,
		cfg.Exchange,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue '%s' with key '%s': %w", cfg.Queue, cfg.Queue, err)
	}

	// Also bind with event_type routing key (e.g., job.created)
	err = ch.QueueBind(
		cfg.Queue,
		"job.created",
		cfg.Exchange,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue '%s' with key 'job.created': %w", cfg.Queue, err)
	}

	return nil
}
