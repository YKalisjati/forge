package rabbitmq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/YKalisjati/forge/internal/domain/outbox"
	"github.com/YKalisjati/forge/pkg/config"
)

func setupRabbitMQTest(t *testing.T) (*Client, *config.Config, func()) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("failed to load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewClient(ctx, &cfg.RabbitMQ)
	if err != nil {
		t.Skipf("skipping RabbitMQ test: broker unreachable: %v", err)
	}

	// Ensure topology declared
	if err := DeclareTopology(ctx, client, &cfg.RabbitMQ); err != nil {
		_ = client.Close()
		t.Fatalf("failed to declare topology: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
	}

	return client, cfg, cleanup
}

func TestPublisher_DurableTopologyAndDelivery(t *testing.T) {
	client, cfg, cleanup := setupRabbitMQTest(t)
	defer cleanup()

	publisher := NewPublisher(client, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() {
		_ = publisher.Close()
	}()

	testJobID := uuid.New()
	env := &outbox.Envelope{
		EventID:       1001,
		JobID:         testJobID,
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: testJobID.String(),
		Payload:       json.RawMessage(`{"task":"send_email","recipient":"alice@example.com"}`),
		Timestamp:     time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Publish with publisher confirms
	if err := publisher.Publish(ctx, env); err != nil {
		t.Fatalf("failed to publish envelope: %v", err)
	}

	// 2. Consume from queue to verify persistent delivery and envelope integrity
	ch, err := client.Connection().Channel()
	if err != nil {
		t.Fatalf("failed to open consume channel: %v", err)
	}
	defer func() {
		_ = ch.Close()
	}()

	msg, ok, err := ch.Get(cfg.RabbitMQ.Queue, true)
	if err != nil {
		t.Fatalf("failed to get message from queue: %v", err)
	}
	if !ok {
		t.Fatalf("expected message in queue '%s', but queue was empty", cfg.RabbitMQ.Queue)
	}

	// Verify delivery mode is Persistent
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("expected DeliveryMode %d (Persistent), got %d", amqp.Persistent, msg.DeliveryMode)
	}

	// Verify headers
	if msg.Headers["event_id"] != int64(1001) {
		t.Errorf("expected header event_id 1001, got %v", msg.Headers["event_id"])
	}
	if msg.Headers["job_id"] != testJobID.String() {
		t.Errorf("expected header job_id %s, got %v", testJobID.String(), msg.Headers["job_id"])
	}
	if msg.Headers["schema_version"] != int32(1) { // AMQP unmarshals int as int32
		t.Errorf("expected header schema_version 1, got %v", msg.Headers["schema_version"])
	}

	// Verify body envelope
	var receivedEnv outbox.Envelope
	if err := json.Unmarshal(msg.Body, &receivedEnv); err != nil {
		t.Fatalf("failed to deserialize received envelope: %v", err)
	}
	if receivedEnv.EventID != env.EventID || receivedEnv.JobID != env.JobID {
		t.Errorf("received envelope mismatch: %+v vs %+v", receivedEnv, env)
	}
}

func TestPublisher_ContextTimeoutOrCancelled(t *testing.T) {
	client, cfg, cleanup := setupRabbitMQTest(t)
	defer cleanup()

	publisher := NewPublisher(client, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer func() {
		_ = publisher.Close()
	}()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	env := &outbox.Envelope{
		EventID:       1002,
		JobID:         uuid.New(),
		EventType:     "job.created",
		SchemaVersion: 1,
		CorrelationID: "cancelled-corr",
		Payload:       json.RawMessage(`{}`),
		Timestamp:     time.Now().UTC(),
	}

	err := publisher.Publish(canceledCtx, env)
	if err == nil {
		t.Fatalf("expected error publishing with cancelled context, got nil")
	}
}
