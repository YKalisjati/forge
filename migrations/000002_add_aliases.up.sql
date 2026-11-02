-- Views aliasing idempotency_records to idempotency_keys and outbox_messages to outbox_events
CREATE OR REPLACE VIEW idempotency_keys AS SELECT * FROM idempotency_records;
CREATE OR REPLACE VIEW outbox_events AS SELECT * FROM outbox_messages;
