package job

import (
	"encoding/json"
	"testing"
)

func TestCalculateStableRequestHash(t *testing.T) {
	// Two payloads with identical content but different JSON key ordering
	payloadA := json.RawMessage(`{"zebra": 100, "apple": "pie", "nested": {"b": 2, "a": 1}}`)
	payloadB := json.RawMessage(`{"apple": "pie", "nested": {"a": 1, "b": 2}, "zebra": 100}`)

	inputA := SubmitJobInput{
		Type:         "email.transactional",
		Payload:      payloadA,
		Priority:     10,
		MaxRetries:   3,
		DelaySeconds: 0,
	}

	inputB := SubmitJobInput{
		Type:         "email.transactional",
		Payload:      payloadB,
		Priority:     10,
		MaxRetries:   3,
		DelaySeconds: 0,
	}

	hashA, err := CalculateStableRequestHash(inputA)
	if err != nil {
		t.Fatalf("failed to hash inputA: %v", err)
	}

	hashB, err := CalculateStableRequestHash(inputB)
	if err != nil {
		t.Fatalf("failed to hash inputB: %v", err)
	}

	if hashA != hashB {
		t.Errorf("expected stable identical hashes for different key orders, got:\nhashA: %s\nhashB: %s", hashA, hashB)
	}

	// Changing priority must alter hash
	inputC := inputA
	inputC.Priority = 20
	hashC, err := CalculateStableRequestHash(inputC)
	if err != nil {
		t.Fatalf("failed to hash inputC: %v", err)
	}

	if hashA == hashC {
		t.Errorf("expected hash to change when priority changes")
	}

	// Changing payload content must alter hash
	inputD := inputA
	inputD.Payload = json.RawMessage(`{"zebra": 101, "apple": "pie"}`)
	hashD, err := CalculateStableRequestHash(inputD)
	if err != nil {
		t.Fatalf("failed to hash inputD: %v", err)
	}

	if hashA == hashD {
		t.Errorf("expected hash to change when payload values change")
	}
}
