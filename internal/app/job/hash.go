package job

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SubmitJobInput encapsulates the validated client submission parameters.
type SubmitJobInput struct {
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Priority     int             `json:"priority"`
	MaxRetries   int             `json:"max_retries"`
	DelaySeconds int             `json:"delay_seconds"`
}

// CalculateStableRequestHash produces a deterministic SHA-256 fingerprint of the normalized request.
// It parses the JSON payload into an interface structure and marshals it to ensure map keys
// are sorted canonically, preventing arbitrary key ordering from altering the hash.
func CalculateStableRequestHash(input SubmitJobInput) (string, error) {
	var parsedPayload interface{}
	if err := json.Unmarshal(input.Payload, &parsedPayload); err != nil {
		return "", fmt.Errorf("failed to parse payload for hashing: %w", err)
	}

	normalizedPayload, err := json.Marshal(parsedPayload)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize payload: %w", err)
	}

	canonicalString := fmt.Sprintf("type:%s|priority:%d|max_retries:%d|delay_seconds:%d|payload:%s",
		input.Type,
		input.Priority,
		input.MaxRetries,
		input.DelaySeconds,
		string(normalizedPayload),
	)

	hasher := sha256.New()
	hasher.Write([]byte(canonicalString))
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
