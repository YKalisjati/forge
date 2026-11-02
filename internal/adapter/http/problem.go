package http

import (
	"encoding/json"
	"net/http"
)

// InvalidParam represents a field-level validation issue conforming to RFC 7807.
type InvalidParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// ProblemDetails represents an RFC 7807 Problem Details object.
type ProblemDetails struct {
	Type          string         `json:"type"`
	Title         string         `json:"title"`
	Status        int            `json:"status"`
	Detail        string         `json:"detail"`
	Instance      string         `json:"instance,omitempty"`
	Code          string         `json:"code"`
	InvalidParams []InvalidParam `json:"invalid_params,omitempty"`
}

// WriteProblem responds with an RFC 7807 JSON error body.
func WriteProblem(w http.ResponseWriter, p ProblemDetails) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}
