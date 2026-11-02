package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/YKalisjati/forge/pkg/config"
)

func TestLoggerJSONOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	cfg := &config.AppConfig{
		Env:       "test",
		LogLevel:  "info",
		LogFormat: "json",
	}

	l := NewWithOutput("forge-test", cfg, buf)
	l.Info("test message", slog.String("component", "engine"))

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("expected valid JSON log output, got error: %v, raw: %s", err, buf.String())
	}

	if entry["msg"] != "test message" {
		t.Errorf("expected msg 'test message', got %v", entry["msg"])
	}
	if entry["service"] != "forge-test" {
		t.Errorf("expected service 'forge-test', got %v", entry["service"])
	}
	if entry["env"] != "test" {
		t.Errorf("expected env 'test', got %v", entry["env"])
	}
	if entry["level"] != "INFO" {
		t.Errorf("expected level 'INFO', got %v", entry["level"])
	}
	if entry["component"] != "engine" {
		t.Errorf("expected component 'engine', got %v", entry["component"])
	}
}

func TestLoggerTextOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	cfg := &config.AppConfig{
		Env:       "development",
		LogLevel:  "debug",
		LogFormat: "text",
	}

	l := NewWithOutput("forge-dev", cfg, buf)
	l.Debug("debug message")

	out := buf.String()
	if !strings.Contains(out, "debug message") {
		t.Errorf("expected text log to contain 'debug message', got: %s", out)
	}
	if !strings.Contains(out, "service=forge-dev") {
		t.Errorf("expected text log to contain service attribute, got: %s", out)
	}
}

func TestLoggerRedaction(t *testing.T) {
	buf := &bytes.Buffer{}
	cfg := &config.AppConfig{
		Env:       "production",
		LogLevel:  "info",
		LogFormat: "json",
	}

	l := NewWithOutput("forge-prod", cfg, buf)
	l.Info("user authentication",
		slog.String("user_password", "plaintext_secret"),
		slog.String("auth_token", "jwt.secret.token"),
		slog.String("api_key", "sec-12345"),
	)

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if entry["user_password"] != "[REDACTED]" {
		t.Errorf("expected user_password to be [REDACTED], got: %v", entry["user_password"])
	}
	if entry["auth_token"] != "[REDACTED]" {
		t.Errorf("expected auth_token to be [REDACTED], got: %v", entry["auth_token"])
	}
	if entry["api_key"] != "[REDACTED]" {
		t.Errorf("expected api_key to be [REDACTED], got: %v", entry["api_key"])
	}
}

func TestLoggerContextCorrelation(t *testing.T) {
	buf := &bytes.Buffer{}
	cfg := &config.AppConfig{
		Env:       "staging",
		LogLevel:  "info",
		LogFormat: "json",
	}

	l := NewWithOutput("forge-api", cfg, buf)

	ctx := context.WithValue(context.Background(), RequestIDKey, "req-uuid-12345")
	ctx = context.WithValue(ctx, JobIDKey, "job-uuid-67890")

	l.InfoContext(ctx, "handling request")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if entry["request_id"] != "req-uuid-12345" {
		t.Errorf("expected request_id 'req-uuid-12345', got: %v", entry["request_id"])
	}
	if entry["job_id"] != "job-uuid-67890" {
		t.Errorf("expected job_id 'job-uuid-67890', got: %v", entry["job_id"])
	}
}
