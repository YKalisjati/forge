package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/YKalisjati/forge/pkg/config"
)

type contextKey struct{}

var loggerKey = contextKey{}

// Sensitive key patterns to redact automatically
var sensitivePatterns = []string{
	"password", "secret", "token", "authorization", "auth", "api_key", "apikey",
}

// New initializes and returns a structured slog.Logger according to AppConfig.
func New(serviceName string, cfg *config.AppConfig) *slog.Logger {
	return NewWithOutput(serviceName, cfg, os.Stdout)
}

// NewWithOutput initializes a structured slog.Logger writing to a custom io.Writer.
func NewWithOutput(serviceName string, cfg *config.AppConfig, out io.Writer) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: sanitizeAttr,
	}

	var handler slog.Handler
	if strings.ToLower(cfg.LogFormat) == "text" {
		handler = slog.NewTextHandler(out, opts)
	} else {
		handler = slog.NewJSONHandler(out, opts)
	}

	// Add service metadata to all log outputs
	handler = handler.WithAttrs([]slog.Attr{
		slog.String("service", serviceName),
		slog.String("env", cfg.Env),
	})

	return slog.New(&contextualHandler{Handler: handler})
}

// CorrelationKey defines context key types for logger correlation.
type CorrelationKey string

const (
	// RequestIDKey is the context key for request IDs.
	RequestIDKey CorrelationKey = "request_id"
	// CorrelationIDKey is the context key for distributed correlation IDs.
	CorrelationIDKey CorrelationKey = "correlation_id"
	// JobIDKey is the context key for job IDs.
	JobIDKey CorrelationKey = "job_id"
)

// sanitizeAttr redacts values whose keys match known sensitive substrings.
func sanitizeAttr(_ []string, a slog.Attr) slog.Attr {
	keyLower := strings.ToLower(a.Key)
	for _, pattern := range sensitivePatterns {
		if strings.Contains(keyLower, pattern) {
			return slog.String(a.Key, "[REDACTED]")
		}
	}
	return a
}

// contextualHandler automatically extracts correlation identifiers from context.Context.
type contextualHandler struct {
	slog.Handler
}

func (h *contextualHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if reqID, ok := ctx.Value(RequestIDKey).(string); ok && reqID != "" {
			r.AddAttrs(slog.String("request_id", reqID))
		} else if reqID, ok := ctx.Value("request_id").(string); ok && reqID != "" {
			r.AddAttrs(slog.String("request_id", reqID))
		}

		if corrID, ok := ctx.Value(CorrelationIDKey).(string); ok && corrID != "" {
			r.AddAttrs(slog.String("correlation_id", corrID))
		} else if corrID, ok := ctx.Value("correlation_id").(string); ok && corrID != "" {
			r.AddAttrs(slog.String("correlation_id", corrID))
		}

		if jobID, ok := ctx.Value(JobIDKey).(string); ok && jobID != "" {
			r.AddAttrs(slog.String("job_id", jobID))
		} else if jobID, ok := ctx.Value("job_id").(string); ok && jobID != "" {
			r.AddAttrs(slog.String("job_id", jobID))
		}
	}
	return h.Handler.Handle(ctx, r)
}

// WithContext stores a logger instance in the context.
func WithContext(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

// FromContext retrieves a logger from context, or falls back to slog.Default().
func FromContext(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok && l != nil {
			return l
		}
	}
	return slog.Default()
}
