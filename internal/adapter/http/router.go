package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/YKalisjati/forge/internal/adapter/redis"
	"github.com/YKalisjati/forge/pkg/health"
	"github.com/YKalisjati/forge/pkg/logger"
	"github.com/YKalisjati/forge/pkg/metrics"
)

// ServerOptions configures HTTP handler behaviors.
type ServerOptions struct {
	Registry    *health.Registry
	Logger      *slog.Logger
	JobHandler  *JobHandler
	RateLimiter redis.RateLimiter
	Metrics     *metrics.Metrics
}

// NewRouter constructs the HTTP handler with standard middleware and routes.
func NewRouter(opts ServerOptions) http.Handler {
	if opts.Metrics == nil {
		opts.Metrics = metrics.Default()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	r := chi.NewRouter()

	// Base middleware stack
	r.Use(CorrelationMiddleware)
	r.Use(MetricsMiddleware(opts.Metrics))
	r.Use(StructuredLoggerMiddleware(opts.Logger))
	r.Use(middleware.Recoverer)

	// Liveness & readiness probes
	r.Get("/healthz", HandleLiveness(opts.Registry))
	r.Get("/readyz", HandleReadiness(opts.Registry))

	// Prometheus metrics endpoint
	r.Handle("/metrics", promhttp.HandlerFor(opts.Metrics.Registry, promhttp.HandlerOpts{}))

	// API v1 routes
	if opts.JobHandler != nil {
		r.Route("/v1/jobs", func(sub chi.Router) {
			if opts.RateLimiter != nil {
				sub.With(RateLimitMiddleware(opts.RateLimiter, opts.Metrics, opts.Logger)).Post("/", opts.JobHandler.HandleSubmitJob)
			} else {
				sub.Post("/", opts.JobHandler.HandleSubmitJob)
			}
			sub.Get("/", opts.JobHandler.HandleListJobs)
			sub.Get("/{id}", opts.JobHandler.HandleGetJob)
		})
	}

	return r
}

type contextKey string

// RequestIDKey is the context key for the correlation request ID.
const RequestIDKey contextKey = "request_id"

// CorrelationIDKey is the context key for the distributed correlation ID.
const CorrelationIDKey contextKey = "correlation_id"

// CorrelationMiddleware ensures each request has X-Request-ID and X-Correlation-ID headers and context values.
func CorrelationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", reqID)

		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = reqID
		}
		w.Header().Set("X-Correlation-ID", corrID)

		ctx := r.Context()
		ctx = context.WithValue(ctx, RequestIDKey, reqID)
		ctx = context.WithValue(ctx, logger.RequestIDKey, reqID)
		ctx = context.WithValue(ctx, CorrelationIDKey, corrID)
		ctx = context.WithValue(ctx, logger.CorrelationIDKey, corrID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDMiddleware retains backwards compatibility for RequestIDKey.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return CorrelationMiddleware(next)
}

// MetricsMiddleware tracks in-flight requests, request counts, and latency distributions.
func MetricsMiddleware(m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.APIActiveRequests.Inc()
			defer m.APIActiveRequests.Dec()

			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			duration := time.Since(start)
			endpoint := normalizePath(r.URL.Path)
			m.ObserveHTTPRequest(r.Method, endpoint, ww.Status(), duration)
		})
	}
}

// normalizePath converts variable route segments into normalized low-cardinality labels.
func normalizePath(path string) string {
	if strings.HasPrefix(path, "/v1/jobs/") {
		return "/v1/jobs/{id}"
	}
	switch path {
	case "/v1/jobs", "/v1/jobs/":
		return "/v1/jobs"
	case "/healthz":
		return "/healthz"
	case "/readyz":
		return "/readyz"
	case "/metrics":
		return "/metrics"
	default:
		return path
	}
}

// RateLimitMiddleware enforces configurable submission rate limiting with a fail-open policy.
func RateLimitMiddleware(limiter redis.RateLimiter, m *metrics.Metrics, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientIP := r.Header.Get("X-Forwarded-For")
			if clientIP != "" {
				// Take first proxy hop
				if idx := strings.Index(clientIP, ","); idx != -1 {
					clientIP = strings.TrimSpace(clientIP[:idx])
				}
			} else {
				host, _, err := net.SplitHostPort(r.RemoteAddr)
				if err == nil {
					clientIP = host
				} else {
					clientIP = r.RemoteAddr
				}
			}

			if clientIP == "" {
				clientIP = "unknown"
			}

			res, err := limiter.Allow(r.Context(), clientIP)
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(res.ResetAt.Unix(), 10))

			if err != nil {
				// Record limiter failure metric and log warning
				if m != nil {
					m.RateLimitErrorsTotal.Inc()
				}
				if log != nil {
					log.WarnContext(r.Context(), "rate limiter error, falling open",
						slog.String("error", err.Error()),
						slog.String("client_ip", clientIP),
					)
				}
			}

			if !res.Allowed {
				if m != nil {
					m.RateLimitExceededTotal.Inc()
				}
				retryAfter := math.Ceil(time.Until(res.ResetAt).Seconds())
				if retryAfter < 1 {
					retryAfter = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter)))
				WriteProblem(w, ProblemDetails{
					Type:   "https://forge.dev/errors/rate-limit-exceeded",
					Title:  "Too Many Requests",
					Status: http.StatusTooManyRequests,
					Detail: "Rate limit exceeded. Try again later.",
					Code:   "RATE_LIMIT_EXCEEDED",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// StructuredLoggerMiddleware logs each HTTP request using slog.
func StructuredLoggerMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			defer func() {
				duration := time.Since(start)
				log.InfoContext(r.Context(), "http_request",
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.Int("status", ww.Status()),
					slog.Int("bytes", ww.BytesWritten()),
					slog.Duration("duration", duration),
					slog.String("remote_addr", r.RemoteAddr),
				)
			}()

			next.ServeHTTP(ww, r)
		})
	}
}

// HandleLiveness returns the liveness probe response.
func HandleLiveness(reg *health.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if reg == nil {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(health.LivenessResponse{Status: health.StatusPass, Service: "forge-api"})
			return
		}
		res := reg.Liveness()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
	}
}

// HandleReadiness returns the readiness probe response.
func HandleReadiness(reg *health.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if reg == nil {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(health.ReadinessResponse{Status: health.StatusPass})
			return
		}
		res := reg.Readiness(r.Context())
		if res.Status == health.StatusPass {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		_ = json.NewEncoder(w).Encode(res)
	}
}
