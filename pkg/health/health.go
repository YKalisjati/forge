package health

import (
	"context"
	"sync"
	"time"
)

// Status values matching api-contract.md
const (
	StatusPass = "pass"
	StatusFail = "fail"
)

// CheckResult represents the outcome of an individual dependency readiness probe.
type CheckResult struct {
	Status    string   `json:"status"`
	LatencyMS *float64 `json:"latency_ms,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// LivenessResponse represents the payload for GET /healthz.
type LivenessResponse struct {
	Status    string `json:"status"`
	Service   string `json:"service"`
	Timestamp string `json:"timestamp"`
}

// ReadinessResponse represents the payload for GET /readyz.
type ReadinessResponse struct {
	Status    string                 `json:"status"`
	Checks    map[string]CheckResult `json:"checks"`
	Timestamp string                 `json:"timestamp"`
}

// Probe is a function that checks a specific dependency.
type Probe func(ctx context.Context) (latencyMs float64, err error)

// Registry manages and executes registered health and readiness probes.
type Registry struct {
	serviceName string
	mu          sync.RWMutex
	probes      map[string]Probe
}

// NewRegistry creates a health probe registry for a service.
func NewRegistry(serviceName string) *Registry {
	return &Registry{
		serviceName: serviceName,
		probes:      make(map[string]Probe),
	}
}

// Register adds a named probe to the registry.
func (r *Registry) Register(name string, probe Probe) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes[name] = probe
}

// Liveness returns the immediate liveness status of the service.
func (r *Registry) Liveness() LivenessResponse {
	return LivenessResponse{
		Status:    StatusPass,
		Service:   r.serviceName,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

// Readiness executes all registered probes concurrently with a probe timeout.
func (r *Registry) Readiness(ctx context.Context) ReadinessResponse {
	r.mu.RLock()
	probes := make(map[string]Probe, len(r.probes))
	for k, v := range r.probes {
		probes[k] = v
	}
	r.mu.RUnlock()

	results := make(map[string]CheckResult, len(probes))
	var mu sync.Mutex
	var wg sync.WaitGroup

	allHealthy := true

	for name, probe := range probes {
		wg.Add(1)
		go func(name string, p Probe) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			latency, err := p(probeCtx)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				allHealthy = false
				results[name] = CheckResult{
					Status: StatusFail,
					Error:  err.Error(),
				}
			} else {
				results[name] = CheckResult{
					Status:    StatusPass,
					LatencyMS: &latency,
				}
			}
		}(name, probe)
	}

	wg.Wait()

	overallStatus := StatusPass
	if !allHealthy {
		overallStatus = StatusFail
	}

	return ReadinessResponse{
		Status:    overallStatus,
		Checks:    results,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}
