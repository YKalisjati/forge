package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/YKalisjati/forge/internal/domain/job"
)

// Handler processes a claimed background job.
type Handler interface {
	Execute(ctx context.Context, j *job.Job) error
}

// HandlerFunc allows using a plain function as a Handler.
type HandlerFunc func(ctx context.Context, j *job.Job) error

// Execute calls the underlying function.
func (f HandlerFunc) Execute(ctx context.Context, j *job.Job) error {
	return f(ctx, j)
}

// Registry maintains an allowlist of job handlers mapped by job type.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry creates a new Handler Registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register adds a handler for an allowed job type. Returns an error if the job type is not permitted.
func (r *Registry) Register(jobType string, h Handler) error {
	if !job.IsAllowedJobType(jobType) {
		return fmt.Errorf("%w: %q", job.ErrInvalidJobType, jobType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
	return nil
}

// Get retrieves a handler for the given job type.
func (r *Registry) Get(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}
