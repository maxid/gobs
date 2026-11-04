package gobs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/maxid/gable/health"
	"github.com/maxid/gable/metrics"
)

var (
	// ErrDuplicateAdapter rejects two adapters with the same name.
	ErrDuplicateAdapter = errors.New("gobs: duplicate adapter")
	// ErrInvalidAdapter rejects an empty name or nil factory.
	ErrInvalidAdapter = errors.New("gobs: invalid adapter")
)

// Factory builds one adapter from the shared metric registry.
type Factory func(*metrics.Registry) (any, error)

// Registry is the local adapter registry. It is safe for concurrent lookup
// after the composition root finished registering.
type Registry struct {
	metrics *metrics.Registry
	health  *health.Registry

	mu       sync.RWMutex
	adapters map[string]any
	order    []string
}

// NewRegistry creates an empty registry. A nil metrics registry gets a fresh
// one; pass the same registry that is mounted on /metrics in production.
func NewRegistry(registry *metrics.Registry) *Registry {
	if registry == nil {
		registry = metrics.New()
	}
	return &Registry{metrics: registry, adapters: map[string]any{}}
}

// WithHealth attaches a health registry used by RegisterHealth.
func (r *Registry) WithHealth(registry *health.Registry) *Registry {
	r.health = registry
	return r
}

// Metrics returns the shared Prometheus registry.
func (r *Registry) Metrics() *metrics.Registry { return r.metrics }

// Health returns the attached health registry, or nil.
func (r *Registry) Health() *health.Registry { return r.health }

// Add registers one adapter under name. Registration is explicit and
// deterministic; duplicate names are errors rather than silent replacement.
func (r *Registry) Add(name string, factory Factory) error {
	if name == "" || factory == nil {
		return ErrInvalidAdapter
	}
	r.mu.RLock()
	_, exists := r.adapters[name]
	r.mu.RUnlock()
	if exists {
		return fmt.Errorf("%w: %q", ErrDuplicateAdapter, name)
	}
	adapter, err := factory(r.metrics)
	if err != nil {
		return fmt.Errorf("gobs: adapter %q: %w", name, err)
	}
	if adapter == nil {
		return fmt.Errorf("gobs: adapter %q returned nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.adapters[name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateAdapter, name)
	}
	r.adapters[name] = adapter
	r.order = append(r.order, name)
	return nil
}

// Names returns the registered adapter names in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.order...)
}

// Get looks up one adapter by name and asserts its type.
func Get[T any](r *Registry, name string) (T, bool) {
	var zero T
	if r == nil {
		return zero, false
	}
	r.mu.RLock()
	value, ok := r.adapters[name]
	r.mu.RUnlock()
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	return typed, ok
}

// HealthProbe is the optional health contract implemented by stores and
// clients that can verify connectivity. gcache.Cache satisfies it.
type HealthProbe interface {
	Health(context.Context) error
}

// RegisterHealth adds a readiness check backed by probe. A nil registry or
// probe is a no-op so wiring stays optional.
func RegisterHealth(registry *health.Registry, name string, critical bool, probe HealthProbe) {
	if registry == nil || probe == nil {
		return
	}
	registry.Add(name, critical, func(ctx context.Context) error {
		return probe.Health(ctx)
	})
}

// RegisterHealth adds a readiness check to the registry's attached health
// registry. It is a no-op when WithHealth was not used.
func (r *Registry) RegisterHealth(name string, critical bool, probe HealthProbe) {
	if r == nil {
		return
	}
	RegisterHealth(r.health, name, critical, probe)
}
