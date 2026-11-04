// Package gobs adapts the g* libraries' neutral observability hooks to the
// gable metrics and health registries, and carries the trace context used by
// application instrumentation.
//
// It is an integration library, not a framework: it depends on gable's
// registry types and on the hook contracts of the protocol libraries, but
// neither gable nor a protocol library depends on gobs. The composition root
// decides which adapters to register and where to mount /metrics.
package gobs

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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

// TraceContext is the W3C traceparent portion used for log correlation and
// outbound propagation. It deliberately has no dependency on an OTel SDK.
type TraceContext struct {
	TraceID string
	SpanID  string
	Sampled bool
}

// Valid reports whether both ids are lowercase hex of the W3C lengths and are
// not all zero.
func (t TraceContext) Valid() bool {
	return validTraceID(t.TraceID) && validSpanID(t.SpanID)
}

func validTraceID(id string) bool {
	if len(id) != 32 || id == strings.Repeat("0", 32) {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}

func validSpanID(id string) bool {
	if len(id) != 16 || id == strings.Repeat("0", 16) {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}

// ParseTraceparent parses "00-<32hex>-<16hex>-<2hex>". A malformed or
// unsupported header is reported as absent rather than guessed.
func ParseTraceparent(value string) (TraceContext, bool) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) != 4 || parts[0] != "00" {
		return TraceContext{}, false
	}
	trace := TraceContext{TraceID: parts[1], SpanID: parts[2]}
	if !trace.Valid() || len(parts[3]) != 2 || strings.ToLower(parts[3]) != parts[3] {
		return TraceContext{}, false
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil {
		return TraceContext{}, false
	}
	trace.Sampled = flags[0]&0x01 == 1
	return trace, true
}

// Traceparent renders the context in W3C form. An invalid context renders "".
func (t TraceContext) Traceparent() string {
	if !t.Valid() {
		return ""
	}
	flags := "00"
	if t.Sampled {
		flags = "01"
	}
	return "00-" + t.TraceID + "-" + t.SpanID + "-" + flags
}

type traceContextKey struct{}

// WithTrace attaches a valid trace context. An invalid context is ignored.
func WithTrace(ctx context.Context, trace TraceContext) context.Context {
	if ctx == nil || !trace.Valid() {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey{}, trace)
}

// TraceFromContext returns the attached trace context.
func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	if ctx == nil {
		return TraceContext{}, false
	}
	trace, ok := ctx.Value(traceContextKey{}).(TraceContext)
	return trace, ok
}
