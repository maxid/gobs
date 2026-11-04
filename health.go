package gobs

import (
	"context"

	"github.com/maxid/gable/health"
)

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
