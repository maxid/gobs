package gobs

import (
	"github.com/maxid/gable/metrics"
	gbus "github.com/maxid/gbus"
)

const adapterGBus = "gbus"

type gbusAdapter struct {
	published    *metrics.Counter
	consumed     *metrics.Counter
	retried      *metrics.Counter
	deadLettered *metrics.Counter
}

func newGBusAdapter(registry *metrics.Registry) *gbusAdapter {
	return &gbusAdapter{
		published:    registry.Counter("bus_published_total", "Bus messages published.", "result"),
		consumed:     registry.Counter("bus_consumed_total", "Bus messages consumed.", "outcome", "reason"),
		retried:      registry.Counter("bus_retried_total", "Bus retries."),
		deadLettered: registry.Counter("bus_dead_lettered_total", "Bus dead-lettered messages.", "reason"),
	}
}

// RegisterGBus registers the message-bus adapter.
func RegisterGBus(r *Registry) error {
	return r.Add(adapterGBus, func(registry *metrics.Registry) (any, error) {
		return newGBusAdapter(registry), nil
	})
}

// GBus returns the registered message-bus adapter.
func GBus(r *Registry) (gbus.Metrics, bool) {
	return Get[gbus.Metrics](r, adapterGBus)
}

func (m *gbusAdapter) Published(_ string, _ int, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.published.With(result).Inc()
}
func (m *gbusAdapter) Consumed(_ string, outcome gbus.Outcome, reason string) {
	m.consumed.With(string(outcome), reason).Inc()
}
func (m *gbusAdapter) Retried(_ string, _ int) { m.retried.Inc() }
func (m *gbusAdapter) DeadLettered(_ string, reason string, _ error) {
	m.deadLettered.With(reason).Inc()
}
