package gobs

import (
	"github.com/maxid/gable/metrics"
	gmqxhook "github.com/maxid/gmqx/hook"
)

const adapterGMQX = "gmqx"

type gmqxAdapter struct {
	connections *metrics.Gauge
	accepted    *metrics.Counter
	published   *metrics.Counter
	subs        *metrics.Gauge
	subEvents   *metrics.Counter
	disconnects *metrics.Counter
	dropped     *metrics.Counter
	overflows   *metrics.Counter
	queueDepth  *metrics.Gauge
}

func newGMQXAdapter(registry *metrics.Registry) *gmqxAdapter {
	return &gmqxAdapter{
		connections: registry.Gauge("gmqx_connections", "Current MQTT connections."),
		accepted:    registry.Counter("gmqx_connections_total", "MQTT connections accepted."),
		published:   registry.Counter("gmqx_published_total", "MQTT PUBLISH packets accepted."),
		subs:        registry.Gauge("gmqx_subscriptions", "Active MQTT subscriptions."),
		subEvents:   registry.Counter("gmqx_subscribe_events_total", "MQTT subscribe/unsubscribe events."),
		disconnects: registry.Counter("gmqx_disconnects_total", "MQTT disconnects."),
		dropped:     registry.Counter("gmqx_dropped_messages_total", "Messages the broker refused or dropped.", "reason"),
		overflows:   registry.Counter("gmqx_overflows_total", "Broker limits hit without losing a message.", "sink"),
		queueDepth:  registry.Gauge("gmqx_queue_depth", "Messages currently queued by the broker.", "queue"),
	}
}

// RegisterGMQX registers the gmqx broker adapter.
func RegisterGMQX(r *Registry) error {
	return r.Add(adapterGMQX, func(registry *metrics.Registry) (any, error) {
		return newGMQXAdapter(registry), nil
	})
}

// GMQX returns the registered gmqx adapter.
func GMQX(r *Registry) (gmqxhook.MetricsManager, bool) {
	return Get[gmqxhook.MetricsManager](r, adapterGMQX)
}

func (m *gmqxAdapter) IncrementConnections() {
	m.connections.Inc()
	m.accepted.Inc()
}
func (m *gmqxAdapter) DecrementConnections()       { m.connections.Dec() }
func (m *gmqxAdapter) IncrementPublishedMessages() { m.published.Inc() }
func (m *gmqxAdapter) IncrementSubscribeEvents() {
	m.subs.Inc()
	m.subEvents.Inc()
}
func (m *gmqxAdapter) DecrementSubscribeEvents() {
	m.subs.Dec()
	m.subEvents.Inc()
}
func (m *gmqxAdapter) IncrementDisconnectEvents() { m.disconnects.Inc() }
func (m *gmqxAdapter) RecordDroppedMessage(reason string) {
	m.dropped.With(reason).Inc()
}
func (m *gmqxAdapter) RecordOverflow(sink string) { m.overflows.With(sink).Inc() }
func (m *gmqxAdapter) RecordQueueDepth(queue string, depth int) {
	m.queueDepth.With(queue).Set(float64(depth))
}
func (m *gmqxAdapter) RecordConnections(count int) { m.connections.Set(float64(count)) }
