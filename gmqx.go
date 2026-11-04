package gobs

import (
	"context"
	"strconv"

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

// TraceMQTTSink wraps a broker EventSink with spans for connect/publish and
// disconnect events. The broker's context is used as the parent; MQTT 3.1.1
// has no wire metadata, so these are internal spans rather than a propagated
// end-to-end trace.
func TraceMQTTSink(base gmqxhook.EventSink, tracer Tracer) gmqxhook.EventSink {
	if base == nil {
		return nil
	}
	if tracer == nil {
		tracer = NoopTracer{}
	}
	return mqttSinkTracer{base: base, tracer: tracer}
}

type mqttSinkTracer struct {
	base   gmqxhook.EventSink
	tracer Tracer
}

func (s mqttSinkTracer) OnConnect(ctx context.Context, info gmqxhook.ConnectInfo) {
	_, span := s.tracer.Start(ctx, "mqtt.connect",
		Attribute{Key: "messaging.system", Value: "mqtt"},
	)
	defer span.End(nil)
	s.base.OnConnect(ctx, info)
}

func (s mqttSinkTracer) OnDisconnect(ctx context.Context, clientID string, err error) {
	_, span := s.tracer.Start(ctx, "mqtt.disconnect",
		Attribute{Key: "messaging.system", Value: "mqtt"},
	)
	defer span.End(err)
	s.base.OnDisconnect(ctx, clientID, err)
}

func (s mqttSinkTracer) OnConnectionLost(ctx context.Context, clientID string, err error) {
	_, span := s.tracer.Start(ctx, "mqtt.connection_lost",
		Attribute{Key: "messaging.system", Value: "mqtt"},
	)
	defer span.End(err)
	s.base.OnConnectionLost(ctx, clientID, err)
}

func (s mqttSinkTracer) OnPublish(ctx context.Context, ev gmqxhook.PublishEvent) gmqxhook.PublishResult {
	ctx, span := s.tracer.Start(ctx, "mqtt.publish",
		Attribute{Key: "messaging.system", Value: "mqtt"},
		Attribute{Key: "messaging.destination.name", Value: ev.Topic},
		Attribute{Key: "messaging.qos", Value: strconv.Itoa(int(ev.QoS))},
		Attribute{Key: "messaging.retain", Value: strconv.FormatBool(ev.Retain)},
	)
	result := s.base.OnPublish(ctx, ev)
	span.SetAttributes(Attribute{Key: "messaging.disposition", Value: strconv.Itoa(int(result.Disposition))})
	span.End(nil)
	return result
}

func (s mqttSinkTracer) OnPing(ctx context.Context, clientID string) {
	s.base.OnPing(ctx, clientID)
}
