package gobs

import (
	"context"
	"strconv"

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

// TraceparentHeader is the W3C header key used on bus messages. It is a
// conventional key, not a driver feature: Kafka preserves headers, Redis
// Streams does not, so callers must not assume universal support.
const TraceparentHeader = "traceparent"

// WithBusTrace injects the traceparent from ctx into a copy of headers. Without
// a valid context trace the original slice is returned unchanged.
func WithBusTrace(ctx context.Context, headers []gbus.Header) []gbus.Header {
	trace, ok := TraceFromContext(ctx)
	if !ok || !trace.Valid() {
		return headers
	}
	out := make([]gbus.Header, 0, len(headers)+1)
	for _, header := range headers {
		if header.Key != TraceparentHeader {
			out = append(out, header)
		}
	}
	out = append(out, gbus.Header{Key: TraceparentHeader, Value: []byte(trace.Traceparent())})
	return out
}

// TraceFromBusHeaders extracts a W3C traceparent from bus headers.
func TraceFromBusHeaders(headers []gbus.Header) (TraceContext, bool) {
	for _, header := range headers {
		if header.Key != TraceparentHeader {
			continue
		}
		return ParseTraceparent(string(header.Value))
	}
	return TraceContext{}, false
}

// TraceBusHandler wraps a gbus Handler with one consumer span per record. The
// traceparent in the record headers is used as the parent when present.
func TraceBusHandler(base gbus.Handler, tracer Tracer) gbus.Handler {
	if base == nil {
		return nil
	}
	if tracer == nil {
		tracer = NoopTracer{}
	}
	return busTracer{base: base, tracer: tracer}
}

type busTracer struct {
	base   gbus.Handler
	tracer Tracer
}

func (h busTracer) Handle(ctx context.Context, record gbus.Record) gbus.Result {
	if trace, ok := TraceFromBusHeaders(record.Headers); ok {
		ctx = WithTrace(ctx, trace)
	}
	ctx, span := h.tracer.Start(ctx, "bus.consume",
		Attribute{Key: "messaging.system", Value: "gbus"},
		Attribute{Key: "messaging.destination.name", Value: record.Topic},
		Attribute{Key: "messaging.partition", Value: strconv.Itoa(int(record.Partition))},
		Attribute{Key: "messaging.offset", Value: strconv.FormatInt(record.Offset, 10)},
	)
	result := h.base.Handle(ctx, record)
	span.SetAttributes(
		Attribute{Key: "messaging.outcome", Value: string(result.Outcome)},
		Attribute{Key: "messaging.reason", Value: result.Reason},
	)
	span.End(nil)
	return result
}
