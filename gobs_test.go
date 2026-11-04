package gobs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gable/health"
	"github.com/maxid/gable/metrics"
	gbus "github.com/maxid/gbus"
	"github.com/maxid/gcache"
	gjob "github.com/maxid/gjob"
)

func newDefault(t *testing.T) (*Registry, *metrics.Registry) {
	t.Helper()
	registry := metrics.New()
	adapters := NewRegistry(registry)
	if err := Default(adapters); err != nil {
		t.Fatal(err)
	}
	return adapters, registry
}

func TestDefaultRegistry(t *testing.T) {
	adapters, _ := newDefault(t)
	want := []string{"gnety", "gmqx", "gmodbus-client", "gmodbus-server", "gbus", "gjob"}
	got := adapters.Names()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("names = %v, want %v", got, want)
	}
	if _, ok := GNety(adapters); !ok {
		t.Fatal("gnety adapter missing")
	}
	if _, ok := GMQX(adapters); !ok {
		t.Fatal("gmqx adapter missing")
	}
	if _, ok := GModbusClient(adapters); !ok {
		t.Fatal("gmodbus client adapter missing")
	}
	if _, ok := GModbusServer(adapters); !ok {
		t.Fatal("gmodbus server adapter missing")
	}
	if _, ok := GBus(adapters); !ok {
		t.Fatal("gbus adapter missing")
	}
	if _, ok := GJob(adapters); !ok {
		t.Fatal("gjob adapter missing")
	}
}

func TestCustomAdapterRegistration(t *testing.T) {
	registry := NewRegistry(metrics.New())
	type custom struct{ value int }
	if err := registry.Add("custom", func(*metrics.Registry) (any, error) {
		return &custom{value: 7}, nil
	}); err != nil {
		t.Fatal(err)
	}
	got, ok := Get[*custom](registry, "custom")
	if !ok || got.value != 7 {
		t.Fatalf("custom = %+v ok=%v", got, ok)
	}
	if _, ok := Get[*custom](registry, "missing"); ok {
		t.Fatal("missing adapter should not resolve")
	}
	if _, ok := Get[*gjobAdapter](registry, "custom"); ok {
		t.Fatal("adapter should not resolve as another type")
	}
	if err := registry.Add("custom", func(*metrics.Registry) (any, error) { return &custom{}, nil }); !errors.Is(err, ErrDuplicateAdapter) {
		t.Fatalf("duplicate err = %v", err)
	}
	if err := registry.Add("", func(*metrics.Registry) (any, error) { return &custom{}, nil }); !errors.Is(err, ErrInvalidAdapter) {
		t.Fatalf("empty name err = %v", err)
	}
	if err := registry.Add("nil", func(*metrics.Registry) (any, error) { return nil, nil }); err == nil {
		t.Fatal("nil adapter should fail")
	}
}

func TestGNetyMetrics(t *testing.T) {
	adapters, registry := newDefault(t)
	m, _ := GNety(adapters)
	m.SessionOpen("tcp")
	m.MessageIn("tcp")
	m.MessageOut("tcp")
	m.WriteFailed("tcp", "write_error")
	m.SessionClose("tcp", "closed")

	text := registry.Render()
	for _, want := range []string{
		`gnety_sessions_opened_total{transport="tcp"} 1`,
		`gnety_sessions_closed_total{transport="tcp",reason="closed"} 1`,
		`gnety_messages_in_total{transport="tcp"} 1`,
		`gnety_messages_out_total{transport="tcp"} 1`,
		`gnety_write_failures_total{transport="tcp",reason="write_error"} 1`,
		`gnety_sessions_active{transport="tcp"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

func TestGMQXMetrics(t *testing.T) {
	adapters, registry := newDefault(t)
	m, _ := GMQX(adapters)
	m.IncrementConnections()
	m.IncrementPublishedMessages()
	m.IncrementSubscribeEvents()
	m.RecordQueueDepth("offline", 7)
	m.RecordDroppedMessage("acl_publish")
	m.RecordOverflow("inflight")
	m.RecordConnections(3)
	m.DecrementSubscribeEvents()
	m.DecrementConnections()
	m.IncrementDisconnectEvents()

	text := registry.Render()
	for _, want := range []string{
		`gmqx_connections 2`,
		`gmqx_connections_total 1`,
		`gmqx_published_total 1`,
		`gmqx_subscriptions 0`,
		`gmqx_subscribe_events_total 2`,
		`gmqx_disconnects_total 1`,
		`gmqx_dropped_messages_total{reason="acl_publish"} 1`,
		`gmqx_overflows_total{sink="inflight"} 1`,
		`gmqx_queue_depth{queue="offline"} 7`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

func TestGModbusMetrics(t *testing.T) {
	adapters, registry := newDefault(t)
	client, _ := GModbusClient(adapters)
	server, _ := GModbusServer(adapters)
	client.Request(3, "ok", 10*time.Millisecond)
	server.Request(16, "exception", 20*time.Millisecond)

	text := registry.Render()
	for _, want := range []string{
		`modbus_client_requests_total{function="3",result="ok"} 1`,
		`modbus_client_request_duration_seconds_count{function="3",result="ok"} 1`,
		`modbus_server_requests_total{function="16",result="exception"} 1`,
		`modbus_server_request_duration_seconds_count{function="16",result="exception"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

func TestGBusMetrics(t *testing.T) {
	adapters, registry := newDefault(t)
	m, _ := GBus(adapters)
	m.Published("devices/secret/topic", 10, nil)
	m.Published("devices/secret/topic", 10, errors.New("broker down"))
	m.Consumed("devices/secret/topic", gbus.Ack, "")
	m.Retried("devices/secret/topic", 1)
	m.DeadLettered("devices/secret/topic", "bad_payload", errors.New("decode"))

	text := registry.Render()
	for _, want := range []string{
		`bus_published_total{result="ok"} 1`,
		`bus_published_total{result="error"} 1`,
		`bus_consumed_total{outcome="ack",reason=""} 1`,
		`bus_retried_total 1`,
		`bus_dead_lettered_total{reason="bad_payload"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
	if strings.Contains(text, "devices/secret/topic") {
		t.Fatalf("topic leaked into metric labels:\n%s", text)
	}
}

func TestGJobMetrics(t *testing.T) {
	adapters, registry := newDefault(t)
	m, _ := GJob(adapters)
	m.TriggerClaimed("nightly", gjob.TriggerCron)
	m.TriggerDeferred("nightly", "lease_held")
	m.ExecutionFinished("nightly", gjob.StateSucceeded, time.Second)
	m.LeaseLost("nightly")

	text := registry.Render()
	for _, want := range []string{
		`job_trigger_claimed_total{job="nightly",trigger="cron"} 1`,
		`job_trigger_deferred_total{job="nightly",reason="lease_held"} 1`,
		`job_executions_total{job="nightly",state="SUCCEEDED"} 1`,
		`job_execution_duration_seconds_count{job="nightly",state="SUCCEEDED"} 1`,
		`job_lease_lost_total{job="nightly"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
}

type fakeProbe struct{ err error }

func (p fakeProbe) Health(context.Context) error { return p.err }

func TestRegisterHealth(t *testing.T) {
	registry := health.New()
	registry.SetStarted(true)
	adapters := NewRegistry(metrics.New()).WithHealth(registry)
	probe := fakeProbe{}
	adapters.RegisterHealth("cache", true, probe)
	if report := registry.Ready(context.Background()); !report.OK() {
		t.Fatalf("ready = %+v", report)
	}
	probe.err = errors.New("cache down")
	adapters.RegisterHealth("cache", true, probe)
	if report := registry.Ready(context.Background()); report.OK() {
		t.Fatalf("expected down, got %+v", report)
	}
	RegisterHealth(nil, "ignored", true, probe)
	adapters.RegisterHealth("nil-probe", true, nil)
}

func TestNewRegistryWithNilMetrics(t *testing.T) {
	if NewRegistry(nil).Metrics() == nil {
		t.Fatal("expected a metrics registry")
	}
	if Default(nil) == nil {
		t.Fatal("nil registry should fail")
	}
}

func TestRegisterGCacheSnapshot(t *testing.T) {
	cache, err := gcache.New(gcache.Config{Namespace: "gobs-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	registry := NewRegistry(metrics.New())
	if err := RegisterGCache(registry, cache); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := cache.Get(ctx, "missing"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set(ctx, "key", []byte("value"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cache.Get(ctx, "key"); err != nil || !ok {
		t.Fatalf("get key ok=%v err=%v", ok, err)
	}

	text := registry.Metrics().Render()
	for _, want := range []string{
		"# TYPE cache_local_entries gauge",
		"# TYPE cache_local_bytes gauge",
		"# TYPE cache_local_hits_total counter",
		"# TYPE cache_local_misses_total counter",
		"# TYPE cache_remote_hits_total counter",
		"# TYPE cache_loads_total counter",
		"# TYPE cache_invalidations_published_total counter",
		"# TYPE cache_health_failures_total counter",
		"cache_local_hits_total 1",
		"cache_local_misses_total 1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}
	if err := RegisterGCache(registry, nil); !errors.Is(err, ErrInvalidAdapter) {
		t.Fatalf("nil cache err = %v", err)
	}
	if err := RegisterGCache(registry, cache); !errors.Is(err, ErrDuplicateAdapter) {
		t.Fatalf("duplicate cache err = %v", err)
	}
}

func TestDuplicateRegistrationDoesNotInvokeFactory(t *testing.T) {
	registry := NewRegistry(metrics.New())
	calls := 0
	factory := func(*metrics.Registry) (any, error) {
		calls++
		return &struct{}{}, nil
	}
	if err := registry.Add("once", factory); err != nil {
		t.Fatal(err)
	}
	if err := registry.Add("once", factory); !errors.Is(err, ErrDuplicateAdapter) {
		t.Fatalf("duplicate err = %v", err)
	}
	if calls != 1 {
		t.Fatalf("factory called %d times, want 1", calls)
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	const header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	trace, ok := ParseTraceparent(header)
	if !ok {
		t.Fatal("valid traceparent rejected")
	}
	if trace.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || trace.SpanID != "00f067aa0ba902b7" || !trace.Sampled {
		t.Fatalf("trace = %+v", trace)
	}
	if got := trace.Traceparent(); got != header {
		t.Fatalf("rendered = %q", got)
	}
	ctx := WithTrace(context.Background(), trace)
	from, ok := TraceFromContext(ctx)
	if !ok || from != trace {
		t.Fatalf("context = %+v ok=%v", from, ok)
	}
}

func TestTraceparentRejectsMalformed(t *testing.T) {
	for _, header := range []string{
		"",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
		"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
	} {
		if trace, ok := ParseTraceparent(header); ok {
			t.Fatalf("%q accepted: %+v", header, trace)
		}
	}
	if got := (TraceContext{}).Traceparent(); got != "" {
		t.Fatalf("invalid render = %q", got)
	}
	if ctx := WithTrace(context.Background(), TraceContext{}); ctx != context.Background() {
		// context.Background is a singleton; an invalid trace must be a no-op.
		t.Fatal("invalid trace changed the context")
	}
}

func TestTraceparentFlags(t *testing.T) {
	base := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-"
	for flags, sampled := range map[string]bool{"00": false, "01": true, "03": true} {
		trace, ok := ParseTraceparent(base + flags)
		if !ok || trace.Sampled != sampled {
			t.Fatalf("%s sampled = %v ok=%v", flags, trace.Sampled, ok)
		}
	}
}

type testSpan struct {
	ended bool
	err   error
	attrs []Attribute
	trace TraceContext
}

func (s *testSpan) End(err error)                    { s.ended, s.err = true, err }
func (s *testSpan) SetAttributes(attrs ...Attribute) { s.attrs = append(s.attrs, attrs...) }
func (s *testSpan) TraceContext() TraceContext       { return s.trace }

type testTracer struct {
	span  *testSpan
	name  string
	attrs []Attribute
}

func (t *testTracer) Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span) {
	t.name = name
	t.attrs = append(t.attrs, attrs...)
	t.span = &testSpan{trace: TraceContext{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:  "00f067aa0ba902b7",
	}}
	return WithSpan(ctx, t.span), t.span
}

func TestTracerPort(t *testing.T) {
	tracer := &testTracer{}
	ctx, span := tracer.Start(context.Background(), "op", Attribute{Key: "k", Value: "v"})
	if tracer.name != "op" {
		t.Fatalf("name = %q", tracer.name)
	}
	if got, ok := SpanFromContext(ctx); !ok || got != span {
		t.Fatal("span not attached to context")
	}
	span.SetAttributes(Attribute{Key: "a", Value: "b"})
	span.End(errors.New("boom"))
	if !tracer.span.ended || tracer.span.err == nil || len(tracer.span.attrs) != 1 {
		t.Fatalf("span = %+v", tracer.span)
	}
	if trace := span.TraceContext(); !trace.Valid() {
		t.Fatalf("trace = %+v", trace)
	}
}

func TestNoopTracerAndNilSpan(t *testing.T) {
	ctx := context.Background()
	got, span := (NoopTracer{}).Start(ctx, "op")
	if got != ctx {
		t.Fatal("noop tracer changed the context")
	}
	span.SetAttributes(Attribute{Key: "x", Value: "y"})
	span.End(errors.New("ignored"))
	if trace := span.TraceContext(); trace.Valid() {
		t.Fatalf("noop span has a trace: %+v", trace)
	}
	if ctx := WithSpan(ctx, nil); ctx != context.Background() {
		t.Fatal("nil span changed the context")
	}
}

func TestHTTPTransportInjectsTraceparent(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	trace, ok := ParseTraceparent(traceparent)
	if !ok {
		t.Fatal("invalid fixture")
	}
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &http.Client{Transport: Transport{}}
	req, _ := http.NewRequestWithContext(WithTrace(context.Background(), trace), http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got != traceparent {
		t.Fatalf("traceparent = %q, want %q", got, traceparent)
	}
}

func TestHTTPTransportWithoutTraceDoesNotAddHeader(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	resp, err := (&http.Client{Transport: Transport{}}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got != "" {
		t.Fatalf("unexpected traceparent %q", got)
	}
}

func TestBusTraceHeaders(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	trace, ok := ParseTraceparent(traceparent)
	if !ok {
		t.Fatal("invalid fixture")
	}
	headers := []gbus.Header{
		{Key: "k", Value: []byte("v")},
		{Key: TraceparentHeader, Value: []byte("stale")},
	}
	got := WithBusTrace(WithTrace(context.Background(), trace), headers)
	if len(got) != 2 {
		t.Fatalf("headers = %+v", got)
	}
	if got[0].Key != "k" || string(got[0].Value) != "v" {
		t.Fatalf("unrelated header changed: %+v", got)
	}
	if string(got[1].Value) != traceparent {
		t.Fatalf("traceparent = %q", got[1].Value)
	}
	if extracted, ok := TraceFromBusHeaders(got); !ok || extracted != trace {
		t.Fatalf("extract = %+v ok=%v", extracted, ok)
	}
	if unchanged := WithBusTrace(context.Background(), headers); len(unchanged) != 2 || string(unchanged[1].Value) != "stale" {
		t.Fatalf("no-trace context changed headers: %+v", unchanged)
	}
}

func TestTraceBusHandler(t *testing.T) {
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	parent, _ := ParseTraceparent(traceparent)
	tracer := &testTracer{}
	var child TraceContext
	var have bool
	base := gbus.HandlerFunc(func(ctx context.Context, record gbus.Record) gbus.Result {
		child, have = TraceFromContext(ctx)
		return gbus.Result{Outcome: gbus.Ack, Reason: "ok"}
	})
	handler := TraceBusHandler(base, tracer)
	result := handler.Handle(context.Background(), gbus.Record{
		Message: gbus.Message{
			Topic: "devices/events",
			Headers: []gbus.Header{
				{Key: TraceparentHeader, Value: []byte(traceparent)},
			},
		},
		Partition: 2,
		Offset:    17,
	})
	if result.Outcome != gbus.Ack || result.Reason != "ok" {
		t.Fatalf("result = %+v", result)
	}
	if !have || child != parent {
		t.Fatalf("handler parent = %+v ok=%v", child, have)
	}
	if tracer.name != "bus.consume" {
		t.Fatalf("span name = %q", tracer.name)
	}
	for _, want := range []struct{ key, value string }{
		{"messaging.destination.name", "devices/events"},
		{"messaging.partition", "2"},
		{"messaging.offset", "17"},
		{"messaging.outcome", "ack"},
		{"messaging.reason", "ok"},
	} {
		attrs := append(append([]Attribute(nil), tracer.attrs...), tracer.span.attrs...)
		if !hasAttr(attrs, want.key, want.value) {
			t.Fatalf("missing %s=%s in %+v", want.key, want.value, attrs)
		}
	}
	if !tracer.span.ended {
		t.Fatal("span was not ended")
	}
	if TraceBusHandler(nil, tracer) != nil {
		t.Fatal("nil base handler should return nil")
	}
}

func hasAttr(attrs []Attribute, key, value string) bool {
	for _, attr := range attrs {
		if attr.Key == key && attr.Value == value {
			return true
		}
	}
	return false
}
