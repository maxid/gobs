package gobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gable/health"
	"github.com/maxid/gable/metrics"
	gbus "github.com/maxid/gbus"
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
