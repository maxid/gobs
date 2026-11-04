package gobs

import (
	"strconv"
	"time"

	"github.com/maxid/gable/metrics"
	gmodbushook "github.com/maxid/gmodbus/hook"
)

const (
	adapterGModbusClient = "gmodbus-client"
	adapterGModbusServer = "gmodbus-server"
)

type gmodbusAdapter struct {
	requests *metrics.Counter
	latency  *metrics.Histogram
}

func newGModbusAdapter(registry *metrics.Registry, prefix string) *gmodbusAdapter {
	return &gmodbusAdapter{
		requests: registry.Counter(prefix+"_requests_total", "Modbus requests.", "function", "result"),
		latency:  registry.Histogram(prefix+"_request_duration_seconds", "Modbus request duration.", nil, "function", "result"),
	}
}

// RegisterGModbus registers the Modbus client and server adapters.
func RegisterGModbus(r *Registry) error {
	if err := r.Add(adapterGModbusClient, func(registry *metrics.Registry) (any, error) {
		return newGModbusAdapter(registry, "modbus_client"), nil
	}); err != nil {
		return err
	}
	return r.Add(adapterGModbusServer, func(registry *metrics.Registry) (any, error) {
		return newGModbusAdapter(registry, "modbus_server"), nil
	})
}

// GModbusClient returns the registered Modbus client adapter.
func GModbusClient(r *Registry) (gmodbushook.Metrics, bool) {
	return Get[gmodbushook.Metrics](r, adapterGModbusClient)
}

// GModbusServer returns the registered Modbus server adapter.
func GModbusServer(r *Registry) (gmodbushook.ServerMetrics, bool) {
	return Get[gmodbushook.ServerMetrics](r, adapterGModbusServer)
}

func (m *gmodbusAdapter) Request(function byte, result string, d time.Duration) {
	labels := []string{strconv.Itoa(int(function)), result}
	m.requests.With(labels...).Inc()
	m.latency.With(labels...).Observe(d.Seconds())
}
