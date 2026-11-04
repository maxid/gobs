package gobs

import (
	"github.com/maxid/gable/metrics"
	gnetyhook "github.com/maxid/gnety/hook"
)

const adapterGNety = "gnety"

type gnetyAdapter struct {
	active   *metrics.Gauge
	opened   *metrics.Counter
	closed   *metrics.Counter
	reject   *metrics.Counter
	msgIn    *metrics.Counter
	msgOut   *metrics.Counter
	writeBad *metrics.Counter
}

func newGNetyAdapter(registry *metrics.Registry) *gnetyAdapter {
	return &gnetyAdapter{
		active:   registry.Gauge("gnety_sessions_active", "Active gnety sessions.", "transport"),
		opened:   registry.Counter("gnety_sessions_opened_total", "gnety sessions opened.", "transport"),
		closed:   registry.Counter("gnety_sessions_closed_total", "gnety sessions closed.", "transport", "reason"),
		reject:   registry.Counter("gnety_sessions_rejected_total", "gnety sessions rejected before establishment.", "transport", "reason"),
		msgIn:    registry.Counter("gnety_messages_in_total", "Messages decoded by gnety.", "transport"),
		msgOut:   registry.Counter("gnety_messages_out_total", "Messages written by gnety.", "transport"),
		writeBad: registry.Counter("gnety_write_failures_total", "gnety write failures.", "transport", "reason"),
	}
}

// RegisterGNety registers the gnety transport adapter.
func RegisterGNety(r *Registry) error {
	return r.Add(adapterGNety, func(registry *metrics.Registry) (any, error) {
		return newGNetyAdapter(registry), nil
	})
}

// GNety returns the registered gnety adapter.
func GNety(r *Registry) (gnetyhook.Metrics, bool) {
	return Get[gnetyhook.Metrics](r, adapterGNety)
}

func (m *gnetyAdapter) SessionOpen(transport string) {
	m.active.With(transport).Add(1)
	m.opened.With(transport).Inc()
}
func (m *gnetyAdapter) SessionClose(transport, reason string) {
	m.active.With(transport).Add(-1)
	m.closed.With(transport, reason).Inc()
}
func (m *gnetyAdapter) SessionRejected(transport, reason string) {
	m.reject.With(transport, reason).Inc()
}
func (m *gnetyAdapter) MessageIn(transport string)  { m.msgIn.With(transport).Inc() }
func (m *gnetyAdapter) MessageOut(transport string) { m.msgOut.With(transport).Inc() }
func (m *gnetyAdapter) WriteFailed(transport, reason string) {
	m.writeBad.With(transport, reason).Inc()
}
