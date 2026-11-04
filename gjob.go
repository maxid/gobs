package gobs

import (
	"time"

	"github.com/maxid/gable/metrics"
	gjob "github.com/maxid/gjob"
)

const adapterGJob = "gjob"

type gjobAdapter struct {
	claimed  *metrics.Counter
	deferred *metrics.Counter
	finished *metrics.Counter
	latency  *metrics.Histogram
	lease    *metrics.Counter
}

func newGJobAdapter(registry *metrics.Registry) *gjobAdapter {
	return &gjobAdapter{
		claimed:  registry.Counter("job_trigger_claimed_total", "Job triggers claimed.", "job", "trigger"),
		deferred: registry.Counter("job_trigger_deferred_total", "Job triggers deferred.", "job", "reason"),
		finished: registry.Counter("job_executions_total", "Job executions finished.", "job", "state"),
		latency:  registry.Histogram("job_execution_duration_seconds", "Job execution duration.", nil, "job", "state"),
		lease:    registry.Counter("job_lease_lost_total", "Job executions whose lease was lost.", "job"),
	}
}

// RegisterGJob registers the scheduler adapter.
func RegisterGJob(r *Registry) error {
	return r.Add(adapterGJob, func(registry *metrics.Registry) (any, error) {
		return newGJobAdapter(registry), nil
	})
}

// GJob returns the registered scheduler adapter.
func GJob(r *Registry) (gjob.Metrics, bool) {
	return Get[gjob.Metrics](r, adapterGJob)
}

func (m *gjobAdapter) TriggerClaimed(job string, trigger gjob.TriggerType) {
	m.claimed.With(job, string(trigger)).Inc()
}
func (m *gjobAdapter) TriggerDeferred(job, reason string) {
	m.deferred.With(job, reason).Inc()
}
func (m *gjobAdapter) ExecutionFinished(job string, state gjob.State, d time.Duration) {
	labels := []string{job, string(state)}
	m.finished.With(labels...).Inc()
	m.latency.With(labels...).Observe(d.Seconds())
}
func (m *gjobAdapter) LeaseLost(job string) { m.lease.With(job).Inc() }
