package gobs

import (
	"github.com/maxid/gable/metrics"
	"github.com/maxid/gcache"
)

const adapterGCache = "gcache"

// RegisterGCache registers scrape-time cache metrics for one gcache instance.
// A cache is usually created by a module rather than the platform, so this is
// an explicit registration instead of part of Default.
func RegisterGCache(r *Registry, cache *gcache.Cache) error {
	if r == nil || cache == nil {
		return ErrInvalidAdapter
	}
	return r.Add(adapterGCache, func(registry *metrics.Registry) (any, error) {
		a := &gcacheAdapter{cache: cache}
		a.register(registry)
		return a, nil
	})
}

type gcacheAdapter struct {
	cache *gcache.Cache
}

func (a *gcacheAdapter) register(registry *metrics.Registry) {
	cache := a.cache

	registry.GaugeFunc("cache_local_entries", "Entries in the gcache process-local tier.", func() float64 {
		return float64(cache.LocalStats().Entries)
	})
	registry.GaugeFunc("cache_local_bytes", "Bytes held by the gcache process-local tier.", func() float64 {
		return float64(cache.LocalStats().Bytes)
	})

	registry.CounterFunc("cache_local_hits_total", "gcache local-tier hits.", func() float64 {
		return float64(cache.Metrics().LocalHits)
	})
	registry.CounterFunc("cache_local_misses_total", "gcache local-tier misses.", func() float64 {
		return float64(cache.Metrics().LocalMisses)
	})
	registry.CounterFunc("cache_local_evictions_total", "gcache local-tier LRU evictions.", func() float64 {
		return float64(cache.LocalStats().Evictions)
	})
	registry.CounterFunc("cache_local_expirations_total", "gcache local-tier TTL expirations.", func() float64 {
		return float64(cache.LocalStats().Expirations)
	})
	registry.CounterFunc("cache_remote_hits_total", "gcache shared-tier hits.", func() float64 {
		return float64(cache.Metrics().RemoteHits)
	})
	registry.CounterFunc("cache_remote_misses_total", "gcache shared-tier misses.", func() float64 {
		return float64(cache.Metrics().RemoteMisses)
	})
	registry.CounterFunc("cache_remote_errors_total", "gcache shared-tier errors.", func() float64 {
		return float64(cache.Metrics().RemoteErrors)
	})
	registry.CounterFunc("cache_loads_total", "gcache loader runs.", func() float64 {
		return float64(cache.Metrics().Loads)
	})
	registry.CounterFunc("cache_load_errors_total", "gcache loader failures.", func() float64 {
		return float64(cache.Metrics().LoadErrors)
	})
	registry.CounterFunc("cache_load_waiters_total", "gcache callers that joined an in-flight load.", func() float64 {
		return float64(cache.Metrics().LoadWaiters)
	})
	registry.CounterFunc("cache_invalidations_published_total", "gcache invalidation messages published.", func() float64 {
		return float64(cache.Metrics().InvalidationsPublished)
	})
	registry.CounterFunc("cache_invalidations_received_total", "gcache invalidation messages received.", func() float64 {
		return float64(cache.Metrics().InvalidationsReceived)
	})
	registry.CounterFunc("cache_invalidation_errors_total", "gcache invalidation failures.", func() float64 {
		return float64(cache.Metrics().InvalidationErrors)
	})
	registry.CounterFunc("cache_stale_served_total", "gcache stale-if-error responses.", func() float64 {
		return float64(cache.Metrics().StaleServed)
	})
	registry.CounterFunc("cache_health_failures_total", "gcache shared-store health failures.", func() float64 {
		return float64(cache.Metrics().HealthFailures)
	})
	registry.CounterFunc("cache_negative_hits_total", "gcache negative-cache hits.", func() float64 {
		return float64(cache.Metrics().NegativeHits)
	})
}
