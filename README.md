# gobs

Observability adapters for the g\* family: it turns the neutral metric and
health hooks exposed by protocol and infrastructure libraries into one
Prometheus registry.

> Status: **experimental** (v0.0.0-dev). No tags are published until the API is
> stable. License: MIT.

## Why a separate library

- Protocol libraries expose event hooks, not an exporter. They do not depend on
  Prometheus, OpenTelemetry or a framework.
- Applications own the registry, health endpoint and exporter choice.
- The adapters need both sides (gable's registry + each library's hook), so they
  live in this integration layer. Neither gable nor a protocol library depends
  on gobs.

## Dependency direction

```text
gobs ──> gable/metrics + gable/health
  └────> gnety, gmqx, gmodbus, gbus, gjob (hook contracts)
```

No reverse dependency: gable and the protocol libraries stay usable without
gobs, and gobs is not a framework.

## Install

```bash
go get github.com/maxid/gobs
```

## Usage

```go
registry := gablemetrics.New()
obs := gobs.NewRegistry(registry).WithHealth(healthRegistry)
if err := gobs.Default(obs); err != nil {
    return err
}

gnetyCfg.Metrics, _ = gobs.GNety(obs)
gmqxCfg.Metrics, _ = gobs.GMQX(obs)
busCfg.Metrics, _ = gobs.GBus(obs)
jobCfg.Metrics, _ = gobs.GJob(obs)
modbusCfg.Metrics, _ = gobs.GModbusClient(obs)
modbusServerCfg.Metrics, _ = gobs.GModbusServer(obs)
obs.RegisterHealth("cache", true, cache)
if err := gobs.RegisterGCache(obs, cache); err != nil { return err }
```

`Default` is only the reference set. Instance-bound adapters such as `gcache` are registered explicitly because the cache is created by an application module, not by gobs. A composition root can register a subset:

```go
if err := gobs.RegisterGNety(obs); err != nil { return err }
gnetyCfg.Metrics, _ = gobs.GNety(obs)
```

Custom adapters use the same registry:

```go
err := obs.Add("my-adapter", func(m *gablemetrics.Registry) (any, error) {
    return newMyAdapter(m), nil
})
mine, ok := gobs.Get[*myAdapter](obs, "my-adapter")
```

## Adapters and metrics

| Adapter | Metrics |
|---|---|
| `gnety` | sessions active/opened/closed/rejected, messages in/out, write failures |
| `gmqx` | connections, published, subscriptions, disconnects, dropped, overflow, queue depth |
| `gmodbus-client` / `gmodbus-server` | requests and latency by function/result |
| `gbus` | published, consumed, retried, dead-lettered |
| `gjob` | trigger claimed/deferred, executions and duration, lease lost |
| `gcache` | local entries/bytes, local/remote hits/misses/errors, loads, invalidations, stale, health failures, negative hits (registered explicitly because a cache instance is application-owned) |
| health | any `Health(context.Context) error` probe (for example a cache) |

Metric labels are bounded: transport, result, reason, function, outcome, job,
state, trigger, queue, sink. Client IDs, topics, tenant IDs, peer addresses and
payloads are never labels.

## Registry, not a service registry

Registration is explicit and local:

- `NewRegistry` returns an empty registry; `Default` is a convenience;
- duplicate names fail with `ErrDuplicateAdapter`;
- `Get[T]` is a type assertion, so wiring errors fail early;
- there is no package-level `init()` and no remote registration center.

## Scope

gobs currently adapts metrics and health. Trace/log correlation and protocol
trace propagation belong to later layers; see the project design notes where
this library is consumed.

## Development

```bash
GOWORK=off go test ./...
GOWORK=off go test -race ./...
```
