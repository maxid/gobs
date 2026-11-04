# gobs

g\* 家族的可观测性适配库：把协议/基础设施库暴露的中立指标与健康探针，转换为一个
Prometheus registry。

> 状态：**实验性**（v0.0.0-dev）。稳定前不打 tag。License: MIT。

## 为什么单独成库

- 协议库只暴露事件 hook，不绑定 exporter，也不依赖 Prometheus/OpenTelemetry/框架；
- 应用拥有 registry、健康端点和导出器选择；
- 适配器同时需要两侧（gable registry + 各库 hook），因此放在集成层；gable 与
  协议库都不依赖 gobs。

## 依赖方向

```text
gobs ──> gable/metrics + gable/health
  └────> gnety、gmqx、gmodbus、gbus、gjob（hook 契约）
```

没有反向依赖：不用 gobs 时 gable 和协议库仍可独立使用，gobs 不是框架。

## 安装

```bash
go get github.com/maxid/gobs
```

## 用法

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

`Default` 只是参考集合；像 gcache 这种绑定实例的适配器需要显式注册（cache 由应用模块创建）。组合根也可以只注册需要的适配器：

```go
if err := gobs.RegisterGNety(obs); err != nil { return err }
gnetyCfg.Metrics, _ = gobs.GNety(obs)
```

自定义适配器使用同一张注册表：

```go
err := obs.Add("my-adapter", func(m *gablemetrics.Registry) (any, error) {
    return newMyAdapter(m), nil
})
mine, ok := gobs.Get[*myAdapter](obs, "my-adapter")
```

## 适配器与指标

| 适配器 | 指标 |
|---|---|
| `gnety` | 活跃/建立/关闭/拒绝会话、收发消息、写失败 |
| `gmqx` | 连接、发布、订阅、断开、丢弃、溢出、队列深度 |
| `gmodbus-client` / `gmodbus-server` | 按 function/result 的请求数与延迟 |
| `gbus` | 发布、消费、重试、死信 |
| `gjob` | 触发领取/延后、执行与耗时、租约丢失 |
| `gcache` | 本地 entries/bytes、local/remote 命中和未命中/错误、loader、失效、stale、health 失败、negative hit（cache 由应用模块持有，需显式注册） |
| health | 任意 `Health(context.Context) error` 探针，例如 cache |

标签保持低基数：transport、result、reason、function、outcome、job、state、
trigger、queue、sink。client ID、topic、tenant ID、对端地址、payload 永远不做标签。

## 注册表，不是服务中心

注册是显式、本地的：

- `NewRegistry` 返回空表，`Default` 只是便利集合；
- 重名返回 `ErrDuplicateAdapter`；
- `Get[T]` 做类型断言，接线错误尽早暴露；
- 没有包级 `init()`，也没有远端注册中心。

## 范围

当前包含指标与健康适配。trace/log 关联、协议级 trace 传播属于后续层；具体由
消费方设计决定。

## 开发

```bash
GOWORK=off go test ./...
GOWORK=off go test -race ./...
```
