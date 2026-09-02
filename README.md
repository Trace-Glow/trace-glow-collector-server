# Trace Glow Collector Server

基于 Gin 和 Kafka 的遥测接收服务。复制 `.env.example` 后设置环境变量：

```sh
go run ./cmd/collector
```

服务提供 `GET /healthz`、`GET /readyz` 和 `POST /v1/events`。Kafka broker 在消息确认后返回 `202`；ClickHouse 地址通过 `CLICKHOUSE_ADDR` 预留，后续 sink 接入时无需修改部署配置。

`/v1/events` 使用 contracts `f9b775e57cb4eb77ba8ec0e8321f47cbacb1bfe8` 定义的 v1 协议：标准请求通过 `X-Trace-Glow-Key` 鉴权，Beacon 请求通过 body 中的 `apiKey` 鉴权；请求可使用 gzip，服务会严格校验事件枚举、时间格式、trace/span 字段和未知字段。

## 项目结构

- `cmd/collector`：进程入口和依赖组装
- `internal/config`：环境变量配置与启动校验
- `internal/protocol`：事件协议类型和校验
- `internal/auth`：恒定时间 write key 比较
- `internal/transport/http`：Gin 路由、请求解码和响应处理
- `internal/queue`：Kafka 发布实现及可替换的 `Publisher` 接口
