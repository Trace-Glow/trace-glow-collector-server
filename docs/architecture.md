# Trace Glow Collector Server 技术栈与实现方案

> 状态：规划稿（不包含实现代码）
> contracts 上下文固定版本：`f9b775e57cb4eb77ba8ec0e8321f47cbacb1bfe8`

## 1. 目标与边界

Collector 是 SDK 遥测数据的写入边界，负责：

- 接收 `POST /v1/events` 批量请求；
- 校验 `Envelope` / `BeaconRequest` v1 协议并消费生成的 Go contracts；
- 使用项目 write key 鉴权，支持 gzip 解压；
- 执行请求大小、批量大小、速率和项目配额限制；
- 将事件可靠写入持久化队列，并返回“已持久接受”语义的 2xx；
- 异步处理、按 `(projectId, id)` 去重，并将失败事件留在可重试队列中。

Collector 不负责平台管理认证、查询 API、告警计算或事件协议定义；这些分别属于 platform-server 和 contracts 仓库。

## 2. 协议基线

- Endpoint：`POST /v1/events`。
- 标准鉴权：`X-Trace-Glow-Key`；Beacon 鉴权：Body 中的 `apiKey`。
- Content-Type：`application/json`；可选 `Content-Encoding: gzip`。
- Envelope 必填：`sentAt`、`events`。
- TelemetryEvent 必填：`schemaVersion=1`、`id`、`timestamp`、`type`、`name`、`level`、`projectId`、`sdk`、`payload`。
- 传递语义：at-least-once；下游去重键为 `(projectId, id)`。
- 默认不采集 request/response body、cookies、Authorization、URL query/fragment 或 DOM 文本。

所有共享字段以 contracts 仓库的 JSON Schema/OpenAPI/生成代码为准，Collector 不手写重复定义。

## 3. 推荐技术栈（v1）

| 层次 | 选择 | 原因 |
|---|---|---|
| 语言 | Go 1.25+ | 与 pinned contracts 模块保持工具链兼容，并适合高吞吐写入服务 |
| HTTP | Gin (`github.com/gin-gonic/gin`) | 团队指定的 Go Web 框架；提供路由、中间件、绑定和统一错误处理 |
| 协议校验 | contracts 生成 Go 类型 + JSON Schema 校验器 | 保持跨仓库协议一致；拒绝未知字段和非法枚举 |
| 配置 | 环境变量 + typed config | 适合容器部署；启动时 fail-fast 校验必填配置 |
| 日志 | `log/slog`，JSON 输出 | 标准库结构化日志，避免记录敏感数据 |
| 指标/追踪 | OpenTelemetry Go + Prometheus exporter | 与系统观测标准兼容，支持请求、队列和处理延迟指标 |
| 事实库 | ClickHouse（按时间/项目分区的事件表） | 遥测数据写入量大、追加为主，适合聚合、时间范围查询和低成本压缩 |
| 可靠接收 | Kafka/Redpanda 或 NATS JetStream | Collector 只负责上传接入；消息系统承担 durable log、重放和消费确认 |
| 搜索索引 | Elasticsearch（可选独立 sink） | 需要全文检索、字段探索或 Kibana 时启用；不是主写入依赖 |
| 测试 | Go `testing`、httptest、testcontainers-go | 覆盖协议、并发、数据库约束和故障恢复 |
| 静态检查 | `gofmt`、`go vet`、`staticcheck`、`golangci-lint` | 在 CI 中形成固定质量门禁 |

### 开源替代方案与适用条件

- HTTP：本项目采用 Gin；Chi 适合希望最大化标准库兼容性的服务，Fiber 适合明确接受非 `net/http` 生态的高吞吐服务。
- 校验：`github.com/santhosh-tekuri/jsonschema/v6` 适合严格 Draft 2020-12 校验；`go-playground/validator` 适合纯 Go 结构体约束，但不能替代 contracts JSON Schema。
- 数据库访问：Collector 不直接依赖 PostgreSQL；ClickHouse 使用官方 Go driver；不建议 v1 引入重量 ORM。
- 队列：NATS JetStream 适合低运维复杂度；Kafka 适合多消费者、分区扩展和长期保留；Redis Streams 适合已有 Redis 运维体系。无论选择哪种，都不能省略 `(projectId,id)` 幂等处理。
- 限流：单实例可用内存 token bucket；多实例严格配额使用 Redis 或网关层限流，并保留 Collector 的最终保护。

## 3.1 面向多租户 SaaS 的存储分层

类 Sentry 系统不应让 Collector 直接承担租户控制面，也不应让单个数据库同时承担海量遥测写入、全文搜索和长期归档。推荐职责如下：

| 数据类别 | 推荐存储 | 设计要点 |
|---|---|---|
| 租户、组织、项目、成员、权限、write key、配额 | SaaS platform-server | Collector 通过鉴权服务、签名凭证或只读 key registry 获取验证信息；权威数据不在 Collector 保存 |
| 事件事实库、聚合和时间范围查询 | ClickHouse | `tenant_id/project_id` 作为首要过滤维度，按事件时间分区、排序，使用 TTL 实现每租户保留策略 |
| 接收确认、重试、投递状态 | Kafka/Redpanda/NATS JetStream | 必须可恢复、可重放；Collector 不维护业务数据库状态 |
| 全文搜索和调试探索 | Elasticsearch/OpenSearch（可选） | 作为 ClickHouse 的异步投影，不作为唯一事实源；mapping 和索引生命周期按租户/环境治理 |
| 长期原始归档、冷数据 | S3/MinIO 等对象存储 | 按 `tenant_id/date` 分区写 Parquet/压缩 JSON，生命周期转冷存储，支持灾备和重放 |

### 多租户隔离策略

- v1 采用共享 ClickHouse 集群、共享表、显式 `tenant_id`/`project_id` 列；租户过滤和管理由 platform-server 负责。
- 高价值或强合规租户可升级到独立数据库、独立表或独立集群；不要一开始为每个租户创建数据库。
- write key 只映射到一个 project，project 再映射到 tenant；Collector 永远不接受客户端任意指定的租户权限。
- 配额、限流、保留期和删除请求由 platform-server 管理，并驱动 ClickHouse TTL/任务和对象存储生命周期；Collector 只执行已下发的限流配置。
- Elasticsearch/OpenSearch、ClickHouse、对象存储都保存 `tenant_id`，删除租户时按同一删除工作流清理所有投影和归档。

### 推荐数据流

```text
SDK -> Gin Collector -> 鉴权/校验 -> durable log
                                  |-> ClickHouse（主查询）
                                  |-> Elasticsearch/OpenSearch（可选搜索）
                                  `-> S3/MinIO（原始归档）
platform-server：租户控制面、write key 权威数据、配额和保留策略
```

### 为什么 ClickHouse 是主库

Sentry 类产品的主要查询是按项目、时间、事件类型、级别和标签过滤并聚合，数据模型以追加为主，ClickHouse 在压缩、列式扫描和聚合成本上更适合。Collector 本身不需要 PostgreSQL；租户和项目控制面由 platform-server 管理，Elasticsearch 也不应成为唯一事实源。

## 4. 逻辑架构

```text
SDK
  -> Gin HTTP Handler
     -> request limits / gzip / auth
     -> JSON decode + contracts validation
     -> durable log publish + broker ack
     -> 2xx
                |
                v
        Downstream consumer groups
          -> bounded processing workers
          -> idempotent processor
          -> ClickHouse / optional Elasticsearch / S3
          -> retry with backoff / dead-letter state
```

### 请求路径

1. 在读取 Body 前限制压缩后和解压后的大小，限制事件数量与 JSON 深度。
2. 校验 Content-Type、gzip、鉴权头或 Beacon Body；禁止两种凭证混用造成歧义。
3. 反序列化并按 pinned contracts 校验，检查事件 `projectId` 与 write key 所属项目一致。
4. 将原始事件发布到 Kafka/Redpanda/NATS JetStream，等待 broker 确认后返回 2xx；broker 不可用时返回 5xx，允许 SDK 重试。
5. Sink 将事件批量写入 ClickHouse，并可并行写入 Elasticsearch；任一 sink 失败只影响该 sink 的重试状态，不得阻塞或丢弃其他目的地。

### 异步处理路径

- 下游消费者使用 consumer group、ack/redelivery 和有限重试；Collector 不运行事件处理 worker。
- 处理器必须幂等；成功标记完成，暂时性错误按指数退避重试并设置上限。
- 超过重试上限进入 dead-letter 状态并告警，不得无限重试或导致进程崩溃。
- 优雅停机先停止接收，再等待有限时间完成 broker 发布确认并关闭连接；事件处理由下游消费者负责。

## 5. 数据模型（建议）

- ClickHouse `events`：`project_id`、`event_id`、`timestamp`、`type`、`level`、`name`、SDK 信息、context、payload、`received_at`；按项目和时间排序/分区，使用 TTL 控制保留期。
- Broker message key 使用 `projectId`，消息头保留 schema version；下游消费者按 `(projectId,eventId)` 做幂等，必要时维护 sink 自己的去重存储。
- ClickHouse 的 ReplacingMergeTree 去重是后台合并的最终结果，不能单独作为入口“立即唯一”保证；查询层必要时需显式去重或由可靠接收层先去重。
- 原始协议 JSON 仅在明确的数据保留策略下保存；日志和错误信息不得带 payload、凭证或 URL 敏感部分。

## 6. 限制、安全与隐私

- 默认最大请求体、解压后体积、事件数、单事件 payload 大小均配置化，并在网关与应用双重保护。
- write key 只允许写入对应项目；使用恒定时间比较或哈希后比较，避免泄露密钥。
- 认证失败、限流、校验失败分别返回 401/403、429、400；错误响应不回显凭证或完整事件。
- TLS 在部署入口终止；服务间连接和数据库连接使用加密配置。
- 只记录 request id、项目标识、批次大小、耗时和结果等低敏元数据。

## 7. 可观测性与 SLO 初稿

核心指标：接收 QPS、2xx/4xx/5xx、429、校验失败数、持久化延迟、outbox backlog、处理成功率、重试数、dead-letter 数、去重命中数。

建议初始目标：99.9% 可用性；已接受请求的 p95 响应时间 < 300ms；backlog 告警阈值按 5 分钟持续增长和最老消息年龄配置。具体数值需结合 SDK 批量和部署规模压测后确认。

## 8. 实施阶段

1. **基础骨架**：Go module、配置、`/healthz`、`/readyz`、优雅停机、CI 质量门禁。
2. **协议接收**：引入 pinned contracts 生成代码，实现 gzip、鉴权、限制和严格校验。
3. **可靠接收**：确定 Kafka/Redpanda 或 NATS JetStream；实现 broker publish confirm、重试和故障测试。
4. **分析落库**：ClickHouse 表结构、批量写入、分区/TTL、写入重试和查询去重策略。
5. **可选搜索**：实现 Elasticsearch sink、mapping/template、重放和索引生命周期管理。
6. **生产化**：OpenTelemetry、压测、容量模型、告警、备份恢复和安全审计。

每阶段都必须补充单元测试、HTTP 合约测试和故障场景测试；共享协议变化回到 contracts 仓库先完成兼容性评审。

## 9. 待确认决策

- PostgreSQL outbox 是否作为 v1 默认队列，还是部署环境已有 NATS/Kafka。
- 单租户项目配额和全局限流的目标值。
- 事件原始 payload 的保留期限、加密和删除策略。
- 处理器的首批下游目标及其幂等接口。
- Go 版本、部署平台和数据库高可用方式。
