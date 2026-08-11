# 架构概览

Magpie 是一个架在 RabbitMQ Stream 之上的消息总线，负责消息的接入、分发与投递治理。

## 技术栈

- **语言/运行时**：Java 25（虚拟线程）
- **构建**：Maven 多模块
- **消息骨干**：RabbitMQ Stream（rabbitmq stream-client）
- **服务框架**：Spring Boot 4（Web、Data JPA、Validation、Actuator）
- **持久化**：MySQL（JPA，DDL 见 `docs/database/schema.sql`）
- **消息协议**：CloudEvents SDK（HTTP 绑定 + JSON）
- **基础库**：Lombok、Guava、Caffeine、Jackson、java-uuid-generator、SLF4J

## Maven 模块

| 模块            | 说明                                                      |
|-----------------|-----------------------------------------------------------|
| `magpie`        | 父 POM，统一管理依赖与插件版本                            |
| `magpie-core`   | 引擎核心库：连接器体系、Stream 抽象、调度运行时、领域模型 |
| `magpie-server` | Spring Boot 启动模块：装配引擎、暴露 HTTP 接口            |

## 主要包结构

`magpie-core`（`ravenworks.magpie`）：

引擎按 `engine.api` / `engine.impl` 双树组织：前者是抽象契约（接口/SPI/枚举/记录类型/异常），后者是全部实现。

- `engine.api.stream` / `engine.impl.stream` — Stream 抽象（生产者/消费者/注册表/偏移跟踪）及其实现，`impl.rabbitmq` 为 RabbitMQ Stream 实现
- `engine.api.source` / `engine.impl.source` — Source 连接器 SPI 及实现（`http` / `mysql` / `sample`），`api.source.http` 为发布契约
- `engine.api.sink` / `engine.impl.sink` — Sink 连接器 SPI 及实现（`http` / `print`），`impl.sink.worker` 为统一投递 worker 骨架，`impl.sink.deliverer` 为按 DeliveryMode 划分的投递处置（ORDERED / KEY_ORDERED / BEST_EFFORT）；`HttpSinkConnector` 内置调谐循环按分区自主维持 SinkWorker，worker 死亡原地重建、不上报为连接器死亡
- `engine.impl.runtime` — 中枢调度（Coordinator），以 reconcile 循环按期望状态收敛 Source / Sink 连接器（Leader 事件 / 唤醒 / IDLE 节拍仅作触发器）；实际态经 `Lifecycle.isAlive()` 观测，配置未变但已死亡的连接器同样退役重建
- `engine.api.retry` / `engine.impl.retry` — 投递失败消息的重试存储
- `engine.api.election` / `engine.impl.election` — 基于数据库锁的 Leader 选举，领导权跳变经监听器回调
- `domain` — JPA 实体与 Repository（`entity` / `repository` / `converter`）
- `common` — 基础设施（`runtime` 事件循环、`util` 工具、`json`）

`magpie-server`（`ravenworks.magpie.server`）：

- `config` — 引擎装配与配置属性
- `controller` / `web` / `dto` — HTTP 发布端点与 CloudEvent 消息转换
