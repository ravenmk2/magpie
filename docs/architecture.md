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

- `engine.runtime` — 中枢调度（Coordinator），基于 Leader 锁选主后启停连接器
- `engine.source` — Source 连接器 SPI 及实现（`http` / `mysql` / `sample`）
- `engine.sink` — Sink 连接器 SPI 及实现（`http` / `print`），`sink.common` 为投递 worker
- `engine.stream` — Stream 抽象（生产者/消费者/注册表/偏移跟踪）
- `engine.rabbitmq` — Stream 抽象的 RabbitMQ Stream 实现
- `engine.retry` — 投递失败消息的重试存储
- `engine.lock` — 基于数据库的 Leader 锁
- `domain` — JPA 实体与 Repository（`entity` / `repository` / `converter`）
- `common` — 基础设施（`runtime` 事件循环、`util` 工具、`json`）

`magpie-server`（`ravenworks.magpie.server`）：

- `config` — 引擎装配与配置属性
- `controller` / `web` / `dto` — HTTP 发布端点与 CloudEvent 消息转换
