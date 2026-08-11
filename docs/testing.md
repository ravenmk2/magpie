# 测试

## 分层

| 层 | 命名 | 运行方式 | 覆盖 |
|----|------|----------|------|
| 单元测试 | `*Test`（surefire） | `mvn test` | 引擎内部逻辑，手写 fake（不用 Mockito），H2 见 `JpaTestSupport` |
| 集成测试 | `*IT`（failsafe） | `mvn verify -Pit` | 真实中间件边界：RabbitMQ Stream、MySQL（Testcontainers） |

原则：能手写 fake 的内部逻辑留在单元测试层；IT 只花在 fake 验证不了的真实边界上
（RabbitMQ offset 语义、MySQL 行锁/native query、`docs/database/schema.sql` 在真实 MySQL 上的可执行性）。

## 集成测试基建（`magpie-core/src/test/java/ravenworks/magpie/testsupport/`）

- `TestMySql` — 共享 MySQL 8.4 容器（singleton，共用一个测试库；容器 test 用户无
  CREATE DATABASE 权限），`reset()` 建表一次（`docs/database/schema.sql`，不维护副本）
  并 TRUNCATE 全部 magpie_* 表，每个测试类在 @BeforeAll 调用做隔离。
- `TestRabbitMq` — 共享 RabbitMQ 容器（singleton），`withPluginsEnabled("rabbitmq_stream")` 启用 stream 插件，
  `streamUri()` 返回与生产配置同格式的 `rabbitmq-stream://` URI。
- `TestJpa` — 最小 Spring 上下文（真实 Hibernate + Spring Data JPA，不依赖 Spring Boot），
  `hbm2ddl=none`，表结构一律来自 schema.sql。
- `RecordingSinkProvider` / `RecordingSinkHandler` — 录制型 Sink：按 http sink 同款链路装配
  （SinkWorker + 三种 DeliveryMode 的 Deliverer），handler 录制经手消息并可动态注入故障，
  供 e2e 断言投递语义（熔断不生效是刻意简化）。

约定：stream / schema 名带随机后缀或按测试类独立；异步断言用 Awaitility，禁止 sleep；
容器随测试 JVM 退出由 Ryuk 回收，IT 不做显式清理。

现有 IT：

- `engine/impl/rabbitmq/RabbitStreamIT` — send/poll 往返、offset 提交语义（未提交重启重投、已提交不重投）、
  flow credit 补给边界（消息数越过初始 credit）、stream create 幂等语义、Single Active Consumer 单活与接管
- `domain/repository/LeaderLockRepositoryIT` — Leader 锁条件 UPDATE 的抢锁/续约/释放/过期接管
- `engine/impl/election/LeaderElectionImplIT` — 选举生命周期（抢锁、事件、优雅关停放锁、重新接管）
- `engine/impl/runtime/CoordinatorE2eIT` — 引擎级 e2e：真实 Coordinator + Stream + DB，
  录制 Sink 断言三种 DeliveryMode 的顺序/失败隔离/重试排空，以及重启后 RetryStore 存量排空
- `engine/impl/runtime/CoordinatorCrashRecoveryIT` — 崩溃（非优雅停机，直接关 RabbitMQ Environment）
  恢复：未提交 offset 重投 × RetryStore 排空的交互；target 动态禁用/启用时真实 consumer
  建拆与从 committed offset 续传（禁用期零投递、重启用恰好补收积压、无全量重放）
- `engine/impl/source/mysql/MySqlPollSourceIT` — MySQL Source outbox 轮询（用
  `docs/database/source-mysql.sql` 建表）：行按 (created_at, id) 序入 stream 并删除、readLag 生效、
  发送后删除前宕机的跨实例续传（端到端 at-least-once 重复）、未提交事务行对 poller 不可见
  （含 DB 默认 `CURRENT_TIMESTAMP(3)` 路径）
- `server/ServerE2eIT`（magpie-server）— `@SpringBootTest` 随机端口 + 模块内自起 Testcontainers
  （core 测试基建不跨模块），POST CloudEvent 全链路冒烟：structured/binary 两种绑定、
  非法 topic 403、print sink 消费后 offset 提交，以及失败契约：字段超长 400、
  source 注销后 503、stream 发送失败 502

## 运行环境

- 本地 / 远程测试机：需要 Docker。宿主机无 JDK 25 时用 `scripts/install-toolchain.sh`
  （从 maven 官方镜像拷贝 JDK + Maven，免包管理器），之后 `scripts/run-it.sh`（可透传
  `-Dit.test=XxxIT` 跑单个）。
- CI：`.github/workflows/tests.yml`，unit（`mvn test`，快速门禁）与 it（`mvn verify -Pit`，
  runner 自带 Docker）两个独立 job。
