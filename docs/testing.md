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

约定：stream / schema 名带随机后缀或按测试类独立；异步断言用 Awaitility，禁止 sleep；
容器随测试 JVM 退出由 Ryuk 回收，IT 不做显式清理。

现有 IT：

- `engine/impl/rabbitmq/RabbitStreamIT` — send/poll 往返、offset 提交语义（未提交重启重投、已提交不重投）
- `domain/repository/LeaderLockRepositoryIT` — Leader 锁条件 UPDATE 的抢锁/续约/释放/过期接管
- `engine/impl/election/LeaderElectionImplIT` — 选举生命周期（抢锁、事件、优雅关停放锁、重新接管）

待补（下一增量）：

- 引擎级 e2e：装配 Coordinator（真实 Stream + 真实 DB），Source 灌消息、Sink 到录制端，
  断言三种 DeliveryMode 的顺序/重试/排空语义与重启恢复
- 服务端 e2e：`@SpringBootTest` 随机端口 + Testcontainers，POST CloudEvent 全链路冒烟（2-3 个用例）
- MySQL Source 的 outbox 轮询（第二个带 `docs/database/source-mysql.sql` 的 schema）

## 运行环境

- 本地 / 远程测试机：需要 Docker。宿主机无 JDK 25 时用 `scripts/install-toolchain.sh`
  （从 maven 官方镜像拷贝 JDK + Maven，免包管理器），之后 `scripts/run-it.sh`（可透传
  `-Dit.test=XxxIT` 跑单个）。
- CI：`.github/workflows/tests.yml`，unit（`mvn test`，快速门禁）与 it（`mvn verify -Pit`，
  runner 自带 Docker）两个独立 job。
