# AGENTS.md

Magpie 是一个架在 RabbitMQ Stream 之上的消息总线，负责消息的接入、分发与投递治理。

## 文档索引

```
docs/
├── architecture.md          # 架构概览（技术栈、Maven 模块、包结构）
├── delivery-mode.md         # 三种投递模式（ORDERED / KEY_ORDERED / BEST_EFFORT）的语义与机制
├── testing.md               # 测试分层（单测 *Test / 集成 *IT）、Testcontainers 基建与运行方式
├── database/
│   ├── schema.sql           # 核心库建表脚本
│   └── source-mysql.sql     # MySQL Source 的 outbox 表建表脚本
└── convention/
    └── java.md              # Java 编码规范（实例成员访问必须加 this. 前缀）
```

## 测试

- 单元测试：`mvn test`（surefire，`*Test`）；手写 fake，不用 Mockito。
- 集成测试：`mvn verify -Pit`（failsafe，`*IT`，Testcontainers 起 MySQL / RabbitMQ Stream，需 Docker）； 远程测试机见
  `scripts/install-toolchain.sh` / `scripts/run-it.sh`。
- 长期 soak：`magpie-testkit` 模块（loadgen/verifier 系统测试工装）+ `deploy/soak-test/`（Compose 常驻环境）， 镜像构建用
  `scripts/build-image.sh` / `scripts/build-testkit-image.sh`，操作见 `deploy/soak-test/README.md`。
- 分层约定与基建说明见 `docs/testing.md`。
