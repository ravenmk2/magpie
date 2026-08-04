# AGENTS.md

Magpie 是一个架在 RabbitMQ Stream 之上的消息总线，负责消息的接入、分发与投递治理。

## 文档索引

```
docs/
├── architecture.md          # 架构概览（技术栈、Maven 模块、包结构）
└── database/
    ├── schema.sql           # 核心库建表脚本
    └── source-mysql.sql     # MySQL Source 的 outbox 表建表脚本
```
