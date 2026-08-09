# Magpie

Magpie 是一个基于 RabbitMQ Stream 的消息总线，负责消息的接入、分发与投递。

## 功能特点

- **多源接入**：HTTP 发布（CloudEvents 协议）、MySQL Outbox 等 Source 连接器
- **投递模式**：分区严格有序（`ORDERED`）、业务键有序（`KEY_ORDERED`）、尽力投递（`BEST_EFFORT`）
- **可靠投递**：失败消息落库异步重试、熔断保护、消费偏移量持久化
- **消息留痕**：失败消息连同消息头、消息体完整落库（`magpie_message_log`），支撑异步重试与问题追踪

## 架构示意

```txt
       发布方                                      订阅方
          │                                         ▲
          │ HTTP (CloudEvents)                      │ HTTP (CloudEvents)
          ▼                                         │
┌───────────────────────── magpie-server ──────────────────────────┐
│  ┌──────────────┐      ┌────────────────┐      ┌──────────────┐  │
│  │    Source    │      │   RabbitMQ     │      │     Sink     │  │
│  │  Connector   │─────►│    Stream      │─────►│  Connector   │  │
│  │ http / mysql │      │                │      │ http / print │  │
│  └──────────────┘      └────────────────┘      └──────────────┘  │
│                                                                  │
│   Coordinator: DB Leader 锁选主, 统一调度 Source / Sink 连接器     │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
                         数据存储 (MySQL)
                连接器配置 / 消费偏移 / 失败重试 / 消息记录
```
