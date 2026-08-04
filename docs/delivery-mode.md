# 投递模式（DeliveryMode）

Sink 连接器通过 `deliveryMode` 属性选择投递模式：`ORDERED` / `KEY_ORDERED` / `BEST_EFFORT`，缺省或非法值回落 `ORDERED`。
三种模式 **都保证最少一次投递（at-least-once）**：消息要么投递成功，要么无限重试或重启后重投，可能重复送达，下游必须幂等。
区别只在顺序保证与失败消息的处理方式。线程或进程异常退出时不影响顺序保证。

## 模式对照

|            | ORDERED                      | KEY_ORDERED                                    | BEST_EFFORT        |
|------------|------------------------------|------------------------------------------------|--------------------|
| 顺序保证   | 分区内严格有序               | 同 BusinessKey 按 Offset 顺序，跨 Key 互不影响 | 不保证顺序         |
| 失败消息   | 原地重试，**不落库、不跳过** | 落 RetryStore，同 Key 后续消息分流落库         | 落 RetryStore      |
| 失败影响   | 阻塞整个流                   | 只阻塞失败的 Key                               | 无阻塞             |
| RetryStore | 不使用                       | 使用                                           | 使用               |
| 重试节奏   | 200ms 原地 + 熔断器限速      | 5s×2^(n-1) 指数退避，封顶 5min                 | 同 KEY_ORDERED     |
| 适用场景   | 分区内强顺序、可容忍停顿     | 实体级顺序（订单/账户等维度）                  | 顺序无关、追求吞吐 |

## ORDERED 模式

分区内严格有序，逐条处理，只有成功才前进。

- `FAILURE`、`BACKOFF`（熔断开启）、Handler 抛异常，一律原地重试，节奏 200ms；连续失败由熔断器接管进一步限速。Handler 以 `maxAttempts=-1` 装配，Handler 内不设次数上限。
- 失败消息 **永远不存入 RetryStore**，不跳过、不提交 Offset；重启后从未提交的 Offset 处继续重试。
- 单条消息失败时阻塞后续所有消息的投递，换取最强顺序保证。投递目标长期故障时整个 Sink 停滞（受熔断器节制，不会空转）。

## KEY_ORDERED 模式

任何情况下同一 BusinessKey 的消息按 Offset 顺序完成投递，不同 Key 并行互不影响。
Null BusinessKey 归一为空字符串，即无 Key 消息视为同一条队列。

正常路径（NORMAL 模式）：

- 失败消息 **先落 RetryStore 再推进水位**：不落库就不提交 offset，保证最少一次。
- 某 Key 失败后进入 `BlockedKeys`，后续同 Key 消息不再尝试投递，直接分流进 RetryStore，排在已存消息之后；其他 Key 的正常投递不受影响。

重试路径（RETRYING 模式）：

- 流空闲（连续 5 次空轮询）时触发，按 `retryAt` 到期并且 Offset 升序取出重试项，按 Key 分组逐批重投；该批有任何失败则切换回 NORMAL 等下一轮，全部排空后解除 Key 阻塞。
- 重试退避：5s × 2^(attempts-1)，封顶 5min，**无重试次数上限、无 DLQ（永久停放）**。
- 顺序不变性由仓储层维持：新落库条目的 `retryAt` 不早于同 Key 更老条目；重试失败退避时把更晚的同 Key 条目一并推后。因此任何时刻（含进程重启后）同 Key 都按 Offset 顺序重试，不存在新消息越过退避中旧消息抢先投递的窗口。

## BEST_EFFORT 模式

不关心顺序，整批发送追求吞吐：

- 失败消息同样先落 RetryStore 再推进水位，最少一次保证与 KEY_ORDERED 一致。
- 启动时先进入 RETRYING 排空存量重试消息，此后的重试触发、退避、存储不变式与 KEY_ORDERED 完全相同（无 DLQ、落库瞬断原地重试）。

## 公共机制

- **Offset 提交**：先处理（或持久化）再推进水位最后 Commit；正常关闭时提交最后 Offset。未提交的消息在重启后由 Stream 重新投递，这是重启恢复的兜底。
- **落库瞬断**：`saveWithRetry` 每秒原地重试直到成功（DB 故障期 Commit 同样写不了库，停顿无损）；EventLoop 关闭中则放弃，Offset 未提交等重启重投。
- **Worker 循环健壮性**：Poll/处理/状态更新抛异常时停顿 1s 后继续，不中断轮询、不空转。
- **HTTP Sink Handler 失败分类**：
    - 系统性失败（连接/IO 错误、非法 URL 等 RuntimeException、5xx/408/429 等可重试状态码）：记熔断、按退避重试，受 `retry.inplaceAttempts` 约束；
    - 消息自身非法（CloudEvent 序列化失败）：只本条 `FAILURE`，不计熔断、不重试；
    - 不可重试状态码：直接 `FAILURE`；
    - 熔断开启期间：返回 `BACKOFF`，由 Worker 层处理。

## 已知限制

- KEY_ORDERED / BEST_EFFORT 的重试只在流空闲时触发：持续大流量下失败消息的重试会推迟到流量间隙（不违反最少一次，仅影响重试及时性）。
- 三种模式都是最少一次，重复投递是语义的一部分，不提供恰好一次（exactly-once）。
