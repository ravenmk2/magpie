# Magpie 长期 soak 环境

常驻测试环境：真实中间件拓扑（RabbitMQ Stream 三节点集群 + MySQL + 双实例 magpie-server）
上持续施加负载与故障，由 verifier 对投递语义不变量（不丢、有序、可观测的重复率）做长期裁决。

## 拓扑

```
loadgen ──publish──▶ magpie-1/2 ──▶ RabbitMQ Stream ×3 ──▶ magpie-1/2 ──deliver──▶ verifier
   │outbox 插行         ▲                                              (HTTP sink 目标)
   └──────▶ MySQL ──────┘ (mysql-poll source 轮询)
Prometheus 抓取 verifier / loadgen / magpie / rabbitmq 指标，Grafana 展示，告警规则见 prometheus/alerts.yml
```

- 三条 HTTP 链路对应三种 DeliveryMode：topic `soak-ordered` / `soak-key-ordered` / `soak-best-effort`，
  各 3 分区、3 副本（`x-initial-cluster-size`）、3 天保留
- 一条 outbox 链路（topic `soak-outbox`）覆盖 mysql-poll source
- 连接器注册见 `init/10-seed.sql`（MySQL 首次启动时随 schema 一起执行）

## 启动

前置：远程机器已装 Docker，仓库已 clone（compose 按相对路径挂载 `docs/database/*.sql`，
必须在仓库内运行）。

一键完成（构建两个镜像 → 首次复制 .env → compose up）：

```bash
scripts/soak-up.sh                # 可加 -DskipTests 跳过构建期单测；--skip-build 跳过构建
```

或分步执行：

```bash
# 1. 构建镜像（被测系统 + harness）
scripts/build-image.sh
scripts/build-soak-image.sh

# 2. 配置
cp deploy/soak/.env.example deploy/soak/.env   # 默认值即可起步

# 3. 启动
docker compose -f deploy/soak/compose.yml up -d

# 4. 确认
docker compose -f deploy/soak/compose.yml ps
docker logs -f soak-verifier                    # 等 "soak report" 周期性输出
```

## 观测

只对外暴露 **Grafana 一个端口**。默认**公开访问**：绑定 `0.0.0.0`，匿名只读（Viewer），
浏览器直接打开 `http://<远程机器>:3000`；admin 操作需登录（密码在 .env）。

### 收紧访问（可选）

- **仅 SSH 隧道**：`.env` 里 `SOAK_GRAFANA_BIND=127.0.0.1`，然后
  `ssh -L 3000:127.0.0.1:3000 <远程机器>`，浏览器打开 http://localhost:3000
- **登录才能看**：`GRAFANA_ANONYMOUS_ENABLED=false` + 强密码

注意：Grafana 走 **HTTP 明文**，凭据可被同网段嗅探——`GRAFANA_ADMIN_PASSWORD` 不要复用
重要密码；若机器上有 nginx/traefik 一类反向代理，挂在其后走 HTTPS 更稳妥。

### 指标与告警

- Grafana 自带 "Magpie Soak" 看板（匿名只读；admin 密码在 .env）
- 关键指标：`soak_lost_total` / `soak_out_of_order_total`（语义违规，恒应为 0）、
  `soak_duplicates_total`（at-least-once 的正常重复）、`soak_e2e_latency_seconds`（端到端延迟）
- Prometheus 告警规则（`prometheus/alerts.yml`）：丢失/乱序为 critical，停滞/发布持续失败为 warning。
  未接 Alertmanager，告警在 Prometheus / Grafana 界面查看

## 故障注入

手动注入（宿主机直接执行）：

```bash
deploy/soak/fault-inject.sh kill-magpie         # SIGKILL 随机一台 magpie，短暂停机后拉起
deploy/soak/fault-inject.sh restart-rabbit      # 滚动重启随机 RabbitMQ 节点
deploy/soak/fault-inject.sh bounce-mysql        # SIGKILL MySQL，短暂停机后拉起
deploy/soak/fault-inject.sh partition-rabbit 60 # 随机 RabbitMQ 节点断网 60s
deploy/soak/fault-inject.sh loop                # 循环随机注入（默认间隔 300~900s）
```

自动化注入（推荐）：compose 内置 `chaos` 服务，随环境启停、崩溃自动拉起：

```bash
# 方式一：命令行启用
docker compose -f deploy/soak/compose.yml --profile chaos up -d
# 方式二：.env 里加 COMPOSE_PROFILES=chaos，之后 up -d 即自动带上
```

- 注入间隔由 `CHAOS_MIN_INTERVAL` / `CHAOS_MAX_INTERVAL` 控制（秒，区间内随机，默认 300~900）
- 注入时间线：`docker logs -f soak-chaos`，与 Grafana 看板对照即可把语义违规归因到具体故障
- **安全警告**：chaos 容器挂载 `/var/run/docker.sock`，等于持有宿主机 Docker 全权，
  只在专用 soak 机器上启用；不要把它 profile 带进任何共享环境

注入期间观察看板：违规计数应保持为 0，重复与延迟会上升，恢复后回落。

## 健康检查与自愈

- rabbitmq / mysql / magpie / loadgen / verifier 均带 healthcheck（rabbitmq 用
  `rabbitmq-diagnostics ping`，mysql 用 `mysqladmin ping`，应用容器用
  `curl -fsS localhost:8080/actuator/health`，镜像内已装 curl）
- `autoheal` 服务（willfarrell/autoheal）每 30s 巡检，把持续 unhealthy 的容器转为重启，
  配合 `restart: unless-stopped` 与 `depends_on: service_healthy` 形成自愈闭环
- magpie 的探针是全量 `/actuator/health`（含 DB 指示器）：mysql 宕机期间 magpie 也会
  unhealthy 并被级联重启，属预期行为
- **前置**：应用探针依赖镜像内的 curl，更新 compose 后必须重新执行
  `scripts/build-image.sh` / `scripts/build-soak-image.sh`，否则探针永远失败、
  autoheal 会陷入重启循环
- **安全警告**：autoheal 与 chaos 一样挂载 `/var/run/docker.sock`（宿主机 Docker 全权），
  且 autoheal 默认常驻、不在 profile 之后——本环境整体只应在专用 soak 机器上运行

## 负载调节

发送速率与 key 数直接在 `.env` 里调（`LOADGEN_RATE_PER_SEC` / `LOADGEN_KEY_COUNT`），
改后 `docker compose -f deploy/soak/compose.yml up -d` 重建 loadgen 生效，无需重新构建镜像。

注意速率上限：链数 = topics × keyCount，每链串行"发送→等确认→间隔"，全局上限
≈ 链数 ÷ 单次发布往返延迟。例：默认 120 链、往返 10ms 时上限约 12000 msg/s；
要把目标速率提得很高，先加 `LOADGEN_KEY_COUNT` 再加 `LOADGEN_RATE_PER_SEC`。

其余参数（改 compose 里 loadgen 的 environment）：`LOADGEN_BURST_EVERY`（峰值周期，
如 1h，默认 0=关闭）、`LOADGEN_BURST_DURATION` / `LOADGEN_BURST_MULTIPLIER`、
`OUTBOX_RATE_PER_SEC` / `OUTBOX_KEY_COUNT`（outbox 链路强度）。

## 重置

```bash
docker compose -f deploy/soak/compose.yml down -v   # -v 清卷：MySQL 重跑 schema+seed，stream 重建
docker compose -f deploy/soak/compose.yml up -d
```

## 注意事项

- 改 `MYSQL_ROOT_PASSWORD` 时同步改 `init/10-seed.sql` 中 mysql-poll source 的 `password`
- Docker 日志：长跑前确认宿主机日志轮转（dockerd `log-opts max-size`），否则磁盘先被日志写满
- 测发布版镜像：.env 里把 `MAGPIE_IMAGE` 换成 `ghcr.io/<owner>/magpie:vX.Y.Z`
- harness 与被测系统版本同 repo 演进；升级仓库后重新执行两个 build 脚本再 `up -d`
