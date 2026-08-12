#!/usr/bin/env bash
#
# soak 环境故障注入：对运行中的环境施加真实故障，验证语义不变量在故障下是否成立。
#
# 用法：
#   deploy/soak/fault-inject.sh kill-magpie        # docker kill（SIGKILL）随机一台 magpie
#   deploy/soak/fault-inject.sh restart-rabbit     # 滚动重启随机一个 RabbitMQ 节点
#   deploy/soak/fault-inject.sh bounce-mysql       # docker kill MySQL（restart 策略自动拉起）
#   deploy/soak/fault-inject.sh partition-rabbit [秒]  # 随机 RabbitMQ 节点断网 N 秒（默认 30）后恢复
#   deploy/soak/fault-inject.sh random             # 随机执行上述一种
#   deploy/soak/fault-inject.sh loop [最小间隔秒] [最大间隔秒]  # 循环随机注入（默认 300~900s）
#
# 依赖 restart: unless-stopped 策略，kill 类故障由 Docker 自动恢复；partition 由脚本恢复。
#
set -euo pipefail

MAGPIES=(soak-magpie-1 soak-magpie-2)
RABBITS=(soak-rabbitmq-1 soak-rabbitmq-2 soak-rabbitmq-3)
NETWORK=magpie-soak
MYSQL=soak-mysql

pick() {
    local -n items=$1
    echo "${items[RANDOM % ${#items[@]}]}"
}

kill_magpie() {
    local target
    target=$(pick MAGPIES)
    echo "[fault-inject] docker kill --signal=KILL $target"
    docker kill --signal=KILL "$target"
}

restart_rabbit() {
    local target
    target=$(pick RABBITS)
    echo "[fault-inject] docker restart $target"
    docker restart "$target"
}

bounce_mysql() {
    echo "[fault-inject] docker kill --signal=KILL $MYSQL"
    docker kill --signal=KILL "$MYSQL"
}

partition_rabbit() {
    local seconds=${1:-30}
    local target
    target=$(pick RABBITS)
    echo "[fault-inject] partition $target from $NETWORK for ${seconds}s"
    docker network disconnect "$NETWORK" "$target"
    sleep "$seconds"
    docker network connect "$NETWORK" "$target"
    echo "[fault-inject] reconnected $target"
}

random_action() {
    local actions=(kill_magpie restart_rabbit bounce_mysql partition_rabbit)
    local action=${actions[RANDOM % ${#actions[@]}]}
    echo "[fault-inject] $(date -u +%FT%TZ) action=$action"
    "$action"
}

case "${1:-}" in
    kill-magpie) kill_magpie ;;
    restart-rabbit) restart_rabbit ;;
    bounce-mysql) bounce_mysql ;;
    partition-rabbit) partition_rabbit "${2:-30}" ;;
    random) random_action ;;
    loop)
        min=${2:-300}
        max=${3:-900}
        echo "[fault-inject] looping, interval ${min}~${max}s, Ctrl-C to stop"
        while true; do
            random_action
            sleep $((min + RANDOM % (max - min + 1)))
        done
        ;;
    *)
        sed -n '2,17p' "$0"
        exit 1
        ;;
esac
