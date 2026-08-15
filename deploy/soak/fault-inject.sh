#!/usr/bin/env bash
#
# soak 环境故障注入：对运行中的环境施加真实故障，验证语义不变量在故障下是否成立。
#
# 用法：
#   deploy/soak/fault-inject.sh kill-magpie        # docker kill（SIGKILL）随机一台 magpie
#   deploy/soak/fault-inject.sh restart-rabbit     # 滚动重启随机一个 RabbitMQ 节点
#   deploy/soak/fault-inject.sh bounce-mysql       # docker kill MySQL（短暂停机后脚本拉起）
#   deploy/soak/fault-inject.sh partition-rabbit [秒]  # 随机 RabbitMQ 节点断网 N 秒（默认 30）后恢复
#   deploy/soak/fault-inject.sh random             # 随机执行上述一种
#   deploy/soak/fault-inject.sh loop [最小间隔秒] [最大间隔秒]  # 循环随机注入（默认 300~900s）
#
# 注意：docker kill 被 Docker 视为主动停止，restart 策略不会拉起容器，
# 故 kill 类故障由本脚本在短暂停机（5~30s）后显式 start 恢复；partition 由脚本恢复。
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
    local target delay
    target=$(pick MAGPIES)
    delay=$((5 + RANDOM % 26))
    echo "[fault-inject] docker kill --signal=KILL $target (down ${delay}s)"
    docker kill --signal=KILL "$target"
    # docker kill 被 Docker 视为主动停止，restart 策略不会拉起，须由脚本补 start；
    # 中间留停机窗口，让故障真实传导（SAC 接管、探针翻转、autoheal 观测）
    sleep "$delay"
    docker start "$target"
}

restart_rabbit() {
    local target
    target=$(pick RABBITS)
    echo "[fault-inject] docker restart $target"
    docker restart "$target"
}

bounce_mysql() {
    local delay=$((5 + RANDOM % 26))
    echo "[fault-inject] docker kill --signal=KILL $MYSQL (down ${delay}s)"
    docker kill --signal=KILL "$MYSQL"
    sleep "$delay"
    docker start "$MYSQL"
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
