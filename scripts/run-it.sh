#!/usr/bin/env bash
#
# 在远程测试机上运行集成测试（Testcontainers 起 MySQL / RabbitMQ Stream）。
# 前置：先执行 scripts/install-toolchain.sh 装好工具链（或自行安装 JDK 25 + Maven），
# 且本机 Docker daemon 可用。
#
# 用法：
#   scripts/run-it.sh                # 全部 IT（mvn verify -Pit）
#   scripts/run-it.sh -Dit.test=RabbitStreamIT   # 透传参数，跑单个 IT
#
set -euo pipefail

PREFIX="${TOOLCHAIN_PREFIX:-/opt/toolchain}"
if [ -d "$PREFIX" ]; then
    export JAVA_HOME="${JAVA_HOME:-$PREFIX/jdk}"
    export PATH="$PREFIX/maven/bin:$JAVA_HOME/bin:$PATH"
fi

if ! command -v mvn >/dev/null 2>&1; then
    echo "error: mvn not found — run scripts/install-toolchain.sh first" >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "error: Docker daemon not reachable — integration tests need it" >&2
    exit 1
fi

cd "$(dirname "$0")/.."
mvn -B verify -Pit "$@"
