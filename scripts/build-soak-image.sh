#!/usr/bin/env bash
#
# 构建 magpie-soak 镜像（loadgen/verifier 两用，角色由 SPRING_PROFILES_ACTIVE 决定）。
# 前置：先执行 scripts/install-toolchain.sh 装好工具链（或自行安装 JDK 25 + Maven），
# 且 Docker daemon 可用。
#
# 用法：
#   scripts/build-soak-image.sh                 # mvn clean package（含单测）+ docker build
#   scripts/build-soak-image.sh -DskipTests     # 透传 Maven 参数，跳过测试
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
    echo "error: Docker daemon not reachable" >&2
    exit 1
fi

cd "$(dirname "$0")/.."

mvn -B clean package -pl magpie-soak -am "$@"

docker build -f magpie-soak/Containerfile -t local/magpie-soak:latest .
