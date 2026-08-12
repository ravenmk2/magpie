#!/usr/bin/env bash
#
# 构建 magpie-server 镜像：先用工具链编译、跑单测并打包 jar，再 docker build。
# 前置：先执行 scripts/install-toolchain.sh 装好工具链（或自行安装 JDK 25 + Maven），
# 且 Docker daemon 可用。
#
# 用法：
#   scripts/build-image.sh                 # mvn clean package（含单测）+ docker build
#   scripts/build-image.sh -DskipTests     # 透传 Maven 参数，跳过测试
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

mvn -B clean package "$@"

docker build -f magpie-server/Containerfile -t local/magpie-server:latest .
