#!/usr/bin/env bash
#
# 一键启动 soak 环境：构建镜像（magpie-server + magpie-testkit）→ 准备 .env → docker compose up。
# 前置：scripts/install-toolchain.sh 已装好工具链，且 Docker daemon 可用。
#
# 用法：
#   scripts/soak-test-up.sh                # 构建（含单测）+ 启动
#   scripts/soak-test-up.sh -DskipTests    # 透传 Maven 参数给两个构建脚本
#   scripts/soak-test-up.sh --skip-build   # 跳过构建，仅准备 .env 并启动
#
set -euo pipefail

cd "$(dirname "$0")/.."

SKIP_BUILD=false
MAVEN_ARGS=()
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        *) MAVEN_ARGS+=("$arg") ;;
    esac
done

if ! docker info >/dev/null 2>&1; then
    echo "error: Docker daemon not reachable" >&2
    exit 1
fi

if [ "$SKIP_BUILD" = false ]; then
    echo "==> build magpie-server image"
    scripts/build-image.sh ${MAVEN_ARGS[@]+"${MAVEN_ARGS[@]}"}
    echo "==> build magpie-testkit image"
    scripts/build-testkit-image.sh ${MAVEN_ARGS[@]+"${MAVEN_ARGS[@]}"}
fi

if [ ! -f deploy/soak-test/.env ]; then
    echo "==> create deploy/soak-test/.env from .env.example (edit as needed)"
    cp deploy/soak-test/.env.example deploy/soak-test/.env
fi

echo "==> docker compose up -d"
docker compose -f deploy/soak-test/compose.yml up -d

cat <<'EOF'

soak 环境已启动。常用后续：
  状态         docker compose -f deploy/soak-test/compose.yml ps
  校验器报告   docker logs -f magpie-soak-verifier
  Grafana      http://<远程机器>:3000（默认公开只读；admin 密码在 deploy/soak-test/.env）
  自动化故障注入  在 deploy/soak-test/.env 启用 COMPOSE_PROFILES=chaos，
               然后 scripts/soak-test-up.sh --skip-build 重建编排
EOF
