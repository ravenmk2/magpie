#!/usr/bin/env bash
#
# 免包管理器安装 JDK 25 + Maven：从官方 maven 镜像中直接拷贝文件。
# 宿主机只需有 Docker（Linux x86_64、glibc），适合专用测试机。
#
# 用法：
#   scripts/install-toolchain.sh
# 可覆盖的环境变量：
#   TOOLCHAIN_IMAGE   工具链来源镜像（默认 maven:3.9.16-eclipse-temurin-25）
#   TOOLCHAIN_PREFIX  安装目录（默认 /opt/toolchain，需有写权限）
#
set -euo pipefail

IMAGE="${TOOLCHAIN_IMAGE:-maven:3.9.16-eclipse-temurin-25}"
PREFIX="${TOOLCHAIN_PREFIX:-/opt/toolchain}"

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker not found — this script only needs a Docker daemon" >&2
    exit 1
fi

echo ">> pulling $IMAGE"
docker pull "$IMAGE"

cid="$(docker create "$IMAGE")"
trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT

echo ">> installing to $PREFIX"
rm -rf "$PREFIX/jdk" "$PREFIX/maven"
mkdir -p "$PREFIX"
docker cp "$cid":/opt/java/openjdk "$PREFIX/jdk"
docker cp "$cid":/usr/share/maven "$PREFIX/maven"

cat <<EOF

Done. Add to your shell profile (e.g. ~/.bashrc):

  export JAVA_HOME="$PREFIX/jdk"
  export PATH="$PREFIX/maven/bin:\$JAVA_HOME/bin:\$PATH"

Then verify with: java -version && mvn -v
EOF
