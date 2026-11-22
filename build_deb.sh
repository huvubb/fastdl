#!/usr/bin/env bash
# fastdl deb 包构建脚本（在 Debian/Ubuntu 或 WSL 内运行）
# 依赖: python3-venv + pip；产物: dist-linux/fastdl_<版本>_amd64.deb
# 包内是 PyInstaller 单文件二进制（自包含），目标机只需 libc，curl 为可选推荐。
set -e
cd "$(dirname "$0")"

VER=$(.venv-wsl/bin/python -c "import fastdl; print(fastdl.__version__)" 2>/dev/null \
    || python3 -c "import fastdl; print(fastdl.__version__)")
echo "版本: $VER"

# 1) 构建单文件二进制（在本地文件系统构建，/mnt/c 上会慢）
BUILD_DIR=~/build-fastdl
rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"
cp -r fastdl dl.py fastdl.spec servers.txt requirements.txt "$BUILD_DIR/"
PY="$(pwd)/.venv-wsl/bin/python"; [ -x "$PY" ] || PY=$(command -v python3)
( cd "$BUILD_DIR" && "$PY" -m PyInstaller --clean --noconfirm fastdl.spec )

# 2) 组装 deb 目录结构
DEBDIR="$BUILD_DIR/deb/fastdl_${VER}_amd64"
rm -rf "$BUILD_DIR/deb"
mkdir -p "$DEBDIR/DEBIAN" "$DEBDIR/usr/bin" "$DEBDIR/usr/share/doc/fastdl"
cp "$BUILD_DIR/dist/fastdl" "$DEBDIR/usr/bin/fastdl"
cp LICENSE "$DEBDIR/usr/share/doc/fastdl/copyright"
cat > "$DEBDIR/DEBIAN/control" << EOF
Package: fastdl
Version: ${VER}
Section: utils
Priority: optional
Architecture: amd64
Maintainer: huvubb <810372789@qq.com>
Depends: libc6 (>= 2.17)
Recommends: curl
Description: fastdl - 多线程直链 + ed2k 完整客户端下载工具
 多连接分片下载、断点续传、GitHub 镜像加速竞速；
 速度过低时自动拉起 curl 兜底增加并行连接，恢复后宽限收队。
 支持协议: http/https/ftp 直链、ed2k。
EOF

# 3) 打包（--root-owner-group 让包内文件属主为 root，无需 fakeroot）
mkdir -p dist-linux
dpkg-deb --build --root-owner-group "$DEBDIR" "dist-linux/fastdl_${VER}_amd64.deb"
dpkg-deb -I "dist-linux/fastdl_${VER}_amd64.deb"

# 4) 验证：解包试运行
TMP=$(mktemp -d)
dpkg -x "dist-linux/fastdl_${VER}_amd64.deb" "$TMP"
"$TMP/usr/bin/fastdl" --version
rm -rf "$TMP"
echo "[OK] dist-linux/fastdl_${VER}_amd64.deb"
