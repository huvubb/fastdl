#!/usr/bin/env bash
# fastdl Linux 打包脚本（与 build_fastdl.bat 对应）
# 用法: ./build_fastdl.sh   依赖: python3 + pip install -r requirements.txt pyinstaller
set -e
cd "$(dirname "$0")"

PYTHON=${PYTHON:-python3}

echo "[1/3] Checking Python sources..."
"$PYTHON" -m py_compile dl.py fastdl/*.py fastdl/ed2k/*.py

echo "[2/3] Cleaning and packaging..."
"$PYTHON" -m PyInstaller --clean --noconfirm fastdl.spec

echo "[3/3] Verifying executable..."
./dist/fastdl --version

echo
echo "[OK] dist/fastdl created."
