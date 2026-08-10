#!/usr/bin/env python3
"""本地 mock 全链路验证：mock_server → mock_peer → dl.py ed2k。

两条路径：
  A) 成功路径    — mock_peer 按测试文件真实顶层 hash 服务 → 校验通过(exit 0)
  B) 校验拦截路径 — mock_peer 伪装成主人链接的 hash 服务 → 顶层校验拦截伪源(exit 2)

跑：python tests/full_chain_test.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

OUT_DIR = os.path.join(BASE, "tests", "out")
TESTFILE = os.path.join(BASE, "tests", "ed2k_test.bin")
TEST_SIZE = 10_020_288  # 与 SC_MSDOS622sc.exe 同大小 → 2 parts

MASTER_LINK = "ed2k://|file|SC_MSDOS622sc.exe|10020288|0B2B0878B8BBD2233D23022EE5339637|/"
MASTER_HASH = "0B2B0878B8BBD2233D23022EE5339637"


def make_test_file(path: str, size: int = TEST_SIZE) -> None:
    """确定性内容：0..255 循环字节，可复现，够 2 parts。"""
    if os.path.exists(path) and os.path.getsize(path) == size:
        return
    pattern = bytes(range(256))
    full = (pattern * ((size // len(pattern)) + 1))[:size]
    with open(path, "wb") as f:
        f.write(full)


def run(cmd: list[str], env=None) -> subprocess.CompletedProcess:
    e = dict(os.environ)
    e["PYTHONIOENCODING"] = "utf-8"
    if env:
        e.update(env)
    return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace", env=e, cwd=BASE)


def start_daemon(cmd: list[str]) -> subprocess.Popen:
    p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=BASE)
    time.sleep(1.2)  # 等服务端口就绪
    return p


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    make_test_file(TESTFILE)

    # 测试文件真实顶层 hash
    h = run([sys.executable, "dl.py", "hash", TESTFILE])
    real_hash = ""
    for line in h.stdout.splitlines():
        line = line.strip()
        if line.startswith("顶层 hash"):
            real_hash = line.split(":", 1)[1].strip()
    print(f"测试文件: {TESTFILE} ({TEST_SIZE}B)\n真实顶层 hash: {real_hash}\n")
    if not real_hash:
        print("未能算得测试文件 hash。")
        return 1

    procs: list[subprocess.Popen] = []
    rc = 0
    try:
        # ============ A) 成功路径 ============
        print("=" * 56)
        print("路径 A：真实 hash 全链路 → 期望校验通过")
        print("=" * 56)
        srvA = start_daemon([sys.executable, "tests/mock_server.py", "11001",
                             "--source", "127.0.0.1:11002"])
        peerA = start_daemon([sys.executable, "tests/mock_peer.py", "11002",
                              TESTFILE, "--hash", real_hash])
        procs += [srvA, peerA]
        linkA = f"ed2k://|file|ed2k_test.bin|{TEST_SIZE}|{real_hash}|/"
        r = run([sys.executable, "dl.py", "ed2k", linkA,
                 "--servers", "127.0.0.1:11001", "-o", OUT_DIR])
        print(r.stdout)
        print(r.stderr)
        if r.returncode == 0:
            print("[A] PASS: 校验通过，退出码 0\n")
        else:
            print(f"[A] FAIL: 退出码 {r.returncode}\n")
            rc = 1
        srvA.terminate(); peerA.terminate(); procs = []

        # ============ B) 主人链接路径（校验拦截） ============
        print("=" * 56)
        print("路径 B：主人链接 + 伪装源 → 期望顶层 hash 校验拦截伪源")
        print("=" * 56)
        srvB = start_daemon([sys.executable, "tests/mock_server.py", "12001",
                             "--source", "127.0.0.1:12002"])
        peerB = start_daemon([sys.executable, "tests/mock_peer.py", "12002",
                              TESTFILE, "--hash", MASTER_HASH])
        procs += [srvB, peerB]
        r = run([sys.executable, "dl.py", "ed2k", MASTER_LINK,
                 "--servers", "127.0.0.1:12001", "-o", OUT_DIR])
        print(r.stdout)
        print(r.stderr)
        if r.returncode == 2:
            print("[B] PASS: 校验正确拦截伪源，退出码 2\n")
        else:
            print(f"[B] WARN: 退出码 {r.returncode}（预期 2）\n")
            rc = 1
    finally:
        for p in procs:
            try:
                p.terminate()
            except OSError:
                pass

    print("全链路验证完成。")
    return rc


if __name__ == "__main__":
    sys.exit(main())
