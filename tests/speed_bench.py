#!/usr/bin/env python3
"""实测 GitHub 下载吞吐：不同线程/分片配置对比。

跑：python tests/speed_bench.py
"""
from __future__ import annotations

import glob
import os
import subprocess
import sys
import time

URL = "https://github.com/ollama/ollama/releases/latest/download/OllamaSetup.exe"
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, "tests", "out", "bench")


def part_bytes(root: str) -> int:
    n = 0
    for f in glob.glob(os.path.join(root, "**", ".part.*"), recursive=True):
        try:
            n += os.path.getsize(f)
        except OSError:
            pass
    return n


def bench(threads: int, warmup: int = 12, window: int = 15) -> float:
    d = os.path.join(OUT, f"t{threads}")
    os.makedirs(d, exist_ok=True)
    p = subprocess.Popen([sys.executable, "dl.py", "direct", URL, "-o", d, "-t", str(threads)],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=BASE)
    time.sleep(warmup)
    a = part_bytes(d)
    time.sleep(window)
    b = part_bytes(d)
    p.terminate()
    try:
        p.wait(timeout=15)
    except Exception:  # noqa: BLE001
        p.kill()
    rate = (b - a) / window / (1 << 20)
    print(f"  线程 {threads:>3}: {rate:6.2f} MB/s   (窗口 {window}s 内下载 {(b - a) / 1048576:.0f} MB)")
    return rate


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    print("GitHub 下载吞吐实测 (OllamaSetup.exe 1.5GB):")
    for t in (8, 16, 32, 64):
        bench(t)
