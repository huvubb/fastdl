#!/usr/bin/env python3
"""curl 兜底验证（低速触发）：服务器全局限速 ~1.2MB/s（低于 2MB/s 触发阈值），
模拟 Fastly 等按连接 QoS 的慢 CDN。速度持续过低 10s 后应自动拉 curl 接手分片。

跑：python tests/curl_fallback_test.py
"""
from __future__ import annotations

import hashlib
import os
import random
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "tests", "curl_src.bin")
OUT = os.path.join(BASE, "tests", "out")
PORT = 13077
SIZE = 20 << 20  # 20MB → 10 块 × 2MiB

# 全局令牌桶：所有连接共享 ~1.2MB/s 的出口带宽（低于 2MB/s 兜底阈值）
_bucket_lock = threading.Lock()
_bucket = [0.0]
_bucket_last = [time.monotonic()]
RATE = 1.2 * 1024 * 1024


def _take_tokens(n: int) -> None:
    while True:
        with _bucket_lock:
            now = time.monotonic()
            _bucket[0] = min(RATE, _bucket[0] + (now - _bucket_last[0]) * RATE)
            _bucket_last[0] = now
            if _bucket[0] >= n:
                _bucket[0] -= n
                return
        time.sleep(0.02)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        rng = self.headers.get("Range")
        size = os.path.getsize(SRC)
        if rng and rng.startswith("bytes="):
            a, b = rng[len("bytes="):].split("-", 1)
            start, end = int(a), (int(b) + 1) if b else size
        else:
            start, end = 0, size
        self.send_response(206 if rng else 200)
        if rng:
            self.send_header("Content-Range", f"bytes {start}-{end-1}/{size}")
        self.send_header("Content-Length", str(end - start))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        with open(SRC, "rb") as f:
            f.seek(start)
            remaining = end - start
            while remaining > 0:
                b = f.read(min(64 << 10, remaining))
                _take_tokens(len(b))
                self.wfile.write(b)
                remaining -= len(b)

    def log_message(self, *a):
        pass


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    if not os.path.exists(SRC):
        rnd = random.Random(42)
        with open(SRC, "wb") as f:
            f.write(bytes(rnd.getrandbits(8) for _ in range(SIZE // 8)) * 8)
    expect_sha = hashlib.sha256(open(SRC, "rb").read()).hexdigest()

    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    dest = os.path.join(OUT, "curl_fallback.bin")
    if os.path.exists(dest):
        os.remove(dest)
    workdir = dest + ".fastdl"
    if os.path.isdir(workdir):
        for f in os.listdir(workdir):
            os.remove(os.path.join(workdir, f))

    url = f"http://127.0.0.1:{PORT}/curl_fallback.bin"
    t0 = time.time()
    r = subprocess.run(
        [sys.executable, os.path.join(BASE, "dl.py"), "direct", url,
         "-o", OUT, "--no-resume"],
        cwd=BASE, capture_output=True, text=True, timeout=300,
        encoding="utf-8", errors="replace",
    )
    dt = time.time() - t0
    out = r.stdout or ""
    srv.shutdown()

    used_curl = "curl 兜底" in out
    ok = os.path.exists(dest) and os.path.getsize(dest) == SIZE
    actual_sha = hashlib.sha256(open(dest, "rb").read()).hexdigest() if ok else "-"
    ok = ok and actual_sha == expect_sha

    print(f"耗时 {dt:.1f}s  使用curl兜底: {used_curl}  大小/内容校验: {'PASS' if ok else 'FAIL'}")
    print(f"dl.py 退出码: {r.returncode}")
    print("---- dl.py 完整输出 ----")
    print(out)
    print("---- stderr ----")
    print(r.stderr or "(空)")
    for line in out.splitlines():
        if "curl" in line or "分片" in line:
            print("  |", line.strip())
    return 0 if (ok and used_curl) else 1


if __name__ == "__main__":
    sys.exit(main())
