#!/usr/bin/env python3
"""curl 兜底验证：对 python-requests UA 故意卡死（发 1MiB 后挂起），
对其它 UA（curl）正常供数 → 只能靠 curl 兜底跑完整个下载。

跑：python tests/curl_fallback_test.py
"""
from __future__ import annotations

import hashlib
import os
import random
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "tests", "curl_src.bin")
OUT = os.path.join(BASE, "tests", "out")
PORT = 13077
SIZE = 20 << 20  # 20MB → 5 块 × 4MiB


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        ua = self.headers.get("User-Agent", "")
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
            sent = 0
            while remaining > 0:
                b = f.read(min(1 << 20, remaining))
                self.wfile.write(b)
                sent += len(b)
                remaining -= len(b)
                # python-requests 的连接：先给 1MiB 再永久挂起（模拟 CDN 卡死）
                if "python" in ua.lower() and sent >= (1 << 20):
                    threading.Event().wait()   # 永久挂起
                time.sleep(0.05)  # 全局限速，让 fastdl 也有点压力

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
    for line in out.splitlines():
        if "curl" in line or "100.0%" in line or "分片" in line:
            print("  |", line.strip())
    return 0 if (ok and used_curl) else 1


if __name__ == "__main__":
    sys.exit(main())
