#!/usr/bin/env python3
"""验证下载器在连接被中途重置时能自动重连续传（模拟 GFW/代理抖动）。

服务器行为：
  - 前 3 个请求：建立后立刻断开（模拟连接被重置，测探针/连接层重试）
  - 第 4~6 个请求：发一半数据后断开（模拟传输中途断流，测分片级续传）
  - 之后正常服务
断言：下载最终成功，且文件与源逐字节一致。

跑：python tests/flaky_test.py
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "tests", "enhance_src.bin")  # 4MB 确定性内容
OUT = os.path.join(BASE, "tests", "out", "flaky")
PORT = 14002
SIZE = os.path.getsize(SRC)

_lock = threading.Lock()
_count = {"n": 0}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _abort(self):
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def do_GET(self):
        with _lock:
            _count["n"] += 1
            n = _count["n"]
        # 前 3 次：连接建立后立即掐断
        if n <= 3:
            self._abort()
            return
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            a, b = rng[len("bytes="):].split(",", 1)[0].split("-", 1)
            start = int(a)
            end = (int(b) + 1) if b else SIZE
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end-1}/{SIZE}")
        else:
            start, end = 0, SIZE
            self.send_response(200)
        length = end - start
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        # 第 4~6 次：只发一半就断（模拟中途断流）
        half = n <= 6
        limit = length // 2 if half else length
        sent = 0
        with open(SRC, "rb") as f:
            f.seek(start)
            while sent < limit:
                b = f.read(min(1 << 16, limit - sent))
                if not b:
                    break
                try:
                    self.wfile.write(b)
                except OSError:
                    return
                sent += len(b)
        if half:
            self._abort()

    def log_message(self, *a):
        pass


def main() -> int:
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    if os.path.isdir(OUT):
        import shutil
        shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(OUT, exist_ok=True)

    r = subprocess.run(
        [sys.executable, "dl.py", "direct", f"http://127.0.0.1:{PORT}/enhance_src.bin",
         "-o", OUT, "-t", "8"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        env=dict(os.environ, PYTHONIOENCODING="utf-8"), cwd=BASE)
    srv.shutdown()
    print(f"退出码: {r.returncode}   服务器共收到 {_count['n']} 个请求（含被掐断的）")
    if r.returncode != 0:
        print("下载失败:")
        print(r.stdout[-2000:])
        print(r.stderr[-2000:])
        return 1
    got = os.path.join(OUT, "enhance_src.bin")
    if not os.path.exists(got):
        print("FAIL: 未生成目标文件")
        return 1
    a = open(SRC, "rb").read()
    b = open(got, "rb").read()
    if a != b:
        print(f"FAIL: 内容不一致 (源 {len(a)} vs 下载 {len(b)})")
        return 1
    print(f"PASS: 经历 3 次连接重置 + 3 次中途断流后，仍完整下载且内容一致 ({len(b)}B)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
