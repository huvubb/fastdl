"""本地支持 Range 的 HTTP 测试服务器（可开关 Range 回退）。

用法：python tests/range_server.py <port> <file> [--no-range] [--slow]
"""
from __future__ import annotations

import argparse
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_handler(path: str, allow_range: bool, slow: bool):
    size = os.path.getsize(path)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _serve(self, start: int, end: int, code: int, headers_extra: dict):
            length = end - start
            self.send_response(code)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Range", f"bytes {start}-{end-1}/{size}")
            for k, v in headers_extra.items():
                self.send_header(k, v)
            self.end_headers()
            with open(path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    b = f.read(min(1 << 20, remaining))
                    if not b:
                        break
                    try:
                        self.wfile.write(b)
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    if slow:
                        time.sleep(0.002)  # 让多线程优势可见
                    remaining -= len(b)

        def do_GET(self):
            rng = self.headers.get("Range")
            if allow_range and rng and rng.startswith("bytes="):
                spec = rng[len("bytes="):].split(",", 1)[0]
                try:
                    a, b = spec.split("-", 1)
                    start = int(a) if a else 0
                    end = (int(b) + 1) if b else size
                except ValueError:
                    self.send_response(400)
                    self.end_headers()
                    return
                if start >= size or (end and end > size):
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.end_headers()
                    return
                end = min(end, size)
                self._serve(start, end, 206, {})
            else:
                self._serve(0, size, 200, {})

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def log_message(self, *a):
            pass

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("port", type=int)
    ap.add_argument("file")
    ap.add_argument("--no-range", action="store_true", help="无视 Range 全量 200")
    ap.add_argument("--slow", action="store_true", help="放慢响应让多线程优势可见")
    a = ap.parse_args()
    handler = make_handler(a.file, allow_range=not a.no_range, slow=a.slow)
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler)
    print(f"serve {a.file} ({os.path.getsize(a.file)}B) on :{a.port} "
          f"range={'on' if not a.no_range else 'off'} slow={a.slow}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
