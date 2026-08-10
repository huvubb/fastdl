#!/usr/bin/env python3
"""验证直链增强：--header/--referer/--limit/--proxy。

- 本地 HTTP 服务器记录每个请求头 + 支持 Range
- 断言 1：下载带 --header X-Test + --referer，服务器所有请求都收到
- 断言 2：--limit 512K 下载 4MB，耗时 ≥ 6s（全局限速生效）
- 断言 3：--proxy 指向死端口，请求发往代理而报错（证明代理生效）

跑：python tests/enhance_test.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BASE, "tests", "enhance_src.bin")
OUT = os.path.join(BASE, "tests", "out")
PORT = 13001
SIZE = 4 << 20  # 4MB

received: list[dict[str, str]] = []
_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        with _lock:
            received.append({k.lower(): v for k, v in self.headers.items()})
        rng = self.headers.get("Range")
        size = os.path.getsize(SRC)
        if rng and rng.startswith("bytes="):
            a, b = rng[len("bytes="):].split("-", 1)
            start = int(a)
            end = (int(b) + 1) if b else size
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end-1}/{size}")
        else:
            start, end = 0, size
            self.send_response(200)
        self.send_header("Content-Length", str(end - start))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        with open(SRC, "rb") as f:
            f.seek(start)
            remaining = end - start
            while remaining > 0:
                b = f.read(min(1 << 20, remaining))
                if not b:
                    break
                self.wfile.write(b)
                remaining -= len(b)

    def log_message(self, *a):
        pass


def dl(args, outdir):
    return subprocess.run(
        [sys.executable, "dl.py", "direct", "http://127.0.0.1:%d/enhance_src.bin" % PORT,
         "-o", outdir] + args,
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        env=dict(os.environ, PYTHONIOENCODING="utf-8"), cwd=BASE)


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    if not os.path.exists(SRC) or os.path.getsize(SRC) != SIZE:
        pattern = bytes(range(256))
        with open(SRC, "wb") as f:
            f.write((pattern * (SIZE // 256 + 1))[:SIZE])
    # 每个用例用运行唯一的新目录，避免续传缓存导致"已存在完整文件跳过下载"误报
    tag = str(int(time.time()))
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    time.sleep(0.5)

    # ---- 1) --header / --referer ----
    received.clear()
    r = dl(["--header", "X-Test: hello-fastdl",
            "--referer", "https://example.com/page", "-t", "4"],
           os.path.join(OUT, f"hdr_{tag}"))
    if r.returncode != 0:
        print("FAIL[1] 下载失败:", r.returncode, r.stderr)
        return 1
    xtest = all(h.get("x-test") == "hello-fastdl" for h in received)
    refer = all(h.get("referer") == "https://example.com/page" for h in received)
    print(f"[header] 服务器收到 {len(received)} 个请求（探针+分片）")
    print(f"         X-Test 全带: {xtest}   Referer 全带: {refer}")
    if not (xtest and refer):
        print("FAIL[1]: 自定义请求头未送达服务器")
        return 1
    print("[1] PASS: --header/--referer 生效\n")

    # ---- 2) --limit 限速（4MB @ 512K ≈ 8s）----
    received.clear()
    t0 = time.time()
    r = dl(["--limit", "512K", "-t", "4"], os.path.join(OUT, f"lim_{tag}"))
    el = time.time() - t0
    if r.returncode != 0:
        print("FAIL[2] 下载失败:", r.returncode, r.stderr)
        return 1
    ok = el >= 6.0
    print(f"[limit] 4MB 带 --limit 512K 耗时 {el:.1f}s（理论≈8s，断言≥6s），限速生效: {ok}")
    if not ok:
        print("FAIL[2]: 限速未生效（太快）")
        return 1
    print("[2] PASS: --limit 全局限速生效\n")

    # ---- 3) --proxy（死代理端口 → 请求发往代理而报错）----
    # 连 127.0.0.1:9 在 Windows 上会挂起（非立即拒绝），--timeout 5 缩短等待；
    # 判据看错误里的连接目标是代理端口 9（而非直连目标 13001），证明流量走了代理。
    r = dl(["--proxy", "http://127.0.0.1:9", "--timeout", "5"], os.path.join(OUT, f"px_{tag}"))
    text = (r.stdout + r.stderr).lower()
    went_proxy = "port=9" in text or "port 9" in text
    print(f"[proxy] 死代理端口 rc={r.returncode}，连接目标为代理端口9: {went_proxy}")
    if r.returncode == 0 or not went_proxy:
        print("FAIL[3]: 未观察到请求发往代理")
        return 1
    print("[3] PASS: --proxy 生效（请求发往代理而非直连）")

    srv.shutdown()
    print("\n直链增强验证全部通过！")
    return 0


if __name__ == "__main__":
    sys.exit(main())
