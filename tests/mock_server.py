"""本地 mock eD2k 服务器：验证 fastdl 客户端协议层（登录 + GETSOURCES）。

跑：python tests/mock_server.py <port> [--sources n]
按真实字节协议回包：登录回 IDCHANGE(0x40)，查源回 FOUNDSOURCES(0x42)。
"""
from __future__ import annotations

import argparse
import socket
import struct
import threading
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastdl.ed2k.const import (  # noqa: E402
    OP_IDCHANGE, OP_GETSOURCES, OP_FOUNDSOURCES, OP_LOGINREQUEST,
    OP_SERVERMESSAGE, PROTO_EDONKEY, make_frame, FrameReader,
)


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            raise EOFError
        buf += c
    return bytes(buf)


def handle_client(conn, addr, sources):
    reader = FrameReader(conn)
    conn.settimeout(15)
    try:
        proto, op, payload = reader.read()
        if op == OP_LOGINREQUEST:
            print(f"[mock] LOGINREQUEST from {addr}, {len(payload)}B")
            conn.sendall(make_frame(PROTO_EDONKEY, OP_IDCHANGE, (12345).to_bytes(4, "little")))
            conn.sendall(make_frame(PROTO_EDONKEY, OP_SERVERMESSAGE, b"Welcome to mock ed2k server"))
        while True:
            proto, op, payload = reader.read()
            if op == OP_GETSOURCES:
                print(f"[mock] GETSOURCES {len(payload)}B hash={payload[:16].hex()}")
                body = payload[:16]
                body += bytes([len(sources)])
                for ip, port in sources:
                    body += socket.inet_aton(ip)
                    body += struct.pack(">H", port)
                conn.sendall(make_frame(PROTO_EDONKEY, OP_FOUNDSOURCES, body))
                print(f"[mock] sent FOUNDSOURCES with {len(sources)} sources")
                time.sleep(1)
    except (EOFError, OSError):
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("port", type=int)
    ap.add_argument("--source", action="append", default=None,
                    help="返回的源 host:port（可重复），默认 10.0.0.x:4662")
    a = ap.parse_args()
    if a.source:
        sources = []
        for s in a.source:
            h, _, p = s.rpartition(":")
            sources.append((h, int(p)))
    else:
        sources = [(f"10.0.0.{i+1}", 4662 + i) for i in range(3)]
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", a.port))
    srv.listen(8)
    print(f"[mock] eD2k server on 127.0.0.1:{a.port}, sources={sources}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr, sources), daemon=True).start()


if __name__ == "__main__":
    main()
