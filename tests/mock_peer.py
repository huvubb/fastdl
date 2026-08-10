"""本地 mock eMule 对等点：用真实传输协议服务本地文件，供 peer.py/transfer.py 测试。

跑：python tests/mock_peer.py <port> <file> [--hash <hex>] [--compressed] [--queue-rank N]
按协议：HELLO→HELLOANSWER+EMULEINFO；SETREQFILEID→FILESTATUS；ACCEPTUPLOADREQ；
REQUESTPARTS→SENDINGPART；END_OF_DOWNLOAD 结束。
"""
from __future__ import annotations

import argparse
import os
import socket
import struct
import sys
import threading
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastdl.ed2k.const import (  # noqa: E402
    OP_HELLO, OP_HELLOANSWER, OP_EMULEINFO, OP_EMULEINFOANSWER, OP_STARTUPLOADREQ,
    OP_SETREQFILEID, OP_REQUESTFILENAME, OP_FILESTATUS, OP_ACCEPTUPLOADREQ,
    OP_REQUESTPARTS, OP_SENDINGPART, OP_END_OF_DOWNLOAD, PROTO_EDONKEY, PROTO_EMULE,
    OP_QUEUERANK, OP_QUEUERANKING, make_frame, FrameReader, PARTSIZE,
)


class MockPeer:
    def __init__(self, path: str, file_hash: bytes, compressed: bool, queue_rank: int):
        self.path = path
        self.size = os.path.getsize(path)
        self.file_hash = file_hash
        self.compressed = compressed
        self.queue_rank = queue_rank
        self.n_parts = (self.size + PARTSIZE - 1) // PARTSIZE if self.size else 0

    def handle(self, conn):
        reader = FrameReader(conn)
        conn.settimeout(60)  # 放宽，避免与客户端 announce 等待赛跑
        try:
            proto, op, payload = reader.read()
            if op == OP_HELLO:
                # 回 HELLOANSWER（[hash16][id4][port2][tagcount4][tags]）
                conn.sendall(make_frame(PROTO_EDONKEY, OP_HELLOANSWER,
                                        b"\x11" * 16 + struct.pack("<IH", 0, 4662) + struct.pack("<I", 0)))
                # 回 EMULEINFO
                conn.sendall(make_frame(PROTO_EMULE, OP_EMULEINFO, b"\x20\x01" + struct.pack("<I", 0)))
            # 循环处理
            filestatus_sent = False
            accept_sent = False
            while True:
                proto, op, payload = reader.read()
                if op == OP_EMULEINFO:
                    conn.sendall(make_frame(PROTO_EMULE, OP_EMULEINFOANSWER, b"\x20\x01" + struct.pack("<I", 0)))
                elif op == OP_STARTUPLOADREQ:
                    pass
                elif op == OP_SETREQFILEID:
                    if not filestatus_sent:
                        bitmap = bytes([0xFF]) * ((self.n_parts + 7) // 8)
                        body = self.file_hash + struct.pack("<H", self.n_parts) + bitmap
                        conn.sendall(make_frame(PROTO_EDONKEY, OP_FILESTATUS, body))
                        filestatus_sent = True
                    conn.sendall(make_frame(PROTO_EDONKEY, OP_ACCEPTUPLOADREQ, b""))
                    accept_sent = True
                elif op == OP_REQUESTFILENAME:
                    name = os.path.basename(self.path).encode()
                    conn.sendall(make_frame(PROTO_EDONKEY, 0x59, self.file_hash + struct.pack("<I", len(name)) + name))
                elif op == OP_REQUESTPARTS or op == 0xA3:  # 0xA3 = REQUESTPARTS_I64
                    if not accept_sent:
                        conn.sendall(make_frame(PROTO_EDONKEY, OP_ACCEPTUPLOADREQ, b""))
                        accept_sent = True
                    if self.queue_rank:
                        conn.sendall(make_frame(PROTO_EDONKEY, OP_QUEUERANK, struct.pack("<I", self.queue_rank)))
                    if op == 0xA3:
                        starts = struct.unpack("<3Q", payload[16:40])
                        ends = struct.unpack("<3Q", payload[40:64])
                    else:
                        starts = struct.unpack("<3I", payload[16:28])
                        ends = struct.unpack("<3I", payload[28:40])
                    start, end = starts[0], ends[0]
                    if end == 0 and starts[1] and ends[1]:
                        start, end = starts[1], ends[1]
                    if end - start > 0:
                        with open(self.path, "rb") as f:
                            f.seek(start)
                            data = f.read(end - start)
                        if self.compressed:
                            body = self.file_hash + struct.pack("<II", start, len(data)) + zlib.compress(data)
                            conn.sendall(make_frame(PROTO_EMULE, 0x40, body))
                        else:
                            body = self.file_hash + struct.pack("<II", start, end) + data
                            conn.sendall(make_frame(PROTO_EDONKEY, OP_SENDINGPART, body))
                elif op == OP_END_OF_DOWNLOAD:
                    break
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
    ap.add_argument("file")
    ap.add_argument("--hash", default=None, help="文件 ed2k 顶层 hash（hex，默认用文件名生成假 hash）")
    ap.add_argument("--compressed", action="store_true", help="用 COMPRESSEDPART 回数据")
    ap.add_argument("--queue-rank", type=int, default=0, help="回 QUEUERANK 值")
    a = ap.parse_args()
    fh = bytes.fromhex(a.hash) if a.hash else (os.path.basename(a.file).encode()[:16].ljust(16, b"\x00"))
    mp = MockPeer(a.file, fh, a.compressed, a.queue_rank)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", a.port))
    srv.listen(8)
    print(f"[mock-peer] {a.file} ({mp.size}B, {mp.n_parts} parts) on 127.0.0.1:{a.port} "
          f"compressed={a.compressed}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=mp.handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
