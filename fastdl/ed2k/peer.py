"""eMule 对等点传输协议客户端：握手 + 上传槽位 + 分块下载。

已核实的握手/传输序列：
    TCP connect
    → HELLO(0xE3/0x01) [0x10:1][hash16][id4][port2][tagcount4][tags]
    → EMULEINFO(0xC5/0x01) [ver:1=0x20][0x01:1][tagcount4][tags]
    → 收 HELLOANSWER(0xE3/0x4C)+EMULEINFO；若收到对方 HELLO(0x01) → 回 HELLOANSWER
    → STARTUPLOADREQ(0xE3/0x54)[hash16] 进上传队列
    → SETREQFILEID(0xE3/0x4F)[hash16] → 收 FILESTATUS(0x50)[hash16][count:2][bitmap]
    → REQUESTFILENAME(0xE3/0x58)[hash16] → 收 REQFILENAMEANSWER(0x59)
    → 收 QUEUERANK(0x5C)/QUEUERANKING(0xC5/0x60)
    → 收 ACCEPTUPLOADREQ(0xE3/0x55)（空 payload）→ 槽位到手
    → REQUESTPARTS(0x47)[hash16][3×start][3×end]（大文件 0xA3 用 8 字节）
    → 收 SENDINGPART(0x46)[hash16][start4][end4][data]（0xA2/0xC5/0x40 变体）
    → END_OF_DOWNLOAD(0xE3/0x49)[hash16]

不认识的消息一律跳过绝不抛；任何协议异常 → SourceGone 交调度器换源。
"""
from __future__ import annotations

import socket
import struct
import time
import zlib

from .const import (
    ET_COMPRESSION, OP_ACCEPTUPLOADREQ, OP_CANCELTRANSFER, OP_COMPRESSEDPART,
    OP_COMPRESSEDPART_I64, OP_EMULEINFO, OP_EMULEINFOANSWER, OP_END_OF_DOWNLOAD,
    OP_FILESTATUS, OP_HELLO, OP_HELLOANSWER, OP_OUTOFPARTREQS, OP_QUEUERANK,
    OP_QUEUERANKING, OP_REQUESTFILENAME, OP_REQUESTPARTS, OP_REQUESTPARTS_I64,
    OP_REQFILENAMEANSWER, OP_SENDINGPART, OP_SENDINGPART_I64, OP_SETREQFILEID,
    OP_STARTUPLOADREQ, PROTO_EDONKEY, PROTO_EMULE, T_UINT8, T_UINT16,
    build_tags, make_frame, str_val, u16_val, u32_val, u8_val,
)
from .const import FrameReader, ProtocolError

OLD_MAX_FILE = 4_290_048_000


class SourceGone(Exception):
    """源失效（协议异常 / 队列太深 / 拒绝 / 超时）→ 调度器换源。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _bitmap_to_parts(bitmap: bytes, part_count: int) -> set[int]:
    """位图 → part 索引集合；位 0 在首字节低比特（LSB-first）。"""
    parts = set()
    for i in range(part_count):
        byte = bitmap[i >> 3] if (i >> 3) < len(bitmap) else 0
        if byte & (1 << (i & 7)):
            parts.add(i)
    return parts


class PeerConnection:
    def __init__(self, ip: str, port: int, file_hash: bytes, file_size: int, cfg,
                 timeout: float = 30, dump: bool = False, max_rank: int = 200):
        self.ip = ip
        self.port = port
        self.file_hash = file_hash
        self.file_size = file_size
        self.cfg = cfg
        self.timeout = timeout
        self.dump = dump
        self.max_rank = max_rank
        self.sock: socket.socket | None = None
        self.reader: FrameReader | None = None
        self.queue_rank = 0
        self.has_parts: set[int] | None = None

    # ---------- 底层 ----------
    def connect(self) -> None:
        self.sock = socket.create_connection((self.ip, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.reader = FrameReader(self.sock)

    def _send(self, proto: int, opcode: int, payload: bytes = b"") -> None:
        if self.sock is None:
            raise SourceGone("未连接")
        frame = make_frame(proto, opcode, payload)
        if self.dump:
            print(f"[peer->{self.ip}] op={opcode:#04x} len={len(payload)} {payload[:40].hex()}")
        self.sock.sendall(frame)

    def _read(self) -> tuple[int, int, bytes]:
        if self.reader is None:
            raise SourceGone("未连接")
        proto, op, payload = self.reader.read()
        if self.dump:
            print(f"[peer<-{self.ip}] op={op:#04x} len={len(payload)} {payload[:40].hex()}")
        return proto, op, payload

    # ---------- 握手 ----------
    def handshake(self, wait: float = 15) -> None:
        hello = b"\x10" + self.cfg.userhash + (0).to_bytes(4, "little")
        hello += u16_val(self.cfg.tcp_port) + (0).to_bytes(4, "little")  # tagcount=0
        self._send(PROTO_EDONKEY, OP_HELLO, hello)

        emule_tags = build_tags([(T_UINT8, ET_COMPRESSION, u8_val(0))])
        emule = b"\x20\x01" + (1).to_bytes(4, "little") + emule_tags
        self._send(PROTO_EMULE, OP_EMULEINFO, emule)

        got_hello = got_emule = False
        deadline = time.time() + wait
        while not (got_hello and got_emule):
            if time.time() > deadline:
                raise SourceGone("握手超时")
            try:
                proto, op, payload = self._read()
            except (socket.timeout, OSError):
                raise SourceGone("握手超时")
            if op == OP_HELLOANSWER:
                got_hello = True
            elif op in (OP_EMULEINFO, OP_EMULEINFOANSWER):
                got_emule = True
            elif op == OP_HELLO:
                # 老客户端发来 HELLO → 回 HELLOANSWER（不含前导 0x10）
                if len(payload) >= 17:
                    reply = payload[1:17] + (0).to_bytes(4, "little") + u16_val(self.cfg.tcp_port)
                    reply += (0).to_bytes(4, "little")
                    self._send(PROTO_EDONKEY, OP_HELLOANSWER, reply)
            else:
                pass  # 其他消息跳过

    def announce(self, wait: float = 20) -> set[int] | None:
        """进队列 + 请求文件状态；返回源声称拥有的 part 集合（未知则 None）。"""
        self._send(PROTO_EDONKEY, OP_STARTUPLOADREQ, self.file_hash)
        self._send(PROTO_EDONKEY, OP_SETREQFILEID, self.file_hash)
        self._send(PROTO_EDONKEY, OP_REQUESTFILENAME, self.file_hash)

        deadline = time.time() + wait
        has = None
        got_accept = False
        while time.time() < deadline:
            try:
                proto, op, payload = self._read()
            except (socket.timeout, OSError):
                break
            except (ProtocolError, SourceGone):
                break
            if op == OP_FILESTATUS:
                if len(payload) >= 19:
                    part_count = struct.unpack("<H", payload[16:18])[0]
                    bitmap = payload[18:]
                    has = _bitmap_to_parts(bitmap, part_count)
            elif op == OP_QUEUERANK:
                if len(payload) >= 4:
                    self.queue_rank = struct.unpack("<I", payload[:4])[0]
                    if self.queue_rank > self.max_rank:
                        raise SourceGone(f"队列太深 rank={self.queue_rank}")
            elif op == OP_QUEUERANKING:
                if len(payload) >= 2:
                    self.queue_rank = struct.unpack("<H", payload[:2])[0]
                    if self.queue_rank > self.max_rank:
                        raise SourceGone(f"队列太深 rank={self.queue_rank}")
            elif op == OP_ACCEPTUPLOADREQ:
                got_accept = True
                break  # 槽位到手，立即结束等待（避免与对端超时赛跑）
            elif op in (OP_OUTOFPARTREQS, OP_CANCELTRANSFER):
                raise SourceGone(f"对端拒绝({op:#x})")
            # 其他消息跳过
        self.has_parts = has
        if not got_accept:
            # 没等到 ACCEPTUPLOADREQ 不代表失败——部分源直接发数据
            pass
        return has

    # ---------- 下载分块 ----------
    def fetch_part(self, start: int, length: int, timeout: float = 180) -> bytes:
        """请求 [start, start+length) 区间并收满数据。失败抛 SourceGone。"""
        large = self.file_size > OLD_MAX_FILE
        if large:
            req_op = OP_REQUESTPARTS_I64
            pack = struct.Struct("<" + "Q" * 6)
        else:
            req_op = OP_REQUESTPARTS
            pack = struct.Struct("<" + "I" * 6)
        end = start + length
        payload = self.file_hash + pack.pack(start, 0, 0, end, 0, 0)
        self._send(PROTO_EDONKEY, req_op, payload)

        buf = bytearray()
        deadline = time.time() + timeout
        while len(buf) < length:
            if time.time() > deadline:
                raise SourceGone(f"fetch_part 超时({len(buf)}/{length})")
            try:
                proto, op, payload = self._read()
            except (socket.timeout, OSError):
                raise SourceGone(f"fetch_part 读超时({len(buf)}/{length})")
            if op == OP_SENDINGPART or op == OP_SENDINGPART_I64:
                data = self._extract_sending(payload, op)
                if data:
                    buf += data
            elif op == OP_COMPRESSEDPART or op == OP_COMPRESSEDPART_I64:
                data = self._extract_compressed(payload, op)
                if data:
                    buf += data
            elif op == OP_QUEUERANK:
                if len(payload) >= 4:
                    self.queue_rank = struct.unpack("<I", payload[:4])[0]
                    if self.queue_rank > self.max_rank:
                        raise SourceGone(f"队列太深 rank={self.queue_rank}")
            elif op == OP_QUEUERANKING:
                if len(payload) >= 2:
                    self.queue_rank = struct.unpack("<H", payload[:2])[0]
                    if self.queue_rank > self.max_rank:
                        raise SourceGone(f"队列太深 rank={self.queue_rank}")
            elif op in (OP_OUTOFPARTREQS, OP_CANCELTRANSFER, OP_ACCEPTUPLOADREQ):
                pass  # 忽略
            else:
                pass  # 未知消息跳过
        return bytes(buf[:length])

    def _extract_sending(self, payload: bytes, op: int) -> bytes:
        """SENDINGPART(0x46): [hash16][start4][end4][data]；I64(0xA2) 用 8 字节。"""
        if op == OP_SENDINGPART:
            if len(payload) < 24:
                return b""
            start = struct.unpack("<I", payload[16:20])[0]
            end = struct.unpack("<I", payload[20:24])[0]
            data = payload[24:]
        else:
            if len(payload) < 32:
                return b""
            start = struct.unpack("<Q", payload[16:24])[0]
            end = struct.unpack("<Q", payload[24:32])[0]
            data = payload[32:]
        if end - start != len(data):
            raise SourceGone(f"SENDINGPART 长度不符: end-start={end-start} data={len(data)}")
        return data

    def _extract_compressed(self, payload: bytes, op: int) -> bytes:
        """COMPRESSEDPART(0xC5/0x40): [hash16][start4][size4][zlib]；I64(0xA1) start 8 字节。"""
        if op == OP_COMPRESSEDPART:
            if len(payload) < 24:
                return b""
            size = struct.unpack("<I", payload[20:24])[0]
            zdata = payload[24:]
        else:
            if len(payload) < 28:
                return b""
            size = struct.unpack("<I", payload[24:28])[0]
            zdata = payload[28:]
        try:
            data = zlib.decompress(zdata)
        except zlib.error as e:
            raise SourceGone(f"压缩数据解压失败: {e}")
        if len(data) != size:
            raise SourceGone(f"COMPRESSEDPART 解压长度不符: {len(data)} != {size}")
        return data

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None
