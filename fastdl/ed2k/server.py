"""eD2k 服务器协议客户端：登录（tag 式 + 旧式变体 B）+ GETSOURCES 查源。

帧格式见 const.py：`[proto:1][size:4 LE][opcode:1][payload]`。
服务器登录（tag 式，已核实）：
    [hash16][id4=0][port:2][tagcount4][tags]
    名字/版本都是 tag：CT_NAME(0x01)=str "fastdl"，CT_VERSION(0x11)=u32 0x3C，
    CT_SERVER_FLAGS(0x20)=u32 0x011D，CT_EMULE_VERSION(0xFB)=u32 1。
旧式变体 B（tag 式被断时）：[hash16][id4][port2][name\0][version4=0x3C]。
查源：GETSOURCES(0x19) = [hash16][size:4]（>4GB: [hash16][0:4][size:8]）。
回包：FOUNDSOURCES(0x42) = [hash16][count:1][count × (ip:4, port:2)]。
"""
from __future__ import annotations

import socket
import struct
import time

from .const import (
    CT_EMULE_VERSION, CT_NAME, CT_SERVER_FLAGS, CT_VERSION,
    OP_FOUNDSOURCES, OP_GETSOURCES, OP_IDCHANGE, OP_LOGINREQUEST,
    OP_SEARCHRESULT, OP_SERVERIDENT, OP_SERVERMESSAGE, PROTO_EDONKEY,
    T_STRING, T_UINT32, build_tags, make_frame, str_val, u16_val, u32_val,
)
from .const import FrameReader, ProtocolError

CLIENT_NAME = "fastdl 0.1"
CLIENT_VERSION = 0x3C
SERVER_FLAGS = 0x011D  # CAP_ZLIB|CAP_AUXPORT|CAP_NEWTAGS|CAP_UNICODE
EMULE_VERSION = 1


class ServerError(Exception):
    pass


def _login_payload_tag(cfg) -> bytes:
    """tag 式登录 payload（aMule 写法，兼容性最好）。"""
    tags = build_tags([
        (T_STRING, CT_NAME, str_val(CLIENT_NAME)),
        (T_UINT32, CT_VERSION, u32_val(CLIENT_VERSION)),
        (T_UINT32, CT_SERVER_FLAGS, u32_val(SERVER_FLAGS)),
        (T_UINT32, CT_EMULE_VERSION, u32_val(EMULE_VERSION)),
    ])
    payload = cfg.userhash + (0).to_bytes(4, "little") + u16_val(cfg.tcp_port)
    payload += (4).to_bytes(4, "little") + tags
    return payload


def _login_payload_old(cfg) -> bytes:
    """旧式变体 B 登录：name\0 + version4。"""
    payload = cfg.userhash + (0).to_bytes(4, "little") + u16_val(cfg.tcp_port)
    payload += CLIENT_NAME.encode() + b"\x00"
    payload += u32_val(CLIENT_VERSION)
    return payload


class ServerConn:
    def __init__(self, host: str, port: int, cfg, timeout: float = 10, dump: bool = False):
        self.host = host
        self.port = port
        self.cfg = cfg
        self.timeout = timeout
        self.dump = dump
        self.sock: socket.socket | None = None
        self.reader: FrameReader | None = None
        self.client_id = 0

    def _connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.reader = FrameReader(self.sock)

    def _send(self, proto: int, opcode: int, payload: bytes = b"") -> None:
        if self.sock is None:
            raise ServerError("未连接")
        frame = make_frame(proto, opcode, payload)
        if self.dump:
            print(f"[server->] {opcode:#04x} len={len(payload)}")
        self.sock.sendall(frame)

    def _read(self) -> tuple[int, int, bytes] | None:
        if self.reader is None:
            return None
        proto, op, payload = self.reader.read()
        if self.dump:
            print(f"[server<-] op={op:#04x} len={len(payload)} {payload.hex()[:80]}")
        return proto, op, payload

    def login(self, attempts: int = 2) -> bool:
        for variant in ("tag", "old"):
            try:
                self._connect()
                payload = _login_payload_tag(self.cfg) if variant == "tag" else _login_payload_old(self.cfg)
                self._send(PROTO_EDONKEY, OP_LOGINREQUEST, payload)
                # 等登录确认：IDCHANGE(0x40)/SERVERIDENT(0x41) 视为成功
                ok = False
                deadline = time.time() + 6
                while time.time() < deadline:
                    try:
                        proto, op, payload = self._read()
                    except socket.timeout:
                        break
                    if op == OP_IDCHANGE:
                        if len(payload) >= 4:
                            self.client_id = struct.unpack("<I", payload[:4])[0]
                        ok = True
                        break
                    if op == OP_SERVERIDENT or op == OP_SERVERMESSAGE:
                        ok = True  # 登录被接受（至少连上了）
                if ok:
                    return True
            except (OSError, ProtocolError):
                pass
            finally:
                if not ok and self.sock is not None:
                    try:
                        self.sock.close()
                    except OSError:
                        pass
        return False

    def get_sources(self, file_hash: bytes, size: int, wait: float = 15) -> list[tuple[str, int]]:
        if self.sock is None:
            raise ServerError("未登录")
        if size > 4_290_048_000:
            payload = file_hash + (0).to_bytes(4, "little") + struct.pack("<Q", size)
        else:
            payload = file_hash + struct.pack("<I", size)
        self._send(PROTO_EDONKEY, OP_GETSOURCES, payload)

        sources: list[tuple[str, int]] = []
        deadline = time.time() + wait
        while time.time() < deadline:
            try:
                proto, op, payload = self._read()
            except socket.timeout:
                break
            except (ProtocolError, OSError):
                break
            if op == OP_FOUNDSOURCES:
                if len(payload) < 17:
                    continue
                count = payload[16]
                if len(payload) < 17 + count * 6:
                    continue
                for i in range(count):
                    off = 17 + i * 6
                    ip = socket.inet_ntoa(payload[off:off + 4])
                    port = struct.unpack(">H", payload[off + 4:off + 6])[0]
                    sources.append((ip, port))
                break
            if op == OP_SEARCHRESULT:
                # 旧服务器可能用 SEARCHRESULT 带源，尝试粗解析（文件首字节为源数）
                try:
                    n = payload[0] if payload else 0
                    if n and len(payload) >= 2 + n * 6:
                        for i in range(n):
                            off = 1 + i * 6
                            ip = socket.inet_ntoa(payload[off:off + 4])
                            port = struct.unpack(">H", payload[off + 4:off + 6])[0]
                            sources.append((ip, port))
                except (struct.error, IndexError):
                    pass
            # 其他消息（SERVERMESSAGE/IDCHANGE 等）跳过
        return sources

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None
