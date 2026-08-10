"""eD2k / eMule 协议常量与字节层工具。

字节布局均已对照 aMule/eMule 源码核实（见实现评审）：
帧 = [protocol:1][size:4 LE][opcode:1][payload:size]，size 不含 6 字节头。
"""
from __future__ import annotations

import socket
import struct
import zlib

# ---------------- 协议字节 ----------------
PROTO_EDONKEY = 0xE3   # client<->server 与 client<->client 普通消息
PROTO_EMULE = 0xC5     # client<->client eMule 扩展消息（opcode 即子消息 id）
PROTO_PACKED = 0xD4    # 整帧 zlib 压缩
PROTO_ED2KV2 = 0xE4

# ---------------- 服务器协议 ----------------
# client -> server
OP_LOGINREQUEST = 0x01
OP_REJECT = 0x05
OP_GETSERVERLIST = 0x14
OP_SEARCHREQUEST = 0x16
OP_DISCONNECT = 0x18
OP_GETSOURCES = 0x19
# server -> client
OP_SERVERLIST = 0x32
OP_SEARCHRESULT = 0x33
OP_SERVERSTATUS = 0x34
OP_SERVERMESSAGE = 0x38
OP_IDCHANGE = 0x40
OP_SERVERIDENT = 0x41
OP_FOUNDSOURCES = 0x42
OP_USERSLIST = 0x43

# ---------------- client<->client 标准 (0xE3) ----------------
OP_HELLO = 0x01
OP_SENDINGPART = 0x46
OP_REQUESTPARTS = 0x47
OP_END_OF_DOWNLOAD = 0x49
OP_HELLOANSWER = 0x4C
OP_MESSAGE = 0x4E
OP_SETREQFILEID = 0x4F
OP_FILESTATUS = 0x50
OP_HASHSETREQUEST = 0x51
OP_HASHSETANSWER = 0x52
OP_STARTUPLOADREQ = 0x54
OP_ACCEPTUPLOADREQ = 0x55
OP_CANCELTRANSFER = 0x56
OP_OUTOFPARTREQS = 0x57
OP_REQUESTFILENAME = 0x58
OP_REQFILENAMEANSWER = 0x59
OP_QUEUERANK = 0x5C

# ---------------- client<->client 扩展 (0xC5) ----------------
OP_EMULEINFO = 0x01
OP_EMULEINFOANSWER = 0x02
OP_COMPRESSEDPART = 0x40
OP_QUEUERANKING = 0x60
OP_REQUESTSOURCES = 0x81
OP_REQUESTSOURCES2 = 0x83
OP_ANSWERSOURCES2 = 0x84
OP_MULTIPACKET = 0x92
OP_MULTIPACKETANSWER = 0x93
OP_COMPRESSEDPART_I64 = 0xA1
OP_SENDINGPART_I64 = 0xA2
OP_REQUESTPARTS_I64 = 0xA3
OP_MULTIPACKET_EXT = 0xA4

# ---------------- 尺寸常量 ----------------
PARTSIZE = 9_728_000           # 0x947000，ed2k 分块大小
BLOCKSIZE = 184_320            # part 内 hashset 块，53 块/part
OLD_MAX_FILE = 4_290_048_000   # > 此值必须用 I64 消息（8 字节偏移）
MAX_FILE = 0x4000_0000_0000    # 256GB
EDONKEYVERSION = 0x3C          # 登录 Version tag 值

# ---------------- tag 类型 ----------------
T_HASH16 = 0x01
T_STRING = 0x02
T_UINT32 = 0x03
T_FLOAT = 0x04
T_BOOL = 0x05
T_BLOB = 0x07
T_UINT16 = 0x08
T_UINT8 = 0x09
T_UINT64 = 0x0B

# ---------------- tag 名 id ----------------
CT_NAME = 0x01
CT_PORT = 0x0F
CT_VERSION = 0x11
CT_SERVER_FLAGS = 0x20
CT_EMULE_UDPPORTS = 0xF9
CT_EMULE_MISCOPTIONS1 = 0xFA
CT_EMULE_VERSION = 0xFB

ET_COMPRESSION = 0x20
ET_UDPPORT = 0x21
ET_UDPVER = 0x22
ET_SOURCEEXCHANGE = 0x23
ET_EXTENDEDREQUEST = 0x25
ET_COMPATIBLECLIENT = 0x26
ET_FEATURES = 0x27

FT_FILENAME = 0x01
FT_FILESIZE = 0x02
FT_SOURCES = 0x15
FT_COMPLETE_SOURCES = 0x30
FT_FILESIZE_HI = 0x3A

# 服务器能力位（登录 ServerFlags tag 用）
CAP_ZLIB = 0x0001
CAP_AUXPORT = 0x0004
CAP_NEWTAGS = 0x0008
CAP_UNICODE = 0x0010
CAP_LARGEFILES = 0x0100


class ProtocolError(Exception):
    """协议解析错误（帧头荒谬、数据截断等）。"""


def make_frame(proto: int, opcode: int, payload: bytes = b"") -> bytes:
    return bytes([proto]) + len(payload).to_bytes(4, "little") + bytes([opcode]) + payload


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ProtocolError("连接关闭")
        buf += chunk
    return bytes(buf)


class FrameReader:
    """从 socket 读取一帧；自动解 0xD4 zlib 压缩帧。"""

    def __init__(self, sock: socket.socket, max_frame: int = 128 << 20):
        self.sock = sock
        self.max_frame = max_frame

    def read(self) -> tuple[int, int, bytes]:
        """返回 (protocol, opcode, payload)。0xD4 帧解压后 opcode/payload 取内层。"""
        header = _recv_exact(self.sock, 6)
        proto = header[0]
        size = int.from_bytes(header[1:5], "little")
        if size > self.max_frame:
            raise ProtocolError(f"帧过大: {size}")
        opcode = header[5]
        payload = _recv_exact(self.sock, size)
        if proto == PROTO_PACKED:
            try:
                payload = zlib.decompress(payload)
            except zlib.error as e:
                raise ProtocolError(f"zlib 解压失败: {e}")
            opcode = payload[0] if payload else 0
            payload = payload[1:]
        return proto, opcode, payload


def is_extended(opcode: int) -> bool:
    return opcode >= 0x80 or opcode in (0x40, 0x60)


# ---------------- tag 值编码 ----------------
def str_val(s: str) -> bytes:
    b = s.encode("utf-8")
    return len(b).to_bytes(2, "little") + b


def u32_val(n: int) -> bytes:
    return n.to_bytes(4, "little")


def u64_val(n: int) -> bytes:
    return n.to_bytes(8, "little")


def u16_val(n: int) -> bytes:
    return n.to_bytes(2, "little")


def u8_val(n: int) -> bytes:
    return bytes([n & 0xFF])


def build_tags(tags: list[tuple[int, int, bytes]]) -> bytes:
    """按 aMule 旧式数字名编码写出 tags：[(type, name_id, value_bytes)]。"""
    out = b""
    for typ, nid, val in tags:
        out += bytes([typ])
        out += (1).to_bytes(2, "little") + bytes([nid])
        out += val
    return out


def parse_tags(buf: bytes, off: int = 0) -> tuple[list[tuple[int, object, object]], int]:
    """解码 tags，兼容新式(type|0x80 + 1字节名)与旧式(type + [len:2] + 名)。

    返回 (tags, 新 offset)。值已按类型解码为 Python 对象。
    """
    tags = []
    while off < len(buf):
        t0 = buf[off]
        off += 1
        if t0 & 0x80:
            typ = t0 & 0x7F
            name = buf[off]
            off += 1
        else:
            typ = t0
            nlen = int.from_bytes(buf[off:off + 2], "little")
            off += 2
            if nlen == 1:
                name = buf[off]
                off += 1
            else:
                name = buf[off:off + nlen].decode("utf-8", "replace")
                off += nlen
        if typ == T_STRING:
            slen = int.from_bytes(buf[off:off + 2], "little")
            val = buf[off + 2:off + 2 + slen].decode("utf-8", "replace")
            off += 2 + slen
        elif typ == T_UINT32:
            val = int.from_bytes(buf[off:off + 4], "little")
            off += 4
        elif typ == T_UINT64:
            val = int.from_bytes(buf[off:off + 8], "little")
            off += 8
        elif typ == T_UINT16:
            val = int.from_bytes(buf[off:off + 2], "little")
            off += 2
        elif typ == T_UINT8 or typ == T_BOOL:
            val = buf[off]
            off += 1
        elif typ == T_HASH16:
            val = buf[off:off + 16]
            off += 16
        elif typ == T_BLOB:
            l = int.from_bytes(buf[off:off + 4], "little")
            val = buf[off + 4:off + 4 + l]
            off += 4 + l
        elif typ == T_FLOAT:
            val = struct.unpack("<f", buf[off:off + 4])[0]
            off += 4
        elif 0x11 <= typ <= 0x26:
            # 旧式小整形值（被当长度）
            l = typ - 0x11 + 1
            val = int.from_bytes(buf[off:off + l], "little")
            off += l
        else:
            break  # 未知类型 → 停止（上层判定失步）
        tags.append((typ, name, val))
    return tags, off
