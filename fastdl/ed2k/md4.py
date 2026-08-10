"""MD4 多后端封装：pycryptodome → openssl CLI → 纯 Python。

ed2k 顶层 hash 与逐 part hash 全部依赖 MD4。`hashlib.new('md4')` 在 OpenSSL3
下不可用，故做三后端自动降级。纯 Python 后端已对照 RFC 1320 + pycryptodome +
passlib 逐位验证通过。
"""
from __future__ import annotations

import os
import struct
import subprocess
import tempfile

_MASK = 0xFFFFFFFF
_rol = lambda v, s: (((v & _MASK) << s) | ((v & _MASK) >> (32 - s))) & _MASK
_F = lambda x, y, z: (x & y) | (~x & z)
_G = lambda x, y, z: (x & y) | (x & z) | (y & z)
_H = lambda x, y, z: x ^ y ^ z

_INIT = (0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476)


def _md4_pure(data: bytes) -> bytes:
    """RFC 1320 纯 Python MD4（已验证）。"""
    ml = len(data) * 8
    d = bytearray(data)
    d.append(0x80)
    while len(d) % 64 != 56:
        d.append(0)
    d += struct.pack("<Q", ml)
    a0, b0, c0, d0 = _INIT
    for off in range(0, len(d), 64):
        X = list(struct.unpack("<16I", bytes(d[off:off + 64])))
        a, b, c, dd = a0, b0, c0, d0
        # Round 1
        a = _rol((a + _F(b, c, dd) + X[0]), 3);   dd = _rol((dd + _F(a, b, c) + X[1]), 7)
        c = _rol((c + _F(dd, a, b) + X[2]), 11);  b = _rol((b + _F(c, dd, a) + X[3]), 19)
        a = _rol((a + _F(b, c, dd) + X[4]), 3);   dd = _rol((dd + _F(a, b, c) + X[5]), 7)
        c = _rol((c + _F(dd, a, b) + X[6]), 11);  b = _rol((b + _F(c, dd, a) + X[7]), 19)
        a = _rol((a + _F(b, c, dd) + X[8]), 3);   dd = _rol((dd + _F(a, b, c) + X[9]), 7)
        c = _rol((c + _F(dd, a, b) + X[10]), 11); b = _rol((b + _F(c, dd, a) + X[11]), 19)
        a = _rol((a + _F(b, c, dd) + X[12]), 3);  dd = _rol((dd + _F(a, b, c) + X[13]), 7)
        c = _rol((c + _F(dd, a, b) + X[14]), 11); b = _rol((b + _F(c, dd, a) + X[15]), 19)
        # Round 2
        a = _rol((a + _G(b, c, dd) + X[0] + 0x5A827999), 3);  dd = _rol((dd + _G(a, b, c) + X[4] + 0x5A827999), 5)
        c = _rol((c + _G(dd, a, b) + X[8] + 0x5A827999), 9);  b = _rol((b + _G(c, dd, a) + X[12] + 0x5A827999), 13)
        a = _rol((a + _G(b, c, dd) + X[1] + 0x5A827999), 3);  dd = _rol((dd + _G(a, b, c) + X[5] + 0x5A827999), 5)
        c = _rol((c + _G(dd, a, b) + X[9] + 0x5A827999), 9);  b = _rol((b + _G(c, dd, a) + X[13] + 0x5A827999), 13)
        a = _rol((a + _G(b, c, dd) + X[2] + 0x5A827999), 3);  dd = _rol((dd + _G(a, b, c) + X[6] + 0x5A827999), 5)
        c = _rol((c + _G(dd, a, b) + X[10] + 0x5A827999), 9); b = _rol((b + _G(c, dd, a) + X[14] + 0x5A827999), 13)
        a = _rol((a + _G(b, c, dd) + X[3] + 0x5A827999), 3);  dd = _rol((dd + _G(a, b, c) + X[7] + 0x5A827999), 5)
        c = _rol((c + _G(dd, a, b) + X[11] + 0x5A827999), 9); b = _rol((b + _G(c, dd, a) + X[15] + 0x5A827999), 13)
        # Round 3
        a = _rol((a + _H(b, c, dd) + X[0] + 0x6ED9EBA1), 3);  dd = _rol((dd + _H(a, b, c) + X[8] + 0x6ED9EBA1), 9)
        c = _rol((c + _H(dd, a, b) + X[4] + 0x6ED9EBA1), 11); b = _rol((b + _H(c, dd, a) + X[12] + 0x6ED9EBA1), 15)
        a = _rol((a + _H(b, c, dd) + X[2] + 0x6ED9EBA1), 3);  dd = _rol((dd + _H(a, b, c) + X[10] + 0x6ED9EBA1), 9)
        c = _rol((c + _H(dd, a, b) + X[6] + 0x6ED9EBA1), 11); b = _rol((b + _H(c, dd, a) + X[14] + 0x6ED9EBA1), 15)
        a = _rol((a + _H(b, c, dd) + X[1] + 0x6ED9EBA1), 3);  dd = _rol((dd + _H(a, b, c) + X[9] + 0x6ED9EBA1), 9)
        c = _rol((c + _H(dd, a, b) + X[5] + 0x6ED9EBA1), 11); b = _rol((b + _H(c, dd, a) + X[13] + 0x6ED9EBA1), 15)
        a = _rol((a + _H(b, c, dd) + X[3] + 0x6ED9EBA1), 3);  dd = _rol((dd + _H(a, b, c) + X[11] + 0x6ED9EBA1), 9)
        c = _rol((c + _H(dd, a, b) + X[7] + 0x6ED9EBA1), 11); b = _rol((b + _H(c, dd, a) + X[15] + 0x6ED9EBA1), 15)
        a0, b0, c0, d0 = (a0 + a) & _MASK, (b0 + b) & _MASK, (c0 + c) & _MASK, (d0 + dd) & _MASK
    return struct.pack("<4I", a0, b0, c0, d0)


def _md4_openssl(data: bytes) -> bytes:
    """openssl dgst -md4 -provider legacy 兜底。"""
    fd, path = tempfile.mkstemp(suffix=".bin")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        r = subprocess.run(
            ["openssl", "dgst", "-md4", "-provider", "legacy", "-provider", "default", path],
            capture_output=True, text=True, timeout=20,
        )
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip() or "openssl md4 失败")
        return bytes.fromhex(r.stdout.split("=")[-1].strip())
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# ---- 后端探测（顺序：pycryptodome -> openssl -> 纯 Python）----
_backends: list[tuple[str, callable]] = []
try:
    from Crypto.Hash import MD4 as _CryptoMD4

    def _md4_crypto(data: bytes) -> bytes:
        return _CryptoMD4.new(data).digest()

    _backends.append(("pycryptodome", _md4_crypto))
except Exception:
    pass

if not any(n == "pycryptodome" for n, _ in _backends):
    try:
        _md4_openssl(b"")
        _backends.append(("openssl", _md4_openssl))
    except Exception:
        pass

if not _backends:
    _backends.append(("pure-python", _md4_pure))

_selected: str | None = None


def md4(data: bytes) -> bytes:
    """计算 MD4 摘要（16 字节）。"""
    name, fn = _backends[0]
    return fn(data)


def md4_hex(data: bytes) -> str:
    return md4(data).hex()


def backend() -> str:
    return _backends[0][0]


def ed2k_part_size() -> int:
    return 9_728_000


def hash_parts(data: bytes, part_size: int = 9_728_000) -> tuple[bytes, list[bytes]]:
    """把整块数据按 part 切成块，逐块 MD4，返回 (顶层hash, [part_hashes])。

    顶层 hash = MD4(各 part MD4 的拼接)。ed2k 链接里的 32hex 就是它。
    """
    part_hashes = []
    for off in range(0, len(data), part_size):
        part_hashes.append(md4(data[off:off + part_size]))
    top = md4(b"".join(part_hashes)) if part_hashes else md4(b"")
    return top, part_hashes


def md4_file(path: str, offset: int = 0, length: int | None = None,
             part_size: int = 9_728_000) -> tuple[bytes, list[bytes]]:
    """对文件流式计算 ed2k hash；返回 (顶层hash, [part_hashes])。

    顶层 hash 的正确算法 = MD4(各 part 16 字节哈希的拼接)——不是 MD4(文件原始字节)！
    此前 pycryptodome 分支用增量 update 算原始字节的 MD4，是错的；现已统一为
    先算逐 part 哈希再拼接。拼接只有 16*n 字节，成本可忽略。
    """
    part_hashes = []
    total = 0
    with open(path, "rb") as f:
        f.seek(offset)
        while True:
            block = f.read(part_size)
            if not block:
                break
            if length is not None and total + len(block) > length:
                block = block[: length - total]
            part_hashes.append(md4(block))
            total += len(block)
            if length is not None and total >= length:
                break
    top = md4(b"".join(part_hashes)) if part_hashes else md4(b"")
    return top, part_hashes
