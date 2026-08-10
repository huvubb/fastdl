"""ed2k:// 链接解析：|file| 与 |server|，可选 |s= 源 / |h= part 哈希。"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import unquote

from .md4 import ed2k_part_size

_PART_SIZE = ed2k_part_size()


@dataclass
class Ed2kLink:
    name: str
    size: int
    file_hash: bytes                     # 顶层 hash，16 字节
    part_hashes: list[bytes] | None = None   # 来自 |h=|（若提供）
    part_size: int = _PART_SIZE
    sources: list[tuple[str, int]] = field(default_factory=list)  # 来自 |s=|

    @property
    def n_parts(self) -> int:
        return (self.size + self.part_size - 1) // self.part_size if self.size else 0


@dataclass
class ServerLink:
    host: str
    port: int


def _parse_sources(s: str) -> list[tuple[str, int]]:
    out = []
    for item in s.split(","):
        item = item.strip()
        if ":" not in item:
            continue
        host, _, port = item.rpartition(":")
        try:
            out.append((host.strip(), int(port)))
        except ValueError:
            continue
    return out


def _parse_part_hashes(s: str) -> list[bytes]:
    out = []
    for h in s.split(","):
        h = h.strip()
        if re.fullmatch(r"[0-9a-fA-F]{32}", h):
            out.append(bytes.fromhex(h))
    return out


def parse_ed2k_link(uri: str) -> Ed2kLink:
    """解析 ed2k://|file|name|size|hash|/（可带 |s=/|h=/|p=/|）。校验失败抛 ValueError。"""
    m = re.fullmatch(r"ed2k://\|file\|(.*?)\|(\d+)\|([0-9a-fA-F]{32})\|(?:s=([^|]*)\|)?(?:h=([^|]*)\|)?(?:p=([^|]*)\|)?/", uri, re.S)
    if not m:
        raise ValueError(f"不是合法的 ed2k://|file| 链接: {uri[:80]!r}")
    name, size_s, hash_s, s_s, h_s, p_s = m.groups()
    name = unquote(name)
    size = int(size_s)
    if size < 0:
        raise ValueError("文件大小非法")
    link = Ed2kLink(
        name=name,
        size=size,
        file_hash=bytes.fromhex(hash_s),
        part_hashes=_parse_part_hashes(h_s) if h_s else None,
        sources=_parse_sources(s_s) if s_s else [],
    )
    if link.part_hashes is not None:
        expect = link.n_parts
        if len(link.part_hashes) != expect:
            raise ValueError(f"|h=| part 数量 {len(link.part_hashes)} != 期望 {expect}")
    return link


def parse_server_uri(uri: str) -> ServerLink | None:
    m = re.fullmatch(r"ed2k://\|server\|([^|]+?)\|(\d+)\|/", uri, re.S)
    if not m:
        return None
    return ServerLink(host=m.group(1), port=int(m.group(2)))


def make_ed2k_link(name: str, size: int, top_hash: bytes,
                   part_hashes: list[bytes] | None = None) -> str:
    """把 (文件名, 大小, 顶层hash) 拼成 ed2k:// 链接。"""
    from urllib.parse import quote
    n = quote(name, safe="")
    s = f"ed2k://|file|{n}|{size}|{top_hash.hex()}|"
    if part_hashes:
        s += "h=" + ",".join(h.hex() for h in part_hashes) + "|"
    s += "/"
    return s
