"""FTP 下载：ftplib 单流 + REST 断点续传。

多线程 FTP 分片需要每个连接 REST 到起点再丢弃前缀，浪费带宽，v1 不做；
要速度请用 `--engine aria2`（aria2 原生多连接 FTP）。
"""
from __future__ import annotations

import os
from ftplib import FTP, error_perm
from urllib.parse import unquote, urlsplit

from .utils import sanitize_filename

DEFAULT_TIMEOUT = 30


class DownloadError(Exception):
    pass


def _parse_ftp_url(url: str) -> tuple[str, str, str, int, str]:
    """返回 (host, user, password, port, path)。匿名则 user/password 为空。"""
    u = urlsplit(url)
    host = u.hostname or ""
    port = u.port or 21
    user = unquote(u.username) if u.username else ""
    pw = unquote(u.password) if u.password else ""
    path = unquote(u.path)
    return host, user, pw, port, path


def ftp_download(url: str, dest_dir: str, resume: bool = True,
                 timeout: int = DEFAULT_TIMEOUT, stop: dict | None = None) -> int:
    stop = stop or {"flag": False}
    host, user, pw, port, path = _parse_ftp_url(url)
    if not host or not path:
        raise DownloadError(f"FTP URL 非法: {url}")

    fn = sanitize_filename(os.path.basename(path) or "download")
    dest = os.path.join(dest_dir, fn)

    ftp = FTP()
    ftp.connect(host, port, timeout=timeout)
    try:
        if user:
            ftp.login(user, pw)
        else:
            ftp.login()
        ftp.voidcmd("TYPE I")  # 二进制
        try:
            total = ftp.size(path)
        except (error_perm, OSError):
            total = None

        have = os.path.getsize(dest) if (resume and os.path.exists(dest)) else 0
        if total is not None and have >= total:
            return 0

        # 检查 REST 支持
        rest_ok = True
        try:
            ftp.sendcmd("REST 0")
        except (error_perm, OSError):
            rest_ok = False

        def _cb(b: bytes):
            if stop.get("flag"):
                raise DownloadError("已停止")

        mode = "ab" if (resume and have > 0 and rest_ok) else "wb"
        with open(dest, mode) as f:
            if mode == "ab":
                ftp.voidcmd(f"REST {have}")
            def write(b):
                _cb(b)
                f.write(b)
            ftp.retrbinary(f"RETR {path}", write)
        return 0 if (total is None or os.path.getsize(dest) == total) else 1
    finally:
        try:
            ftp.quit()
        except Exception:
            pass
