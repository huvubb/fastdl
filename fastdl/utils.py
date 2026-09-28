"""通用工具：重试、格式化、文件名清洗、磁盘检查、块拷贝、速率计。"""
from __future__ import annotations

import os
import re
import shutil
import time


def default_download_dir() -> str:
    """默认下载目录：用户「下载」文件夹（Windows: C:\\Users\\<你>\\Downloads）。"""
    home = os.path.expanduser("~")
    for name in ("Downloads", "下载"):
        p = os.path.join(home, name)
        if os.path.isdir(p):
            return p
    return home


def human_bytes(n: int | float | None) -> str:
    if n is None:
        return "?"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{n:.0f}B"
        n /= 1024
    return f"{n:.1f}TB"


def human_rate(bps: float) -> str:
    return human_bytes(bps) + "/s"


def human_eta(secs: float | None) -> str:
    if secs is None or secs < 0 or secs != secs:  # 也兜住 NaN
        return "--"
    secs = int(secs)
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    if m:
        return f"{m}:{s:02d}"
    return f"{s}s"


_RESERVED = {"con", "prn", "aux", "nul"}
_RESERVED |= {f"com{i}" for i in range(1, 10)}
_RESERVED |= {f"lpt{i}" for i in range(1, 10)}


def sanitize_filename(name: str, fallback: str = "download") -> str:
    """清洗 Windows 非法文件名：非法字符、结尾点/空格、保留名、超长名。"""
    name = (name or "").strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = name.rstrip(". ")
    if name.lower() in _RESERVED:
        name = "_" + name
    if not name:
        return fallback
    # 去掉 Windows 不能容忍的尾部（点在 Windows 上会丢失）
    while name.endswith("."):
        name = name[:-1]
    if not name:
        return fallback
    return name[:200]


def disk_free(path: str) -> int:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 0


def parse_size(s: str) -> int:
    """'8M' -> 8MiB；支持 K/M/G/T/B 后缀（B 结尾时按裸字节数）。"""
    s = s.strip().upper()
    if not s:
        raise ValueError("空的大小")
    mult = 1
    if s.endswith("K"):
        mult, s = 1 << 10, s[:-1]
    elif s.endswith("M"):
        mult, s = 1 << 20, s[:-1]
    elif s.endswith("G"):
        mult, s = 1 << 30, s[:-1]
    elif s.endswith("T"):
        mult, s = 1 << 40, s[:-1]
    elif s.endswith("B"):
        s = s[:-1]
    return int(float(s) * mult)


def parse_limit(s: str | None) -> float:
    """限速字符串 → 字节/秒；'5M'=5MiB/s，'512K'=512KiB/s。None/空 → 0（不限速）。"""
    if not s:
        return 0.0
    return float(parse_size(s))


def retry(fn, attempts: int = 3, base_delay: float = 1.0, backoff: float = 2.0,
          exceptions=(Exception,), on_retry=None):
    """指数退避重试；最后一次异常原样抛出。"""
    last = None
    for i in range(attempts):
        try:
            return fn()
        except exceptions as e:
            last = e
            if i == attempts - 1:
                break
            delay = base_delay * (backoff ** i)
            if on_retry:
                on_retry(i + 1, e, delay)
            time.sleep(delay)
    raise last


class RateMeter:
    """滑动窗口速率计。tick(delta_bytes) 返回当前 bps。"""

    def __init__(self, window: float = 3.0):
        self.window = window
        self._history: list[tuple[float, int]] = []  # (now, bytes)

    def tick(self, delta_bytes: int, now: float | None = None) -> float:
        now = now or time.time()
        self._history.append((now, delta_bytes))
        cutoff = now - self.window
        while self._history and self._history[0][0] < cutoff:
            self._history.pop(0)
        return self.rate(now)

    def rate(self, now: float | None = None) -> float:
        now = now or time.time()
        if not self._history:
            return 0.0
        t0 = self._history[0][0]
        total = sum(b for _, b in self._history)
        dt = now - t0
        return total / dt if dt > 0 else 0.0


def chunked_copy(src: str, dst: str, chunk: int = 1 << 20,
                 progress=None) -> int:
    """流式拷贝文件；progress(done, total) 可选回调。返回拷贝字节数。"""
    total = os.path.getsize(src)
    done = 0
    with open(src, "rb") as f, open(dst, "wb") as g:
        while True:
            b = f.read(chunk)
            if not b:
                break
            g.write(b)
            done += len(b)
            if progress:
                progress(done, total)
    return done
