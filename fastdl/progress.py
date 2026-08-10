"""单行 ANSI 进度渲染：百分比 / 总速度 / ETA / 各线程速度。"""
from __future__ import annotations

import sys
import time

from .utils import human_bytes, human_eta, human_rate

_ANSI = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


class SingleLineProgress:
    """线程安全：update() 可被任意工作线程调用，渲染线程负责打印。

    设计：worker 线程只更新原子字段；一个渲染线程每 0.2s 打一次 \r 行。
    """

    def __init__(self, total: int, label: str = "", detail_names: list[str] | None = None):
        self.total = total
        self.label = label
        self.done = 0
        self.rates: dict[str, float] = {}
        self.detail_names = detail_names or []
        self._t0 = time.time()
        self._last_done = 0.0
        self._last_t = self._t0

    def add(self, delta: int, rate_key: str | None = None, rate_val: float = 0.0):
        self.done += delta
        if rate_key is not None:
            self.rates[rate_key] = rate_val

    def _rate(self) -> float:
        now = time.time()
        dt = now - self._last_t
        if dt <= 0:
            return 0.0
        r = (self.done - self._last_done) / dt
        self._last_done = self.done
        self._last_t = now
        return r

    def render(self) -> str:
        if self.total <= 0:
            pct = 100.0 if self.done else 0.0
        else:
            pct = min(100.0, self.done / self.total * 100)
        rate = self._rate()
        eta = (self.total - self.done) / rate if rate > 0 else None
        bar_w = 22
        filled = int(bar_w * pct / 100)
        # 用 ASCII 块，避免 GBK 控制台编码错误（Windows 中文系统默认 GBK）
        bar = "#" * filled + "-" * (bar_w - filled)
        line = f"{self.label}{bar} {pct:5.1f}%  {human_bytes(self.done)}/{human_bytes(self.total)}  "
        line += f"{human_rate(rate)}  ETA {human_eta(eta)}"
        if self.detail_names:
            parts = [f"{n}:{human_rate(self.rates.get(n, 0.0))}"
                     for n in self.detail_names[:5] if n in self.rates]
            if parts:
                line += "  | " + " ".join(parts)
        return line

    def _safe(self, line: str) -> str:
        """编码兜底：GBK/ascii 都写不了的字替换成 '?'。"""
        try:
            line.encode(sys.stdout.encoding or "utf-8", "strict")
            return line
        except (UnicodeEncodeError, LookupError):
            return line.encode("ascii", "replace").decode("ascii")

    def paint(self):
        line = self._safe(self.render())
        if _ANSI:
            sys.stdout.write("\r\033[K" + line)
        else:
            sys.stdout.write("\r" + line)
        sys.stdout.flush()

    def finish(self, msg: str = ""):
        sys.stdout.write("\r\033[K" + self._safe(msg) + "\n")
        sys.stdout.flush()
