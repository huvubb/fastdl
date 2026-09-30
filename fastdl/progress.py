"""单行 ANSI 进度渲染：百分比 / 总速度 / ETA / 各线程速度。"""
from __future__ import annotations

import sys
import time
from collections import deque

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
        self._samples: deque[tuple[float, int]] = deque()
        self._last_plain_print = 0.0
        self._last_width = 0
        self.current_rate = 0.0

    def add(self, delta: int, rate_key: str | None = None, rate_val: float = 0.0):
        self.done += delta
        if rate_key is not None:
            self.rates[rate_key] = rate_val

    def _rate(self) -> float:
        now = time.time()
        # 用滑动窗口，避免 CDN 分块到达间隔稍长时速度瞬间跳成 0B/s。
        self._samples.append((now, self.done))
        cutoff = now - 3.0
        while len(self._samples) > 1 and self._samples[0][0] < cutoff:
            self._samples.popleft()
        t0, d0 = self._samples[0]
        dt = now - t0
        r = (self.done - d0) / dt if dt > 0 else 0.0
        self._last_done = self.done
        self._last_t = now
        return r

    def render(self) -> str:
        if self.total <= 0:
            pct = 100.0 if self.done else 0.0
        else:
            pct = min(100.0, self.done / self.total * 100)
        rate = self._rate()
        self.current_rate = rate
        eta = (self.total - self.done) / rate if rate > 0 else None
        bar_w = 22
        # 方括号进度条，中间固定显示百分比，兼容 Windows 控制台编码。
        inner = max(8, bar_w - 8)
        filled = int(inner * pct / 100)
        bar = "=" * filled + " " * (inner - filled)
        line = f"{self.label}[{bar[:inner//2]} {pct:5.1f}% {bar[inner//2:]}]  "
        line += f"{human_bytes(self.done)}/{human_bytes(self.total)}  "
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
            # 清掉上一行残留字符，避免 ETA 后粘上文件名等旧内容。
            sys.stdout.write("\r\033[K" + line)
        elif not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
            # 重定向/IDE 控制台通常不处理 \r；限频换行，避免输出被拼成乱码。
            now = time.time()
            if now - self._last_plain_print < 1.0 and self.done < self.total:
                return
            self._last_plain_print = now
            sys.stdout.write(line + "\n")
        else:
            padding = max(0, self._last_width - len(line))
            sys.stdout.write("\r" + line + (" " * padding))
            self._last_width = len(line)
        sys.stdout.flush()

    def finish(self, msg: str = ""):
        if _ANSI or (hasattr(sys.stdout, "isatty") and sys.stdout.isatty()):
            sys.stdout.write("\r\033[K" + self._safe(msg) + "\n")
        elif msg:
            sys.stdout.write(self._safe(msg) + "\n")
        sys.stdout.flush()
