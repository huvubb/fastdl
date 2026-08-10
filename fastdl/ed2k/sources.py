"""源发现：并行探测 eD2k 服务器、登录、GETSOURCES 查源、去重。"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .server import ServerConn, ServerError


def probe_servers(servers: list[tuple[str, int]], cfg, timeout: float = 8,
                  parallel: int = 8) -> list[tuple[str, int, str]]:
    """并行尝试登录各服务器，返回能登录的 (host, port, 说明)。"""
    alive: list[tuple[str, int, str]] = []
    lock = threading.Lock()

    def one(s):
        host, port = s
        try:
            conn = ServerConn(host, port, cfg, timeout=timeout)
            if conn.login():
                with lock:
                    alive.append((host, port, f"client_id={conn.client_id}"))
            conn.close()
        except Exception as e:
            pass  # 死服务器，跳过

    with ThreadPoolExecutor(max_workers=parallel) as ex:
        for s in servers:
            ex.submit(one, s)
    return alive


class SourceFetcher:
    """从服务器列表查某个文件的源；去重、失败冷却。"""

    def __init__(self, servers: list[tuple[str, int]], cfg, parallel: int = 8,
                 dump: bool = False, timeout: float = 8):
        self.servers = servers
        self.cfg = cfg
        self.parallel = parallel
        self.dump = dump
        self.timeout = timeout
        self._blacklist: dict[tuple[str, int], float] = {}  # server -> cooldown until
        self._lock = threading.Lock()

    def _alive_servers(self) -> list[tuple[str, int]]:
        return probe_servers(self.servers, self.cfg, self.timeout, self.parallel)

    def fetch_sources(self, file_hash: bytes, size: int,
                      limit: int = 200, wait: float = 15) -> set[tuple[str, int]]:
        """对每个可登录服务器查源，汇总去重。"""
        out: set[tuple[str, int]] = set()
        servers = self._alive_servers()
        if not servers:
            return out

        lock = threading.Lock()
        now = time.time()

        def one(s):
            host, port = s
            with self._lock:
                cd = self._blacklist.get((host, port), 0)
            if cd > now:
                return
            try:
                conn = ServerConn(host, port, self.cfg, timeout=self.timeout, dump=self.dump)
                if not conn.login():
                    self._penalize(host, port)
                    return
                src = conn.get_sources(file_hash, size, wait=wait)
                conn.close()
                if src:
                    with lock:
                        out.update(src)
            except Exception:
                self._penalize(host, port)

        # probe_servers 返回三元组 (host, port, msg)，取前两元
        with ThreadPoolExecutor(max_workers=min(self.parallel, len(servers))) as ex:
            for s in servers:
                ex.submit(one, (s[0], s[1]))
        return set(list(out)[:limit])

    def _penalize(self, host: str, port: int) -> None:
        with self._lock:
            self._blacklist[(host, port)] = time.time() + 600  # 冷却 10 分钟
