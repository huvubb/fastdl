"""ed2k 多源下载调度器：part 分配 + 源轮换 + 组装 + 顶层 hash 校验 + 断点续传。

策略（对照实现评审）：
- 一个源同时最多 1 个在途 part（eMule 单客户端单 part 上传）。
- part 与源 1:1 配对；失败换源 + 源冷却；part 重试有上限。
- 优先用 |h=| 或 HASHSET 拿到的 part 哈希逐块校验；拿不到只验顶层。
- 断点续传：.part/<file>/part_%06d 大小即完成度，不完整 part 整块重下。
"""
from __future__ import annotations

import itertools
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from ..config import load_config
from ..progress import SingleLineProgress
from ..utils import human_bytes, sanitize_filename
from .const import PARTSIZE
from .md4 import md4, md4_file
from .peer import PeerConnection, SourceGone

MAX_PART_RETRIES = 4


class Ed2kDownloader:
    def __init__(self, link, dest_dir: str = ".", fetcher=None,
                 max_sources: int = 6, max_rank: int = 200, dump: bool = False):
        self.link = link
        self.dest_dir = dest_dir
        self.fetcher = fetcher
        self.max_sources = max_sources
        self.max_rank = max_rank
        self.dump = dump
        self.cfg = getattr(fetcher, "cfg", None) or load_config()
        self._source_pool: list[tuple[str, int]] = []
        self._cooldown: dict[tuple[str, int], float] = {}
        self._src_round = itertools.count()
        self._pool_lock = threading.Lock()

    # ---------- 源管理 ----------
    def _load_sources(self) -> list[tuple[str, int]]:
        if self.fetcher is None:
            return []
        s = self.fetcher.fetch_sources(self.link.file_hash, self.link.size)
        return list(s)

    def _penalize(self, src: tuple[str, int], seconds: int = 120) -> None:
        with self._pool_lock:
            self._cooldown[src] = time.time() + seconds

    def _next_source(self) -> tuple[str, int] | None:
        """轮询选一个不在冷却中的源；耗尽则重新查源一次。"""
        now = time.time()
        for _ in range(max(1, len(self._source_pool) * 2)):
            if not self._source_pool:
                break
            idx = next(self._src_round) % len(self._source_pool)
            s = self._source_pool[idx]
            if self._cooldown.get(s, 0) <= now:
                return s
        # 重新查源
        fresh = self._load_sources()
        if fresh:
            with self._pool_lock:
                self._source_pool = list(fresh)
            return fresh[0]
        return None

    # ---------- part 工具 ----------
    def _part_len(self, index: int) -> int:
        start = index * PARTSIZE
        return min(PARTSIZE, self.link.size - start)

    def _part_path(self, index: int) -> str:
        return os.path.join(self.workdir, f"part_{index:06d}")

    # ---------- 主流程 ----------
    def run(self) -> int:
        link = self.link
        self.workdir = os.path.join(self.dest_dir, f"{sanitize_filename(link.name)}.ed2kpart")
        os.makedirs(self.workdir, exist_ok=True)
        dest = os.path.join(self.dest_dir, sanitize_filename(link.name))

        if link.size == 0:
            open(dest, "wb").close()
            print("空文件，已创建。")
            return 0

        n_parts = link.n_parts
        part_done = [False] * n_parts
        for i in range(n_parts):
            pp = self._part_path(i)
            if os.path.exists(pp) and os.path.getsize(pp) == self._part_len(i):
                if link.part_hashes and md4(open(pp, "rb").read()) != link.part_hashes[i]:
                    os.remove(pp)  # part 哈希不符 → 重下
                else:
                    part_done[i] = True

        pending = [i for i in range(n_parts) if not part_done[i]]
        if not pending:
            return self._finish(dest, n_parts, part_done)

        print(f"共 {n_parts} 个 part，待下载 {len(pending)}，"
              f"单块 {human_bytes(PARTSIZE)}。正在从 eD2k 网络查源...")
        self._source_pool = self._load_sources()
        if not self._source_pool:
            print("在 eD2k 网络未找到该文件的可用源（服务器不可达或无源）。")
            return 2
        print(f"找到 {len(self._source_pool)} 个源，最多 {self.max_sources} 路并行。")

        q = queue.Queue()
        for i in pending:
            q.put(i)
        retries = {i: 0 for i in pending}
        stop = {"flag": False}
        lock = threading.Lock()
        progress = SingleLineProgress(link.size, label=f"{link.name} ")
        done_bytes = [sum(self._part_len(i) for i in range(n_parts) if part_done[i])]

        def worker(initial_src):
            src = initial_src
            conn: PeerConnection | None = None
            try:
                while True:
                    if stop["flag"]:
                        break
                    try:
                        i = q.get_nowait()
                    except queue.Empty:
                        break
                    if part_done[i]:
                        continue
                    try:
                        if conn is None:
                            conn = PeerConnection(src[0], src[1], link.file_hash, link.size,
                                                  self.cfg, dump=self.dump, max_rank=self.max_rank)
                            conn.connect()
                            conn.handshake()
                            conn.announce()
                        data = conn.fetch_part(i * PARTSIZE, self._part_len(i))
                        if len(data) != self._part_len(i):
                            raise SourceGone("part 长度不足")
                        if link.part_hashes and md4(data) != link.part_hashes[i]:
                            raise SourceGone("part MD4 校验不符")
                        pp = self._part_path(i)
                        with open(pp, "wb") as f:
                            f.write(data)
                        with lock:
                            part_done[i] = True
                            done_bytes[0] += len(data)
                        progress.add(len(data))
                    except (SourceGone, OSError):
                        # 源失效（协议异常 / 连接失败 / 超时）→ 换源重试
                        if conn is not None:
                            try:
                                conn.close()
                            except OSError:
                                pass
                            conn = None
                        with lock:
                            retries[i] += 1
                            if retries[i] <= MAX_PART_RETRIES:
                                q.put(i)
                        self._penalize(src)
                        src = self._next_source()
                        if src is None:
                            break
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except OSError:
                        pass

        try:
            with ThreadPoolExecutor(max_workers=self.max_sources) as ex:
                futs = [ex.submit(worker, s) for s in self._source_pool[:self.max_sources]]
                while True:
                    progress.paint()
                    if stop["flag"]:
                        ex.shutdown(cancel_futures=True)
                        break
                    if all(f.done() for f in futs):
                        break
                    time.sleep(0.2)
        except KeyboardInterrupt:
            stop["flag"] = True
            return 1
        finally:
            progress.finish("")

        undone = [i for i in range(n_parts) if not part_done[i]]
        if undone:
            print(f"还有 {len(undone)} 个 part 未完成（源耗尽或全部失败），可续传。")
            return 2

        return self._finish(dest, n_parts, part_done)

    def _finish(self, dest: str, n_parts: int, part_done: list[bool]) -> int:
        """合并 part 文件 → 最终文件 → 顶层 hash 校验。"""
        if not all(part_done):
            return 2
        print("合并 part 文件...")
        with open(dest, "wb") as out:
            for i in range(n_parts):
                pp = self._part_path(i)
                if not os.path.exists(pp):
                    print(f"缺少 part_{i:06d}，合并失败。")
                    return 2
                with open(pp, "rb") as f:
                    while True:
                        b = f.read(1 << 20)
                        if not b:
                            break
                        out.write(b)
                os.remove(pp)
        os.rmdir(self.workdir) if os.path.isdir(self.workdir) else None

        print("校验顶层 ed2k hash...")
        top, _ = md4_file(dest)
        if top != self.link.file_hash:
            print(f"顶层 hash 校验失败: 期望 {self.link.file_hash.hex()} 实际 {top.hex()}")
            print("文件可能被污染源破坏，建议重下失败 part。")
            return 2
        print(f"下载完成并校验通过: {dest} ({human_bytes(os.path.getsize(dest))})")
        return 0
