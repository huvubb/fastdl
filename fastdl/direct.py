"""直链多线程下载器（HTTP/HTTPS）：Range 分片 + 多连接并行 + 断点续传。

设计要点（对照实现评审）：
- 权威探针 = GET `Range: bytes=0-0`：206+Content-Range 判定分片支持并拿总长；
  200 说明服务器无视 Range → 单线程流式回退。
- 必须 `Accept-Encoding: identity`，否则透明 gzip 会破坏字节偏移。
- 固定 8MiB chunk，chunk 边界与线程数解耦；`.part.<i>` 文件大小即进度。
- 续传：`.dlmeta` 记录 url/size/etag/chunk_size/n_chunks，ETag 变了清空重来。
- 合并按索引顺序流式拷贝，拷完即删 .part → 峰值磁盘 ≈ 最终文件 + 最大一块。
"""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

import requests
from requests.adapters import HTTPAdapter

from .progress import SingleLineProgress
from .utils import disk_free, sanitize_filename

CHUNK_SIZE = 8 << 20  # 8 MiB
DEFAULT_TIMEOUT = 30


class DownloadError(Exception):
    pass


class RateLimiter:
    """跨线程共享的下载限速器（令牌桶）。limit<=0 表示不限速。

    所有分片线程共用一个实例，保证全局总速率 ≤ limit（而非每线程各限速）。
    """

    def __init__(self, limit_bps: float = 0.0):
        self.limit = limit_bps
        self._tokens = 0.0
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def wait(self, n: int) -> None:
        """消耗 n 字节配额；不足则休眠补齐，使整体速率不超过 limit。"""
        if self.limit <= 0 or n <= 0:
            return
        with self._lock:
            now = time.monotonic()
            self._tokens += (now - self._last) * self.limit
            self._last = now
            if self._tokens > self.limit:  # 最多攒 1 秒的配额
                self._tokens = self.limit
            self._tokens -= n
            if self._tokens < 0:
                time.sleep(-self._tokens / self.limit)
                self._last = time.monotonic()


@dataclass
class ProbeResult:
    url: str
    size: int
    ranges: bool
    etag: str | None = None
    last_modified: str | None = None
    filename: str | None = None
    method: str = "range-probe"


def _make_session(threads: int) -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_maxsize=threads + 8, pool_connections=threads + 8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _filename_from_headers(headers: dict, url: str) -> str:
    cd = headers.get("Content-Disposition") or ""
    if "filename=" in cd:
        fn = cd.split("filename=")[-1].split(";")[0].strip('"')
        if fn:
            return unquote(fn)
    return unquote(urlsplit(url).path.rstrip("/").split("/")[-1] or "download")


def probe(url: str, threads: int = 16, timeout: int = DEFAULT_TIMEOUT,
          headers: dict | None = None, proxy: str | None = None) -> ProbeResult:
    """权威探针：GET Range bytes=0-0。返回总长 + 是否支持分片 + 元数据。"""
    s = _make_session(threads)
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    h = {"Accept-Encoding": "identity"}
    if headers:
        h.update(headers)
    try:
        r = s.get(url, headers={**h, "Range": "bytes=0-0"}, timeout=timeout, stream=True)
    except requests.RequestException as e:
        raise DownloadError(f"探针失败: {e}") from e
    try:
        if r.status_code == 206 and r.headers.get("Content-Range"):
            size = int(r.headers["Content-Range"].rsplit("/", 1)[1])
            ranges = True
        elif r.status_code == 200:
            cl = r.headers.get("Content-Length")
            if not cl:
                raise DownloadError("服务器未返回 Content-Length")
            size = int(cl)
            ranges = False
        else:
            raise DownloadError(f"探针 HTTP {r.status_code}")
        try:
            r.raw.read(1)
        except Exception:
            pass
        return ProbeResult(
            url=url, size=size, ranges=ranges,
            etag=r.headers.get("ETag"),
            last_modified=r.headers.get("Last-Modified"),
            filename=_filename_from_headers(r.headers, url),
        )
    finally:
        r.close()


def _chunk_range(i: int, chunk_size: int, size: int) -> tuple[int, int]:
    start = i * chunk_size
    end = min((i + 1) * chunk_size, size)
    return start, end


def _part_path(workdir: str, index: int) -> str:
    return os.path.join(workdir, f".part.{index}")


def _fetch_chunk(url: str, index: int, size: int, chunk_size: int, session: requests.Session,
                 resume: bool, timeout: int, progress, stop: dict,
                 workdir: str, dlmeta_path: str, headers: dict | None = None,
                 limiter: RateLimiter | None = None) -> None:
    """下载一个 chunk 到 .part.<index>；支持从已有大小续传。失败抛 DownloadError。"""
    start, end = _chunk_range(index, chunk_size, size)
    expect = end - start
    part_path = _part_path(workdir, index)
    existing = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    if existing >= expect:
        return  # 已完成

    free = disk_free(workdir)
    if free < (expect - existing) + (1 << 20):
        raise DownloadError("磁盘空间不足")

    range_from = start + existing
    range_to = end - 1
    h = {"Accept-Encoding": "identity"}
    if headers:
        h.update(headers)
    h["Range"] = f"bytes={range_from}-{range_to}"
    r = session.get(url, headers=h, timeout=timeout, stream=True)
    try:
        if r.status_code == 416:
            if os.path.exists(part_path):
                os.remove(part_path)
            raise DownloadError(f"chunk {index} 416")
        if r.status_code == 206:
            pass
        elif r.status_code == 200:
            raise DownloadError(f"chunk {index} 服务器无视 Range(200)")
        else:
            raise DownloadError(f"chunk {index} HTTP {r.status_code}")
        mode = "ab" if (resume and existing > 0) else "wb"
        got = 0
        with open(part_path, mode) as f:
            while True:
                if stop.get("flag"):
                    raise DownloadError("已停止")
                b = r.raw.read(1 << 20)
                if not b:
                    break
                f.write(b)
                got += len(b)
                if limiter:
                    limiter.wait(len(b))
                progress.add(len(b), rate_key=f"t{index % 8}")
        if got < (expect - existing):
            raise DownloadError(f"chunk {index} 数据不足: {got} < {expect-existing}")
    finally:
        r.close()


def _write_meta(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _merge(dest: str, workdir: str, n_chunks: int, size: int, progress=None) -> None:
    """按索引顺序把 .part.<i> 流式合并进最终文件，拷完即删。"""
    with open(dest, "wb") as out:
        done = 0
        for i in range(n_chunks):
            src = _part_path(workdir, i)
            if not os.path.exists(src):
                raise DownloadError(f"合并时缺少 .part.{i}")
            with open(src, "rb") as f:
                while True:
                    b = f.read(1 << 20)
                    if not b:
                        break
                    out.write(b)
                    done += len(b)
                    if progress:
                        progress(done, size)
            os.remove(src)
    if os.path.getsize(dest) != size:
        raise DownloadError(f"合并后大小不符: {os.path.getsize(dest)} != {size}")


def _download_native(url: str, dest_dir: str, threads: int, chunk_size: int,
                     resume: bool, timeout: int, stop: dict, headers: dict | None = None,
                     proxy: str | None = None, limit_bps: float = 0.0) -> int:
    session = _make_session(threads)
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    limiter = RateLimiter(limit_bps) if limit_bps > 0 else None
    pr = probe(url, threads, timeout, headers=headers, proxy=proxy)

    fn = sanitize_filename(pr.filename or "download")
    final = os.path.join(dest_dir, fn)
    workdir = os.path.join(dest_dir, f"{fn}.fastdl")
    os.makedirs(workdir, exist_ok=True)
    dlmeta_path = os.path.join(workdir, ".dlmeta")

    meta = None
    if resume and os.path.exists(dlmeta_path):
        try:
            with open(dlmeta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError):
            meta = None
    if meta:
        if meta.get("url") != url or meta.get("size") != pr.size or meta.get("etag") != pr.etag:
            for f in os.listdir(workdir):
                if f.startswith(".part."):
                    os.remove(os.path.join(workdir, f))
            meta = None

    # 已完整下载过 → 直接跳过
    if (resume and meta is not None and pr.size > 0
            and os.path.exists(final) and os.path.getsize(final) == pr.size):
        print(f"已存在完整文件，跳过: {final}")
        return 0

    if pr.size == 0:
        open(final, "wb").close()
        return 0

    if not pr.ranges:
        # 单线程流式回退（仍支持按已有大小续传）
        have = os.path.getsize(final) if (resume and os.path.exists(final)) else 0
        h = {"Accept-Encoding": "identity"}
        if headers:
            h.update(headers)
        if have > 0:
            h["Range"] = f"bytes={have}-"
        r = session.get(url, headers=h, timeout=timeout, stream=True)
        try:
            mode = "ab" if (resume and have > 0 and r.status_code == 206) else "wb"
            with open(final, mode) as f:
                while True:
                    if stop.get("flag"):
                        print("\n已停止，进度保留。")
                        return 1
                    b = r.raw.read(1 << 20)
                    if not b:
                        break
                    f.write(b)
                    if limiter:
                        limiter.wait(len(b))
        finally:
            r.close()
        return 0 if os.path.getsize(final) == pr.size else 1

    # 分片模式
    n_chunks = (pr.size + chunk_size - 1) // chunk_size
    if meta is None:
        _write_meta(dlmeta_path, {
            "url": url, "size": pr.size, "etag": pr.etag,
            "last_modified": pr.last_modified, "chunk_size": chunk_size, "n_chunks": n_chunks,
        })

    progress = SingleLineProgress(pr.size, label=f"{fn} ")
    # 各线程速率暂不显示（聚合速率已覆盖），避免显示 0B/s

    def worker(i):
        if stop.get("flag"):
            return
        last = None
        for attempt in range(3):
            if stop.get("flag"):
                return
            try:
                _fetch_chunk(url, i, pr.size, chunk_size, session, resume, timeout,
                             progress, stop, workdir, dlmeta_path,
                             headers=headers, limiter=limiter)
                return
            except DownloadError as e:
                last = e
                time.sleep(1 * (2 ** attempt))
        if stop.get("flag"):
            return
        raise RuntimeError(f"chunk {i} 下载失败: {last}")

    try:
        with ThreadPoolExecutor(max_workers=threads) as ex:
            futs = [ex.submit(worker, i) for i in range(n_chunks)]
            while True:
                progress.paint()
                if stop.get("flag"):
                    ex.shutdown(cancel_futures=True)
                    break
                if all(f.done() for f in futs):
                    break
                time.sleep(0.2)
            for f in futs:
                if f.exception() is not None:
                    raise f.exception()
    except KeyboardInterrupt:
        stop["flag"] = True
        return 1
    finally:
        progress.finish("")

    complete = all(
        os.path.getsize(_part_path(workdir, i)) >= (_chunk_range(i, chunk_size, pr.size)[1]
                                                    - _chunk_range(i, chunk_size, pr.size)[0])
        for i in range(n_chunks)
    )
    if not complete:
        return 1

    if os.path.exists(final):
        os.remove(final)
    _merge(final, workdir, n_chunks, pr.size)
    return 0


def _download_aria2(url: str, dest_dir: str, threads: int, resume: bool,
                    timeout: int, stop: dict, proxy: str | None = None,
                    limit_bps: float = 0.0, headers: dict | None = None) -> int:
    import subprocess
    fn = sanitize_filename(unquote(urlsplit(url).path.rstrip("/").split("/")[-1]) or "download")
    cmd = ["aria2c", "-x", str(threads), "-s", str(threads), "-k", "8M",
           "--max-tries=3", "--retry-wait=1", "--timeout", str(timeout),
           "--auto-file-renaming=false"]
    for k, v in (headers or {}).items():
        cmd += ["--header", f"{k}: {v}"]
    if proxy:
        cmd += ["--all-proxy", proxy]
    if limit_bps > 0:
        cmd += ["--max-download-limit", str(int(limit_bps))]
    if resume:
        cmd.append("--continue=true")
    cmd += ["-o", fn, url]
    print("调用 aria2c:", " ".join(cmd))
    try:
        r = subprocess.run(cmd, cwd=dest_dir, check=False)
        return 0 if r.returncode == 0 else 1
    except FileNotFoundError:
        raise DownloadError("aria2c 未安装")


def download(url: str, dest_dir: str = ".", threads: int = 16, chunk_size: int = CHUNK_SIZE,
             resume: bool = True, engine: str = "native", timeout: int = DEFAULT_TIMEOUT,
             stop: dict | None = None, headers: dict | None = None,
             proxy: str | None = None, limit_bps: float = 0.0) -> int:
    """返回：0 成功，1 部分/可续传，2 失败。"""
    stop = stop or {"flag": False}
    os.makedirs(dest_dir, exist_ok=True)
    if engine == "aria2":
        return _download_aria2(url, dest_dir, threads, resume, timeout, stop,
                               proxy=proxy, limit_bps=limit_bps, headers=headers)
    if url.lower().startswith("ftp://"):
        from .ftp import ftp_download
        return ftp_download(url, dest_dir, resume, timeout, stop)
    if not url.lower().startswith(("http://", "https://")):
        raise DownloadError(f"不支持的协议: {url.split(':', 1)[0]}")
    return _download_native(url, dest_dir, threads, chunk_size, resume, timeout, stop,
                            headers=headers, proxy=proxy, limit_bps=limit_bps)
