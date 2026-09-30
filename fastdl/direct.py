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
import socket as _socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

import requests
from requests.adapters import HTTPAdapter

from .progress import SingleLineProgress
from .utils import disk_free, sanitize_filename

CHUNK_SIZE = 8 << 20  # 8 MiB（上限；实际会按文件大小/线程数自适应）
MIN_CHUNK = 1 << 20   # 1 MiB（下限）
DEFAULT_TIMEOUT = 30
MAX_CHUNK_RETRIES = 8      # 分片级重试（断流后从已写入部分续传）
MAX_CONNECT_RETRIES = 4    # 单次请求的连接重试（GFW/代理重置时换新连接重试）
READ_TIMEOUT_FLOOR = 120    # 大文件 CDN 长连接允许短暂空闲，避免过早断线

# socket 调优：小分片请求对延迟敏感 → Nagle 必须关；接收缓冲加大提升大带宽吞吐
_SOCKET_OPTIONS = [
    (_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1),
    (_socket.SOL_SOCKET, _socket.SO_RCVBUF, 2 << 20),
]


def _auto_chunk_size(size: int, threads: int) -> int:
    """按文件大小/线程数自适应分片：块太多太小→开销大，太少→并行度不足。"""
    if size <= 0:
        return CHUNK_SIZE
    want = max(1, threads * 4)          # 每线程约 4 块，兼顾并行与负载均衡
    cs = size // want
    cs = max(MIN_CHUNK, min(CHUNK_SIZE, cs))
    return max(MIN_CHUNK, (cs // MIN_CHUNK) << 20)


class DownloadError(Exception):
    pass


def _build_retry():
    """传输层重试策略：连接被重置/丢弃时自动换新连接（对 GFW、代理抖动有效）。

    只重试"连接建立"和 5xx，不重试 read（流中途断开由上层按 Range 续传处理，
    否则会重复追加数据）。
    """
    try:
        from urllib3.util.retry import Retry
    except ImportError:
        return None
    kw = dict(total=4, connect=4, read=0, status=4, backoff_factor=0.5,
              status_forcelist=(429, 500, 502, 503, 504), raise_on_status=False)
    try:
        return Retry(allowed_methods=frozenset(["GET", "HEAD"]), **kw)
    except TypeError:  # urllib3 < 1.26
        return Retry(method_whitelist=frozenset(["GET", "HEAD"]), **kw)


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
            delay = -self._tokens / self.limit if self._tokens < 0 else 0
        # 不能持锁睡眠，否则会把其它下载线程全部堵住，表现为速度归零。
        if delay > 0:
            time.sleep(delay)
            with self._lock:
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
    final_url: str | None = None   # 跟随重定向后的真实地址（如 GitHub release → 签名直链）


def _is_loopback(proxy: str) -> bool:
    try:
        host = urlsplit(proxy).hostname or proxy
    except Exception:  # noqa: BLE001
        host = proxy
    return host in ("127.0.0.1", "localhost", "::1")


def _make_session(threads: int, source_ip: str | None = None,
                  proxy: str | None = None) -> requests.Session:
    s = requests.Session()
    # 行为完全由参数决定，不偷偷使用环境变量里的代理（HTTPS_PROXY 等）
    s.trust_env = False
    bind_ip = source_ip
    if bind_ip and proxy and _is_loopback(proxy):
        # 本地代理必须走回环：源地址绑到物理网卡会连不上（WinError 10049）
        bind_ip = None
    retry = _build_retry()
    adapter = _FastAdapter(bind_ip, pool_maxsize=threads + 8, pool_connections=threads + 8,
                           max_retries=retry if retry is not None else 0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    return s


class _FastAdapter(HTTPAdapter):
    """给连接池注入 socket 调优；可选把源地址绑定到指定网卡。

    - TCP_NODELAY：Range 小请求对延迟敏感，关 Nagle
    - SO_RCVBUF：加大接收缓冲提升大带宽吞吐
    - source_address：只有用它的流量走那张网卡
    """

    def __init__(self, source_ip: str | None = None, **kw):
        self._source_ip = source_ip
        super().__init__(**kw)

    def _extra(self) -> dict:
        d = {"socket_options": _SOCKET_OPTIONS}
        if self._source_ip:
            d["source_address"] = (self._source_ip, 0)
        return d

    def init_poolmanager(self, *args, **kwargs):
        kwargs.update(self._extra())
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs.update(self._extra())
        return super().proxy_manager_for(*args, **kwargs)


def _get_with_retry(session: requests.Session, url: str, headers: dict, timeout: int,
                    attempts: int = MAX_CONNECT_RETRIES, stream: bool = True):
    """带重试的 GET：连接被重置/超时（GFW、代理不稳）时退避后换新连接重试。

    最终仍失败则抛 DownloadError，交由上层（分片级续传）处理。
    """
    last: Exception | None = None
    for i in range(attempts):
        try:
            return session.get(url, headers=headers,
                               timeout=(timeout, max(timeout, READ_TIMEOUT_FLOOR)),
                               stream=stream)
        except requests.RequestException as e:
            last = e
            if i < attempts - 1:
                time.sleep(min(0.8 * (2 ** i), 8))
    raise DownloadError(f"连接失败（已重试 {attempts} 次）: {last}")


def _filename_from_headers(headers: dict, url: str) -> str:
    cd = headers.get("Content-Disposition") or ""
    if "filename=" in cd:
        fn = cd.split("filename=")[-1].split(";")[0].strip('"')
        if fn:
            return unquote(fn)
    return unquote(urlsplit(url).path.rstrip("/").split("/")[-1] or "download")


def probe(url: str, threads: int = 16, timeout: int = DEFAULT_TIMEOUT,
          headers: dict | None = None, proxy: str | None = None,
          source_ip: str | None = None) -> ProbeResult:
    """权威探针：GET Range bytes=0-0。返回总长 + 是否支持分片 + 元数据。"""
    s = _make_session(threads, source_ip=source_ip, proxy=proxy)
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    h = {"Accept-Encoding": "identity"}
    if headers:
        h.update(headers)
    try:
        r = _get_with_retry(s, url, {**h, "Range": "bytes=0-0"}, timeout, attempts=6)
    except DownloadError as e:
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
            final_url=r.url if r.url and r.url != url else None,
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
    existing = os.path.getsize(part_path) if resume and os.path.exists(part_path) else 0
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
    try:
        r = _get_with_retry(session, url, h, timeout)
    except DownloadError as e:
        raise DownloadError(f"chunk {index} 连接失败: {e}") from e
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
            # iter_content 会把底层的断流/解码异常抛出；不能把一次短读误判为 EOF。
            for b in r.iter_content(chunk_size=1 << 20):
                if stop.get("flag"):
                    raise DownloadError("已停止")
                if not b:
                    continue
                f.write(b)
                got += len(b)
                if limiter:
                    limiter.wait(len(b))
                progress.add(len(b), rate_key=f"t{index % 8}")
        if got < (expect - existing):
            raise DownloadError(f"chunk {index} 数据不足: {got} < {expect-existing}")
        if got > (expect - existing):
            raise DownloadError(f"chunk {index} 数据超量: {got} > {expect-existing}")
    except (requests.RequestException, OSError) as e:
        raise DownloadError(f"chunk {index} 传输中断: {e}") from e
    finally:
        r.close()


def _write_meta(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _cleanup_workdir(workdir: str) -> None:
    """下载完成后移除 <name>.fastdl 目录（.fastdl 只在"未完成"时存在）。"""
    try:
        for f in os.listdir(workdir):
            os.remove(os.path.join(workdir, f))
        os.rmdir(workdir)
    except OSError:
        pass


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
                     proxy: str | None = None, limit_bps: float = 0.0,
                     source_ip: str | None = None) -> int:
    session = _make_session(threads, source_ip=source_ip, proxy=proxy)
    limiter = RateLimiter(limit_bps) if limit_bps > 0 else None
    try:
        pr = probe(url, threads, timeout, headers=headers, proxy=proxy, source_ip=source_ip)
    except DownloadError as e:
        raise DownloadError(
            f"{e}\n提示：境外站点（如 github）连接被重置/超时多为网络或代理问题——"
            f"请确认加速器节点可用，或用 --proxy 指定可用代理；境内站点不受影响。"
        ) from e

    fetch_url = pr.final_url or url          # 直接打重定向后的真实地址，省一次 302 往返
    if chunk_size <= 0:
        chunk_size = _auto_chunk_size(pr.size, threads)

    fn = sanitize_filename(pr.filename or "download")
    final = os.path.join(dest_dir, fn)
    workdir = os.path.join(dest_dir, f"{fn}.fastdl")
    dlmeta_path = os.path.join(workdir, ".dlmeta")
    # 注意：workdir 只在「分片模式」下创建（目录）；「单流模式」用的是同名
    # <name>.fastdl 文件，二者不能同时存在，所以此处不能预先建目录。

    meta = None
    if resume and os.path.exists(dlmeta_path):
        try:
            with open(dlmeta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError):
            meta = None
    if meta:
        # chunk_size 必须一致，否则 .part.<i> 与字节区间的映射会错位
        if (meta.get("url") != url or meta.get("size") != pr.size
                or meta.get("etag") != pr.etag or meta.get("chunk_size") != chunk_size):
            for f in os.listdir(workdir):
                if f.startswith(".part."):
                    os.remove(os.path.join(workdir, f))
            meta = None

    # 已完整下载过 → 直接跳过
    if (resume and meta is not None and pr.size > 0
            and os.path.exists(final) and os.path.getsize(final) == pr.size):
        print(f"已存在完整文件，跳过: {final}")
        _cleanup_workdir(workdir)
        return 0

    if pr.size == 0:
        open(final, "wb").close()
        return 0

    if not pr.ranges:
        # 单线程流式回退：写 <文件名>.fastdl，下完才改回正式名（半成品一眼可辨）
        part_file = final + ".fastdl"
        if os.path.isdir(part_file):
            # 极少见：同一 URL 之前走过分片模式留下了同名目录，避开冲突
            part_file = final + ".fastdl.part"
        # 兼容旧行为：若正式名已是完整文件，直接用
        if (resume and os.path.exists(final) and os.path.getsize(final) == pr.size):
            return 0
        have = os.path.getsize(part_file) if (resume and os.path.exists(part_file)) else 0
        for attempt in range(MAX_CHUNK_RETRIES):
            if stop.get("flag"):
                return 1
            h = {"Accept-Encoding": "identity"}
            if headers:
                h.update(headers)
            if have > 0:
                h["Range"] = f"bytes={have}-"
            try:
                r = _get_with_retry(session, fetch_url, h, timeout)
            except DownloadError as e:
                if attempt == MAX_CHUNK_RETRIES - 1:
                    print(f"\n连接中断，进度已保留（.fastdl 文件续传）: {e}")
                    return 1
                time.sleep(min(1 * (2 ** attempt), 15))
                continue
            try:
                mode = "ab" if (resume and have > 0 and r.status_code == 206) else "wb"
                with open(part_file, mode) as f:
                    for b in r.iter_content(chunk_size=1 << 20):
                        if stop.get("flag"):
                            print("\n已停止，进度保留。")
                            return 1
                        if not b:
                            continue
                        f.write(b)
                        have += len(b)
                        if limiter:
                            limiter.wait(len(b))
            except (requests.RequestException, OSError) as e:
                if attempt == MAX_CHUNK_RETRIES - 1:
                    print(f"\n连接中断，进度已保留（.fastdl 文件续传）: {e}")
                    return 1
                time.sleep(min(1 * (2 ** attempt), 15))
                continue
            finally:
                r.close()
            if os.path.getsize(part_file) == pr.size:
                os.replace(part_file, final)      # 下完才改回正式名
                return 0
        return 0 if os.path.getsize(final) == pr.size else 1

    # 分片模式
    n_chunks = (pr.size + chunk_size - 1) // chunk_size
    os.makedirs(workdir, exist_ok=True)
    if meta is None:
        _write_meta(dlmeta_path, {
            "url": url, "size": pr.size, "etag": pr.etag,
            "last_modified": pr.last_modified, "chunk_size": chunk_size, "n_chunks": n_chunks,
        })
    print(f"分片：{n_chunks} 块 × {chunk_size >> 20}MiB，{threads} 线程并行"
          + ("（已启用重定向直链）" if pr.final_url else ""))

    progress = SingleLineProgress(pr.size, label=f"{fn} ")
    # 各线程速率暂不显示（聚合速率已覆盖），避免显示 0B/s

    def worker(i):
        if stop.get("flag"):
            return
        # 优先用重定向后的直链（省 302）；若连续失败则回退到原始 URL
        # （签名直链可能有时效或 CDN 限流，回退可保证长下载不中断）
        urls = [fetch_url] if fetch_url == url else [fetch_url, url]
        last = None
        for attempt in range(MAX_CHUNK_RETRIES):
            if stop.get("flag"):
                return
            use = urls[min(attempt // 2, len(urls) - 1)]
            try:
                _fetch_chunk(use, i, pr.size, chunk_size, session, resume, timeout,
                             progress, stop, workdir, dlmeta_path,
                             headers=headers, limiter=limiter)
                return
            except DownloadError as e:
                last = e
                time.sleep(min(1 * (2 ** attempt), 15))
        if stop.get("flag"):
            return
        # 不抛出：保留其它分片进度，整块标记为未完成 → 返回 1 可续传
        print(f"\n分片 {i} 多次重试仍失败（进度已保留，重新运行可续传）: {last}")

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
    _cleanup_workdir(workdir)
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


def download(url: str, dest_dir: str = ".", threads: int = 16, chunk_size: int = 0,
             resume: bool = True, engine: str = "native", timeout: int = DEFAULT_TIMEOUT,
             stop: dict | None = None, headers: dict | None = None,
             proxy: str | None = None, limit_bps: float = 0.0,
             source_ip: str | None = None) -> int:
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
                            headers=headers, proxy=proxy, limit_bps=limit_bps,
                            source_ip=source_ip)
