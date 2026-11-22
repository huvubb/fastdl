"""直链多线程下载器（HTTP/HTTPS）：Range 分片 + 多连接并行 + 断点续传。

设计要点（对照实现评审）：
- 权威探针 = GET `Range: bytes=0-0`：206+Content-Range 判定分片支持并拿总长；
  200 说明服务器无视 Range → 单线程流式回退。
- 必须 `Accept-Encoding: identity`，否则透明 gzip 会破坏字节偏移。
- 4~128MiB 自适应 chunk：CDN 按单连接限速时（GitHub ~400KB/s/连接），
  分片越细、并行连接越多、聚合带宽越高；`.part.<i>` 文件大小即进度。
- 续传：`.dlmeta` 记录 url/size/etag/chunk_size/n_chunks，ETag 变了清空重来。
- 合并按索引顺序流式拷贝，拷完即删 .part → 峰值磁盘 ≈ 最终文件 + 最大一块。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import socket as _socket
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlsplit

import requests
from requests.adapters import HTTPAdapter

from .progress import SingleLineProgress
from .utils import disk_free, sanitize_filename

CHUNK_SIZE = 128 << 20 # 128 MiB（减少大型下载的请求轮换）
MIN_CHUNK = 2 << 20    # 2 MiB（Fastly/GitHub 等按单连接限速的 CDN，必须靠更多连接聚合带宽；
                       #  每块仍 >=2MiB，请求数不失控，且分片请求可复用 keep-alive 连接）
DEFAULT_TIMEOUT = 30
MAX_CHUNK_RETRIES = 4      # 限制故障连接占用时间，失败后保留进度供续传
MAX_CONNECT_RETRIES = 2    # 连接失败快速重试，避免多层退避叠加
READ_TIMEOUT_CAP = 15        # 无数据时最多等 15 秒，及时释放卡住的 CDN 分片

# ---------- curl 兜底 ----------
# 下载速度过低时（CDN 单连接 QoS、偶发卡死、路由抖动），拉起系统 curl 以 Range
# 续传方式接手未完成的分片，为主下载增加并行连接；fastdl 自身速度恢复后不立刻
# 收队，给足宽限期再终止。
CURL_ASSIST_RATE = 2 << 20    # 聚合速度低于 2MB/s 视为"速度过低"，可触发兜底
CURL_ASSIST_TRIGGER = 10.0    # 速度过低持续 N 秒 → 拉起 curl 兜底
CURL_RECOVER_RATE = 8 << 20   # fastdl 自身速度高于 8MB/s 视为"已恢复"
CURL_RECOVER_GRACE = 15.0     # fastdl 自身速度恢复后，curl 再宽限 N 秒才终止
CURL_MAX_CONCURRENT = 6       # 同时最多几个 curl 分片进程
CURL_RESPAWN_COOLDOWN = 15.0  # 同一分片两次拉 curl 的最小间隔（防抖）

# ---------- GitHub 直连加速 ----------
# GitHub 直连国际链路常被 QoS（实测 0~4MB/s 随机抖动），纯多线程无解；
# 用公益加速镜像（URL 前缀代理）直连可达 10~20MB/s。节点会轮换，竞速动态选优。
_GH_HOSTS = {"github.com", "codeload.github.com", "raw.githubusercontent.com",
             "gist.githubusercontent.com", "objects.githubusercontent.com"}
_GH_MIRRORS = [
    "https://edgeone.gh-proxy.org",   # 国内 EdgeOne 优选，实测 20MB+/s
    "https://gh-proxy.org",
    "https://v6.gh-proxy.org",
    "https://ghfast.top",
    "https://ghproxy.net",
    "https://gh-proxy.com",
]
_GH_PROBE_BYTES = 1 << 20    # 竞速探测每节点拉 1MiB（再小会被 TLS 握手开销带偏）
_GH_PROBE_BUDGET = 3.0       # 单节点探测超时（秒）
_GH_RACE_BUDGET = 4.0        # 竞速总预算（秒）
_GH_PROBE_GOOD = 3.0         # 已有节点 >=3MB/s 就提前收工（MB/s）
_GH_CACHE_TTL = 7 * 86400    # 选中的镜像缓存 7 天，避免每次下载都重赛

# socket 调优：小分片请求对延迟敏感 → Nagle 必须关；接收缓冲加大提升大带宽吞吐
_SOCKET_OPTIONS = [
    (_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1),
    (_socket.SOL_SOCKET, _socket.SO_RCVBUF, 2 << 20),
    (_socket.SOL_SOCKET, _socket.SO_SNDBUF, 2 << 20),
    (_socket.SOL_SOCKET, _socket.SO_KEEPALIVE, 1),
]


def _auto_chunk_size(size: int, threads: int) -> int:
    """按文件大小/线程数自适应分片：块太多太小→开销大，太少→并行度不足。"""
    if size <= 0:
        return CHUNK_SIZE
    want = max(1, threads)              # 约每线程 1 块，减少请求与 TLS 握手开销
    cs = size // want
    cs = max(MIN_CHUNK, min(CHUNK_SIZE, cs))
    # cs 已经是字节数；这里必须乘以 MIN_CHUNK，不能再次左移 20 位。
    # 例如目标约 16MiB 时，错误写法会把它压成 4MiB。
    return max(MIN_CHUNK, (cs // MIN_CHUNK) * MIN_CHUNK)


def _is_github_url(url: str) -> bool:
    try:
        host = (urlsplit(url).hostname or "").lower()
    except Exception:  # noqa: BLE001
        return False
    return host in _GH_HOSTS


# 签名 release-assets 直链所在主机（浏览器"复制下载链接"得到的就是它）
_GH_ASSET_HOSTS = {"release-assets.githubusercontent.com",
                   "objects.githubusercontent.com",
                   "github-releases.githubusercontent.com"}


def _dns_poisoned(url: str) -> bool:
    """域名被本地 hosts/DNS 指向回环（127.0.0.1 / 0.0.0.0）→ 判定被拦截。

    典型场景：本机 hosts 黑名单把 GitHub 域名全部指向 127.0.0.1，此时直连
    必然 WinError 10049（绑着物理网卡源地址连回环）。镜像节点域名不在黑名单
    里，不受影响——这是 DNS 拦截下的唯一自救通道。
    """
    if _is_loopback_url(url):
        return False
    try:
        host = (urlsplit(url).hostname or "").lower()
        infos = _socket.getaddrinfo(host, 443, proto=_socket.IPPROTO_TCP)
    except Exception:  # noqa: BLE001
        return False
    addrs = {i[4][0] for i in infos}
    return bool(addrs) and all(a == "::1" or a.startswith(("127.", "0.")) for a in addrs)


def _canonicalize_github_asset(url: str) -> str | None:
    """签名 release-assets 直链 → 规范 github.com/releases/download/ URL。

    签名 URL 有两个坑：多数镜像节点不代理该主机形态；签名约 1 小时过期，
    长下载中途续传会 403。规范化后走统一加速链。
    映射关系（repo id → 全名 → 资产名）通过镜像代理的 GitHub API 完成，
    因为本机 api.github.com 可能同样被 hosts 拦截。
    """
    try:
        sp = urlsplit(url)
        host = (sp.hostname or "").lower()
        if host not in _GH_ASSET_HOSTS or "github-production-release-asset" not in sp.path:
            return None
        parts = sp.path.strip("/").split("/")
        repo_id = parts[1] if len(parts) > 1 else ""
        q = parse_qs(sp.query)
        disp = (q.get("response-content-disposition") or [""])[0]
        m = re.search(r'filename="?([^";]+)"?', disp)
        if not repo_id.isdigit() or not m:
            return None
        fname = unquote(m.group(1))
        for prefix in _GH_MIRRORS:
            try:
                r = requests.get(f"{prefix}/https://api.github.com/repositories/{repo_id}",
                                 headers={"Accept": "application/vnd.github+json",
                                          "User-Agent": "fastdl"},
                                 timeout=(3, 6))
                full = r.json().get("full_name")
                if not full:
                    continue
                r2 = requests.get(f"{prefix}/https://api.github.com/repos/{full}/releases",
                                  params={"per_page": 100},
                                  headers={"Accept": "application/vnd.github+json",
                                           "User-Agent": "fastdl"},
                                  timeout=(3, 10))
                for rel in r2.json():
                    for a in rel.get("assets", []):
                        if a.get("name") == fname:
                            return a["browser_download_url"]
            except Exception:  # noqa: BLE001
                continue
    except Exception:  # noqa: BLE001
        pass
    return None


def _gh_cache_path() -> str:
    base = os.path.join(os.path.expanduser("~"), ".fastdl")
    return os.path.join(base, "gh_mirror.json")


def _load_mirror_cache() -> str | None:
    """读上次竞速选中的镜像前缀（带 TTL），命中可省掉整轮竞速。"""
    try:
        with open(_gh_cache_path(), "r", encoding="utf-8") as f:
            data = json.load(f)
        if time.time() - float(data.get("ts", 0)) > _GH_CACHE_TTL:
            return None
        prefix = str(data.get("mirror") or "")
        return prefix or None
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _save_mirror_cache(prefix: str) -> None:
    try:
        p = _gh_cache_path()
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"mirror": prefix, "ts": time.time()}, f)
    except OSError:
        pass


def _probe_one(mu: str, source_ip: str | None, results: dict[str, float],
               lock: threading.Lock, good: threading.Event) -> None:
    """探测单个候选：必须支持 Range(206)，把 bytes/s 写进 results。"""
    s: requests.Session | None = None
    try:
        s = _make_session(2, source_ip=source_ip)
        t0 = time.monotonic()
        r = s.get(mu, headers={"Accept-Encoding": "identity",
                               "Range": f"bytes=0-{_GH_PROBE_BYTES - 1}"},
                  timeout=(3, int(_GH_PROBE_BUDGET)), stream=True)
        if r.status_code != 206:   # 必须支持 Range，否则无法分片下载
            r.close()
            return
        got = 0
        for b in r.iter_content(64 << 10):
            got += len(b)
            if got >= _GH_PROBE_BYTES:
                break
        r.close()
        dt = time.monotonic() - t0
        if dt > 0 and got > 0:
            rate = got / dt
            with lock:
                results[mu] = rate
            if rate >= _GH_PROBE_GOOD * 1e6:
                good.set()      # 够快了，不必等慢节点
    except Exception:  # noqa: BLE001
        pass
    finally:
        if s is not None:
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass


def _race_github_mirror(url: str, source_ip: str | None = None
                        ) -> tuple[list[str], float] | None:
    """并行竞速 GitHub 加速镜像，返回按速度排好序的候选链（含直连兜底）。

    返回 (候选URL列表, 最块节点MB/s)；全不可用返回 None。
    命中缓存时缓存节点排第一，否则全节点竞速（<=4s，找到快节点提前收工）。
    注意：探测值受握手延迟影响，普遍低于实际吞吐，因此只作相对排序用，
    真正的取舍放在下面的判定里——直连已死时任何活镜像都值得用。
    """
    cached = _load_mirror_cache()
    cands = [f"{cached}/{url}"] if cached else []
    cands += [f"{m}/{url}" for m in _GH_MIRRORS if m != cached] + [url]

    results: dict[str, float] = {}
    lock = threading.Lock()
    good = threading.Event()
    ths = [threading.Thread(target=_probe_one, args=(m, source_ip, results, lock, good),
                            daemon=True) for m in cands]
    for t in ths:
        t.start()
    # 全局截止时间：所有 join 共享预算；任一节点达标就提前收工
    deadline = time.monotonic() + _GH_RACE_BUDGET
    for t in ths:
        if good.is_set():
            break
        t.join(max(0.0, deadline - time.monotonic()))

    with lock:
        snapshot = dict(results)
    if not snapshot:
        return None
    direct = snapshot.get(url, 0.0)
    mirrors = sorted((u for u in snapshot if u != url), key=snapshot.get, reverse=True)
    if not mirrors:
        return None
    best = mirrors[0]
    best_rate = snapshot[best]
    # 判定只做相对比较：探测值被 TLS 握手压低，绝对值不可靠。
    # 直连已经死透(0)时任何活镜像都该上；直连很快(20MB/s)时镜像自然比不过。
    if best_rate < max(256 << 10, direct * 1.2):
        return None
    prefix = best[: -len(url)].rstrip("/")
    if prefix:
        _save_mirror_cache(prefix)
    # 候选链：最快镜像 → 其余活镜像 → 直连兜底。任何一环挂了自动降级。
    chain = [u for u in mirrors if snapshot[u] >= 256 << 10] + [url]
    return chain, best_rate / 1e6


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
    # 统一由 _get_with_retry / 分片层重试，禁用 urllib3 内部重试避免退避叠加。
    kw = dict(total=0, connect=0, read=0, status=0, backoff_factor=0,
              status_forcelist=(), raise_on_status=False)
    try:
        return Retry(allowed_methods=frozenset(["GET", "HEAD"]), **kw)
    except TypeError:  # urllib3 < 1.26
        return Retry(method_whitelist=frozenset(["GET", "HEAD"]), **kw)


def _resolve_curl() -> str | None:
    """找一个真正能执行的 curl。

    仅 shutil.which 不够：WSL 的 PATH 互操作会把 Windows 侧同名文件排进来
    （DrvFS 上权限位全是合成值，which 视为可执行，Popen 直接 Exec format error）。
    候选逐个试跑 --version，选第一个能跑的；都没有则禁用 curl 兜底。
    """
    cands = [shutil.which("curl"), "/usr/bin/curl", "/usr/local/bin/curl",
             "/bin/curl", "/snap/bin/curl"]
    tried: set[str] = set()
    for c in cands:
        if not c or c in tried:
            continue
        tried.add(c)
        try:
            r = subprocess.run([c, "--version"], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=5)
            if r.returncode == 0:
                return c
        except (OSError, subprocess.SubprocessError):
            continue
    return None


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


def _is_loopback_url(url: str) -> bool:
    """目标是本机/回环地址时，不能把源地址绑到物理网卡（WinError 10049）。"""
    try:
        host = (urlsplit(url).hostname or "").lower()
    except Exception:  # noqa: BLE001
        return False
    return host in ("localhost", "::1") or host.startswith("127.")


def _bind_ip_ok(ip: str) -> bool:
    """验证源 IP 当前真能发起外连（WLAN 重连窗口期默认路由可能短暂变成
    169.254.* APIPA 地址，绑定它会 10049；宁可放弃绑定也不能让下载全挂）。"""
    try:
        sk = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        sk.settimeout(1.5)
        sk.bind((ip, 0))
        sk.connect(("223.5.5.5", 443))
        sk.close()
        return True
    except OSError:
        return False


def _make_session(threads: int, source_ip: str | None = None,
                  proxy: str | None = None, auto_bind: bool = True) -> requests.Session:
    s = requests.Session()
    # 行为完全由参数决定，不偷偷使用环境变量里的代理（HTTPS_PROXY 等）
    s.trust_env = False
    bind_ip = source_ip
    if not bind_ip and not proxy and auto_bind:
        # 自动锁定当前系统默认物理出口，避免多网卡时下载流量走错接口。
        try:
            from .netif import default_interface
            default = default_interface()
            bind_ip = default.get("ip") if default else None
        except Exception:  # noqa: BLE001
            bind_ip = None
    if bind_ip and proxy and _is_loopback(proxy):
        # 本地代理必须走回环：源地址绑到物理网卡会连不上（WinError 10049）
        bind_ip = None
    if bind_ip and not _bind_ip_ok(bind_ip):
        # 源 IP 瞬态失效（APIPA/网卡切换）→ 放弃绑定，跟随系统默认路由
        bind_ip = None
    retry = _build_retry()
    pool_size = max(threads + 16, 256)
    adapter = _FastAdapter(bind_ip, pool_maxsize=pool_size, pool_connections=pool_size,
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
                               timeout=(min(timeout, 10), min(timeout, READ_TIMEOUT_CAP)),
                               stream=stream)
        except requests.RequestException as e:
            last = e
            if i < attempts - 1:
                time.sleep(min(0.25 * (2 ** i), 0.5))
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
    if _dns_poisoned(url):
        host = (urlsplit(url).hostname or "").lower()
        raise DownloadError(
            f"DNS 被本地拦截: {host} 解析到 127.0.0.1"
            f"（hosts 黑名单或 DNS 缓存，Windows 检查 C:\\Windows\\System32\\drivers\\etc\\hosts）"
        )
    s = _make_session(threads, source_ip=source_ip, proxy=proxy,
                      auto_bind=not _is_loopback_url(url))
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
                 limiter: RateLimiter | None = None,
                 yield_flags: dict | None = None) -> None:
    """下载一个 chunk 到 .part.<index>；支持从已有大小续传。失败抛 DownloadError。

    yield_flags: 让位标志（index -> True）。curl 兜底接手前置位，worker 在每个
    数据块写入前检查，置位后立即放弃该块（在途数据丢弃，不落盘），保证 curl
    从快照偏移接管后不会与 worker 双写错位。
    """
    start, end = _chunk_range(index, chunk_size, size)
    expect = end - start
    part_path = _part_path(workdir, index)
    existing = os.path.getsize(part_path) if resume and os.path.exists(part_path) else 0
    if existing > expect:
        # 上次异常退出留下了超量数据（如服务端多发了字节）：必须截断，
        # 否则下面会把它当"已完成"，合并出来的文件是错的。
        with open(part_path, "r+b") as f:
            f.truncate(expect)
        existing = expect
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
        # buffering=0 直写：写入实时落盘，curl 兜底接管时快照偏移才真实可靠；
        # 且让位/中断时缓冲区里不会残留待写数据，避免 close() 时补写出错位字节。
        # 读取块取 64KB：让位标志的检查粒度 = 传输 64KB 的时间，慢速链路也能及时让位。
        with open(part_path, mode, buffering=0) as f:
            # iter_content 会把底层的断流/解码异常抛出；不能把一次短读误判为 EOF。
            for b in r.iter_content(chunk_size=64 << 10):
                if stop.get("flag"):
                    raise DownloadError("已停止")
                if yield_flags is not None and yield_flags.get(index):
                    raise DownloadError(f"chunk {index} 已让位给 curl")
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
    with open(dest, "wb", buffering=8 << 20) as out:
        done = 0
        for i in range(n_chunks):
            src = _part_path(workdir, i)
            if not os.path.exists(src):
                raise DownloadError(f"合并时缺少 .part.{i}")
            with open(src, "rb", buffering=8 << 20) as f:
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


def _has_secret_headers(headers: dict | None) -> bool:
    """带鉴权/隐私的请求头绝不能发给第三方加速镜像。"""
    if not headers:
        return False
    sensitive = ("authorization", "cookie", "x-api-key", "x-auth", "token", "private")
    return any(k.lower() in sensitive or "token" in k.lower() or "auth" in k.lower()
               for k in headers)


def _github_expected_digest(url: str, headers: dict | None) -> str | None:
    """查 GitHub release 资产的官方 sha256（api.github.com 提供的 digest）。

    走第三方镜像时用它兜底：镜像再怎么不可信，哈希对不上我们就报错。
    非 release 直链 / API 不可达时返回 None（跳过校验）。
    """
    m = re.match(r"https://github\.com/([^/]+)/([^/]+)/releases/download/([^/]+)/(.+)$", url)
    if not m:
        return None
    owner, repo, tag, asset = m.groups()
    try:
        # 走 GitHub API 而不是镜像：这是校验基准，必须来自可信源
        r = requests.get(f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}",
                         timeout=(5, 10),
                         headers={"User-Agent": "fastdl", "Accept": "application/vnd.github+json"})
        if r.status_code != 200:
            return None
        for a in r.json().get("assets", []):
            if a.get("name") == unquote(asset):
                digest = a.get("digest") or ""
                return digest.split(":", 1)[1] if digest.startswith("sha256:") else None
    except Exception:  # noqa: BLE001
        return None
    return None


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(4 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _download_native(url: str, dest_dir: str, threads: int, chunk_size: int,
                     resume: bool, timeout: int, stop: dict, headers: dict | None = None,
                     proxy: str | None = None, limit_bps: float = 0.0,
                     source_ip: str | None = None, gh_accel: bool = True) -> int:
    # 回环目标（本机服务/测试服务器）不做网卡绑定，否则源地址不在目标网段直接 10049
    if _is_loopback_url(url):
        source_ip = None
    session = _make_session(threads, source_ip=source_ip, proxy=proxy,
                            auto_bind=not _is_loopback_url(url))
    limiter = RateLimiter(limit_bps) if limit_bps > 0 else None
    # 签名 release-assets 直链 → 规范 github.com URL：
    # 镜像更友好、签名不过期（长下载中途续传不会 403）
    canon = _canonicalize_github_asset(url)
    if canon:
        print(f"签名直链已规范化: {canon.rsplit('/', 2)[-2]}/{canon.rsplit('/', 1)[-1]}")
        url = canon
    # DNS 拦截检测：hosts 黑名单把 GitHub 域名指向 127.0.0.1 时直连必死（10049），
    # 唯一自救通道是镜像（镜像域名不在黑名单里）
    poisoned = _dns_poisoned(url)
    if poisoned:
        print("检测到 DNS 拦截（本机 hosts 把该域名指向 127.0.0.1），直连已跳过，走镜像候选链")
    # GitHub 直连加速：先竞速选镜像（并行 512KiB 探测，~1~4s），选中后探针也走镜像，
    # 彻底绕开直连链路的慢握手/重试；无合适节点则回退普通直连流程。
    # 候选链：正常情况下就是 [url]；开了加速则是 [镜像1, 镜像2, ..., 直连]
    chain: list[str] = []
    if gh_accel and not proxy and _is_github_url(url):
        if _has_secret_headers(headers):
            # Cookie/Token 发到第三方镜像等于泄露账号，宁可不加速
            print("检测到鉴权请求头，跳过镜像加速（避免泄露凭据）")
        else:
            raced = _race_github_mirror(url, source_ip=source_ip)
            if raced:
                chain, mbs = raced
                print(f"GitHub 加速: 首选 {urlsplit(chain[0]).hostname}"
                      f"（探测 {mbs:.1f}MB/s，共 {len(chain)} 个候选可自动降级）")
    if poisoned and not chain and _is_github_url(url):
        # 竞速没选出候选（可能全慢）但直连已死 → 强制按默认顺序全试镜像
        chain = [f"{m}/{url}" for m in _GH_MIRRORS]
        print("竞速未选出快节点，按默认顺序尝试全部镜像...")

    # 探针沿候选链逐个尝试：某个镜像挂了自动换下一个，全挂才报错
    pr = None
    probed = url
    probe_err: Exception | None = None
    for cand in (chain or [url]):
        try:
            pr = probe(cand, threads, timeout, headers=headers, proxy=proxy, source_ip=source_ip)
            probed = cand
            break
        except DownloadError as e:
            probe_err = e
            if cand != url:
                print(f"节点 {urlsplit(cand).hostname} 探针失败，换下一个...")
    if pr is None:
        hint = ("本机 DNS 把该域名指向 127.0.0.1（hosts 黑名单）且镜像候选全部不可用；"
                "可检查拦截工具/hosts，或用 --proxy 指定代理"
                if poisoned else
                "境外站点（如 github）连接被重置/超时多为网络问题——"
                "可尝试 --proxy 指定可用代理，或稍后重试（镜像节点可能临时限流）；"
                "境内站点不受影响。")
        raise DownloadError(f"探针失败: {probe_err}\n提示：{hint}")

    if probed != url:
        # 走镜像：用镜像前缀地址（镜像内部会处理跳转），不要直接打签名 CDN 直链
        fetch_url = probed
    else:
        # 直连：优先打重定向后的真实地址，省掉每块一次 302
        fetch_url = pr.final_url or url
    # 候选链第一位必须是刚探通的地址，其余保持原顺序做降级
    chain = [fetch_url] + [c for c in chain if c != fetch_url and c != probed]
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
        same_file = (meta.get("url") == url and meta.get("size") == pr.size
                     and meta.get("etag") == pr.etag)
        if not same_file:
            for f in os.listdir(workdir):
                if f.startswith(".part."):
                    os.remove(os.path.join(workdir, f))
            meta = None
        else:
            # 分片索引由 chunk_size 决定。保留旧布局续传，避免升级时清掉已有下载量。
            old_chunk_size = int(meta.get("chunk_size") or 0)
            if old_chunk_size > 0 and old_chunk_size != chunk_size:
                print(f"沿用未完成下载的 {old_chunk_size >> 20}MiB 分片布局，保留已有进度")
                chunk_size = old_chunk_size

    # 已完整下载过 → 直接跳过。
    # 注意：下完后 .fastdl 目录会被清掉（meta 为 None），所以不能只靠 meta 判断，
    # 否则每次重跑同一个链接都会把几 GB 重新下一遍。
    if (resume and pr.size > 0 and os.path.exists(final)
            and os.path.getsize(final) == pr.size
            and (meta is not None or not os.path.isdir(workdir))):
        print(f"已存在完整文件（大小一致），跳过: {final}")
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
        urls = chain or [fetch_url]
        retries = max(MAX_CHUNK_RETRIES, len(urls) * 2)
        for attempt in range(retries):
            if stop.get("flag"):
                return 1
            use = urls[min(attempt // 2, len(urls) - 1)]
            h = {"Accept-Encoding": "identity"}
            if headers:
                h.update(headers)
            if have > 0:
                h["Range"] = f"bytes={have}-"
            try:
                r = _get_with_retry(session, use, h, timeout)
            except DownloadError as e:
                if attempt == retries - 1:
                    print(f"\n连接中断，进度已保留（.fastdl 文件续传）: {e}")
                    return 1
                time.sleep(min(1 * (2 ** attempt), 15))
                continue
            try:
                resume_ok = (resume and have > 0 and r.status_code == 206)
                mode = "ab" if resume_ok else "wb"
                if not resume_ok:
                    have = 0      # 文件会被截断重写，进度必须归零，否则下次 Range 会跳数据
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
                if attempt == retries - 1:
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

    # ---------- curl 兜底监督器 ----------
    # 状态全部收在闭包里：curl_pool(分片->进程)、curl_owned(curl 接手的分片集合)、
    # curl_bytes(curl 累计写入字节，用于分离 fastdl 自身速率)、stalled_since(归零起点)、
    # grace_deadline(速度恢复后的收队截止时刻)。
    curl_path = _resolve_curl()
    curl_pool: dict[int, subprocess.Popen] = {}
    curl_owned: set[int] = set()
    chunk_yield: dict[int, bool] = {}          # 让位标志：worker 写入前检查
    curl_bytes = [0]
    curl_cooldown: dict[int, float] = {}
    curl_state_lock = threading.Lock()
    stalled_since: list[float | None] = [None]
    grace_deadline: list[float | None] = [None]
    fastdl_win: list[tuple[float, int]] = []   # (t, fastdl 自身累计字节) 滑动窗口
    last_progress = [time.monotonic()]         # 最近一次有实际进展的时刻
    last_done = [0]
    curl_writer_threads: list[threading.Thread] = []

    def _part_size(i: int) -> int:
        p = _part_path(workdir, i)
        return os.path.getsize(p) if os.path.exists(p) else 0

    def _stop_curls() -> None:
        with curl_state_lock:
            items = list(curl_pool.items())
            curl_pool.clear()
            curl_owned.clear()
            chunk_yield.clear()
            grace_deadline[0] = None
        for _, p in items:
            try:
                p.terminate()
            except OSError:
                pass
        # 写盘线程随管道 EOF 自然退出；terminate 前已写盘的数据留在 part 文件里可续传

    def _spawn_curl(i: int) -> bool:
        """用 curl 接手分片 i：先让 worker 让位，再从当前已写字节续传。

        正确性关键：worker 在每个数据块写入前检查让位标志，因此置位之后
        不会再有 worker 字节落盘，curl 从快照偏移接管不会双写错位。
        """
        start, end = _chunk_range(i, chunk_size, pr.size)
        expect = end - start
        if _part_size(i) >= expect:
            return False
        chunk_yield[i] = True
        time.sleep(0.5)   # 给 worker 让位留一个检查周期
        existing = _part_size(i)
        if existing >= expect:
            chunk_yield.pop(i, None)
            return False
        cmd = ["curl", "-sS", "-L", "--fail", "--connect-timeout", "10",
               "--speed-limit", "1024", "--speed-time", "30", "--retry", "2",
               "-r", f"{start + existing}-{end - 1}",
               "-H", "Accept-Encoding: identity"]
        for k, v in (headers or {}).items():
            cmd += ["-H", f"{k}: {v}"]
        cmd.append(fetch_url)
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
        except OSError:
            # 拉起失败也进冷却，避免监督循环 0.2s 一次疯狂重试
            curl_cooldown[i] = time.monotonic() + CURL_RESPAWN_COOLDOWN
            chunk_yield.pop(i, None)
            return False

        def writer() -> None:
            written = existing
            try:
                # buffering=0：terminate 时缓冲区里的字节不会丢
                with open(_part_path(workdir, i), "ab", buffering=0) as f:
                    while True:
                        b = p.stdout.read(256 << 10)
                        if not b:
                            break
                        if written + len(b) > expect:
                            # 服务器无视 Range(200) 把整个文件灌下来了 → 数据错位，
                            # 立即终止并删除该分片重下，绝不合并脏数据
                            f.close()
                            try:
                                os.remove(_part_path(workdir, i))
                            except OSError:
                                pass
                            try:
                                p.terminate()
                            except OSError:
                                pass
                            return
                        f.write(b)
                        written += len(b)
                        curl_bytes[0] += len(b)
                        if limiter:
                            limiter.wait(len(b))
                        progress.add(len(b), rate_key=f"c{i % 8}")
            except OSError:
                pass

        wt = threading.Thread(target=writer, daemon=True)
        wt.start()
        curl_writer_threads.append(wt)
        now = time.monotonic()
        with curl_state_lock:
            curl_pool[i] = p
            curl_owned.add(i)
            curl_cooldown[i] = now + CURL_RESPAWN_COOLDOWN
        return True

    def _curl_supervise() -> None:
        """每 0.2s 巡检一次：速度过低拉 curl 增加并行连接；fastdl 恢复后宽限再收队。"""
        if not curl_path:
            return   # 系统没有 curl，兜底机制整体禁用
        now = time.monotonic()
        # 1) 收尸：自然退出/死亡/完成的 curl
        with curl_state_lock:
            running = {i: p for i, p in curl_pool.items()}
        for i, p in running.items():
            if p.poll() is None:
                continue
            with curl_state_lock:
                curl_pool.pop(i, None)
                curl_owned.discard(i)
                chunk_yield.pop(i, None)
            start, end = _chunk_range(i, chunk_size, pr.size)
            sz = _part_size(i)
            if sz > (end - start):
                # 超量：数据错位（对端无视 Range），删掉重下
                try:
                    os.remove(_part_path(workdir, i))
                except OSError:
                    pass
            elif sz >= (end - start):
                print(f"\ncurl 兜底完成: 分片 {i}", flush=True)
                last_progress[0] = now
        # 2) 速度监测：聚合速率 vs fastdl 自身速率（3s 滑动窗口）
        if progress.done > last_done[0]:
            last_done[0] = progress.done
            last_progress[0] = now
        rate = progress.current_rate
        fastdl_win.append((now, max(0, progress.done - curl_bytes[0])))
        cutoff = now - 3.0
        while len(fastdl_win) > 1 and fastdl_win[0][0] < cutoff:
            fastdl_win.pop(0)
        t0, d0 = fastdl_win[0]
        dt = now - t0
        fastdl_rate = (fastdl_win[-1][1] - d0) / dt if dt > 0.2 else 0.0
        with curl_state_lock:
            n_curl = len(curl_pool)
        # 3) 聚合速度过低持续 ≥ 触发阈值 且有可接手分片 → 拉 curl 增加并行连接。
        #    显式限速（--limit）时速度低是用户意图，不触发兜底。
        pending = [i for i in range(n_chunks)
                   if i not in curl_owned
                   and _part_size(i) < (_chunk_range(i, chunk_size, pr.size)[1]
                                        - _chunk_range(i, chunk_size, pr.size)[0])
                   and curl_cooldown.get(i, 0) <= now]
        if rate < CURL_ASSIST_RATE and not limiter and pending:
            if stalled_since[0] is None:
                stalled_since[0] = now
            if now - stalled_since[0] >= CURL_ASSIST_TRIGGER and n_curl < CURL_MAX_CONCURRENT:
                for i in pending[:CURL_MAX_CONCURRENT - n_curl]:
                    if _spawn_curl(i):
                        n_curl += 1
                        print(f"\ncurl 兜底: 分片 {i} 由 curl 接手"
                              f"（速度 {rate / 1e6:.2f}MB/s 低于阈值，"
                              f"持续 {CURL_ASSIST_TRIGGER:.0f}s）", flush=True)
        else:
            stalled_since[0] = None
        # 4) fastdl 自身速度恢复 → 不立刻收 curl，宽限 N 秒；期间再掉速则取消收队
        if n_curl and fastdl_rate > CURL_RECOVER_RATE:
            if grace_deadline[0] is None:
                grace_deadline[0] = now + CURL_RECOVER_GRACE
            elif now >= grace_deadline[0]:
                print(f"\ncurl 收队: fastdl 速度已恢复"
                      f"（{fastdl_rate / 1e6:.1f}MB/s），宽限 {CURL_RECOVER_GRACE:.0f}s 结束",
                      flush=True)
                _stop_curls()
        elif n_curl:
            grace_deadline[0] = None

    def worker(i):
        if stop.get("flag"):
            return
        # 候选链逐级降级：[镜像1, 镜像2, ..., 直连]。
        # 每个候选给 2 次机会，全挂才算这块失败（进度保留，重跑续传）。
        urls = chain or [fetch_url]
        if urls[0] != fetch_url:
            urls = [fetch_url] + [u for u in urls if u != fetch_url]
        last = None
        # 候选越多给的重试次数越多，保证每个候选都能轮到（每个 2 次机会）
        retries = max(MAX_CHUNK_RETRIES, len(urls) * 2)
        for attempt in range(retries):
            if stop.get("flag"):
                return
            if i in curl_owned:
                return   # curl 已接手该分片，fastdl 让位，避免双写同一 part 文件
            use = urls[min(attempt // 2, len(urls) - 1)]
            try:
                _fetch_chunk(use, i, pr.size, chunk_size, session, resume, timeout,
                             progress, stop, workdir, dlmeta_path,
                             headers=headers, limiter=limiter,
                             yield_flags=chunk_yield)
                return
            except DownloadError as e:
                last = e
                if "让位" in str(e):
                    return   # curl 已接手，静默让位，不算失败
                if attempt == 0:
                    print(f"\n分片 {i} 首次请求失败，快速重连: {e}", flush=True)
                time.sleep(min(0.25 * (2 ** attempt), 1.0))
        if stop.get("flag"):
            return
        # 不抛出：保留其它分片进度，整块标记为未完成 → 返回 1 可续传
        print(f"\n分片 {i} 多次重试仍失败（进度已保留，重新运行可续传）: {last}")

    try:
        with ThreadPoolExecutor(max_workers=threads) as ex:
            futs = [ex.submit(worker, i) for i in range(n_chunks)]
            while True:
                progress.paint()
                _curl_supervise()
                if stop.get("flag"):
                    ex.shutdown(cancel_futures=True)
                    break
                # workers 全退且 curl 也不再持有分片（全完成或全放弃）才结束；
                # curl 死亡的分片若速度仍为零，监督器会在冷却后重新拉起
                if all(f.done() for f in futs) and not curl_pool:
                    break
                # workers 已退但 curl 长时间无实际进展 → 停止兜底，按可续传收尾
                if (all(f.done() for f in futs) and curl_pool
                        and time.monotonic() - last_progress[0] > 120):
                    print("\ncurl 兜底多次尝试无进展，停止兜底（进度已保留，可续传）",
                          flush=True)
                    _stop_curls()
                    break
                time.sleep(0.2)
            for f in futs:
                if f.exception() is not None:
                    raise f.exception()
    except KeyboardInterrupt:
        stop["flag"] = True
        return 1
    finally:
        _stop_curls()
        # 等所有 curl 写盘线程退出（管道 EOF 后很快），释放 part 文件锁再走合并/续传
        for wt in list(curl_writer_threads):
            wt.join(5.0)
        progress.finish("")

    def _part_done(i: int) -> bool:
        p = _part_path(workdir, i)
        if not os.path.exists(p):      # 这块一次都没连上 → 未完成，不是错误
            return False
        start, end = _chunk_range(i, chunk_size, pr.size)
        return os.path.getsize(p) >= (end - start)

    complete = all(_part_done(i) for i in range(n_chunks))
    if not complete:
        return 1

    if os.path.exists(final):
        os.remove(final)
    _merge(final, workdir, n_chunks, pr.size)

    # 走了第三方镜像 → 用 GitHub 官方 sha256 校验，防止镜像投毒/截断。
    # 校验失败说明拿到的是错文件，绝不能留成正式名，也不能删掉证据。
    if probed != url:
        expect = _github_expected_digest(url, headers)
        if expect:
            print("校验 SHA-256（镜像下载，对照 GitHub 官方值）...")
            actual = _sha256_file(final)
            if actual != expect:
                bad = final + ".bad"
                try:
                    os.replace(final, bad)
                except OSError:
                    bad = final
                _cleanup_workdir(workdir)
                raise DownloadError(
                    f"镜像文件校验失败！期望 {expect[:16]}... 实际 {actual[:16]}...\n"
                    f"损坏文件已改名为 {os.path.basename(bad)}，请重试或换节点。"
                )
            print("SHA-256 校验通过。")
        else:
            print("（未取到官方 sha256，跳过校验）")

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


def download(url: str, dest_dir: str = ".", threads: int = 128, chunk_size: int = 0,
             resume: bool = True, engine: str = "native", timeout: int = DEFAULT_TIMEOUT,
             stop: dict | None = None, headers: dict | None = None,
             proxy: str | None = None, limit_bps: float = 0.0,
             source_ip: str | None = None, gh_accel: bool = True) -> int:
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
                            source_ip=source_ip, gh_accel=gh_accel)
