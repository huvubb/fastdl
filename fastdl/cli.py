"""CLI 入口：argparse 子命令分发 + 进度线程 + Ctrl+C 优雅退出。"""
from __future__ import annotations

import argparse
import os
import sys

from . import __version__
from .config import load_config, save_config
from .utils import human_bytes


def _cmd_hash(args) -> int:
    from .ed2k import md4
    from .ed2k.link import make_ed2k_link

    path = args.file
    if not os.path.exists(path):
        print(f"文件不存在: {path}", file=sys.stderr)
        return 2
    size = os.path.getsize(path)
    print(f"计算 ed2k hash: {path} ({human_bytes(size)}) ...")
    top, part_hashes = md4.md4_file(path)
    link = make_ed2k_link(os.path.basename(path), size, top, part_hashes)
    print(f"顶层 hash : {top.hex()}")
    print(f"part 数量 : {len(part_hashes)}")
    print(f"ed2k 链接: {link}")
    return 0


def _parse_headers(args) -> dict | None:
    """汇总 --header/--cookie/--referer/--ua 成请求头 dict；全空则 None。"""
    out: dict[str, str] = {}
    for h in (args.header or []):
        if ":" in h:
            k, _, v = h.partition(":")
            out[k.strip()] = v.strip()
        else:
            raise ValueError(f"--header 格式应为 KEY:VALUE，收到: {h}")
    if args.cookie:
        out["Cookie"] = args.cookie
    if args.referer:
        out["Referer"] = args.referer
    if args.ua:
        out["User-Agent"] = args.ua
    return out or None


def _cmd_direct(args) -> int:
    from .direct import download
    from .config import load_config
    from .utils import parse_limit

    cfg = load_config()
    threads = args.threads or cfg.threads
    stop = {"flag": False}

    def on_sigint(signum, frame):
        stop["flag"] = True

    import signal
    signal.signal(signal.SIGINT, on_sigint)

    rc = download(
        args.url,
        dest_dir=args.output,
        threads=threads,
        resume=not args.no_resume,
        engine=args.engine,
        timeout=args.timeout,
        stop=stop,
        headers=_parse_headers(args),
        proxy=args.proxy,
        limit_bps=parse_limit(args.limit),
    )
    return rc


def _cmd_servers(args) -> int:
    from .ed2k.sources import probe_servers
    from .config import load_config

    cfg = load_config()
    servers = list(cfg.servers)
    if args.servers:
        servers = [parse_host_port(s) for s in args.servers]
    if args.server_list:
        servers += parse_server_file(args.server_list)
    if not servers and os.path.exists(_default_server_file()):
        servers = parse_server_file(_default_server_file())

    if args.test:
        return _servers_test(servers)
    for s in servers:
        print(f"{s[0]}:{s[1]}")
    return 0


def _servers_test(servers) -> int:
    from .ed2k.sources import probe_servers
    from .config import load_config

    cfg = load_config()
    print(f"测试 {len(servers)} 台服务器...")
    alive = probe_servers(servers, cfg, timeout=args_timeout())
    if not alive:
        print("全部服务器不可达（可能被墙）。ed2k 功能需要能连上 eD2k 服务器。")
        return 1
    print("可达服务器:")
    for host, port, msg in alive:
        print(f"  {host}:{port}  {msg}")
    return 0


# 全局 timeout 兜底
_TIMEOUT = 10


def args_timeout():
    return _TIMEOUT


def parse_host_port(s: str) -> tuple[str, int]:
    host, _, port = s.rpartition(":")
    if not port:
        raise ValueError(f"服务器格式应为 host:port: {s}")
    return host.strip(), int(port)


def parse_server_file(path: str) -> list[tuple[str, int]]:
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.split("#")[0].strip()
            if not line:
                continue
            try:
                out.append(parse_host_port(line))
            except ValueError:
                continue
    return out


def _default_server_file() -> str:
    """servers.txt 定位：PyInstaller onefile 下数据文件在 sys._MEIPASS，源码下在项目根。"""
    base = getattr(sys, "_MEIPASS", None) or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "servers.txt")


def _cmd_ed2k(args) -> int:
    from .ed2k.transfer import Ed2kDownloader
    from .ed2k.link import parse_ed2k_link
    from .ed2k.sources import SourceFetcher
    from .config import load_config

    cfg = load_config()
    link = parse_ed2k_link(args.link)
    servers = list(cfg.servers)
    if args.servers:
        servers = [parse_host_port(s) for s in args.servers]
    if args.server_list:
        servers += parse_server_file(args.server_list)
    if not servers and os.path.exists(_default_server_file()):
        servers = parse_server_file(_default_server_file())

    fetcher = SourceFetcher(servers, cfg, parallel=args.parallel_servers, dump=args.dump_traffic)
    dl = Ed2kDownloader(link, dest_dir=args.output, fetcher=fetcher,
                        max_sources=args.max_sources or cfg.max_sources,
                        dump=args.dump_traffic)
    return dl.run()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dl",
        description="fastdl：多线程直链 + ed2k 完整客户端下载工具",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    h = sub.add_parser("hash", help="计算文件的 ed2k 顶层 hash 并生成链接")
    h.add_argument("file")
    h.set_defaults(func=_cmd_hash)

    d = sub.add_parser("direct", help="多线程直链下载")
    d.add_argument("url")
    d.add_argument("-t", "--threads", type=int, default=0, help="线程数（默认读配置）")
    d.add_argument("-o", "--output", default=".", help="保存目录")
    d.add_argument("--no-resume", action="store_true", help="禁用断点续传")
    d.add_argument("--engine", choices=["native", "aria2"], default="native",
                   help="直链引擎（aria2 需已安装 aria2c）")
    d.add_argument("--timeout", type=int, default=30)
    d.add_argument("--header", action="append", default=None, metavar="KEY:VALUE",
                   help="自定义请求头（可多次），如 --header 'Referer: https://x'")
    d.add_argument("--cookie", default=None, help="Cookie 请求头")
    d.add_argument("--referer", default=None, help="Referer 请求头")
    d.add_argument("--ua", "--user-agent", default=None, help="自定义 User-Agent")
    d.add_argument("--proxy", default=None,
                   help="代理地址，如 http://127.0.0.1:7890 或 socks5://127.0.0.1:1080")
    d.add_argument("--limit", default=None, help="下载限速，如 5M / 512K（不限速省略）")
    d.set_defaults(func=_cmd_direct)

    s = sub.add_parser("servers", help="管理/测试 eD2k 服务器")
    s.add_argument("--test", action="store_true", help="实测各服务器是否可登录")
    s.add_argument("--servers", nargs="*", default=None, help="覆盖服务器 host:port...")
    s.add_argument("--server-list", default=None, help="服务器列表文件")
    s.set_defaults(func=_cmd_servers)

    e = sub.add_parser("ed2k", help="ed2k 多源下载")
    e.add_argument("link", help="ed2k://|file|...|/ 链接")
    e.add_argument("-o", "--output", default=".", help="保存目录")
    e.add_argument("--servers", nargs="*", default=None, help="覆盖服务器 host:port...")
    e.add_argument("--server-list", default=None, help="服务器列表文件")
    e.add_argument("--max-sources", type=int, default=0, help="最大并发源数")
    e.add_argument("--parallel-servers", type=int, default=8, help="并行探测的服务器数")
    e.add_argument("--dump-traffic", action="store_true", help="打印原始帧 hex（调试）")
    e.set_defaults(func=_cmd_ed2k)

    return p


def _download_link(link: str, dest_dir: str, cfg) -> int:
    """按链接类型分发下载。返回：0 完成，1 部分/可续传，2 失败。"""
    low = link.lower()
    if low.startswith("ed2k://"):
        from .ed2k.link import parse_ed2k_link
        from .ed2k.sources import SourceFetcher
        from .ed2k.transfer import Ed2kDownloader

        parsed = parse_ed2k_link(link)
        servers = list(cfg.servers)
        if not servers and os.path.exists(_default_server_file()):
            servers = parse_server_file(_default_server_file())
        fetcher = SourceFetcher(servers, cfg, parallel=8)
        dl = Ed2kDownloader(parsed, dest_dir=dest_dir, fetcher=fetcher,
                            max_sources=cfg.max_sources)
        return dl.run()
    if low.startswith(("http://", "https://", "ftp://")):
        from .direct import download
        return download(link, dest_dir=dest_dir, threads=cfg.threads)
    raise ValueError(f"无法识别的链接类型: {link[:60]}")


def _interactive(cfg) -> int:
    """无参数启动：交互模式，粘贴链接即下载，自动识别直链/ed2k。"""
    print("=" * 52)
    print(f"  fastdl {__version__} —— 多线程直链 + ed2k 下载器")
    print("  不用记参数：粘贴链接即可下载，可一次粘贴多个（每行一个）")
    print("=" * 52)

    default_dir = cfg.download_dir or os.path.join(os.getcwd(), "downloads")
    try:
        ans = input(f"\n下载目录 [默认: {default_dir}，直接回车使用]：").strip()
    except EOFError:
        return 0
    dest_dir = ans or default_dir
    if dest_dir != cfg.download_dir:
        cfg.download_dir = dest_dir
        save_config(cfg)
    os.makedirs(dest_dir, exist_ok=True)
    print(f"下载到: {dest_dir}\n")

    print("请输入下载链接（http/https/ftp 直链 或 ed2k://；可一次粘贴多个，每行一个；输入 q 退出）")
    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break
        except KeyboardInterrupt:
            print("\n已退出。")
            break
        if not line:
            continue
        if line.lower() in ("q", "quit", "exit", "退出"):
            break
        try:
            print(f"\n[开始下载] {line}")
            rc = _download_link(line, dest_dir, cfg)
            if rc == 0:
                print("[完成]")
            elif rc == 1:
                print("[部分完成] 重新运行可续传")
            else:
                print("[失败] 请检查链接或重试")
        except Exception as e:
            print(f"[下载出错] {e}")
    print("\n已退出，下次见！")
    return 0


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        # 无参数直接运行 → 进入交互模式
        return _interactive(load_config())
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\n已中断（Ctrl+C），进度已保留，可续传。")
        return 130
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        if os.environ.get("FASTDL_DEBUG"):
            raise
        return 2
