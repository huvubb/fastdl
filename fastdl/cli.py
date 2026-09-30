"""CLI 入口：argparse 子命令分发 + 进度线程 + Ctrl+C 优雅退出。"""
from __future__ import annotations

import argparse
import os
import sys

from . import __version__
from .config import load_config, save_config
from .utils import default_download_dir, human_bytes


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


def _download_link(link: str, dest_dir: str, cfg, source_ip: str | None = None) -> int:
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
        return download(link, dest_dir=dest_dir, threads=cfg.threads, source_ip=source_ip)
    raise ValueError(f"无法识别的链接类型: {link[:60]}")


def _ask(prompt: str) -> str | None:
    """读一行输入；EOF / Ctrl+C 返回 None（上层据此退出）。"""
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):
        return None


def _pick_iface(cfg) -> None:
    """选择下载网卡：只有下载器的 TCP 会绑定到这张网卡。"""
    from .netif import default_interface, list_interfaces

    ifaces = list_interfaces()
    print("\n本机网卡（选中后，只有下载器的 TCP 会走它）：")
    print("  [0] 自动（跟随系统默认路由）")
    for i, it in enumerate(ifaces, 1):
        tag = {"physical": "物理", "virtual": "虚拟", "loopback": "回环"}.get(it["kind"], "?")
        cur = "  <-当前" if cfg.iface_ip and it["ip"] == cfg.iface_ip else ""
        print(f"  [{i}] {it['name']}    {it['ip']}    ({tag}){cur}")
    d = default_interface()
    if d:
        print(f"  提示：系统当前默认出口是 {d['ip']}")

    sel = _ask("请选择编号（直接回车=取消）：")
    if sel is None:
        return
    sel = sel.strip()
    if not sel:
        print("  已取消。")
        return
    if not sel.isdigit():
        print("  输入无效。")
        return
    n = int(sel)
    if n == 0:
        cfg.iface_ip, cfg.iface_name = "", ""
        save_config(cfg)
        print("  已设为：自动（跟随系统）")
    elif 1 <= n <= len(ifaces):
        it = ifaces[n - 1]
        cfg.iface_ip, cfg.iface_name = it["ip"], it["name"]
        save_config(cfg)
        print(f"  已选定：{it['name']} ({it['ip']})")
        print("  之后下载的 TCP 会绑定这张网卡，其它软件不受影响。")
    else:
        print("  编号超出范围。")


def _clear_conns() -> None:
    """清理半死/僵尸 TCP 连接。"""
    from .conns import clear_invalid, scan

    items = scan(only_invalid=True)
    if not items:
        print("\n没有发现无效连接（半死/僵尸状态）。")
        return
    print(f"\n发现 {len(items)} 条无效连接：")
    for c in items[:20]:
        print(f"  {c['status']:<10} {c['laddr_s']} -> {c['raddr_s']}   pid={c['pid']}")
    if len(items) > 20:
        print(f"  ... 还有 {len(items) - 20} 条")
    ans = _ask("确认全部清理？[y/N]：")
    if ans is None or ans.strip().lower() not in ("y", "yes"):
        print("  已取消。")
        return
    tried, ok, errs = clear_invalid()
    print(f"  已清理 {ok}/{tried} 条。")
    for e in errs:
        print(f"  提示：{e}")


def _show_conns() -> None:
    from collections import Counter

    from .conns import scan

    items = scan()
    if not items:
        print("\n（读不到连接：可能需要权限，或系统不支持）")
        return
    cnt = Counter(c["status"] for c in items)
    print(f"\n本机 TCP 连接共 {len(items)} 条：")
    for st, n in cnt.most_common():
        print(f"  {st:<12} {n}")


def _set_dir(cfg) -> None:
    ans = _ask(f"新的下载目录 [当前: {cfg.download_dir}]：")
    if ans is None or not ans.strip():
        print("  已取消。")
        return
    cfg.download_dir = ans.strip()
    save_config(cfg)
    os.makedirs(cfg.download_dir, exist_ok=True)
    print(f"  已设为：{cfg.download_dir}")


def _set_threads(cfg) -> None:
    ans = _ask(f"并行线程数 [当前: {cfg.threads}，建议 32~64，越大越快但吃带宽]：")
    if ans is None or not ans.strip():
        print("  已取消。")
        return
    try:
        n = int(ans.strip())
    except ValueError:
        print("  请输入数字。")
        return
    if not (1 <= n <= 256):
        print("  范围 1~256。")
        return
    cfg.threads = n
    save_config(cfg)
    print(f"  已设为：{n} 线程")


def _menu_download(cfg) -> None:
    print("\n粘贴下载链接（http/https/ftp 直链 或 ed2k://；可一次粘贴多个，每行一个）")
    print("输入 q 返回主菜单")
    while True:
        line = _ask("> ")
        if line is None:
            return
        line = line.strip()
        if not line:
            continue
        if line.lower() in ("q", "quit", "exit", "退出", "返回"):
            return
        try:
            print(f"\n[开始下载] {line}")
            rc = _download_link(line, cfg.download_dir, cfg, cfg.iface_ip or None)
            if rc == 0:
                print("[完成]")
            elif rc == 1:
                print("[部分完成] 重新运行可续传")
            else:
                print("[失败] 请检查链接或重试")
        except Exception as e:  # noqa: BLE001
            print(f"[下载出错] {e}")


def _interactive(cfg) -> int:
    """无参数启动：直接粘贴链接下载，不显示菜单。"""
    print("=" * 56)
    print(f"  fastdl {__version__} —— 多线程直链 + ed2k 下载器")
    print("  高速模式：自动分片、重试、断点续传")
    print("=" * 56)
    if not cfg.download_dir:
        cfg.download_dir = default_download_dir()
        save_config(cfg)
    os.makedirs(cfg.download_dir, exist_ok=True)
    iface = f"{cfg.iface_name} ({cfg.iface_ip})" if cfg.iface_ip else "系统默认网卡"
    print(f"下载目录：{cfg.download_dir}")
    print(f"下载网卡：{iface}")
    print(f"并行线程：{cfg.threads}")
    print("直接粘贴下载链接并回车；输入 q 退出。")
    while True:
        line = _ask("> ")
        if line is None or line.strip().lower() in ("q", "quit", "exit", "退出"):
            break
        line = line.strip()
        if not line:
            continue
        try:
            print(f"\n[开始下载] {line}")
            rc = _download_link(line, cfg.download_dir, cfg, cfg.iface_ip or None)
            print("[完成]" if rc == 0 else "[未完成，已保存 .fastdl，可重新运行续传]")
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

