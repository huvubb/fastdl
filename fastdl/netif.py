"""本机网络接口（网卡）枚举：供"选择下载网卡"用。

下载时把 socket 的源地址绑定到选中的网卡 IP，即可让【只有下载器的 TCP】走那张网卡。
"""
from __future__ import annotations

import socket

_VIRTUAL_HINTS = (
    "vmware", "vethernet", "hyper-v", "virtual", "tap", "tun", "wintun", "loopback",
    "bluetooth", "teredo", "isatap", "vpn", "clash", "mihomo", "虚拟", "蓝牙", "加速",
)


def _is_virtual(name: str) -> bool:
    low = name.lower()
    return any(h in low for h in _VIRTUAL_HINTS)


def list_interfaces(include_down: bool = False) -> list[dict]:
    """返回 [{'name','ip','up','kind'}]；kind ∈ physical / virtual / loopback。"""
    out: list[dict] = []
    try:
        import psutil
    except ImportError:
        return _fallback()
    stats = {}
    try:
        stats = psutil.net_if_stats()
    except Exception:
        pass
    for name, addrs in psutil.net_if_addrs().items():
        ips = [a.address for a in addrs if a.family == socket.AF_INET]
        if not ips:
            continue
        st = stats.get(name)
        up = bool(st.isup) if st else True
        for ip in ips:
            if ip.startswith("127."):
                kind = "loopback"
            elif _is_virtual(name):
                kind = "virtual"
            else:
                kind = "physical"
            if not include_down and not up:
                continue
            out.append({"name": name, "ip": ip, "up": up, "kind": kind})
    # 物理网卡排前面
    out.sort(key=lambda x: (x["kind"] != "physical", not x["up"], x["name"]))
    return out


def _fallback() -> list[dict]:
    """无 psutil 时的兜底：只能拿到 IP，拿不到网卡名。"""
    out = []
    try:
        host = socket.gethostname()
        for info in socket.getaddrinfo(host, None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127."):
                out.append({"name": f"(未知网卡) {ip}", "ip": ip, "up": True,
                            "kind": "physical"})
    except Exception:
        pass
    return out


def default_interface() -> dict | None:
    """默认出口网卡：用连外网的方式探测本机源 IP。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("223.5.5.5", 80))  # 不真正发包，只为问出默认路由的源地址
        ip = s.getsockname()[0]
        s.close()
    except Exception:
        return None
    for it in list_interfaces():
        if it["ip"] == ip:
            return it
    return {"name": "默认出口", "ip": ip, "up": True, "kind": "physical"}
