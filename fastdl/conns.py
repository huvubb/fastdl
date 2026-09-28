"""TCP 连接扫描与无效连接清理（Windows）。

- 扫描：列出本机 TCP 连接（psutil）
- 清理：用 iphlpapi!SetTcpEntry 把连接置为 DELETE_TCB(12) 强制断开
  （与 TCPView / CurrPorts 的"关闭连接"同理；关闭他人进程的连接需要管理员权限）
"""
from __future__ import annotations

import ctypes
import socket
import sys

# 这些状态的连接属于"半死/僵尸"，容易堆积：握手未完成、等待对端关闭等
INVALID_STATES = ("SYN_SENT", "SYN_RECV", "FIN_WAIT1", "FIN_WAIT2",
                  "CLOSE_WAIT", "LAST_ACK", "CLOSING")


def scan(kind: str = "tcp", only_invalid: bool = False) -> list[dict]:
    """返回连接列表；only_invalid=True 只返回半死状态。"""
    try:
        import psutil
    except ImportError:
        return []
    out = []
    try:
        conns = psutil.net_connections(kind=kind)
    except Exception:
        return []
    for c in conns:
        if not c.laddr:
            continue
        status = c.status
        if only_invalid and status not in INVALID_STATES:
            continue
        out.append({
            "status": status,
            "laddr": c.laddr,
            "raddr": c.raddr,
            "laddr_s": f"{c.laddr.ip}:{c.laddr.port}",
            "raddr_s": f"{c.raddr.ip}:{c.raddr.port}" if c.raddr else "*",
            "pid": c.pid,
            "invalid": status in INVALID_STATES,
        })
    return out


class _MIB_TCPROW(ctypes.Structure):
    _fields_ = [("dwState", ctypes.c_ulong), ("dwLocalAddr", ctypes.c_ulong),
                ("dwLocalPort", ctypes.c_ulong), ("dwRemoteAddr", ctypes.c_ulong),
                ("dwRemotePort", ctypes.c_ulong)]


_DELETE_TCB = 12


def _dword_ip(ip: str) -> int:
    return int.from_bytes(socket.inet_aton(ip), "little")


def close_connection(laddr, raddr) -> tuple[bool, str]:
    """强制断开一条 TCP 连接。返回 (成功, 说明)。"""
    if sys.platform != "win32":
        return False, "仅支持 Windows"
    if not raddr:
        return False, "无远端地址（监听态），跳过"
    if ":" in laddr.ip or ":" in raddr.ip:
        return False, "IPv6 连接暂不支持清理"
    try:
        row = _MIB_TCPROW(_DELETE_TCB, _dword_ip(laddr.ip), socket.htons(laddr.port),
                          _dword_ip(raddr.ip), socket.htons(raddr.port))
        ret = ctypes.windll.iphlpapi.SetTcpEntry(ctypes.byref(row))
    except Exception as e:  # noqa: BLE001
        return False, str(e)
    if ret == 0:
        return True, ""
    if ret == 5:
        return False, "需要管理员权限"
    return False, f"错误码 {ret}"


def clear_invalid(verbose: bool = False) -> tuple[int, int, list[str]]:
    """清理所有半死状态的连接。返回 (尝试数, 成功数, 错误说明)。"""
    items = [c for c in scan(only_invalid=True) if c["raddr"]]
    ok = 0
    errs: list[str] = []
    for c in items:
        good, msg = close_connection(c["laddr"], c["raddr"])
        if good:
            ok += 1
            if verbose:
                print(f"  已断开 {c['laddr_s']} -> {c['raddr_s']} ({c['status']})")
        elif msg:
            errs.append(msg)
    return len(items), ok, sorted(set(errs))
