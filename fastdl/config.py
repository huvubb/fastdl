"""配置：userhash 持久化、tcp 端口、默认服务器。"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

CONFIG_DIR = os.path.join(os.path.expanduser("~"), ".fastdl")
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.json")


@dataclass
class Config:
    userhash: bytes = b""                     # 身份标识，urandom(16) 生成一次并持久化
    tcp_port: int = 4662                      # 广告端口（不监听也能查源）
    servers: list[tuple[str, int]] = field(default_factory=list)
    threads: int = 16
    max_sources: int = 6

    @property
    def userhash_hex(self) -> str:
        return self.userhash.hex()


def load_config(path: str | None = None) -> Config:
    path = path or CONFIG_PATH
    cfg = Config()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data.get("userhash"), str) and len(data["userhash"]) == 32:
            cfg.userhash = bytes.fromhex(data["userhash"])
        cfg.tcp_port = int(data.get("tcp_port", 4662))
        cfg.threads = int(data.get("threads", 16))
        cfg.max_sources = int(data.get("max_sources", 6))
        cfg.servers = [tuple(s) for s in data.get("servers", [])]
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    if not cfg.userhash:
        cfg.userhash = os.urandom(16)
        save_config(cfg, path)
    return cfg


def save_config(cfg: Config, path: str | None = None) -> None:
    path = path or CONFIG_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {
        "userhash": cfg.userhash.hex(),
        "tcp_port": cfg.tcp_port,
        "threads": cfg.threads,
        "max_sources": cfg.max_sources,
        "servers": [list(s) for s in cfg.servers],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
