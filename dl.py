#!/usr/bin/env python3
"""fastdl —— 多线程直链 + ed2k 完整客户端下载工具。

用法示例：
  python dl.py hash <文件>
  python dl.py direct <url> -t 32
  python dl.py ed2k <ed2k://|file|...>
  python dl.py servers --test
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastdl.cli import main  # noqa: E402

if __name__ == "__main__":
    # 控制台编码容错：可替换不可编码字符，绝不因编码崩溃（如 GBK 下的特殊符号）
    for _s in (sys.stdout, sys.stderr):
        try:
            _s.reconfigure(errors="replace")
        except Exception:
            pass
    sys.exit(main())
