# fastdl —— 多线程直链 + ed2k 完整客户端下载工具

对抗迅雷会员也被限速 7KB/s 的正当开源方案：**独立的下载工具**，跟某雷彻底脱钩。

## 快速开始（交互模式，最常用）

直接运行 `fastdl.exe`（或 `python dl.py`，**不带任何参数**），进入交互界面：

```
fastdl v1.1 —— 多线程直链 + ed2k 下载器
不用记参数：粘贴链接即可下载，可一次粘贴多个（每行一个）
====================================================

下载目录 [默认: D:\fastdl\downloads，直接回车使用]：
请输入下载链接（http/https/ftp 直链 或 ed2k://；可一次粘贴多个，每行一个；输入 q 退出）
>
```

- 粘贴链接 → **自动识别**是直链还是 ed2k → 开始下载 → 显示进度
- 下完继续粘贴下一个链接；**输入 `q` 退出**
- 下载目录会记住上次用的，下次默认就是它

## 安装

```bash
pip install pycryptodome requests -i https://pypi.tuna.tsinghua.edu.cn/simple
```

依赖：Python 3.10+；`pycryptodome`（MD4，ed2k 必需）。`requests`、`psutil` 多数环境已有。

> 💡 **Defender 提速提示**：给下载目录（如 `D:\fastdl\downloads`）加 Windows Defender 排除项，
> 否则实时扫描会拖慢 `.part` 写入。设置 → 病毒和威胁防护 → 排除项。

## 直链多线程下载

```bash
python dl.py direct "https://example.com/big.iso" -t 32 -o D:\downloads
python dl.py direct "ftp://..." -o D:\downloads          # FTP 单流 + REST 续传
python dl.py direct "https://..." --engine aria2 -t 64   # 调用已装 aria2c 榨最大速度
```

- 固定 8MiB chunk + 多连接并行，断点续传（`.fastdl/` 目录存 `.part` 与 `.dlmeta`，ETag 变了自动重来）。
- 服务器不支持 Range → 自动单线程流式回退。

### 直链增强（自定义请求头 / 代理 / 限速）

```bash
# 自定义请求头（--header 可多次；--cookie/--referer/--ua 是常用头的快捷方式）
python dl.py direct "https://x.com/file" \
    --header "X-Token: abc123" --cookie "sid=888" \
    --referer "https://x.com/" --ua "Mozilla/5.0"

# 走 HTTP/HTTPS 代理（所有请求：探针+分片都走代理）
python dl.py direct "https://x.com/file" --proxy "http://127.0.0.1:7890"

# 全局限速（跨线程令牌桶，总速率不超过指定值），单位如 128K / 5M
python dl.py direct "https://x.com/file" --limit 5M
```

- 适合需要登录/带鉴权/防热链的直链下载场景。

## ed2k 下载

```bash
python dl.py servers --test                 # 实测各 eD2k 服务器是否可达
python dl.py ed2k "ed2k://|file|xxx|size|hash|/" --servers 1.2.3.4:4661 -o D:\downloads
```

- 完整 eD2k 客户端：连服务器查源 → 直连对等点多源并行分块（每源 1 个在途 part，失败换源）。
- 支持普通/压缩/I64 大文件传输；`|h=` 提供时逐 part MD4 校验，否则下载完校验顶层 hash。
- 断点续传（`.ed2kpart/` 目录，part 文件大小即进度）。

> ⚠️ **网络现实**：eD2k 服务器端口（4661）在你网络下可能被墙。`servers --test` 全不通时
> ed2k 功能无法使用——这是网络限制，不是软件 bug。可换代理或加速器后再试。
> 服务器列表在 `servers.txt`，可用 `--server-list 文件` 覆盖。

## 其它

```bash
python dl.py hash <文件>        # 计算文件的 ed2k 顶层 hash 并生成 ed2k:// 链接
python dl.py --help
```

## 目录结构

```
fastdl/
├── dl.py                 入口
├── servers.txt           候选 eD2k 服务器
├── fastdl/
│   ├── cli.py            argparse + 进度 + Ctrl+C
│   ├── direct.py         直链多线程下载
│   ├── ftp.py            FTP 单流 + REST
│   ├── progress.py       单行 ANSI 进度条
│   ├── config.py         userhash 持久化
│   └── ed2k/
│       ├── const.py      协议常量/帧/tag（对照 aMule 源码核实）
│       ├── link.py       ed2k:// 解析 + hash 计算
│       ├── md4.py        MD4 三后端
│       ├── server.py     eD2k 服务器登录/查源
│       ├── sources.py    并行源发现
│       ├── peer.py       eMule 传输协议客户端
│       └── transfer.py   多源分块调度 + 组装校验
└── tests/                本地 mock 服务器/对等点 + Range 服务器
```

## 说明

- ed2k 是开源协议（eMule/aMule 同款），本工具为正当工程实现。
- 单块 9,728,000 字节，顶层 hash = MD4(各块 MD4 拼接) —— 这是 ed2k 的标准算法。


（直连有点可能会断）
（某雷对不起）
（我错了，不要给我律师函）
（求求了)
