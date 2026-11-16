"""临时诊断脚本：给直链下载启动链路各阶段计时。"""
import sys, time
sys.path.insert(0, r"D:\fastdl")
import os
os.environ["FASTDL_DEBUG"] = "1"

URL = sys.argv[1] if len(sys.argv) > 1 else "https://mirrors.tuna.tsinghua.edu.cn/debian-cd/current/amd64/iso-cd/debian-13.2.0-amd64-netinst.iso"

t0 = time.time()
from fastdl.direct import probe, _make_session, _get_with_retry
t1 = time.time()
print(f"[1] import 模块: {t1-t0:.2f}s")

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
pr = probe(URL, threads=16, timeout=15, headers=UA)
t2 = time.time()
print(f"[2] 探针(解析): {t2-t1:.2f}s  size={pr.size>>20}MiB  ranges={pr.ranges}  final_url={pr.final_url}")

fetch_url = pr.final_url or URL
s = _make_session(16)
t3 = time.time()
h = {"Accept-Encoding": "identity", "Range": "bytes=0-67108863"}
r = _get_with_retry(s, fetch_url, h, 15)
t4 = time.time()
print(f"[3] 单连接 建连+响应头: {t4-t3:.2f}s  status={r.status_code}")

got = 0
first_ts = None
for b in r.iter_content(chunk_size=4 << 20):
    if not b:
        continue
    got += len(b)
    if first_ts is None:
        first_ts = time.time()
        print(f"[4] 单连接 首块数据延迟: {first_ts-t4:.2f}s")
    if got >= (48 << 20):
        break
t5 = time.time()
r.close()
print(f"[5] 单连接吞吐: {got/(t5-first_ts)/1e6:.1f} MB/s")

# 模拟多线程启动：64 个并发 Range 请求（全部落在文件范围内），量测全部拿到响应头的时间
import threading
t6 = time.time()
results = [None] * 64
def w(i, res):
    ss = _make_session(8)
    hh = {"Accept-Encoding": "identity", "Range": f"bytes={i*(1<<20)}-{i*(1<<20)+(1<<19)-1}"}
    try:
        rr = _get_with_retry(ss, fetch_url, hh, 15)
        res[i] = (time.time(), rr.status_code)
        rr.close()
    except Exception as e:
        res[i] = (time.time(), str(e))
ths = [threading.Thread(target=w, args=(i, results)) for i in range(64)]
for th in ths: th.start()
for th in ths: th.join()
t7 = time.time()
ok = sum(1 for x in results if x and x[1] == 206)
print(f"[6] 64 并发建连: 总耗时 {t7-t6:.2f}s，成功 206 的 {ok} 个")
from collections import Counter
dist = Counter(str(x[1]) for x in results if x)
print(f"    状态分布: {dict(dist)}")
times = sorted(x[0]-t6 for x in results if x)
if times:
    print(f"    首个响应头 {times[0]:.2f}s，中位 {times[len(times)//2]:.2f}s，最慢 {times[-1]:.2f}s")
