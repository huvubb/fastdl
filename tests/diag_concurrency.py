"""对照实验：GitHub objects CDN 在不同并发连接数下的聚合吞吐。"""
import sys, time, threading
sys.path.insert(0, r"D:\fastdl")
from fastdl.direct import probe, _make_session, _get_with_retry

URL = "https://github.com/git-for-windows/git/releases/download/v2.47.1.windows.1/Git-2.47.1-64-bit.exe"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

pr = probe(URL, threads=4, timeout=15, headers=UA)
print(f"size={pr.size>>20}MiB final={pr.final_url}")
fetch = pr.final_url or URL

def bench(nconn, seconds=10):
    got = [0] * nconn
    stop = time.time() + seconds
    errs = []
    def w(i):
        try:
            s = _make_session(2)
            h = {"Accept-Encoding": "identity", "Range": f"bytes={i*(8<<20)}-"}
            t0 = time.time()
            r = _get_with_retry(s, fetch, h, 15)
            th = time.time() - t0
            if r.status_code != 206:
                errs.append(f"conn{i}: HTTP {r.status_code} ({th:.1f}s)")
                r.close(); return
            for b in r.iter_content(1 << 20):
                if time.time() > stop: break
                got[i] += len(b)
            r.close()
        except Exception as e:
            errs.append(f"conn{i}: {type(e).__name__}")
    ths = [threading.Thread(target=w, args=(i,)) for i in range(nconn)]
    t0 = time.time()
    for t in ths: t.start()
    for t in ths: t.join()
    dt = time.time() - t0
    total = sum(got)
    print(f"  {nconn:>3} 连接: {total/dt/1e6:6.1f} MB/s 聚合 (dt={dt:.1f}s)"
          + (f"  错误: {errs}" if errs else ""))

for n in (1, 4, 8, 16, 32):
    bench(n)
