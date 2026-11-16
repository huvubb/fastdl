"""干净对照：1/4/16 连接聚合吞吐，各测两轮，判断并发是否有收益。"""
import sys, time, threading
sys.path.insert(0, r"D:\fastdl")
from fastdl.direct import probe, _make_session, _get_with_retry

URL = "https://github.com/git-for-windows/git/releases/download/v2.47.1.windows.1/Git-2.47.1-64-bit.exe"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
pr = probe(URL, threads=2, timeout=15, headers=UA)
fetch = pr.final_url or URL
SIZE = pr.size
print(f"size={SIZE>>20}MiB")

def bench(nconn, seconds=10, tag=""):
    got = [0] * nconn
    deadline = time.time() + seconds
    start_barrier = threading.Barrier(nconn)
    def w(i):
        try:
            s = _make_session(2)
            # 全部落在文件内的 1MiB 窗口，循环偏移避免越界
            h = {"Accept-Encoding": "identity"}
            start_barrier.wait()
            off = (i * (8 << 20)) % (SIZE - (2 << 20))
            h["Range"] = f"bytes={off}-"
            r = _get_with_retry(s, fetch, h, 15)
            if r.status_code != 206:
                r.close(); return
            pos = off
            for b in r.iter_content(1 << 20):
                if time.time() > deadline: break
                n = min(len(b), SIZE - pos)
                got[i] += n; pos += n
                if pos >= SIZE: break
            r.close()
        except Exception:
            pass
    ths = [threading.Thread(target=w, args=(i,)) for i in range(nconn)]
    t0 = time.time()
    for t in ths: t.start()
    for t in ths: t.join()
    dt = time.time() - t0
    total = sum(got)
    print(f"  [{tag}] {nconn:>2} 连接: {total/dt/1e6:6.2f} MB/s")

for rnd in (1, 2):
    for n in (1, 4, 16):
        bench(n, tag=f"round{rnd}")
