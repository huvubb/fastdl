#!/usr/bin/env python3
"""诊断：签名 release-assets URL → 镜像代理 API → 规范化 github.com URL → 镜像探针。

用法: python tests/diag_canonical.py <镜像前缀> <repo_id> <资产名>
"""
import json
import sys
import urllib.request

mirror = sys.argv[1].rstrip("/")
repo_id = sys.argv[2]
asset_name = sys.argv[3]


def get_json(url: str):
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json",
                                               "User-Agent": "fastdl-diag"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


repo = get_json(f"{mirror}/https://api.github.com/repositories/{repo_id}")
full = repo["full_name"]
print("full_name:", full)

rels = get_json(f"{mirror}/https://api.github.com/repos/{full}/releases?per_page=40")
canonical = None
tag = None
for rel in rels:
    for a in rel.get("assets", []):
        if a["name"] == asset_name:
            canonical = a["browser_download_url"]
            tag = rel["tag_name"]
            break
    if canonical:
        break
print("tag:", tag)
print("canonical:", canonical)

if canonical:
    # 镜像探针: Range 0-0
    probe_url = f"{mirror}/{canonical}"
    req = urllib.request.Request(probe_url, headers={"Range": "bytes=0-0",
                                                     "User-Agent": "fastdl-diag"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            print("镜像探针:", r.status, dict(r.headers).get("Content-Range"))
    except Exception as e:
        print("镜像探针失败:", e)
