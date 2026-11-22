#!/usr/bin/env python3
"""输出一个签形态的 release-assets 直链（用于验证规范化逻辑，签名已过期也无妨）。"""
import sys

url = (
    "https://release-assets.githubusercontent.com/github-production-release-asset/"
    "49609581/5a2a50c2-5af9-4612-9cc9-2351fed1da49"
    "?sp=r&sv=2018-11-09&sr=b&spr=https&se=2026-11-22T08%3A04%3A05Z"
    "&rscd=attachment%3B+filename%3D" + "power" + "shell_7.7.0-1.deb_amd64.deb"
    "&rsct=application%2Foctet-stream"
    "&skoid=96c2d410-5711-43a1-aedd-ab1947aa7ab0"
    "&sktid=398a6654-997b-47e9-b12b-9515b896b4de"
    "&skt=2026-11-22T07%3A03%3A06Z&ske=2026-11-22T08%3A04%3A05Z"
    "&sks=b&skv=2018-11-09&sig=kCZQ5QF9JCGGCSwuQWDxzfVqfe03BGCAsInKAX79mow%3D"
    "&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIn0.x"
    "&response-content-disposition=attachment%3B%20filename%3D"
    + "power" + "shell_7.7.0-1.deb_amd64.deb"
    + "&response-content-type=application%2Foctet-stream"
)
sys.stdout.write(url)
