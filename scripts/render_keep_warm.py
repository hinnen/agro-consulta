"""Ping /healthz do staging para reduzir cold start (Render dorme ~15 min sem tráfego)."""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request

# Rede privada Render (mesmo workspace) + URLs públicas comuns do staging.
_DEFAULT_URLS = (
    "http://agro-consulta-teste:10000/healthz",
    "http://agro-consulta-staging:10000/healthz",
    "https://agro-consulta-staging.onrender.com/healthz",
    "https://agro-consulta-teste.onrender.com/healthz",
)


def _urls() -> list[str]:
    raw = (os.environ.get("AGRO_KEEP_WARM_URLS") or "").strip()
    if raw:
        return [u.strip() for u in raw.split(",") if u.strip()]
    return list(_DEFAULT_URLS)


def _ping(url: str, timeout: float = 45.0) -> bool:
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "agro-keep-warm/1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read(32).decode("utf-8", errors="replace").strip().lower()
        return resp.status == 200 and body == "ok"


def main() -> int:
    ok_any = False
    for url in _urls():
        try:
            if _ping(url):
                print(f"OK {url}")
                ok_any = True
            else:
                print(f"WARN resposta inesperada: {url}", file=sys.stderr)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"FAIL {url}: {exc}", file=sys.stderr)
    return 0 if ok_any else 1


if __name__ == "__main__":
    raise SystemExit(main())
