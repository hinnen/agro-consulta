#!/usr/bin/env python
"""Smoke HTTP: healthz + static dual (sem login)."""
from __future__ import annotations

import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"


def get(path: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(BASE + path, timeout=12) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore") if e.fp else ""
        return e.code, body


def main() -> None:
    s, _ = get("/healthz")
    assert s == 200, s
    print("OK healthz")

    s, js = get("/static/produtos/js/agro_dual_window.js")
    assert s == 200, s
    assert "openNamed(GESTAO_NAME, '')" in js
    assert "Nunca navegar" in js or "só shell" in js
    assert "bootGestaoPendingFocus" in js
    print("OK static dual")
    print("ALL OK smoke")


if __name__ == "__main__":
    main()
