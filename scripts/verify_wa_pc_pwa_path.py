# -*- coding: utf-8 -*-
"""WA-PC-PWA — prova: Zap web instalável no Chrome (PC)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    urls = read("produtos/urls.py")
    views = read("produtos/views_atendimento_whatsapp.py")
    html = read("produtos/templates/produtos/atendimento_whatsapp.html")

    check("atendimento_whatsapp_pc_manifest" in urls, "url manifest PC")
    check("atendimento-whatsapp/manifest.webmanifest" in urls, "path manifest PC")
    check("atendimento_whatsapp_pc_sw" in urls, "url sw PC")
    check("atendimento-whatsapp/sw.js" in urls, "path sw PC")
    check("def atendimento_whatsapp_pc_manifest" in views, "view manifest PC")
    check('"id": "/atendimento-whatsapp/"' in views, "manifest id PC")
    check('"short_name": "Zap PC"' in views, "short_name Zap PC")
    check("def atendimento_whatsapp_pc_sw" in views, "view sw PC")
    check('Service-Worker-Allowed"] = "/atendimento-whatsapp/"' in views, "SW allowed PC")
    check("atendimento_whatsapp_pc_manifest" in html, "html link manifest")
    check("wa-pwa-instalar" in html, "botao Instalar no PC")
    check("beforeinstallprompt" in html, "beforeinstallprompt")
    check("atendimento_whatsapp_pc_sw" in html, "html register SW")
    check((ROOT / "produtos/static/produtos/pwa/zap-loja-192.png").is_file(), "icon 192")
    check((ROOT / "produtos/static/produtos/pwa/zap-loja-512.png").is_file(), "icon 512")
    # celular continua intacto
    check("atendimento_whatsapp_celular_manifest" in urls, "celular manifest ainda existe")
    check("atendimento_whatsapp_celular_sw" in views, "celular sw ainda existe")

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
