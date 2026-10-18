#!/usr/bin/env python3
"""Prova WA-RENDER-SPLIT — WhatsApp fora do worker PDV."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.conf import settings
from django.test import Client, override_settings

OK = 0
FAIL = 0


def check(label: str, cond: bool) -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f" FAIL {label}")


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def main() -> int:
    print("=== WA-RENDER-SPLIT ===\n")

    check("settings AGRO_WA_PUBLIC_ORIGIN", hasattr(settings, "AGRO_WA_PUBLIC_ORIGIN"))
    check("middleware wa_split", "base.wa_split_middleware.AgroWhatsAppSplitMiddleware" in settings.MIDDLEWARE)
    check("context agro_wa_public_urls", "produtos.context_processors.agro_wa_public_urls" in str(settings.TEMPLATES))
    check("render.yaml agro-consulta-teste WA", "agro-consulta-teste" in read("render.yaml"))
    check("doc reaproveita staging", "agro-consulta-staging" in read("docs/DEPLOY-WHATSAPP-RENDER.md"))
    check("doc deploy", (ROOT / "docs/DEPLOY-WHATSAPP-RENDER.md").is_file())

    util = read("produtos/agro_wa_origin_util.py")
    check("util path prefix atendimento", "atendimento-whatsapp" in util)
    check("util bridge api prefix", "/api/atendimento-whatsapp/" in util)

    wa_origin = "https://wa-split-test.example.com"
    with override_settings(AGRO_WA_PUBLIC_ORIGIN=wa_origin):
        c = Client(HTTP_HOST="pdv.example.com")
        r = c.get("/atendimento-whatsapp/", follow=False)
        check("redirect chat para WA", r.status_code == 302 and r["Location"].startswith(wa_origin))

        r2 = c.post(
            "/api/atendimento-whatsapp/bridge/entrada/",
            data="{}",
            content_type="application/json",
            HTTP_X_AGRO_WA_TOKEN="x",
        )
        check("bridge no PDV = 503", r2.status_code == 503)
        body = r2.json()
        check("bridge 503 menciona wa_origin", body.get("wa_origin") == wa_origin)

        from produtos.agro_wa_origin_util import agro_wa_absolute_path

        check(
            "launcher path absoluto",
            agro_wa_absolute_path("/atendimento-whatsapp/").startswith(wa_origin),
        )

    with override_settings(AGRO_WA_PUBLIC_ORIGIN=""):
        c3 = Client()
        r3 = c3.get("/atendimento-whatsapp/", follow=False)
        check("sem split: nao redirect 302 por middleware", r3.status_code != 302 or "wa-split-test" not in (r3.get("Location") or ""))

    print(f"\nVERIFY_OK {OK}/{OK + FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
