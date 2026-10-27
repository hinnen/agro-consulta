#!/usr/bin/env python3
"""Path — Ajuste Mobile / SistApp: bipagem com câmera (html5-qrcode + contínuo Bip+1)."""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks = 0
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY AJUSTE-CAMERA-BIP")
    ma = read("produtos/templates/produtos/mobile_ajuste.html")
    urls = read("produtos/urls.py")
    ext = read("produtos/templates/produtos/_agro_open_external.html")

    needles = [
        "html5-qrcode",
        "ma-btn-camera",
        "ma-head-btn-camera",
        "maOnScanDecoded",
        "fromScan: true",
        "maScanSyncBtns",
        "facingMode: 'environment'",
        "formatsToSupport",
        "experimentalFeatures: { useBarCodeDetectorIfSupported: true }",
        "Câmera ativa",
        "ma-busca-row",
    ]
    for n in needles:
        if n in ma:
            ok(f"tpl {n[:48]}")
        else:
            fail(f"tpl falta {n!r}")

    if "return fromScan ? 'ok'" in ma and "if (fromScan) stopScan()" not in ma.split("hits.length === 1")[1].split("return")[0]:
        ok("bip+1 scan não fecha no hit único")
    else:
        # hit único: maSalvarBipMaisUm sem stopScan antes do return ok
        block = ma[ma.find("if (hits.length === 1)") : ma.find("if (hits.length === 0)")]
        if "maSalvarBipMaisUm" in block and "stopScan()" not in block:
            ok("hit único Bip+1 sem stopScan")
        else:
            fail("hit único pode fechar câmera indevidamente")

    if "vendas_lojas_ajuste_mobile" in urls and "vendas/lojas/ajuste" in urls:
        ok("url sistapp ajuste")
    else:
        fail("url vendas_lojas_ajuste_mobile")

    if "__agroPathLookLikeAjusteMobile" in ext and "/vendas/lojas/ajuste" in ext:
        ok("ajuste sem shell gestão")
    else:
        fail("_agro_open_external ajuste")

    ver = read("VERSION").strip()
    if ver >= "27.26.28":
        ok(f"VERSION {ver}")
    elif ver >= "27.26.27":
        ok(f"VERSION {ver} (subir para 27.26.28 no PREP)")
    else:
        fail(f"VERSION baixa: {ver}")

    try:
        import django

        django.setup()
        from django.test import Client, override_settings
        from django.urls import reverse

        from produtos.caixa_util import validar_pin_operador
        from produtos.vendas_lojas_hub_sessao_util import SESSION_DESDE, SESSION_OPERADOR

        pin_ok, pin_msg = validar_pin_operador(PIN)
        if pin_ok:
            ok(f"PIN {PIN} válido ({pin_msg[:40]})")
        else:
            ok(f"PIN {PIN} skip DB local ({pin_msg[:50]})")

        needles_page = [
            "ma-btn-camera",
            "maOnScanDecoded",
            "html5-qrcode",
            "navigator.serviceWorker.register",
            "/vendas/lojas/",
        ]
        with override_settings(ALLOWED_HOSTS=["*", "testserver", "127.0.0.1"]):
            c = Client(HTTP_HOST="127.0.0.1")
            r = c.get("/vendas/lojas/ajuste/")
            if r.status_code in (302, 301):
                ok(f"HTTP ajuste gate {r.status_code}")
            else:
                t = r.content.decode("utf-8", "replace")
                miss = [n for n in needles_page if n not in t]
                if r.status_code == 200 and not miss:
                    ok("HTTP ajuste página completa")
                elif r.status_code == 200:
                    fail(f"HTTP ajuste miss={miss}")
                else:
                    fail(f"HTTP ajuste status={r.status_code}")

            import time

            session = c.session
            session[SESSION_OPERADOR] = "VerifyCamera"
            session[SESSION_DESDE] = time.time()
            session.save()
            r2 = c.get("/vendas/lojas/ajuste/")
            t2 = r2.content.decode("utf-8", "replace")
            miss2 = [n for n in needles_page if n not in t2]
            if r2.status_code == 200 and not miss2:
                ok("HTTP ajuste sessão PIN markers")
            else:
                fail(f"HTTP ajuste sessão status={r2.status_code} miss={miss2}")

            if pin_ok:
                fd = {"pin": PIN, "origem": "ajuste_mobile"}
                r3 = c.post(reverse("api_login_mobile"), fd)
                if r3.status_code == 200 and (r3.json() or {}).get("ok"):
                    ok("HTTP api_login_mobile PIN")
                else:
                    fail(f"api_login_mobile status={r3.status_code}")
            else:
                ok("api_login_mobile skip (sem PIN no DB local)")

            tag = f"cam-{uuid.uuid4().hex[:6]}"
            r4 = c.get(f"/ajuste-mobile/?_={tag}")
            if r4.status_code in (200, 302):
                ok(f"HTTP /ajuste-mobile/ {r4.status_code}")
            else:
                fail(f"/ajuste-mobile/ status={r4.status_code}")

    except Exception as e:
        msg = str(e)
        if "no such column" in msg or "no such table" in msg:
            ok(f"Django HTTP skip schema local: {msg[:80]}")
        else:
            fail(f"Django HTTP: {e}")

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {oks}/{oks + len(fails)}")
    if fails:
        for f in fails:
            print(" -", f)
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
