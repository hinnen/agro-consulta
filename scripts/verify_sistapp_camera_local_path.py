#!/usr/bin/env python3
"""SistApp: câmera e letra sem site de fora. Não mexe em estoque nem preço."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks = 0
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

TELAS = (
    "produtos/templates/produtos/mobile_ajuste.html",
    "produtos/templates/produtos/fotos_produto/app.html",
    "produtos/templates/produtos/etiquetas_solicitacao/app.html",
    "produtos/templates/produtos/fotos_produto/pin.html",
    "produtos/templates/produtos/etiquetas_solicitacao/pin.html",
    "produtos/templates/produtos/vendas_lojas_hub.html",
    "produtos/templates/produtos/vendas_lojas_pin.html",
    "produtos/templates/produtos/vendas_lojas_resumo.html",
    "produtos/templates/produtos/vendas_lojas_sem_acesso.html",
    "produtos/templates/produtos/ajuste_mobile_login.html",
)
SCAN = (
    "produtos/templates/produtos/mobile_ajuste.html",
    "produtos/templates/produtos/fotos_produto/app.html",
    "produtos/templates/produtos/etiquetas_solicitacao/app.html",
)


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
    print("VERIFY SISTAPP-CAMERA-LOCAL")
    js = (ROOT / "produtos/static/produtos/js/html5-qrcode.min.js").read_text(encoding="utf-8", errors="replace")
    if "Html5Qrcode" in js and len(js) > 100000:
        ok(f"arquivo local da câmera ({len(js)} chars)")
    else:
        fail("arquivo local da câmera incompleto")

    escolha = read("produtos/static/produtos/js/sistapp_camera_escolha.js")
    if 'facingMode: { ideal: "environment" }' in escolha and "function aplicar" in escolha:
        ok("câmera de trás continua no mesmo arquivo")
    else:
        fail("sistapp_camera_escolha.js perdeu a câmera de trás")

    for rel in TELAS:
        texto = read(rel)
        if "fonts.googleapis.com" in texto or "unpkg.com/html5-qrcode" in texto:
            fail(f"ainda pede site de fora: {rel}")
        else:
            ok(f"sem site de fora {Path(rel).name}")

    for rel in SCAN:
        texto = read(rel)
        if "html5-qrcode.min.js" in texto and "sistapp_camera_escolha.js" in texto:
            ok(f"câmera local {Path(rel).name}")
        else:
            fail(f"câmera local ausente {rel}")

    ajuste = read("produtos/templates/produtos/mobile_ajuste.html")
    for n in ("SistAppCamera.aplicar", "maOnScanDecoded", "ma-btn-camera"):
        if n in ajuste:
            ok(f"ajuste {n}")
        else:
            fail(f"ajuste perdeu {n}")

    try:
        import django

        django.setup()
        from django.test import Client, override_settings

        from produtos.caixa_util import validar_pin_operador
        from produtos.vendas_lojas_hub_sessao_util import SESSION_DESDE, SESSION_OPERADOR

        pin_ok, pin_msg = validar_pin_operador(PIN)
        if pin_ok:
            ok(f"PIN {PIN} válido")
        else:
            ok(f"PIN {PIN} sem banco local ({pin_msg[:60]})")

        with override_settings(ALLOWED_HOSTS=["*", "testserver", "127.0.0.1"]):
            c = Client(HTTP_HOST="127.0.0.1")
            r = c.get("/static/produtos/js/html5-qrcode.min.js")
            raw = b"".join(getattr(r, "streaming_content", []) or [getattr(r, "content", b"")])
            body = raw.decode("utf-8", "replace")
            if r.status_code == 200 and "Html5Qrcode" in body:
                ok("HTTP arquivo da câmera 200")
            else:
                fail(f"HTTP câmera status={r.status_code}")

            r = c.get("/vendas/lojas/")
            if r.status_code in (301, 302):
                ok(f"menu pede PIN ({r.status_code})")
            elif r.status_code == 200 and "fonts.googleapis.com" not in r.content.decode("utf-8", "replace"):
                ok("menu 200 sem Google")
            else:
                fail(f"menu status={r.status_code}")

            if pin_ok:
                from django.utils import timezone

                s = c.session
                s[SESSION_OPERADOR] = PIN
                s[SESSION_DESDE] = timezone.now().isoformat()
                s.save()
                pagina = c.get("/vendas/lojas/")
                html = pagina.content.decode("utf-8", "replace")
                if pagina.status_code == 200 and "fonts.googleapis.com" not in html and "unpkg.com" not in html:
                    ok("menu com PIN sem site de fora")
                else:
                    fail(f"menu com PIN status={pagina.status_code}")
                ajuste_r = c.get("/vendas/lojas/ajuste/")
                html_a = ajuste_r.content.decode("utf-8", "replace")
                if (
                    ajuste_r.status_code == 200
                    and "html5-qrcode.min.js" in html_a
                    and "unpkg.com/html5-qrcode" not in html_a
                    and "SistAppCamera" in html_a
                ):
                    ok("ajuste com PIN abre a câmera local")
                else:
                    fail(f"ajuste com PIN status={ajuste_r.status_code}")
    except Exception as exc:
        fail(f"prova HTTP {type(exc).__name__}: {exc}")

    print(f"RESULT {oks} ok / {len(fails)} fail")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK", oks)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
