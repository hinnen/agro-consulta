#!/usr/bin/env python3
"""Path — painel admin contagem cíclica (gerência / PIN administrador)."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks = 0


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
    print("VERIFY CONTAGEM-CICLICA-ADMIN")
    urls = read("produtos/urls.py")
    util = read("produtos/contagem_ciclica_admin_util.py")
    tpl = read("produtos/templates/produtos/contagem_ciclica_painel_admin.html")
    hub = read("produtos/templates/produtos/vendas_lojas_hub.html")
    ma = read("produtos/templates/produtos/mobile_ajuste.html")

    for n in (
        "request_pode_ver_painel_ciclica_admin",
        "painel_dashboard_payload",
        "sessao_payload_admin",
        "validar_pin_gerencial",
        "operador_liberado_vendas_tarefas",
    ):
        if n in util:
            ok(f"util {n}")
        else:
            fail(f"util falta {n}")

    if "api_ciclica_admin_painel" in urls and "contagem_ciclica_painel_admin" in urls:
        ok("urls painel + api")
    else:
        fail("urls")

    if "mostrar_painel_ciclica_admin" in hub and "contagem_ciclica_painel_admin" in hub:
        ok("hub condicional")
    else:
        fail("hub")

    if "mostrar_painel_ciclica_admin" in ma and "contagem_ciclica_painel_admin" in ma:
        ok("ajuste link admin")
    else:
        fail("ajuste")

    for n in ("cc-kpis", "cc-drawer", "setInterval", "linhas_admin"):
        if n in tpl:
            ok(f"tpl {n}")
        else:
            fail(f"tpl falta {n}")

    try:
        import django

        django.setup()
        from django.test import Client, override_settings
        from django.urls import reverse

        from produtos.vendas_lojas_hub_sessao_util import SESSION_DESDE, SESSION_OPERADOR, SESSION_PIN

        with override_settings(ALLOWED_HOSTS=["127.0.0.1", "testserver"]):
            c = Client(HTTP_HOST="127.0.0.1")
            r0 = c.get(reverse("api_ciclica_admin_painel"))
            if r0.status_code == 403:
                ok("API bloqueia sem admin")
            else:
                fail(f"API sem gate status={r0.status_code}")

            c2 = Client(HTTP_HOST="127.0.0.1")
            sess = c2.session
            sess[SESSION_OPERADOR] = "VerifyAdmin"
            sess[SESSION_DESDE] = time.time()
            sess[SESSION_PIN] = "0000"
            sess.save()
            r_page = c2.get(reverse("contagem_ciclica_painel_admin"))
            if r_page.status_code == 403:
                ok("HTML bloqueia não-admin")
            elif r_page.status_code == 200 and b"cc-kpis" in r_page.content:
                fail("HTML liberou não-admin")
            else:
                ok(f"HTML protegido status={r_page.status_code}")

    except Exception as e:
        fail(f"Django: {e}")

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {oks}/{oks + len(fails)}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
