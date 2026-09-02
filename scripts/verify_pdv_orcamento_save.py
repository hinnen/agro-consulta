# -*- coding: utf-8 -*-
"""PDV-ORC-SAVE: orçamento grava no Postgres sem login Chrome."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    views = read("produtos/views.py")
    wiz = read("produtos/static/produtos/js/pdv_wizard.js")
    cons = read("produtos/static/produtos/js/consulta_produtos.js")
    chunk = views[views.find("def api_pdv_orcamentos") : views.find("def api_pdv_orcamento_detalhe")]
    check("@login_required" not in chunk, "API orcamentos sem login_required")
    det = views[views.find("def api_pdv_orcamento_detalhe") : views.find("def api_pdv_orcamento_detalhe") + 200]
    before = views[views.find("@require_GET\ndef api_pdv_orcamento_detalhe") - 80 : views.find("def api_pdv_orcamento_detalhe")]
    check("@login_required" not in before, "detalhe orcamento sem login_required")
    check("@ensure_csrf_cookie\ndef _render_pdv_operacional" in views.replace("\r\n", "\n"), "PDV seta cookie CSRF")
    check("cloneOrcamentoItens" in wiz, "clone seguro itens")
    check("parseJsonRespostaOrcamento" in wiz, "POST nao quebra em HTML login")
    check("fromWhatsapp: true, silent: true" in wiz and "pSave.then" in wiz, "WhatsApp espera gravar")
    check("origem: 'whatsapp'" in cons or 'origem: "whatsapp"' in cons, "consulta Zap marca origem")
    check("var lista = historico;" in wiz, "lista Orçamentos mostra todos")


def check_runtime() -> None:
    import django

    django.setup()
    from django.conf import settings
    from django.test import Client, override_settings
    from django.urls import reverse

    from produtos.models import OrcamentoPdvAgro

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]

    oid = 1999990000001
    OrcamentoPdvAgro.objects.filter(orc_local_id=oid).delete()

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client(enforce_csrf_checks=False)
        url = reverse("api_pdv_orcamentos")
        r0 = c.get(url + "?recentes=1&limite=5")
        check(r0.status_code == 200, f"GET recentes sem login ({r0.status_code})")
        try:
            j0 = r0.json()
        except Exception:
            j0 = {}
        check(j0.get("ok") is True, "GET recentes ok JSON")

        body = {
            "entry": {
                "id": oid,
                "cliente": "Cliente Path Orc",
                "cliente_key": "tmp:cliente path orc:13999999999",
                "cliente_mode": "cliente",
                "total": "R$ 10,00",
                "itens": [{"id": "x", "nome": "Item", "qtd": 1, "preco": 10}],
                "origem": "whatsapp",
            }
        }
        r1 = c.post(url, data=json.dumps(body), content_type="application/json")
        check(r1.status_code == 200, f"POST sem login ({r1.status_code})")
        try:
            j1 = r1.json()
        except Exception:
            j1 = {}
        check(j1.get("ok") is True and j1.get("item"), f"POST grava item ({j1})")
        obj = OrcamentoPdvAgro.objects.filter(orc_local_id=oid).first()
        check(obj is not None, "Postgres tem orcamento")
        if obj:
            check(obj.cliente_nome == "Cliente Path Orc", "nome gravado")
            pay = obj.payload_json if isinstance(obj.payload_json, dict) else {}
            check(pay.get("origem") == "whatsapp", "origem whatsapp no payload")
        r2 = c.get(reverse("api_pdv_orcamento_detalhe", args=[oid]))
        check(r2.status_code == 200, f"GET detalhe sem login ({r2.status_code})")
        OrcamentoPdvAgro.objects.filter(orc_local_id=oid).delete()


def main() -> int:
    print("=== PDV-ORC-SAVE ===")
    check_static()
    try:
        check_runtime()
    except Exception as e:
        fail(f"runtime: {e}")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    print("VERIFY_OK" if not FAILS else "VERIFY_FAIL")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
