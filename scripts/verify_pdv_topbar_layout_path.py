#!/usr/bin/env python
"""Prova path PDV-TOPBAR-LAYOUT — cores + Fiado quente + prefs Postgres."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import RequestFactory

from produtos.models import PdvTopbarLayoutAgro
from produtos.pdv_topbar_layout_util import (
    FRIO_DEFAULT,
    QUENTE_DEFAULT,
    layout_default,
    normalizar_layout,
    obter_layout,
    salvar_layout,
)
from produtos.views_pdv_topbar import api_pdv_topbar_layout

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = ""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fail += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main():
    print("=== PDV-TOPBAR-LAYOUT ===")

    check("mig_0110", (ROOT / "produtos/migrations/0110_pdv_topbar_layout.py").exists())
    check("util", (ROOT / "produtos/pdv_topbar_layout_util.py").exists())
    check("js_layout", (ROOT / "produtos/static/produtos/js/pdv_topbar_layout.js").exists())

    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    check("html_fiado_quente", 'id="pdv-topbar-fiado-link"' in html and 'id="pdv-topbar-quente"' in html)
    check(
        "html_fiado_antes_mais",
        html.find('id="pdv-topbar-fiado-link"') < html.find('id="pdv-topbar-mais-panel"'),
    )
    check("html_pedir_slate", 'pdv-topbar-pedir-loja-btn" class="pdv-action-btn pdv-wiz-topbar-btn pdv-wiz-topbar-btn--slate' in html)
    check("html_uso_slate", 'pdv-topbar-uso-loja-btn" class="pdv-action-btn pdv-wiz-topbar-btn pdv-wiz-topbar-btn--slate"' in html)
    check("html_mais_destaque", "pdv-wiz-topbar-btn--mais-destaque" in html)
    check("html_organizar", 'id="pdv-topbar-organizar-btn"' in html and "Organizar atalhos" in html)
    check("html_overlay", 'id="pdv-topbar-organizar-overlay"' in html)
    check("html_js", "pdv_topbar_layout.js" in html)

    js_pedir = (ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js").read_text(encoding="utf-8")
    check("js_pedir_slate", "pdv-wiz-topbar-btn--slate relative" in js_pedir)
    check("js_pedir_nao_rose_base", "pdv-wiz-topbar-btn--rose relative" not in js_pedir)

    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("url_layout", "api_pdv_topbar_layout" in urls)

    d = layout_default()
    check("default_quente_fiado", "fiado" in d["quente"] and d["quente"] == QUENTE_DEFAULT)
    check("default_frio_sem_fiado", "fiado" not in d["frio"] and d["frio"] == FRIO_DEFAULT)

    n = normalizar_layout(quente=["fiado", "vendas"], frio=["pin"])
    check("norm_completa", "pedir_loja" in (n["quente"] + n["frio"]))
    check("norm_sem_dup", n["quente"].count("fiado") + n["frio"].count("fiado") == 1)

    PdvTopbarLayoutAgro.objects.filter(chave="default").delete()
    base = obter_layout()
    check("obter_sem_row", base["quente"] == QUENTE_DEFAULT)

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="verify_pdv_topbar_layout",
        defaults={"is_staff": True},
    )
    if not user.has_usable_password():
        user.set_password("verify-topbar-layout")
        user.save(update_fields=["password"])

    salvo = salvar_layout(
        quente=["pedir_loja", "fiado", "nova_venda"],
        frio=["vendas", "uso_loja", "entregas", "caixa", "saldo_vila", "repasse", "pesar", "pin"],
        usuario=user,
    )
    check("salvar_quente", salvo["quente"][:3] == ["pedir_loja", "fiado", "nova_venda"])
    check("row_pg", PdvTopbarLayoutAgro.objects.filter(chave="default").exists())

    rf = RequestFactory()
    req = rf.get("/api/pdv/topbar-layout/")
    req.user = user
    r = api_pdv_topbar_layout(req)
    body = json.loads(r.content.decode("utf-8") or "{}")
    check("api_get", r.status_code == 200 and body.get("ok") is True)
    check("api_get_fiado_quente", "fiado" in (body.get("quente") or []))

    req2 = rf.post(
        "/api/pdv/topbar-layout/",
        data=json.dumps(
            {
                "quente": list(QUENTE_DEFAULT),
                "frio": list(FRIO_DEFAULT),
            }
        ),
        content_type="application/json",
    )
    req2.user = user
    r2 = api_pdv_topbar_layout(req2)
    body2 = json.loads(r2.content.decode("utf-8") or "{}")
    check("api_post", r2.status_code == 200 and body2.get("ok") is True)
    check("api_post_default", body2.get("quente") == QUENTE_DEFAULT)

    print(f"\nRESULTADO: {ok} ok / {fail} fail")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
