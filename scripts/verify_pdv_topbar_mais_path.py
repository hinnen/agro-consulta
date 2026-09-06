#!/usr/bin/env python
"""Prova path PDV-TOPBAR-MAIS — menu Mais + contagem de cliques."""
from __future__ import annotations

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

from produtos.models import PdvTopbarCliqueDiaAgro
from produtos.pdv_topbar_clique_util import BOTAO_KEYS, registrar_clique, resumo_cliques

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
    print("=== PDV-TOPBAR-MAIS ===")

    check("mig_0107", (ROOT / "produtos/migrations/0107_pdv_topbar_clique_dia.py").exists())
    check("util", (ROOT / "produtos/pdv_topbar_clique_util.py").exists())
    check("views", (ROOT / "produtos/views_pdv_topbar.py").exists())
    check("js", (ROOT / "produtos/static/produtos/js/pdv_topbar_mais.js").exists())

    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    check("html_mais", 'id="pdv-topbar-mais"' in html and 'id="pdv-topbar-mais-btn"' in html)
    check("html_mais_panel", 'id="pdv-topbar-mais-panel"' in html and "pdv-topbar-mais-panel hidden" in html)
    check("html_quente_pedir", 'data-pdv-topbar-key="pedir_loja"' in html and "pdv-wiz-topbar-estoque-vila" in html)
    check("html_quente_vendas", 'data-pdv-topbar-key="vendas"' in html)
    check("html_quente_uso", 'data-pdv-topbar-key="uso_loja"' in html)
    check("html_quente_entregas", 'data-pdv-topbar-key="entregas"' in html)
    check("html_quente_fiado", 'id="pdv-topbar-fiado-link"' in html and html.find('id="pdv-topbar-fiado-link"') < html.find('id="pdv-topbar-mais-panel"'))
    check("html_frio_pesar", 'data-pdv-topbar-key="pesar"' in html and "pdv-topbar-mais-panel" in html)
    check("html_frio_pin", 'data-pdv-topbar-key="pin"' in html and "pdv-topbar-mais-panel" in html)
    check("html_mais_destaque", "pdv-wiz-topbar-btn--mais-destaque" in html)
    check("html_js_include", "pdv_topbar_mais.js" in html)
    check("js_btn_toggle", "pdv-topbar-mais-btn" in (ROOT / "produtos/static/produtos/js/pdv_topbar_mais.js").read_text(encoding="utf-8"))
    check("js_overflow_fix", "overflow: visible" in html or "overflow:visible" in html.replace(" ", ""))
    check("catalog_slim_fallback", "catalogo-slim" in (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(encoding="utf-8"))
    check("catalog_no_poison", "catalogo-full-off" in (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(encoding="utf-8"))

    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("url_clique", "api_pdv_topbar_clique" in urls)
    check("url_resumo", "api_pdv_topbar_cliques_resumo" in urls)

    check("keys_pedir", "pedir_loja" in BOTAO_KEYS)
    check("keys_mais", "mais" in BOTAO_KEYS)

    ok_reg, err = registrar_clique(botao="vendas", deposito="vila")
    check("registrar", ok_reg, err)
    ok_bad, _ = registrar_clique(botao="botao_inventado")
    check("registrar_invalido", not ok_bad)

    ranking = resumo_cliques(dias=7)
    check("resumo_lista", isinstance(ranking, list))
    check(
        "resumo_tem_vendas",
        any(r.get("botao") == "vendas" and int(r.get("total") or 0) >= 1 for r in ranking),
    )

    import json

    from django.test import RequestFactory

    from produtos.views_pdv_topbar import api_pdv_topbar_clique, api_pdv_topbar_cliques_resumo

    User = get_user_model()
    user, _ = User.objects.get_or_create(
        username="verify_pdv_topbar_mais",
        defaults={"is_staff": True},
    )
    if not user.has_usable_password():
        user.set_password("verify-topbar-mais")
        user.save(update_fields=["password"])

    rf = RequestFactory()
    req = rf.post(
        "/api/pdv/topbar-clique/",
        data='{"botao":"pedir_loja","deposito":"centro"}',
        content_type="application/json",
    )
    req.user = user
    r = api_pdv_topbar_clique(req)
    body_post = json.loads(r.content.decode("utf-8") or "{}")
    check("api_post", r.status_code == 200 and body_post.get("ok") is True, str(r.status_code))
    req2 = rf.get("/api/pdv/topbar-cliques/?dias=14")
    req2.user = user
    r2 = api_pdv_topbar_cliques_resumo(req2)
    body = json.loads(r2.content.decode("utf-8") or "{}") if r2.status_code == 200 else {}
    check("api_get", r2.status_code == 200 and body.get("ok") is True)
    check(
        "api_ranking",
        any(x.get("botao") == "pedir_loja" for x in (body.get("ranking") or [])),
    )

    n = PdvTopbarCliqueDiaAgro.objects.filter(botao__in=["vendas", "pedir_loja"]).count()
    check("rows_pg", n >= 1, f"rows={n}")

    print(f"\nRESULTADO: {ok} ok / {fail} fail")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
