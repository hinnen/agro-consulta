#!/usr/bin/env python3
"""PDV-MODO-POR-FORMA — modo «por forma» não vira grupos por lixo A/B."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def main() -> int:
    js = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
        encoding="utf-8"
    )
    check("if (m === 'por_forma' || m === 'forma' || m === 'porforma')" in js, "JS: ramo por_forma antes de gruposTemDados")
    i_por = js.find("if (m === 'por_forma'")
    i_gt = js.find("if (gruposTemDados(precosGruposDoItem(item))) return 'grupos';")
    check(i_por > 0 and i_gt > i_por, "JS: gruposTemDados depois de por_forma")
    check("item.precos_modo = modoItem(item);" in js, "JS: copiar usa modoItem")
    check(
        "String(modo).toLowerCase() === 'grupos' || gruposTemDados(item.precos_grupos)"
        not in js,
        "JS: sem forçar grupos no copiar",
    )

    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check('ex["precos_modo"] = "por_forma"' in views, "views: grava precos_modo=por_forma")
    check(
        'ex.pop("precos_grupos", None)' in views
        and "Grava explícito" in views,
        "views: limpa precos_grupos ao salvar por_forma",
    )

    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    check(
        'if pg and modo != "grupos":\n            modo = "grupos"' not in cat,
        "catalogo: não força grupos por lixo A/B",
    )
    check('if modo == "grupos" and pg:' in cat, "catalogo: só manda grupos no modo grupos")

    modal = (
        ROOT / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
    ).read_text(encoding="utf-8")
    check(
        "if (modalEdit._precosModo !== 'grupos')" in modal
        and "preco_a: null" in modal,
        "modal: payload grupos vazio em por_forma",
    )

    # Prova runtime Python: backend já respeita modo
    sys.path.insert(0, str(ROOT))
    import django
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        django.setup()
    except Exception as e:
        check(False, f"django.setup: {e}")
        print(f"\n{PASS} ok · {FAIL} fail")
        return 1 if FAIL else 0

    from produtos.precos_forma_pagamento_util import (
        normalizar_precos_modo,
        preco_venda_para_forma,
    )

    check(normalizar_precos_modo("por_forma") == "por_forma", "util: por_forma")
    check(normalizar_precos_modo("grupos") == "grupos", "util: grupos")
    g = {
        "preco_a": 50.0,
        "preco_b": 60.0,
        "formas_a": ["PIX"],
        "formas_b": ["Dinheiro"],
    }
    ppf = {"PIX": 87.0, "Dinheiro": 90.0}
    # Com modo por_forma + lixo grupos: deve usar mapa por forma (87), não A (50)
    v = preco_venda_para_forma(
        99, ppf, "PIX", precos_modo="por_forma", precos_grupos=g
    )
    check(abs(v - 87.0) < 1e-9, f"util: por_forma+lixo A/B → PIX 87 (foi {v})")
    v2 = preco_venda_para_forma(99, ppf, "PIX", precos_modo="grupos", precos_grupos=g)
    check(abs(v2 - 50.0) < 1e-9, f"util: grupos → PIX 50 (foi {v2})")

    # Simula lógica JS modoItem em Python
    def modo_item(item: dict) -> str:
        m = str(item.get("precos_modo") or "").lower().replace("-", "_").replace(" ", "_")
        if m in ("grupos", "grupo", "2_grupos", "dois_grupos", "ab", "a_b"):
            return "grupos"
        if m in ("por_forma", "forma", "porforma"):
            return "por_forma"
        g0 = item.get("precos_grupos") or {}
        if (g0.get("preco_a") or 0) > 0 or (g0.get("preco_b") or 0) > 0:
            return "grupos"
        if g0.get("formas_a") or g0.get("formas_b"):
            return "grupos"
        return "por_forma"

    check(
        modo_item({"precos_modo": "por_forma", "precos_grupos": g}) == "por_forma",
        "sim JS: por_forma explícito ignora lixo A/B",
    )
    check(
        modo_item({"precos_modo": "grupos", "precos_grupos": g}) == "grupos",
        "sim JS: grupos explícito",
    )
    check(
        modo_item({"precos_grupos": g}) == "grupos",
        "sim JS: sem modo + A/B → grupos (legado)",
    )

    print(f"\n{PASS} ok · {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
