#!/usr/bin/env python3
"""PDV-MODO-POR-FORMA — path detalhado: cadastro → save → catálogo → JS → preço."""
from __future__ import annotations

import os
import subprocess
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


def slim_modo_catalogo(ce: dict) -> tuple[str, bool]:
    """Espelha a decisão de catalogo_agro.listar_slim_rows_pdv (modo + manda grupos?)."""
    from produtos.precos_forma_pagamento_util import (
        extrair_precos_grupos_cadastro_extras,
        extrair_precos_modo_cadastro_extras,
        extrair_precos_por_forma_cadastro_extras,
    )

    modo = extrair_precos_modo_cadastro_extras(ce)
    pg = extrair_precos_grupos_cadastro_extras(ce)
    ppf = extrair_precos_por_forma_cadastro_extras(ce)
    raw_modo = ce.get("precos_modo") if isinstance(ce, dict) else None
    if raw_modo is None or str(raw_modo).strip() == "":
        if pg and not ppf:
            modo = "grupos"
        else:
            modo = "por_forma"
    manda_grupos = modo == "grupos" and bool(pg)
    return modo, manda_grupos


def aplicar_save_extras(ex: dict, payload: dict) -> dict:
    """Espelha o bloco precos_* de views (save cadastro)."""
    from produtos.precos_forma_pagamento_util import (
        normalizar_precos_grupos_payload,
        normalizar_precos_modo,
        normalizar_precos_por_forma_payload,
    )

    out = dict(ex)
    if "precos_por_forma" in payload:
        ppf = normalizar_precos_por_forma_payload(payload.get("precos_por_forma"))
        if ppf:
            out["precos_por_forma"] = ppf
        else:
            out.pop("precos_por_forma", None)
    if "precos_modo" in payload:
        modo = normalizar_precos_modo(payload.get("precos_modo"))
        if modo == "grupos":
            out["precos_modo"] = "grupos"
        else:
            out["precos_modo"] = "por_forma"
            out.pop("precos_grupos", None)
    if "precos_grupos" in payload:
        if normalizar_precos_modo(out.get("precos_modo") or payload.get("precos_modo")) == "grupos":
            pg = normalizar_precos_grupos_payload(payload.get("precos_grupos"))
            if pg:
                out["precos_grupos"] = pg
            else:
                out.pop("precos_grupos", None)
        else:
            out.pop("precos_grupos", None)
    return out


def main() -> int:
    print("=== fontes ===")
    js = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
        encoding="utf-8"
    )
    state = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
    consulta = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(
        encoding="utf-8"
    )
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    modal = (
        ROOT / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
    ).read_text(encoding="utf-8")

    check("if (m === 'por_forma' || m === 'forma' || m === 'porforma')" in js, "JS modoItem: ramo por_forma")
    i_por = js.find("if (m === 'por_forma'")
    i_gt = js.find("if (gruposTemDados(precosGruposDoItem(item))) return 'grupos';")
    check(i_por > 0 and i_gt > i_por, "JS modoItem: inferência A/B só depois")
    check("item.precos_modo === 'grupos' && pg" in js, "JS copiar: grupos só no modo grupos")
    check("delete item.precos_grupos" in js, "JS copiar: limpa grupos em por_forma")

    check("else if (existing.precos_grupos)" not in state, "pdv_state: sem forçar grupos no add existente")
    check("else if (novo.precos_grupos)" not in state, "pdv_state: sem forçar grupos no add novo")
    check("if (!row.precos_modo) row.precos_modo = 'grupos'" not in state, "pdv_state: sem default grupos no hydrate")
    check("copiarPrecosPorFormaDoProduto" in state, "pdv_state: usa copiarPrecosPorFormaDoProduto")

    check(
        "out.precos_modo === 'grupos' && pg" in consulta,
        "consulta: meta só manda A/B se grupos",
    )
    check(
        "item.precos_modo === 'grupos' && pgMeta" in consulta,
        "consulta: addCarrinho só copia A/B se grupos",
    )

    check('ex["precos_modo"] = "por_forma"' in views, "views: grava por_forma")
    check("Grava explícito" in views, "views: comentário limpa A/B")
    check('if pg and modo != "grupos":' not in cat, "catalogo: sem forçar grupos")
    check('if modo == "grupos" and pg:' in cat, "catalogo: só manda grupos no modo")
    check("modalEdit._precosModo !== 'grupos'" in modal, "modal: payload A/B vazio em por_forma")

    print("\n=== django path ===")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    sys.path.insert(0, str(ROOT))
    import django

    django.setup()
    from produtos.precos_forma_pagamento_util import preco_venda_para_forma

    lixo_ab = {
        "preco_a": 50.0,
        "preco_b": 60.0,
        "formas_a": ["PIX"],
        "formas_b": ["Dinheiro"],
    }
    ppf9 = {
        "PIX": 87.0,
        "Dinheiro": 90.0,
        "Cartão de crédito": 92.0,
        "Cartão de débito": 88.0,
        "Fiado": 95.0,
    }

    # Path Renan: estava grupos → mudou para por forma (9 formas) → salvou
    ex0 = {"precos_modo": "grupos", "precos_grupos": lixo_ab, "precos_por_forma": None}
    ex1 = aplicar_save_extras(
        ex0,
        {
            "precos_modo": "por_forma",
            "precos_por_forma": ppf9,
            "precos_grupos": {"preco_a": None, "preco_b": None, "formas_a": [], "formas_b": []},
        },
    )
    check(ex1.get("precos_modo") == "por_forma", "save: modo=por_forma")
    check("precos_grupos" not in ex1, "save: limpou A/B")
    check(abs(float(ex1["precos_por_forma"]["PIX"]) - 87.0) < 1e-9, "save: PIX 87 no mapa")

    modo_s, manda = slim_modo_catalogo(ex1)
    check(modo_s == "por_forma" and not manda, "catálogo após save: por_forma sem A/B")

    # Bug clássico: save antigo apagava modo, sobrava A/B + mapa por forma
    ce_legado = {"precos_grupos": lixo_ab, "precos_por_forma": ppf9}
    modo_l, manda_l = slim_modo_catalogo(ce_legado)
    check(modo_l == "por_forma" and not manda_l, "catálogo legado: mapa+lixo → por_forma sem A/B")

    # Legado só A/B (produto de verdade em 2 grupos sem chave — raro)
    ce_so_ab = {"precos_grupos": lixo_ab}
    modo_ab, manda_ab = slim_modo_catalogo(ce_so_ab)
    check(modo_ab == "grupos" and manda_ab, "catálogo só A/B: continua grupos")

    # Grupos explícito
    ce_g = {"precos_modo": "grupos", "precos_grupos": lixo_ab}
    modo_g, manda_g = slim_modo_catalogo(ce_g)
    check(modo_g == "grupos" and manda_g, "catálogo grupos explícito")

    v = preco_venda_para_forma(
        99, ppf9, "PIX", precos_modo="por_forma", precos_grupos=lixo_ab
    )
    check(abs(v - 87.0) < 1e-9, f"preço util por_forma+lixo → 87 (foi {v})")
    v2 = preco_venda_para_forma(99, ppf9, "PIX", precos_modo="grupos", precos_grupos=lixo_ab)
    check(abs(v2 - 50.0) < 1e-9, f"preço util grupos → 50 (foi {v2})")

    print("\n=== node JS real ===")
    r = subprocess.run(
        ["node", str(ROOT / "scripts" / "verify_pdv_modo_por_forma.js")],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    print(r.stdout or "")
    if r.stderr:
        print(r.stderr)
    check(r.returncode == 0, "node verify_pdv_modo_por_forma.js")

    print(f"\n{PASS} ok · {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
