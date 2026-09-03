#!/usr/bin/env python3
"""TABELA-PRECO-FORMA — path % por forma, arredondamento, overlap, promo max."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

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
    import django

    django.setup()

    from produtos.caixa_util import normalizar_forma_pagamento_caixa
    from produtos.tabela_preco_forma_util import (
        arredondar_dezena_centavos,
        preco_com_percentual,
        preco_pdv_para_forma,
        regra_promo_vs_tabela,
        validar_overlap_formas,
    )

    print("=== arredondamento ===")
    check(arredondar_dezena_centavos("10.43") == Decimal("10.40"), "10,43 → 10,40")
    check(arredondar_dezena_centavos("10.45") == Decimal("10.50"), "10,45 → 10,50")
    check(arredondar_dezena_centavos("10.47") == Decimal("10.50"), "10,47 → 10,50")
    check(arredondar_dezena_centavos("10.40") == Decimal("10.40"), "10,40 → 10,40")

    print("=== percentual ===")
    # 10 * (1 - 0.55/100) = 9.945 → 9.95; com arredonda → 10.00? 
    # User example was 0.55% of 10 = 0.055 → 9.945. They said 10.45 which was wrong math.
    # We test: 10 with -5.5% = 9.45 → arredonda 9.50? 9.45 → resto 5 → 9.50
    p = preco_com_percentual(10, Decimal("-5.5"), arredondar=False)
    check(p == Decimal("9.45"), f"-5,5% de 10 = 9,45 (foi {p})")
    p2 = preco_com_percentual(10, Decimal("-5.5"), arredondar=True)
    check(p2 == Decimal("9.50"), f"arredonda 9,45 → 9,50 (foi {p2})")
    p3 = preco_com_percentual(10, Decimal("1.5"), arredondar=False)
    check(p3 == Decimal("10.15"), f"+1,5% = 10,15 (foi {p3})")

    print("=== overlap ===")
    err = validar_overlap_formas(
        [
            {"slot": 1, "ativo": True, "formas": ["Fiado", "PIX"]},
            {"slot": 2, "ativo": True, "formas": ["PIX"]},
        ]
    )
    check(err is not None and "PIX" in err, "bloqueia PIX nas duas")
    err2 = validar_overlap_formas(
        [
            {"slot": 1, "ativo": True, "formas": ["Fiado"]},
            {"slot": 2, "ativo": True, "formas": ["Cartão de crédito"]},
        ]
    )
    check(err2 is None, "formas distintas OK")

    print("=== PDV preço ===")
    check(
        normalizar_forma_pagamento_caixa("cartao") == "Cartão de crédito",
        "alias cartao → Cartão de crédito",
    )
    check(
        normalizar_forma_pagamento_caixa("dinheiro") == "Dinheiro",
        "alias dinheiro → Dinheiro",
    )
    tabelas = [
        {
            "slot": 1,
            "nome": "Fiado",
            "ativo": True,
            "percentual": -5.5,
            "arredondar_dezena_centavos": True,
            "formas": ["Fiado"],
            "categorias_vetadas": [],
            "produtos_vetados": [],
        }
    ]
    prod = {"id": "1", "preco_padrao": 10.0, "preco_venda": 10.0, "categoria": "Rações"}
    v = preco_pdv_para_forma(prod, "Fiado", tabelas=tabelas, resolucoes={})
    check(abs(v - 9.5) < 0.001, f"Fiado tabela → 9,50 (foi {v})")
    v2 = preco_pdv_para_forma(prod, "PIX", tabelas=tabelas, resolucoes={})
    check(abs(v2 - 10.0) < 0.001, f"PIX sem tabela → 10 (foi {v2})")

    prod_vet = dict(prod)
    tabelas_vet = [dict(tabelas[0], produtos_vetados=["1"])]
    v3 = preco_pdv_para_forma(prod_vet, "Fiado", tabelas=tabelas_vet, resolucoes={})
    check(abs(v3 - 10.0) < 0.001, f"vetado → base 10 (foi {v3})")

    prod_ind = {
        "id": "2",
        "preco_padrao": 10.0,
        "precos_modo": "por_forma",
        "precos_por_forma": {"Fiado": 8.0},
    }
    v4 = preco_pdv_para_forma(
        prod_ind, "Fiado", tabelas=tabelas, resolucoes={}
    )
    check(abs(v4 - 8.0) < 0.001, f"conflito default individual → 8 (foi {v4})")
    v5 = preco_pdv_para_forma(
        prod_ind,
        "Fiado",
        tabelas=tabelas,
        resolucoes={"1": {"2": "tabela"}},
    )
    check(abs(v5 - 9.5) < 0.001, f"resolução tabela → 9,50 (foi {v5})")

    print("=== promo vs tabela ===")
    check(regra_promo_vs_tabela(9, 10, "maior") == 10, "maior → 10")
    check(regra_promo_vs_tabela(9, 10, "promo") == 9, "promo → 9")
    check(regra_promo_vs_tabela(9, 10, "tabela") == 10, "tabela → 10")

    print("=== fontes ===")
    js = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
        encoding="utf-8"
    )
    st = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    check("precosTabelasVisiveis" in js, "JS precosTabelasVisiveis")
    check("carregarTabelasGlobais" in js, "JS carregarTabelasGlobais")
    check("function formaFromMeioEntrega" in js, "JS formaFromMeioEntrega")
    check("formaFromMeioEntrega: formaFromMeioEntrega" in js, "JS exporta formaFromMeioEntrega")
    check("syncFormaPorMeioEntrega" in st, "state aplica tabela no meio da entrega")
    check("formaFromMeioEntrega('cartao')" in wiz, "print entrega usa forma canônica do cartão")
    check(
        (ROOT / "produtos/migrations/0104_tabela_preco_forma.py").is_file(),
        "migration 0104",
    )

    print(f"\n{PASS} ok · {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
