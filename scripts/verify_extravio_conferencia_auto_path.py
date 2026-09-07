#!/usr/bin/env python
"""Prova: CP só BANCO/DINHEIRO + extravio auto = depósito − baixas BANCO."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.conferencia_deposito_extravio_util import (
    aplicar_extravio_auto_no_resumo,
    extravio_auto_periodo,
)
from produtos.extravio_deposito_util import (
    filtrar_formas_baixa_cp,
    forma_eh_banco,
    forma_eh_dinheiro,
    forma_permitida_baixa_cp,
)


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise SystemExit(f"FAIL {name}: {detail or 'assertion'}")
    print(f"OK {name}")


def main() -> None:
    check("banco_ok", forma_eh_banco("BANCO"))
    check("banco_ok2", forma_eh_banco("Banco"))
    check("banco_nao_pix", not forma_eh_banco("Pix"))
    check("banco_nao_cartao", not forma_eh_banco("Cartão crédito"))
    check("banco_nao_adicionar", not forma_eh_banco("ADICIONAR BANCO"))
    check("dinheiro_ok", forma_eh_dinheiro("Dinheiro"))
    check("perm_banco", forma_permitida_baixa_cp("BANCO"))
    check("perm_din", forma_permitida_baixa_cp("Dinheiro"))
    check("perm_nao_pix", not forma_permitida_baixa_cp("Pix"))

    formas = [
        {"id": "1", "nome": "Dinheiro"},
        {"id": "2", "nome": "BANCO"},
        {"id": "3", "nome": "Pix"},
        {"id": "4", "nome": "Cartão"},
    ]
    f2 = filtrar_formas_baixa_cp(formas)
    check("filtro_2", len(f2) == 2, str(f2))
    nomes = {x["nome"] for x in f2}
    check("filtro_nomes", nomes == {"Dinheiro", "BANCO"})

    # fórmula: depósito 1000 − BANCO 700 = 300
    core = {
        "extravio_apos_deposito": Decimal("0"),
        "geracao_caixa": Decimal("5000.00"),
        "retiradas_socios": Decimal("0"),
    }

    # mock applying with known pack via monkeypatch of calc
    from produtos import conferencia_deposito_extravio_util as u

    orig = u.extravio_auto_periodo

    def fake_periodo(*_a, **_k):
        return {
            "depositos_caixa": Decimal("1000.00"),
            "baixas_banco": Decimal("700.00"),
            "extravio_auto": Decimal("300.00"),
        }

    u.extravio_auto_periodo = fake_periodo  # type: ignore
    try:
        from datetime import date

        aplicar_extravio_auto_no_resumo(
            core, date(2026, 8, 1), date(2026, 8, 31), deposito="centro"
        )
    finally:
        u.extravio_auto_periodo = orig  # type: ignore

    check("auto_300", Decimal(core["extravio_apos_deposito"]) == Decimal("300.00"))
    check("dep_key", Decimal(core["depositos_caixa"]) == Decimal("1000.00"))
    check("ban_key", Decimal(core["baixas_banco"]) == Decimal("700.00"))
    check(
        "geracao_cortou",
        Decimal(core["geracao_caixa"]) == Decimal("4700.00"),
        str(core["geracao_caixa"]),
    )

    # manual + auto
    core2 = {
        "extravio_apos_deposito": Decimal("50.00"),
        "geracao_caixa": Decimal("1000.00"),
    }
    u.extravio_auto_periodo = fake_periodo  # type: ignore
    try:
        from datetime import date

        aplicar_extravio_auto_no_resumo(
            core2, date(2026, 8, 1), date(2026, 8, 31)
        )
    finally:
        u.extravio_auto_periodo = orig  # type: ignore
    check(
        "manual_mais_auto",
        Decimal(core2["extravio_apos_deposito"]) == Decimal("350.00"),
    )

    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    check("views_reject", "_forma_pagamento_permitida_baixa_cp" in views)
    check("views_filter_qs", "somente_dinheiro_banco" in views)

    html_cp = (
        ROOT / "produtos" / "templates" / "produtos" / "lancamentos_contas_pagar_teste.html"
    ).read_text(encoding="utf-8")
    check("cp_param", "somente_dinheiro_banco=1" in html_cp)

    html_cl = (
        ROOT / "produtos" / "templates" / "produtos" / "lancamentos_financeiros.html"
    ).read_text(encoding="utf-8")
    check("cl_param", "somente_dinheiro_banco" in html_cl)

    js = (ROOT / "static" / "js" / "agro_resumo_gerencial.js").read_text(encoding="utf-8")
    check("js_auto_hint", "depósitos do caixa" in js or "depositos do caixa" in js.lower() or "baixas CP" in js)

    pg = (
        ROOT / "financeiro" / "services" / "resumo_operacional_pg.py"
    ).read_text(encoding="utf-8")
    check("pg_hook", "aplicar_extravio_auto_no_resumo" in pg)

    # smoke: função real não explode
    from datetime import date

    pack = extravio_auto_periodo(date(2026, 8, 1), date(2026, 8, 31), deposito="centro")
    check("smoke_keys", "extravio_auto" in pack and "depositos_caixa" in pack)

    print("ALL OK")


if __name__ == "__main__":
    main()
