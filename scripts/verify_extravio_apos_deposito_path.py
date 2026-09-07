#!/usr/bin/env python
"""Prova path EXTRAVIO-APOS-DEPOSITO — classificação DRE + util + Mini DRE keys."""
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

from financeiro.models import LancamentoFinanceiro as NF
from financeiro.services.resumo_operacional_mongo import (
    agregar_linhas_dre_em_resumo,
    classificar_despesa_plano,
)
from produtos.extravio_deposito_util import (
    PLANO_EXTRAVIO_APOS_DEPOSITO,
    forma_eh_dinheiro,
    plano_eh_extravio_apos_deposito,
)


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise SystemExit(f"FAIL {name}: {detail or 'assertion'}")
    print(f"OK {name}")


def main() -> None:
    check("nome_oficial", PLANO_EXTRAVIO_APOS_DEPOSITO == "Extravio após Depósito")
    check("detect_plano", plano_eh_extravio_apos_deposito(PLANO_EXTRAVIO_APOS_DEPOSITO))
    check("detect_alias", plano_eh_extravio_apos_deposito("extravio apos deposito"))
    check("nao_socio", not plano_eh_extravio_apos_deposito("Retiradas Geraldo"))
    check("forma_dinheiro", forma_eh_dinheiro("Dinheiro"))
    check("forma_dinheiro_caixa", forma_eh_dinheiro("Dinheiro · Caixa 1") or forma_eh_dinheiro("DINHEIRO"))
    check("forma_banco", not forma_eh_dinheiro("Pix"))

    nat = classificar_despesa_plano(PLANO_EXTRAVIO_APOS_DEPOSITO)
    check("nat_retirada", nat == NF.NATUREZA_RETIRADA_SOCIO, str(nat))

    core = agregar_linhas_dre_em_resumo(
        [
            {"plano": "Salários", "despesa": 1000, "receita": 0},
            {"plano": "Retiradas Geraldo", "despesa": 200, "receita": 0},
            {"plano": PLANO_EXTRAVIO_APOS_DEPOSITO, "despesa": 80, "receita": 0},
            {"plano": "Vendas", "despesa": 0, "receita": 5000},
        ]
    )
    check("extravio_campo", Decimal(core["extravio_apos_deposito"]) == Decimal("80"))
    check("retiradas_sem_extravio", Decimal(core["retiradas_socios"]) == Decimal("200"))
    # Líquido não deve incluir extravio/retirada
    check(
        "liquido_sem_extravio",
        Decimal(core["resultado_liquido_gerencial"])
        == Decimal(core["resultado_operacional"]) - Decimal(core["despesas_financeiras"]),
    )
    esperado_caixa = (
        Decimal(core["resultado_liquido_gerencial"])
        + Decimal(core["emprestimos_entrada"])
        + Decimal(core["aportes_socios"])
        - Decimal(core["amortizacao_emprestimos"])
        - Decimal(core["retiradas_socios"])
        - Decimal(core["extravio_apos_deposito"])
    )
    check("geracao_caixa", Decimal(core["geracao_caixa"]) == esperado_caixa)

    # Migração / util: arquivo existe
    mig = ROOT / "produtos" / "migrations" / "0127_plano_extravio_apos_deposito.py"
    check("migration", mig.is_file())
    util = ROOT / "produtos" / "extravio_deposito_util.py"
    check("util", util.is_file())
    js = (ROOT / "static" / "js" / "agro_resumo_gerencial.js").read_text(encoding="utf-8")
    check("js_linha", "Extravio após Depósito" in js)
    check("js_saldo", "extravioDep" in js or "extravio_apos_deposito" in js)
    html = (ROOT / "produtos" / "templates" / "produtos" / "lancamentos_financeiros.html").read_text(
        encoding="utf-8"
    )
    check("html_checkbox", "bx-retirar-caixa" in html)
    check("html_default_off", "bx-retirar-caixa" in html and "checked" not in html.split("bx-retirar-caixa")[1][:120])
    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    check("views_helper", "_anexar_retirada_caixa_apos_baixa_cp" in views)
    check("views_flag", "retirar_caixa_pdv" in views)
    print("ALL OK", 20)


if __name__ == "__main__":
    main()
