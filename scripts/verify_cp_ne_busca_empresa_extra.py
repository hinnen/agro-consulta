#!/usr/bin/env python
"""Extra deep checks for CP-NE-BUSCA-EMPRESA (no Mongo write)."""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.urls import reverse

from produtos import mongo_financeiro_util as mfu
from produtos.caixa_util import empresa_nome_saida_caixa

OK = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("=== CP-NE-BUSCA-EMPRESA extra ===\n")
    check("empresa centro", empresa_nome_saida_caixa("centro") == "Agro Mais Centro")
    check("empresa vila", empresa_nome_saida_caixa("vila") == "Agro Mais Vila Elias")
    check("empresa vazio = centro", empresa_nome_saida_caixa("") == "Agro Mais Centro")

    dv0 = date(2026, 8, 28)
    check("parc 0 = base", mfu._fin_vencimento_parcela(dv0, 0, 45) == dv0)
    check(
        "parc 1 45d",
        mfu._fin_vencimento_parcela(dv0, 1, 45) == date(2026, 10, 12),
        str(mfu._fin_vencimento_parcela(dv0, 1, 45)),
    )
    check("mensal 28/08 para 28/09", mfu._fin_vencimento_parcela(dv0, 1, 30) == date(2026, 9, 28))
    check("url criar", "emprestimo" in reverse("api_emprestimos_criar"))

    modal = (ROOT / "produtos/templates/produtos/includes/lancamento_novo_emprestimo_modal.html").read_text(
        encoding="utf-8"
    )
    for k in (
        "ne-field--sug",
        "preencherEmpresaPadrao",
        "ne-parc-card-mid",
        "neIntervaloDias",
        "__outro__",
        "attachSuggest",
        "overflow: visible",
        "empresa_padrao",
        "ne-sug-dd",
    ):
        check(f"modal {k}", k in modal)

    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check(
        "view empresa pela loja",
        "empresa_padrao" in views
        and "empresa_nome_saida_caixa" in views
        and "bootstrap_deposito" in views,
    )
    check("view no CP render", "lancamentos_contas_pagar_view" in views)

    print(f"\n=== Extra: {OK} OK · {FAIL} FAIL ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
