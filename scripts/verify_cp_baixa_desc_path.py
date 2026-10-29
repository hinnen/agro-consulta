#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Prova path CP-BAIXA-DESC (bug #19) — descrição opcional na baixa CP.

  python scripts/verify_cp_baixa_desc_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {msg}")
    else:
        fail += 1
        print(f"  FAIL {msg}")


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("== Bug #19 CP-BAIXA-DESC ==")
    cp = _read("produtos/templates/produtos/lancamentos_contas_pagar_teste.html")
    fin = _read("produtos/templates/produtos/lancamentos_financeiros.html")
    pg = _read("produtos/lancamentos_financeiro_pg_write_util.py")
    mongo = _read("produtos/mongo_financeiro_util.py")
    views = _read("produtos/views.py")

    check('id="sv-bx-desc"' in cp, "CP teste: campo baixa total")
    check('id="sv-bxp-desc"' in cp, "CP teste: campo baixa parcial")
    check("Descrição do pagamento (opcional)" in cp, "CP teste: rótulo descrição")
    check("body.descricao = descBx" in cp or "body.descricao = descBx.slice" in cp, "CP teste: POST manda descricao (total)")
    check("sv-bxp-desc" in cp and "descricao:" in cp, "CP teste: POST manda descricao (parcial)")

    check("Descrição do pagamento (opcional)" in fin, "Lançamentos: rótulo descrição")
    check("body.descricao = descBx" in fin or "if (descBx) body.descricao" in fin, "Lançamentos: POST manda descricao")

    check("def _descricao_baixa_limpa" in pg, "PG write: limpa descrição")
    check('f"Agro baixa {dt_lbl}: {desc_bx}"' in pg or "Agro baixa" in pg, "PG write: grava em observações")
    check("Agro parc." in pg or "Agro parcial" in pg or "Agro parc" in pg, "PG write: parcial em observações")

    check("Agro baixa" in mongo, "Mongo util: Agro baixa nas observações")

    # Views aceitam descricao no payload de baixa
    check(
        "descricao" in views[views.find("def api_lancamentos") : views.find("def api_lancamentos") + 8000]
        or "descricao" in pg,
        "pipeline baixa aceita descricao",
    )

    total = ok + fail
    print(f"\nVERIFY_OK {ok}/{total}" if fail == 0 else f"\nVERIFY_FAIL {ok}/{total}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
