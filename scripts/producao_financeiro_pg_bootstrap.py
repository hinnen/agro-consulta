#!/usr/bin/env python
"""Produção: importa DtoLancamento → TituloFinanceiroAgro na build (1ª vez ou reimport forçado)."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()


def main() -> int:
    force = os.environ.get("AGRO_FINANCEIRO_PG_REIMPORT", "").lower() in ("1", "true", "yes")
    from produtos.lancamentos_financeiro_agro_util import maybe_bootstrap_financeiro_pg_producao

    r = maybe_bootstrap_financeiro_pg_producao(force=force)
    if r.get("skipped"):
        print(f"financeiro_pg_bootstrap_producao: skip ({r.get('motivo')})")
        return 0
    if not r.get("ok"):
        print(f"financeiro_pg_bootstrap_producao: ERRO {r.get('erro')}", file=sys.stderr)
        return 1
    print(
        "financeiro_pg_bootstrap_producao: OK "
        f"lidos={r.get('lidos')} pg={r.get('pg_depois')} "
        f"abertos={r.get('abertos')} restante_total={r.get('restante_total')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
