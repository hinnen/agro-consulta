#!/usr/bin/env python
"""Prova Meta C Vila: corte 20/07 + soma Centro+Vila + filtro BI."""
from __future__ import annotations

import ast
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_OK = 0
_FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global _OK, _FAIL
    if cond:
        _OK += 1
        print(f"  OK  {name}")
    else:
        _FAIL += 1
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _fn_src(rel: str, name: str) -> str:
    tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment((ROOT / rel).read_text(encoding="utf-8"), node) or ""
    return ""


def test_static() -> None:
    print("== Estático ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    body = (ROOT / "produtos/templates/produtos/partials/dashboard_gerencial_body.html").read_text(
        encoding="utf-8"
    )
    check("fn_abertura", "def _dashboard_meta_c_vila_abertura" in views)
    check("fn_data_min", "def _dashboard_meta_c_data_min" in views)
    check("fn_valor", "def _dashboard_vendas_meta_c_valor" in views)
    check("abertura_iso", "2026-07-20" in _fn_src("produtos/views.py", "_dashboard_meta_c_vila_abertura"))
    serie = _fn_src("produtos/views.py", "_dashboard_serie_meta_c_vendas")
    check("serie_v2", "dash:metac:v2:" in serie)
    check("serie_soma", "todas-soma" in serie)
    check("serie_recursao_lojas", 'deposito="centro"' in serie and 'deposito="vila"' in serie)
    um = _fn_src("produtos/views.py", "_dashboard_meta_c_um_mes")
    check("um_mes_skip", "data_min" in um and "cur < data_min" in um)
    # BI passa deposito_filtro na série compare
    check(
        "bi_serie_filtro",
        "deposito_filtro" in views
        and "_dashboard_serie_meta_c_vendas" in views
        and "fut[\"serie_compare\"]" in views.replace("'", '"'),
    )
    # Trecho do submit: procura depósito no submit da meta
    idx = views.find('fut["serie_compare"]')
    if idx < 0:
        idx = views.find("fut['serie_compare']")
    trecho = views[idx : idx + 280] if idx >= 0 else ""
    check("bi_submit_deposito", "deposito_filtro" in trecho, trecho[:120])
    check("ajuda_vila_2007", "20/07/2026" in body)
    check("ajuda_soma", "soma" in body.lower() and "Centro + Vila" in body)
    for rel, needle in (
        ("produtos/mongo_vendas_util.py", "_dashboard_vendas_meta_c_valor"),
        ("produtos/mongo_financeiro_util.py", "_dashboard_vendas_meta_c_valor"),
        ("produtos/lancamentos_financeiro_pg_analytics_util.py", "_dashboard_vendas_meta_c_valor"),
    ):
        check(f"caller_{Path(rel).stem}", needle in (ROOT / rel).read_text(encoding="utf-8"))


def test_um_mes_clip() -> None:
    print("== Clip um mês (sem DB) ==")
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.views import _dashboard_meta_c_um_mes, _dashboard_meta_c_vila_abertura

    ab = _dashboard_meta_c_vila_abertura()
    check("abertura_date", ab == date(2026, 7, 20))

    # Julho 2026: segundas = 6, 13, 20, 27. Só 20 e 27 após abertura.
    first_m, last_m = date(2026, 7, 1), date(2026, 7, 31)
    por = {
        "2026-07-06": 1000.0,  # antes — deve ignorar
        "2026-07-13": 2000.0,  # antes — deve ignorar
        "2026-07-20": 400.0,
        "2026-07-27": 600.0,
    }
    # 1ª ocorrência no recorte aberto = 20/07 (400); A = (400+600)/2 = 500; meta = (500+400)/2 = 450
    m = _dashboard_meta_c_um_mes(0, 1, por, first_m, last_m, data_min=ab)  # Monday=0
    check("jul_clip_meta", m == 450.0, str(m))

    # Sem clip: inclui zeros/valores antes → A = (1000+2000+400+600)/4 = 1000; B 1ª=1000; meta=1000
    m0 = _dashboard_meta_c_um_mes(0, 1, por, first_m, last_m, data_min=None)
    check("jul_sem_clip", m0 == 1000.0, str(m0))

    # Junho inteiro antes da abertura → None
    por_jun = {f"2026-06-{d:02d}": 50.0 for d in range(1, 31) if date(2026, 6, d).weekday() == 0}
    m_jun = _dashboard_meta_c_um_mes(
        0, 1, por_jun, date(2026, 6, 1), date(2026, 6, 30), data_min=ab
    )
    check("jun_antes_none", m_jun is None, str(m_jun))


def main() -> int:
    test_static()
    test_um_mes_clip()
    print(f"\nVERIFY {'OK' if _FAIL == 0 else 'FAIL'} {_OK}/{_OK + _FAIL}")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
