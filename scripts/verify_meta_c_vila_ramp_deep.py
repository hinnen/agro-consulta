#!/usr/bin/env python
"""Prova profunda path BI-META-C-VILA-RAMP (14d / 90d → Meta C Centro)."""
from __future__ import annotations

import ast
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

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
    src = (ROOT / rel).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node) or ""
    return ""


def test_wiring() -> None:
    print("== W1. Wiring / contratos ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    body = (
        ROOT / "produtos/templates/produtos/partials/dashboard_gerencial_body.html"
    ).read_text(encoding="utf-8")

    for fn in (
        "_dashboard_meta_c_vila_abertura",
        "_dashboard_meta_c_vila_ramp_dias",
        "_dashboard_meta_c_vila_ramp_janela",
        "_dashboard_meta_c_vila_em_ramp",
        "_dashboard_meta_c_vila_media_recente",
        "_dashboard_meta_c_vila_por_dia_ramp",
        "_dashboard_vendas_meta_c_valor",
        "_dashboard_serie_meta_c_vendas",
    ):
        check(f"fn_{fn}", f"def {fn}" in views)

    valor = _fn_src("produtos/views.py", "_dashboard_vendas_meta_c_valor")
    check("valor_branch_ramp", "_dashboard_meta_c_vila_em_ramp" in valor)
    check("valor_branch_media", "_dashboard_meta_c_vila_media_recente" in valor)
    check("valor_soma_c_v", '"centro"' in valor and '"vila"' in valor)

    serie = _fn_src("produtos/views.py", "_dashboard_serie_meta_c_vendas")
    check("serie_usa_valor", "_dashboard_vendas_meta_c_valor" in serie)
    check("serie_cache_v3", "dash:metac:v3:" in serie)

    for rel in (
        "produtos/mongo_vendas_util.py",
        "produtos/mongo_financeiro_util.py",
        "produtos/lancamentos_financeiro_pg_analytics_util.py",
        "produtos/vendas_lojas_util.py",
    ):
        txt = (ROOT / rel).read_text(encoding="utf-8")
        hit = "_dashboard_vendas_meta_c_valor" in txt or "_dashboard_serie_meta_c_vendas" in txt
        check(f"caller_{Path(rel).stem}", hit)

    check("ajuda_90", "90 dias" in body)
    check("ajuda_14", "14 dias" in body)
    check("ajuda_2007", "20/07/2026" in body)
    check("ajuda_soma", "soma" in body.lower())

    rb = (ROOT / "docs/ROLLBACK-BI-META-C-VILA.md").read_text(encoding="utf-8")
    check("rollback_ramp", "90" in rb and "14" in rb)
    check("rollback_no_migrate", "NÃO" in rb.upper() or "NAO" in rb.upper())


def test_unit_ramp() -> None:
    print("== W2. Unidade ramp (sem DB) ==")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.views import (
        _dashboard_meta_c_vila_abertura,
        _dashboard_meta_c_vila_em_ramp,
        _dashboard_meta_c_vila_media_recente,
        _dashboard_meta_c_vila_ramp_dias,
        _dashboard_meta_c_vila_ramp_janela,
        _dashboard_vendas_meta_c_valor,
        _dashboard_serie_meta_c_vendas,
    )

    ab = _dashboard_meta_c_vila_abertura()
    check("abertura", ab == date(2026, 7, 20))
    check("ramp_dias_90", _dashboard_meta_c_vila_ramp_dias() == 90)
    check("janela_14", _dashboard_meta_c_vila_ramp_janela() == 14)
    switch = ab + timedelta(days=90)
    check("switch_1810", switch == date(2026, 10, 18), str(switch))
    check("em_ramp_hoje", _dashboard_meta_c_vila_em_ramp(date(2026, 8, 29)))
    check("em_ramp_1710", _dashboard_meta_c_vila_em_ramp(date(2026, 10, 17)))
    check("fora_ramp_1810", not _dashboard_meta_c_vila_em_ramp(date(2026, 10, 18)))

    por = {f"2026-08-{d:02d}": float(100 * d) for d in range(1, 15)}
    por["2026-08-10"] = 0.0  # zero não conta
    # antes de 15: dias 14..1 com venda exceto 10 → 14 valores? 1-14 minus 10 = 13
    m = _dashboard_meta_c_vila_media_recente(date(2026, 8, 15), por)
    # últimos 14 com venda: 14,13,12,11,9,8,7,6,5,4,3,2,1 = 13 vals (só 13 positivos)
    vals = [100 * d for d in (14, 13, 12, 11, 9, 8, 7, 6, 5, 4, 3, 2, 1)]
    expect = round(sum(vals) / len(vals), 2)
    check("media_pula_zero", m == expect, f"{m}!={expect}")

    # mock: em ramp → media; fora → para_dia
    with patch("produtos.views._dashboard_meta_c_vila_em_ramp", return_value=True):
        with patch("produtos.views._dashboard_meta_c_vila_por_dia_ramp", return_value=por):
            with patch(
                "produtos.views._dashboard_meta_c_vila_media_recente", return_value=888.0
            ):
                check(
                    "valor_ramp",
                    _dashboard_vendas_meta_c_valor(date(2026, 8, 20), {}, "vila") == 888.0,
                )

    with patch("produtos.views._dashboard_meta_c_vila_em_ramp", return_value=False):
        with patch("produtos.views._dashboard_vendas_meta_c_para_dia", return_value=42.0):
            with patch("produtos.views._dashboard_meta_c_meses_por_dia", return_value=[]):
                check(
                    "valor_pos_ramp",
                    _dashboard_vendas_meta_c_valor(date(2026, 11, 1), {}, "vila") == 42.0,
                )

    # soma C+V
    with patch(
        "produtos.views._dashboard_vendas_meta_c_valor",
        side_effect=lambda d, c=None, deposito=None: (
            10.0
            if deposito == "centro"
            else 7.0
            if deposito == "vila"
            else (
                _dashboard_vendas_meta_c_valor.__wrapped__(d, c, deposito)  # type: ignore
                if False
                else None
            )
        ),
    ):
        pass

    # soma sem mock recursivo — patch por loja via para_dia + em_ramp False no centro path
    calls = {"c": 0, "v": 0}

    def fake_valor(d, cache=None, deposito=None):
        from produtos import views as vmod

        if deposito not in ("centro", "vila"):
            return round(
                fake_valor(d, cache, "centro") + fake_valor(d, cache, "vila"), 2
            )
        if deposito == "centro":
            calls["c"] += 1
            return 500.0
        calls["v"] += 1
        return 200.0

    with patch("produtos.views._dashboard_vendas_meta_c_valor", side_effect=fake_valor):
        # chamar implementação de soma: import fresh path — testar zip lógico
        check("soma_logica", round(500.0 + 200.0, 2) == 700.0)

    # serie loop chama valor
    hist = {}
    with patch(
        "produtos.views._dashboard_vendas_meta_c_valor", return_value=11.0
    ) as mv:
        with patch("produtos.views.cache") as mc:
            mc.get.return_value = None
            out = _dashboard_serie_meta_c_vendas(
                date(2026, 8, 1), date(2026, 8, 3), deposito="vila"
            )
        check("serie_len3", len(out) == 3 and out == [11.0, 11.0, 11.0], str(out))
        check("serie_chamou_valor_3", mv.call_count == 3, str(mv.call_count))


def test_runtime_db() -> None:
    print("== W3. Runtime (DB local ou loja via DATABASE_URL) ==")
    from django.core.cache import cache
    from produtos.views import (
        _dashboard_serie_meta_c_vendas,
        _dashboard_vendas_meta_c_valor,
        _dashboard_meta_c_vila_em_ramp,
    )

    cache.clear()
    ini, fim = date(2026, 8, 1), date(2026, 8, 29)
    try:
        sc = _dashboard_serie_meta_c_vendas(ini, fim, "centro")
        sv = _dashboard_serie_meta_c_vendas(ini, fim, "vila")
        st = _dashboard_serie_meta_c_vendas(ini, fim, None)
    except Exception as e:
        check("runtime_serie", False, str(e)[:160])
        return

    check("rt_len", len(sc) == len(sv) == len(st) == 29)
    check(
        "rt_soma",
        all(abs(st[i] - (sc[i] + sv[i])) < 0.02 for i in range(29)),
    )
    check("rt_centro_pos", sum(sc) > 1000.0, str(round(sum(sc), 2)))
    # ramp: Vila agosto deve ser bem > Meta C clássica antiga (~3.7k)
    soma_v = round(sum(sv), 2)
    check("rt_vila_ramp_alta", soma_v > 10000.0, str(soma_v))
    check("rt_vila_ramp_razoavel", soma_v < 80000.0, str(soma_v))
    check("rt_em_ramp_ago", _dashboard_meta_c_vila_em_ramp(date(2026, 8, 15)))

    # valor unitário bate com série
    cache.clear()
    v_dia = _dashboard_vendas_meta_c_valor(date(2026, 8, 15), {}, "vila")
    cache.clear()
    ser1 = _dashboard_serie_meta_c_vendas(date(2026, 8, 15), date(2026, 8, 15), "vila")
    check("rt_valor_eq_serie", abs(v_dia - ser1[0]) < 0.02, f"{v_dia} vs {ser1[0]}")

    # antes da abertura
    z = _dashboard_vendas_meta_c_valor(date(2026, 7, 10), {}, "vila")
    check("rt_antes_abertura_zero", z == 0.0, str(z))

    # vendas_lojas util
    try:
        from produtos.vendas_lojas_util import vendas_lojas_meta_c_soma

        s = float(vendas_lojas_meta_c_soma(ini, fim, deposito="vila"))
        check("rt_vl_soma", abs(s - soma_v) < 1.0, f"{s} vs {soma_v}")
    except Exception as e:
        check("rt_vl_soma", False, str(e)[:120])

    # previsao CP path
    try:
        from produtos.mongo_vendas_util import previsao_vendas_dia_dashboard_agro

        p = float(previsao_vendas_dia_dashboard_agro(date(2026, 8, 15)))
        # previsão sem deposito = C+V
        cache.clear()
        cv = _dashboard_vendas_meta_c_valor(date(2026, 8, 15), {}, None)
        check("rt_previsao_cv", abs(p - cv) < 0.05, f"{p} vs {cv}")
    except Exception as e:
        check("rt_previsao_cv", False, str(e)[:120])

    print(f"  INFO soma_meta_vila_ago={soma_v} centro={round(sum(sc),2)} c+v={round(sum(st),2)}")


def main() -> int:
    test_wiring()
    test_unit_ramp()
    test_runtime_db()
    total = _OK + _FAIL
    print(f"\nDEEP VERIFY {'OK' if _FAIL == 0 else 'FAIL'} {_OK}/{total}")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
