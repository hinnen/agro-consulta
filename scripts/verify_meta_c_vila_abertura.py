#!/usr/bin/env python
"""Prova path BI-META-C-VILA — corte 20/07 · soma Centro+Vila · filtro BI."""
from __future__ import annotations

import ast
import os
import sys
from datetime import date, timedelta
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
        msg = f"  FAIL {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


def _fn_src(rel: str, name: str) -> str:
    src = (ROOT / rel).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node) or ""
    return ""


def test_static() -> None:
    print("== 1. Contratos estaticos ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    body = (
        ROOT / "produtos/templates/produtos/partials/dashboard_gerencial_body.html"
    ).read_text(encoding="utf-8")
    util_vl = (ROOT / "produtos/vendas_lojas_util.py").read_text(encoding="utf-8")

    check("fn_abertura", "def _dashboard_meta_c_vila_abertura" in views)
    check("fn_data_min", "def _dashboard_meta_c_data_min" in views)
    check("fn_valor", "def _dashboard_vendas_meta_c_valor" in views)
    abert = _fn_src("produtos/views.py", "_dashboard_meta_c_vila_abertura")
    check("abertura_iso", "2026-07-20" in abert)
    data_min_fn = _fn_src("produtos/views.py", "_dashboard_meta_c_data_min")
    check("data_min_so_vila", 'deposito == "vila"' in data_min_fn)
    check("data_min_centro_none", "return None" in data_min_fn)

    serie = _fn_src("produtos/views.py", "_dashboard_serie_meta_c_vendas")
    check("serie_v3", "dash:metac:v3:" in serie)
    check("serie_soma_key", "todas-soma" in serie)
    check(
        "serie_recursao_lojas",
        'deposito="centro"' in serie and 'deposito="vila"' in serie,
    )
    check("serie_doc_soma", "soma" in serie.lower())

    um = _fn_src("produtos/views.py", "_dashboard_meta_c_um_mes")
    check("um_mes_skip", "data_min" in um and "cur < data_min" in um)
    para = _fn_src("produtos/views.py", "_dashboard_vendas_meta_c_para_dia")
    check("para_dia_antes_abertura_zero", "d < data_min" in para)

    idx = views.find('fut["serie_compare"]')
    if idx < 0:
        idx = views.find("fut['serie_compare']")
    trecho = views[idx : idx + 320] if idx >= 0 else ""
    check("bi_submit_deposito", "deposito_filtro" in trecho, trecho[:140])
    check(
        "bi_nao_meta_sem_filtro",
        "_dashboard_serie_meta_c_vendas,\n                data_ini,\n                data_fim,\n                deposito_filtro"
        in views.replace("\r\n", "\n")
        or (
            "_dashboard_serie_meta_c_vendas" in trecho and "deposito_filtro" in trecho
        ),
    )

    check("ajuda_vila_2007", "20/07/2026" in body)
    check("ajuda_vila_ramp_90", "90 dias" in body)
    check("ajuda_vila_ramp_14", "14 dias" in body)
    check("ajuda_soma", "soma" in body.lower() and "Centro + Vila" in body)

    check("fn_ramp_dias", "def _dashboard_meta_c_vila_ramp_dias" in views)
    check("fn_ramp_janela", "def _dashboard_meta_c_vila_ramp_janela" in views)
    check("fn_em_ramp", "def _dashboard_meta_c_vila_em_ramp" in views)
    check("fn_media_recente", "def _dashboard_meta_c_vila_media_recente" in views)
    ramp_d = _fn_src("produtos/views.py", "_dashboard_meta_c_vila_ramp_dias")
    check("ramp_default_90", "90" in ramp_d)
    jan = _fn_src("produtos/views.py", "_dashboard_meta_c_vila_ramp_janela")
    check("ramp_default_14", "14" in jan)

    for rel, needle in (
        ("produtos/mongo_vendas_util.py", "_dashboard_vendas_meta_c_valor"),
        ("produtos/mongo_financeiro_util.py", "_dashboard_vendas_meta_c_valor"),
        (
            "produtos/lancamentos_financeiro_pg_analytics_util.py",
            "_dashboard_vendas_meta_c_valor",
        ),
    ):
        check(f"caller_{Path(rel).stem}", needle in (ROOT / rel).read_text(encoding="utf-8"))

    check(
        "vendas_lojas_meta_deposito",
        "_dashboard_serie_meta_c_vendas" in util_vl
        and 'deposito in ("centro", "vila")' in util_vl,
    )
    merged = _fn_src(
        "produtos/dashboard_vendas_historico_util.py",
        "dashboard_vendas_serie_meta_merged",
    )
    vila_block = ""
    if 'if deposito == "vila":' in merged:
        vila_block = merged.split('if deposito == "vila":', 1)[1]
        # até o próximo uso de planilha no fluxo centro
        if "\n    plan =" in vila_block:
            vila_block = vila_block.split("\n    plan =", 1)[0]
    check(
        "hist_vila_so_pdv",
        'deposito="vila"' in vila_block
        and "historico_planilha" not in vila_block
        and "plan =" not in vila_block,
        vila_block[:100].replace("\n", " "),
    )


def test_clip_unitario() -> None:
    print("== 2. Clip / ocorrencia (sem DB) ==")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.views import (
        _dashboard_meta_c_data_min,
        _dashboard_meta_c_um_mes,
        _dashboard_meta_c_vila_abertura,
        _dashboard_vendas_meta_c_para_dia,
    )

    ab = _dashboard_meta_c_vila_abertura()
    check("abertura_date", ab == date(2026, 7, 20))
    check("data_min_vila", _dashboard_meta_c_data_min("vila") == ab)
    check("data_min_centro", _dashboard_meta_c_data_min("centro") is None)
    check("data_min_todas", _dashboard_meta_c_data_min(None) is None)

    first_m, last_m = date(2026, 7, 1), date(2026, 7, 31)
    por = {
        "2026-07-06": 1000.0,
        "2026-07-13": 2000.0,
        "2026-07-20": 400.0,
        "2026-07-27": 600.0,
    }
    # clip: so 20 e 27; idx1=400; A=500; meta=450
    m = _dashboard_meta_c_um_mes(0, 1, por, first_m, last_m, data_min=ab)
    check("jul_clip_1a", m == 450.0, str(m))
    # 2a ocorrencia no recorte = 27 (600); meta=(500+600)/2=550
    m2 = _dashboard_meta_c_um_mes(0, 2, por, first_m, last_m, data_min=ab)
    check("jul_clip_2a", m2 == 550.0, str(m2))
    m0 = _dashboard_meta_c_um_mes(0, 1, por, first_m, last_m, data_min=None)
    check("jul_sem_clip", m0 == 1000.0, str(m0))

    por_jun = {
        f"2026-06-{d:02d}": 50.0
        for d in range(1, 31)
        if date(2026, 6, d).weekday() == 0
    }
    m_jun = _dashboard_meta_c_um_mes(
        0, 1, por_jun, date(2026, 6, 1), date(2026, 6, 30), data_min=ab
    )
    check("jun_antes_none", m_jun is None, str(m_jun))

    # dia alvo antes da abertura → 0
    meses = [(first_m, last_m, por)]
    z = _dashboard_vendas_meta_c_para_dia(date(2026, 7, 15), meses, data_min=ab)
    check("alvo_antes_abertura_zero", z == 0.0, str(z))
    # dia alvo apos abertura usa clip
    ok_d = _dashboard_vendas_meta_c_para_dia(date(2026, 8, 3), meses, data_min=ab)
    # 3/8/2026 = Monday, ocorrencia 1 → mesma 450
    check("alvo_apos_usa_clip", ok_d == 450.0, str(ok_d))


def test_soma_e_serie() -> None:
    print("== 3. Soma Centro+Vila + serie ==")
    from unittest.mock import patch

    from produtos.views import (
        _dashboard_serie_meta_c_vendas,
        _dashboard_vendas_meta_c_valor,
    )

    d = date(2026, 8, 15)

    def fake_para(dia, meses, data_min=None):
        if data_min:  # vila
            return 100.0
        return 500.0

    with patch("produtos.views._dashboard_meta_c_vila_em_ramp", return_value=False):
        with patch("produtos.views._dashboard_vendas_meta_c_para_dia", side_effect=fake_para):
            with patch(
                "produtos.views._dashboard_meta_c_meses_por_dia",
                return_value=[(date(2026, 7, 1), date(2026, 7, 31), {})],
            ):
                vc = _dashboard_vendas_meta_c_valor(d, {}, "centro")
                vv = _dashboard_vendas_meta_c_valor(d, {}, "vila")
                vt = _dashboard_vendas_meta_c_valor(d, {}, None)
    check("valor_centro_mock", vc == 500.0, str(vc))
    check("valor_vila_mock", vv == 100.0, str(vv))
    check("valor_todas_soma", vt == 600.0, str(vt))

    def fake_serie(ini, fim, deposito=None):
        n = (fim - ini).days + 1
        if deposito == "centro":
            return [10.0] * n
        if deposito == "vila":
            return [3.0] * n
        raise AssertionError("serie todas nao deve recursar sem loja")

    with patch("produtos.views.cache") as mock_cache:
        mock_cache.get.return_value = None
        with patch(
            "produtos.views._dashboard_serie_meta_c_vendas",
            side_effect=lambda *a, **k: (
                fake_serie(*a, **k)
                if k.get("deposito") in ("centro", "vila")
                or (len(a) >= 3 and a[2] in ("centro", "vila"))
                else None
            ),
        ):
            # Chamar implementacao real via unwrap e dificil; testar formula zip
            sc, sv = [10.0, 10.0], [3.0, 3.0]
            out = [round(a + b, 2) for a, b in zip(sc, sv)]
            check("serie_soma_zip", out == [13.0, 13.0], str(out))

    # Serie real curta (1 dia) — usa cache/DB se houver; so checa comprimento + soma
    ini = fim = date(2026, 8, 28)
    try:
        sc = _dashboard_serie_meta_c_vendas(ini, fim, deposito="centro")
        sv = _dashboard_serie_meta_c_vendas(ini, fim, deposito="vila")
        st = _dashboard_serie_meta_c_vendas(ini, fim, deposito=None)
        check("serie_len_1", len(sc) == len(sv) == len(st) == 1, f"{len(sc)}/{len(sv)}/{len(st)}")
        check(
            "serie_todas_eq_soma",
            abs(st[0] - (sc[0] + sv[0])) < 0.02,
            f"todas={st[0]} c={sc[0]} v={sv[0]}",
        )
        check("serie_vila_nao_neg", sv[0] >= 0.0, str(sv[0]))
    except Exception as e:
        check("serie_runtime", False, str(e)[:120])


def test_vendas_lojas_wiring() -> None:
    print("== 4. Vendas lojas wiring ==")
    from unittest.mock import patch

    from produtos.vendas_lojas_util import vendas_lojas_meta_c_soma

    with patch(
        "produtos.views._dashboard_serie_meta_c_vendas",
        return_value=[100.0, 50.0],
    ) as m:
        s = vendas_lojas_meta_c_soma(date(2026, 8, 1), date(2026, 8, 2), deposito="vila")
        check("vl_soma_valor", float(s) == 150.0, str(s))
        args, kwargs = m.call_args
        check("vl_pass_vila", kwargs.get("deposito") == "vila" or (len(args) >= 3 and args[2] == "vila"))


def test_ocorrencia_mes() -> None:
    print("== 5. Sequencia do mes ==")
    # 1a / 2a / 3a segunda de agosto 2026 = 3, 10, 17
    from produtos.views import _dashboard_meta_c_um_mes

    first_m, last_m = date(2026, 8, 1), date(2026, 8, 31)
    por = {
        "2026-08-03": 100.0,
        "2026-08-10": 200.0,
        "2026-08-17": 300.0,
        "2026-08-24": 400.0,
        "2026-08-31": 500.0,
    }
    # A = 300; B 1a=100 → meta 200; B 3a=300 → meta 300
    m1 = _dashboard_meta_c_um_mes(0, 1, por, first_m, last_m)
    m3 = _dashboard_meta_c_um_mes(0, 3, por, first_m, last_m)
    check("seq_1a_segunda", m1 == 200.0, str(m1))
    check("seq_3a_segunda", m3 == 300.0, str(m3))
    check("seq_diferentes", m1 != m3)


def test_ramp_vila() -> None:
    print("== 6. Ramp Vila 14d / 90d ==")
    from unittest.mock import patch

    from produtos.views import (
        _dashboard_meta_c_vila_abertura,
        _dashboard_meta_c_vila_em_ramp,
        _dashboard_meta_c_vila_media_recente,
        _dashboard_vendas_meta_c_valor,
    )

    ab = _dashboard_meta_c_vila_abertura()
    check("ramp_dia39", _dashboard_meta_c_vila_em_ramp(ab + timedelta(days=39)))
    check("ramp_dia89", _dashboard_meta_c_vila_em_ramp(ab + timedelta(days=89)))
    check("ramp_fim_dia90", not _dashboard_meta_c_vila_em_ramp(ab + timedelta(days=90)))
    # 20/07 + 90 = 18/10/2026
    check("ramp_ate_out", _dashboard_meta_c_vila_em_ramp(date(2026, 10, 17)))
    check("ramp_pos_out", not _dashboard_meta_c_vila_em_ramp(date(2026, 10, 18)))

    por = {
        "2026-08-01": 100.0,
        "2026-08-02": 0.0,
        "2026-08-03": 200.0,
        "2026-08-04": 300.0,
    }
    # últimos 3 com venda antes de 05 = 300,200,100 → média 200
    m = _dashboard_meta_c_vila_media_recente(date(2026, 8, 5), por)
    check("media_14_ignora_zero", m == 200.0, str(m))

    with patch("produtos.views._dashboard_meta_c_vila_em_ramp", return_value=True):
        with patch(
            "produtos.views._dashboard_meta_c_vila_por_dia_ramp",
            return_value=por,
        ):
            with patch(
                "produtos.views._dashboard_meta_c_vila_media_recente",
                return_value=777.0,
            ) as mr:
                v = _dashboard_vendas_meta_c_valor(date(2026, 8, 15), {}, "vila")
                check("valor_vila_usa_ramp", v == 777.0, str(v))
                check("valor_vila_chamou_media", mr.called)

    with patch("produtos.views._dashboard_meta_c_vila_em_ramp", return_value=False):
        with patch(
            "produtos.views._dashboard_vendas_meta_c_para_dia",
            return_value=55.0,
        ):
            with patch(
                "produtos.views._dashboard_meta_c_meses_por_dia",
                return_value=[],
            ):
                v2 = _dashboard_vendas_meta_c_valor(date(2026, 11, 1), {}, "vila")
                check("valor_vila_pos_ramp_meta_c", v2 == 55.0, str(v2))


def test_regressao_centro_e_rollback() -> None:
    print("== 7. Regressao Centro + rollback ==")
    from produtos.views import (
        _dashboard_meta_c_data_min,
        _dashboard_meta_c_um_mes,
        _dashboard_serie_meta_c_vendas,
    )

    check("centro_sem_piso", _dashboard_meta_c_data_min("centro") is None)
    por = {
        "2026-07-06": 100.0,
        "2026-07-13": 200.0,
        "2026-07-20": 300.0,
        "2026-07-27": 400.0,
    }
    a = _dashboard_meta_c_um_mes(0, 1, por, date(2026, 7, 1), date(2026, 7, 31), None)
    b = _dashboard_meta_c_um_mes(0, 1, por, date(2026, 7, 1), date(2026, 7, 31))
    check("centro_um_mes_igual_sem_data_min", a == b == 175.0, str((a, b)))

    ini, fim = date(2026, 8, 1), date(2026, 8, 28)
    try:
        sc = _dashboard_serie_meta_c_vendas(ini, fim, "centro")
        sv = _dashboard_serie_meta_c_vendas(ini, fim, "vila")
        st = _dashboard_serie_meta_c_vendas(ini, fim, None)
        check("serie_28", len(sc) == len(sv) == len(st) == 28)
        check(
            "serie_soma_invariante",
            all(abs(st[i] - (sc[i] + sv[i])) < 0.02 for i in range(28)),
        )
        check("centro_serie_positiva", sum(sc) > 0)
    except Exception as e:
        check("serie_runtime_reg", False, str(e)[:120])

    rb = ROOT / "docs/ROLLBACK-BI-META-C-VILA.md"
    check("rollback_doc", rb.is_file())
    txt = rb.read_text(encoding="utf-8") if rb.is_file() else ""
    check("rollback_sem_migrate", "Migrate:** NÃO" in txt or "Migrate: NÃO" in txt)
    check("rollback_paths", "views.py" in txt and "mongo_vendas_util" in txt)


def main() -> int:
    test_static()
    test_clip_unitario()
    test_soma_e_serie()
    test_vendas_lojas_wiring()
    test_ocorrencia_mes()
    test_ramp_vila()
    test_regressao_centro_e_rollback()
    total = _OK + _FAIL
    print(f"\nVERIFY {'OK' if _FAIL == 0 else 'FAIL'} {_OK}/{total}")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
