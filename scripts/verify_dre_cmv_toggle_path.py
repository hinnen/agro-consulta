"""
Prova detalhada DRE-CMV-TOGGLE.
Path: /financeiro/dashboard-gerencial/ -> dashboard_financeiro_completo
      -> get_indicadores_gerencial_pg -> custo_mercadoria_vendida + recalc_indicadores_cmv
      -> indicadores_gerencial.html (chips + json_script + JS toggle)

  python scripts/verify_dre_cmv_toggle_path.py
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    views = _read("financeiro/views.py")
    ind = _read("financeiro/services/indicadores_gerencial_pg.py")
    vendas = _read("produtos/relatorios_vendas_util.py")
    html = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    html_rg = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js_rg = _read("static/js/agro_resumo_gerencial.js")
    util_pdv = _read("financeiro/services/receita_pdv_util.py")
    resumo_pg = _read("financeiro/services/resumo_operacional_pg.py")
    api = _read("financeiro/api/views.py")
    bi = _read("produtos/views.py")

    check("url_indicadores", "dashboard_financeiro_completo" in urls and "financeiro/dashboard-gerencial/" in urls)
    check("view_chama_get", "get_indicadores_gerencial_pg" in views)
    check("fn_recalc", "def recalc_indicadores_cmv" in ind)
    check("fn_cmv_vendida", "def custo_mercadoria_vendida" in vendas)
    check("fn_cmv_rows", "def cmv_vendida_de_rows" in vendas)
    check("default_vendida", 'modo_ssr = "vendida" if cmv_v_ok else "paga"' in ind)
    check("fallback_paga", "if cmv_v_ok:" in ind and "atual_paga" in ind)
    check("pack_js_sem_caixa", '"geracao_caixa"' not in ind.split("_DRE_CMV_JS_FIELDS")[1].split("def recalc_indicadores_cmv")[0])
    check("recalc_nao_mexe_caixa", "geracao_caixa" not in ind.split("def recalc_indicadores_cmv")[1].split("def _pack_cmv_js")[0].split("out.update")[1])
    check("mesmo_deposito_pdv", "deposito_pdv_por_empresa_id" in ind and "custo_mercadoria_vendida" in ind)
    check("cmv_usa_dep_pdv", "deposito=dep_pdv" in ind)
    check("caixa_bloco_sem_pdv", "usar_receita_pdv=False" in ind)

    qs = vendas.split("def _qs_itens")[1].split("def _agg_itens_por_produto")[0]
    bi_fn = bi.split("def _vendas_aplicar_filtro_loja")[1].split("\n\n\n")[0]
    check("qs_vila", 'venda__deposito__iexact="vila"' in qs)
    check("qs_centro_vazio", 'venda__deposito=""' in qs and "venda__deposito__isnull=True" in qs)
    check("bi_vila", 'deposito__iexact="vila"' in bi_fn)
    check("bi_centro_vazio", 'deposito=""' in bi_fn and "deposito__isnull=True" in bi_fn)
    check("relatorio_sem_filtro_loja", "_qs_itens(desde, ate)" in vendas.split("def _agg_itens_por_produto")[1].split("def cmv_vendida_de_rows")[0])

    check("chip_vendida", 'data-dre-cmv="vendida"' in html)
    check("chip_paga", 'data-dre-cmv="paga"' in html)
    check("json_cmv_modos", 'json_script:"ig-cmv-modos"' in html)
    check("js_localstorage", "agro_dre_cmv_modo_v1" in html)
    check("js_apply_modo", "function applyModo" in html)
    check("kpi_lucro_data", 'data-dre-field="lucro_bruto"' in html)
    check("kpi_liquido_data", 'data-dre-field="resultado_liquido"' in html)
    check("caixa_sem_data_dre", not re.search(r'data-dre-field="geracao_caixa"', html))
    caixa_tbl = html.split("Fluxo de caixa")[1].split("</table>")[0]
    check("caixa_tbl_sem_toggle", "data-dre-field" not in caixa_tbl)
    check("ajuda_dois_cmv", "CMV vendida" in html and "CMV paga" in html)
    check("ajuda_caixa_nao_muda", "caixa</strong> não muda" in html or "caixa não muda" in html.lower())

    check("resumo_chip_vendida", 'data-dre-cmv="vendida"' in html_rg)
    check("resumo_chip_paga", 'data-dre-cmv="paga"' in html_rg)
    check("resumo_js_key", "agro_dre_cmv_modo_v1" in js_rg)
    check("resumo_js_apply", "function aplicarCmvNoCore" in js_rg)
    check("resumo_fn_modos", "def aplicar_cmv_modos_no_resumo" in util_pdv)
    check("resumo_flag", "anexar_cmv_modos" in resumo_pg)
    check("api_anexa_cmv", "anexar_cmv_modos=True" in api)
    check("resumo_ajuda_cmv", "CMV vendida" in html_rg and "caixa não muda" in html_rg)


def test_url_reverse() -> None:
    print("== URL ==")
    from django.urls import reverse

    path = reverse("dashboard_financeiro_completo")
    check("reverse_indicadores", path.rstrip("/").endswith("financeiro/dashboard-gerencial"), path)


def test_math_e_integracao() -> None:
    print("== Math + integracao ==")
    from financeiro.services.indicadores_gerencial_pg import (
        _indicadores_from_core,
        _pack_cmv_js,
        get_indicadores_gerencial_pg,
        recalc_indicadores_cmv,
    )
    from financeiro.services.resumo_operacional_mongo import natureza_buckets_from_linhas_dre
    from financeiro.models import LancamentoFinanceiro as NF
    from produtos.relatorios_vendas_util import cmv_vendida_de_rows

    total, ok_skus, sem = cmv_vendida_de_rows(
        [
            {"produto_id_externo": "A", "qtd": 10},
            {"produto_id_externo": "B", "qtd": 2},
            {"produto_id_externo": "", "qtd": 9},
            {"produto_id_externo": "C", "qtd": 0},
        ],
        {"A": {"custo": "3.50"}, "B": {"custo": 0}, "C": {"custo": "10"}},
    )
    check("rows_total", total == Decimal("35.00"), str(total))
    check("rows_ok_sem", ok_skus == 1 and sem == 1, f"ok={ok_skus} sem={sem}")

    buckets = natureza_buckets_from_linhas_dre([])
    buckets[NF.NATUREZA_RECEITA_OPERACIONAL] = Decimal("80")
    buckets[NF.NATUREZA_CMV] = Decimal("180")
    paga = _indicadores_from_core(
        {
            "receita_operacional": Decimal("100000"),
            "receita_nao_operacional": Decimal("10"),
            "cmv": Decimal("60000"),
            "despesas_fixas": Decimal("8000"),
            "despesas_variaveis": Decimal("5000"),
            "despesas_financeiras": Decimal("2000"),
            "resultado_liquido_gerencial": Decimal("25000"),
            "aportes_socios": Decimal("0"),
            "retiradas_socios": Decimal("0"),
            "receita_fonte": "pdv",
            "receita_lancamentos": Decimal("0"),
        },
        caixa_buckets=buckets,
        dias_janela=9,
    )
    caixa_antes = paga["geracao_caixa"]
    vendida = recalc_indicadores_cmv(paga, Decimal("24663"), 9)
    check("lucro_vendida", vendida["lucro_bruto"] == Decimal("75337"), str(vendida["lucro_bruto"]))
    check(
        "liquido_formula",
        vendida["resultado_liquido"] == vendida["ebitda"] - vendida["desp_fin"] + vendida["receita_nao_op"],
    )
    check("caixa_intacto", vendida["geracao_caixa"] == caixa_antes == Decimal("-100"), str(vendida["geracao_caixa"]))
    pack = _pack_cmv_js(vendida)
    check("pack_tem_cmv", "cmv" in pack and "lucro_bruto" in pack)
    check("pack_sem_caixa", "geracao_caixa" not in pack)

    ref60 = _indicadores_from_core(
        {
            "receita_operacional": Decimal("200000"),
            "receita_nao_operacional": Decimal("0"),
            "cmv": Decimal("120000"),
            "despesas_fixas": Decimal("16000"),
            "despesas_variaveis": Decimal("10000"),
            "despesas_financeiras": Decimal("4000"),
            "resultado_liquido_gerencial": Decimal("50000"),
            "aportes_socios": Decimal("0"),
            "retiradas_socios": Decimal("0"),
            "receita_fonte": "pdv",
            "receita_lancamentos": Decimal("0"),
        },
        caixa_buckets=buckets,
        dias_janela=60,
    )

    with patch(
        "financeiro.services.indicadores_gerencial_pg._bloco_periodo",
        side_effect=[(paga, None), (ref60, None)],
    ), patch(
        "financeiro.services.indicadores_gerencial_pg._faturamento_pdv_periodo",
        return_value={"ok": True, "total": Decimal("100000"), "por_dia": {}},
    ), patch(
        "financeiro.services.receita_pdv_util.deposito_pdv_por_empresa_id",
        return_value="centro",
    ), patch(
        "produtos.relatorios_vendas_util.custo_mercadoria_vendida",
        side_effect=[
            {"ok": True, "total": Decimal("24663"), "skus_com_custo": 80, "skus_sem_custo": 3},
            {"ok": True, "total": Decimal("50000"), "skus_com_custo": 90, "skus_sem_custo": 1},
        ],
    ) as mock_cmv, patch(
        "financeiro.services.indicadores_gerencial_pg.gastos_variacao_pg",
        return_value={"ok": True, "chart": {}, "buckets": []},
    ):
        out = get_indicadores_gerencial_pg(7, date(2026, 8, 1), date(2026, 8, 9))

    check("ssr_modo_vendida", out["atual"]["cmv_modo"] == "vendida")
    check("ssr_cmv_vendida", out["atual"]["cmv"] == Decimal("24663"), str(out["atual"]["cmv"]))
    check("guarda_cmv_paga", out["atual"]["cmv_paga"] == Decimal("60000"), str(out["atual"]["cmv_paga"]))
    check("ssr_lucro_vendida", out["atual"]["lucro_bruto"] == Decimal("75337"))
    check("ssr_caixa_igual_paga", out["atual"]["geracao_caixa"] == caixa_antes)
    check("json_vendida_cmv", out["cmv_modos"]["vendida"]["atual"]["cmv"] == 24663.0)
    check("json_paga_cmv", out["cmv_modos"]["paga"]["atual"]["cmv"] == 60000.0)
    check("json_paga_lucro", out["cmv_modos"]["paga"]["atual"]["lucro_bruto"] == float(paga["lucro_bruto"]))
    check("json_sem_caixa", "geracao_caixa" not in out["cmv_modos"]["vendida"]["atual"])
    check("aviso_sem_custo", out["atual"]["cmv_skus_sem_custo"] == 3)
    check(
        "cmv_chamou_centro",
        mock_cmv.call_args_list
        and mock_cmv.call_args_list[0].kwargs.get("deposito") == "centro",
        str(mock_cmv.call_args_list[0].kwargs if mock_cmv.call_args_list else {}),
    )


def main() -> int:
    test_arquivos()
    test_url_reverse()
    test_math_e_integracao()
    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
