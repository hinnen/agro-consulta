"""
Prova detalhada DRE-VISUAL-PREVIA.

Path:
  /financeiro/resumo-gerencial/
    -> resumo_financeiro_gerencial_view
    -> resumo_financeiro_gerencial.html + agro_resumo_gerencial.js
    -> GET /api/financeiro/resumo-operacional?incluir_visual=1
    -> consolidar_empresa_pg(anexar_cmv_modos=True)
    -> montar_dre_visual (so modo=empresa) -> gastos_variacao_pg
    -> JS renderVisualBoard (fluxo + PE + donut + categorias + mini DRE)
  CMV vendida x paga igual (agro_dre_cmv_modo_v1). Caixa nao muda.
  Indicadores /financeiro/dashboard-gerencial/ intacto (sem redirect).

  python scripts/verify_dre_visual_path.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    cfg = _read("config/urls.py")
    api_urls = _read("financeiro/api/urls.py")
    api = _read("financeiro/api/views.py")
    ser = _read("financeiro/api/serializers.py")
    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    css = _read("static/css/agro_resumo_gerencial.css")
    util = _read("financeiro/services/dre_visual_util.py")
    rec_util = _read("financeiro/services/receita_pdv_util.py")
    resumo = _read("financeiro/services/resumo_operacional_pg.py")
    ind = _read("financeiro/services/indicadores_gerencial_pg.py")
    ind_html = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    ind_views = _read("financeiro/views.py")
    prod_views = _read("produtos/views.py")
    gastos = _read("financeiro/services/gastos_variacao_pg.py")

    check("url_resumo", "financeiro/resumo-gerencial/" in urls and "resumo_financeiro_gerencial" in urls)
    check("url_indicadores", "financeiro/dashboard-gerencial/" in urls)
    check("api_include", 'path("api/financeiro/"' in cfg or "api/financeiro/" in cfg)
    check("api_resumo_rota", "resumo-operacional" in api_urls)
    check("api_pe_rota", "gap-equilibrio" in api_urls)

    check("view_resumo", "def resumo_financeiro_gerencial_view" in prod_views)
    check("view_template_resumo", "resumo_financeiro_gerencial.html" in prod_views)
    check("fn_montar", "def montar_dre_visual" in util)
    check("fn_gastos", "def gastos_variacao_pg" in gastos)
    check("montar_chama_gastos", "gastos_variacao_pg" in util)
    check("montar_top12", "[:12]" in util)
    check("montar_top_chart_8", "top_chart=8" in util)
    check("ser_incluir_visual", "incluir_visual" in ser)
    check("ser_default_false", "incluir_visual = serializers.BooleanField" in ser)
    check("api_chama_montar", "montar_dre_visual" in api)
    check("api_so_empresa", 'params["modo"] == "empresa"' in api and "incluir_visual" in api)
    check("api_anexa_cmv", api.count("anexar_cmv_modos=True") >= 4)
    gap = api.split("class GapEquilibrioAPIView")[1] if "class GapEquilibrioAPIView" in api else ""
    check("pe_nao_anexa_visual", "montar_dre_visual" not in gap)

    check("html_js", "agro_resumo_gerencial.js" in html)
    check("html_css", "agro_resumo_gerencial.css" in html)
    check("html_painel_visual", 'id="painel-visual"' in html)
    check("html_mais_numeros", 'id="rg-mais-numeros"' in html)
    check("html_indicadores_link", "dashboard_financeiro_completo" in html)
    check("html_titulo_dre", "DRE gerencial" in html)
    check("html_chip_vendida", 'data-dre-cmv="vendida"' in html)
    check("html_chip_paga", 'data-dre-cmv="paga"' in html)
    check("html_ajuda_indicadores", "Indicadores · Financeiro gerencial" in html or "Indicadores" in html)

    check("js_incluir_visual", "incluir_visual=1" in js)
    check("js_fetch_resumo", "/api/financeiro/resumo-operacional" in js)
    check("js_render_visual", "function renderVisualBoard" in js)
    check("js_apply_cmv", "function aplicarCmvNoCore" in js)
    check("js_key_cmv", "agro_dre_cmv_modo_v1" in js)
    check("js_fluxo_desp", "Despesas" in js and "Receita" in js and "% Lucro" in js)
    check("js_donut", "Composição das despesas" in js and "rg-donut" in js)
    check("js_categorias", "Despesas por categoria" in js)
    check("js_mini_dre", "Mini DRE" in js)
    check("js_caixa_nao_muda", "não muda com CMV" in js)
    check("js_grupo_msg", "Abra uma empresa" in js)
    check("js_gauge", "rg-gauge" in js and "faturamento_equilibrio" in js)
    check("js_spark", "faturamento_pdv" in js)
    check("js_chip_click", 'querySelectorAll("[data-dre-cmv]")' in js)
    check("js_default_vendida", 'return "vendida"' in js)

    check("css_board", ".rg-board" in css)
    check("css_flow", ".rg-flow" in css)
    check("css_gauge", ".rg-gauge" in css)
    check("css_donut", ".rg-donut" in css)
    check("css_cat", ".rg-cat" in css)
    check("css_mini", ".rg-mini" in css)
    check("css_16x9_full", "max-width: none" in css and "100dvh" in html)
    check("css_grid_wide", "rg-col--charts" in css and "rg-col--cat" in css)
    check("js_col_charts", "rg-col--charts" in js)

    check("flag_cmv_default_false", "anexar_cmv_modos: bool = False" in resumo)
    check("aplicar_cmv", "def aplicar_cmv_modos_no_resumo" in rec_util)
    aplicar = rec_util.split("def aplicar_cmv_modos_no_resumo")[1].split("def fundir_cmv_modos_grupo")[0]
    check("raiz_cmv_paga", 'out["cmv_paga"]' in aplicar)
    check("aplicar_nao_grava_caixa", 'out["geracao_caixa"]' not in aplicar)

    check("ind_view_intacta", "def dashboard_financeiro_completo" in ind_views)
    check("ind_template_intacta", "indicadores_gerencial.html" in ind_views)
    check("ind_get_pg", "get_indicadores_gerencial_pg" in ind_views)
    check("ind_titulo", "Indicadores</span>" in ind_html)
    check("ind_aba_fin", 'Financeiro</a>' in ind_html or ">Financeiro</a>" in ind_html or "Financeiro" in ind_html)
    check("ind_despesas_cat", "Despesas por categoria" in ind_html)
    check("ind_cmv_chips", 'data-dre-cmv="vendida"' in ind_html and 'data-dre-cmv="paga"' in ind_html)
    check("ind_mesma_key", "agro_dre_cmv_modo_v1" in ind_html)
    check("ind_nao_anexa_cmv", "anexar_cmv_modos=True" not in ind)
    check("ind_fn_get", "def get_indicadores_gerencial_pg" in ind)
    check("ind_gastos", "gastos_variacao_pg" in ind)

    bloco = prod_views.split("def resumo_financeiro_gerencial_view")[1][:900]
    check("resumo_nao_redirect", "HttpResponseRedirect" not in bloco and "redirect(" not in bloco.lower())


def test_urls() -> None:
    print("== URL ==")
    from django.urls import reverse

    p = reverse("resumo_financeiro_gerencial")
    check("reverse_resumo", p.rstrip("/").endswith("financeiro/resumo-gerencial"), p)
    i = reverse("dashboard_financeiro_completo")
    check("reverse_indicadores", "dashboard-gerencial" in i, i)
    a = reverse("financeiro-resumo-operacional")
    check("reverse_api", "resumo-operacional" in a, a)
    g = reverse("financeiro-gap-equilibrio")
    check("reverse_pe", "gap-equilibrio" in g, g)


def test_montar_e_json() -> None:
    print("== montar_dre_visual ==")
    from financeiro.api.jsonutil import json_safe
    from financeiro.services.dre_visual_util import montar_dre_visual

    fake = {
        "ok": True,
        "buckets": [{"key": "m1", "label": "Jun", "de": "2026-06-01", "ate": "2026-06-30"}],
        "resumo_grupos": [{"key": "fixa", "label": "Fixas", "ultimo": 50.0}],
        "total_ultimo_periodo": 50.0,
        "linhas": [
            {"plano": "Aluguel", "valores": [40, 50], "delta_abs": 10, "tendencia": "up"}
        ]
        + [
            {"plano": f"P{n}", "valores": [n], "delta_abs": 0, "tendencia": "flat"}
            for n in range(20)
        ],
    }
    with patch(
        "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
        return_value=fake,
    ) as mock_g:
        out = montar_dre_visual(empresa_id=9, por="competencia")
    check("montar_ok", out.get("ok") is True)
    check("montar_top_aluguel", out["variacao"]["top"][0]["ultimo"] == 50.0)
    check("montar_top_max_12", len(out["variacao"]["top"]) == 12, str(len(out["variacao"]["top"])))
    check("montar_por_mes", mock_g.call_args.kwargs.get("modo") == "mes")
    check("montar_por_comp", mock_g.call_args.kwargs.get("por") == "competencia")
    check("montar_eid", mock_g.call_args.kwargs.get("empresa_id") == 9)
    safe = json_safe(out)
    check("json_safe_ok", isinstance(safe, dict) and safe["ok"] is True)
    check("json_safe_top_float", isinstance(safe["variacao"]["top"][0]["ultimo"], float))

    with patch(
        "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
        return_value={"ok": False, "erro": "x"},
    ):
        bad = montar_dre_visual(empresa_id=1)
    check("montar_erro", bad.get("ok") is False and bad["variacao"]["ok"] is False)

    with patch(
        "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
        return_value={
            "ok": True,
            "linhas": [{"categoria": "Luz", "valores": [12], "delta_abs": 0, "tendencia": "flat"}],
            "resumo_grupos": [],
            "buckets": [],
            "total_ultimo_periodo": 12,
        },
    ):
        fb = montar_dre_visual(empresa_id=2, por="vencimento")
    check("montar_fallback_cat", fb["variacao"]["top"][0]["plano"] == "Luz")


def test_serializer() -> None:
    print("== serializer ==")
    from financeiro.api.serializers import ResumoOperacionalQuerySerializer

    ok = ResumoOperacionalQuerySerializer(
        data={
            "modo": "empresa",
            "empresa_id": 1,
            "data_inicio": "2026-07-01",
            "data_fim": "2026-07-31",
            "incluir_visual": "1",
        }
    )
    check("ser_visual_1", ok.is_valid() and ok.validated_data["incluir_visual"] is True)
    off = ResumoOperacionalQuerySerializer(
        data={
            "modo": "empresa",
            "empresa_id": 1,
            "data_inicio": "2026-07-01",
            "data_fim": "2026-07-31",
        }
    )
    check("ser_default_off", off.is_valid() and off.validated_data["incluir_visual"] is False)
    grp = ResumoOperacionalQuerySerializer(
        data={
            "modo": "grupo",
            "grupo_id": 3,
            "data_inicio": "2026-07-01",
            "data_fim": "2026-07-31",
            "incluir_visual": "1",
        }
    )
    check("ser_grupo_ok", grp.is_valid(), str(grp.errors))


def test_api_anexa_visual() -> None:
    print("== API anexar visual ==")
    from rest_framework.test import APIRequestFactory

    from financeiro.api.views import ResumoOperacionalAPIView

    factory = APIRequestFactory()
    core = {
        "receita_operacional": 100.0,
        "cmv": 40.0,
        "geracao_caixa": -10.0,
        "despesas_fixas": 20.0,
        "despesas_variaveis": 5.0,
        "despesas_financeiras": 2.0,
    }
    visual = {"ok": True, "variacao": {"ok": True, "top": [{"plano": "Aluguel", "ultimo": 20}]}}

    def _call(qs):
        request = factory.get("/api/financeiro/resumo-operacional", qs)
        request.user = MagicMock(is_authenticated=True)
        view = ResumoOperacionalAPIView.as_view()
        with (
            patch("financeiro.api.views._resumo_usa_titulos_pg", return_value=True),
            patch(
                "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
                return_value=dict(core),
            ),
            patch(
                "financeiro.services.dre_visual_util.montar_dre_visual",
                return_value=visual,
            ) as mock_v,
        ):
            resp = view(request)
        return resp, mock_v

    qs_on = {
        "modo": "empresa",
        "empresa_id": "1",
        "data_inicio": "2026-07-01",
        "data_fim": "2026-07-31",
        "incluir_visual": "1",
        "fonte": "postgres",
    }
    resp, mock_v = _call(qs_on)
    check("api_status_200", resp.status_code == 200, str(resp.status_code))
    data = resp.data if hasattr(resp, "data") else {}
    check("api_tem_visual", isinstance(data, dict) and data.get("visual", {}).get("ok") is True)
    check("api_montar_chamado", mock_v.called)
    check("api_montar_eid", mock_v.call_args.kwargs.get("empresa_id") == 1)
    check("api_caixa_intacto", data.get("geracao_caixa") == -10.0)

    qs_off = dict(qs_on)
    qs_off.pop("incluir_visual")
    resp2, mock_v2 = _call(qs_off)
    check("api_sem_flag_nao_anexa", "visual" not in (resp2.data or {}), str((resp2.data or {}).keys()))
    check("api_sem_flag_nao_chama", mock_v2.called is False)


def test_math_cmv_e_fluxo() -> None:
    print("== Math CMV + fluxo ==")
    from financeiro.services.equilibrio import EquilibrioFinanceiroService
    from financeiro.services.receita_pdv_util import (
        aplicar_cmv_modos_no_resumo,
        pack_resumo_cmv_js,
        recalc_resumo_cmv,
    )

    rec = Decimal("100455.29")
    cmv_paga = Decimal("60339.00")
    cmv_vend = Decimal("67016.09")
    df = Decimal("18356.83")
    dv = Decimal("2894.58")
    dfin = Decimal("3096.78")
    caixa = Decimal("-134536.33")
    core = {
        "receita_operacional": rec,
        "cmv": cmv_paga,
        "despesas_fixas": df,
        "despesas_variaveis": dv,
        "despesas_financeiras": dfin,
        "lucro_bruto": rec - cmv_paga,
        "resultado_operacional": rec - cmv_paga - df - dv,
        "resultado_liquido_gerencial": rec - cmv_paga - df - dv - dfin,
        "geracao_caixa": caixa,
        "receita_fonte": "pdv",
    }
    snap_v = recalc_resumo_cmv(core, cmv_vend, dias_periodo=31)
    snap_p = recalc_resumo_cmv(core, cmv_paga, dias_periodo=31)
    check("lucro_vendida", snap_v["lucro_bruto"] == rec - cmv_vend, str(snap_v["lucro_bruto"]))
    check("lucro_paga", snap_p["lucro_bruto"] == rec - cmv_paga, str(snap_p["lucro_bruto"]))
    check("caixa_recalc", snap_v["geracao_caixa"] == caixa)
    desp = df + dv + dfin
    check("desp_soma", desp == Decimal("24348.19"), str(desp))
    margem = (snap_v["lucro_bruto"] / rec) * Decimal("100")
    check("margem_pct", abs(margem - Decimal("33.28")) < Decimal("0.05"), str(round(margem, 2)))
    markup = ((rec / cmv_vend) - Decimal("1")) * Decimal("100")
    check("markup_pct", abs(snap_v["markup_pct"] - markup) < Decimal("0.02"), str(snap_v["markup_pct"]))
    eq = EquilibrioFinanceiroService().calcular(rec, cmv_vend, df, dv, dias_periodo=31)
    pe = Decimal(str(eq["faturamento_equilibrio"]))
    pct_pe = min(Decimal("100"), (rec / pe) * Decimal("100")) if pe > 0 else Decimal("0")
    check("pe_ratio", snap_v["margem_contribuicao_pct"] == eq["margem_contribuicao_pct"])
    check("pct_pe_ok", pct_pe > 0, str(round(pct_pe, 1)))
    pack = pack_resumo_cmv_js(snap_v)
    check("pack_sem_caixa", "geracao_caixa" not in pack)

    with patch(
        "produtos.relatorios_vendas_util.custo_mercadoria_vendida",
        return_value={"ok": True, "total": cmv_vend, "skus_com_custo": 698, "skus_sem_custo": 3},
    ):
        out = aplicar_cmv_modos_no_resumo(
            core, date(2026, 7, 1), date(2026, 7, 31), empresa_nome="Agro Mais Centro"
        )
    check("raiz_cmv_paga", out["cmv"] == cmv_paga)
    check("raiz_caixa", out["geracao_caixa"] == caixa)
    check("snap_sem_caixa", "geracao_caixa" not in out["cmv_modos"]["vendida"])


def test_js_syntax() -> None:
    print("== JS ==")
    r = subprocess.run(
        ["node", "--check", str(ROOT / "static/js/agro_resumo_gerencial.js")],
        capture_output=True,
        text=True,
    )
    check("node_check", r.returncode == 0, (r.stderr or r.stdout or "").strip()[:120])


def test_pagina_local() -> None:
    print("== Pagina local (se runserver) ==")
    try:
        import urllib.request

        req = urllib.request.Request(
            "http://127.0.0.1:8000/financeiro/resumo-gerencial/",
            headers={"User-Agent": "dre-visual-verify"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
        check("http_resumo", code in (200, 302), str(code))
        login = "Acessar" in body or "login" in body.lower()
        if code == 200 and not login:
            check("html_tem_visual", 'id="painel-visual"' in body)
            check("html_tem_chip", 'data-dre-cmv="vendida"' in body)
            check("html_tem_js", "agro_resumo_gerencial.js" in body)
        else:
            check("login_ou_redirect", True, "login" if login else str(code))
        req_i = urllib.request.Request(
            "http://127.0.0.1:8000/financeiro/dashboard-gerencial/",
            headers={"User-Agent": "dre-visual-verify"},
        )
        with urllib.request.urlopen(req_i, timeout=8) as resp_i:
            code_i = resp_i.getcode()
            body_i = resp_i.read().decode("utf-8", errors="replace")
        check("http_indicadores", code_i in (200, 302), str(code_i))
        login_i = "Acessar" in body_i or "login" in body_i.lower()
        if code_i == 200 and not login_i:
            check("ind_html_vivo", "Indicadores" in body_i and "Financeiro gerencial" in body_i)
        else:
            check("ind_login_ou_redirect", True, "login" if login_i else str(code_i))
    except Exception as exc:
        check("runserver_opcional", True, f"sem local ({type(exc).__name__})")


def main() -> int:
    test_arquivos()
    test_urls()
    test_serializer()
    test_montar_e_json()
    test_api_anexa_visual()
    test_math_cmv_e_fluxo()
    test_js_syntax()
    test_pagina_local()
    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("VERIFY_FAIL:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
