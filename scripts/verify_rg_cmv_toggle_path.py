"""
Prova detalhada RG-CMV-TOGGLE.
Path: /financeiro/resumo-gerencial/ -> resumo_financeiro_gerencial_view
      -> agro_resumo_gerencial.js -> GET /api/financeiro/resumo-operacional
      -> consolidar_empresa_pg(anexar_cmv_modos=True)
      -> aplicar_receita_pdv + aplicar_cmv_modos_no_resumo
      -> custo_mercadoria_vendida (mesmo deposito PDV)
      -> chips JS (agro_dre_cmv_modo_v1) + PE do snap
      Caixa nao muda. Indicadores nao anexa CMV no consolidar (flag default False).

  python scripts/verify_rg_cmv_toggle_path.py
"""
from __future__ import annotations

import os
import subprocess
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
    cfg = _read("config/urls.py")
    api_urls = _read("financeiro/api/urls.py")
    api = _read("financeiro/api/views.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    css = _read("static/css/agro_resumo_gerencial.css")
    util = _read("financeiro/services/receita_pdv_util.py")
    resumo = _read("financeiro/services/resumo_operacional_pg.py")
    ind = _read("financeiro/services/indicadores_gerencial_pg.py")
    ind_html = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    vendas = _read("produtos/relatorios_vendas_util.py")

    check("url_pagina", "financeiro/resumo-gerencial/" in urls and "resumo_financeiro_gerencial" in urls)
    check("view_resumo", "def resumo_financeiro_gerencial_view" in views)
    check("view_template", "resumo_financeiro_gerencial.html" in views)
    check("api_include", 'path("api/financeiro/"' in cfg or "api/financeiro/" in cfg)
    check("api_resumo_rota", "resumo-operacional" in api_urls)
    check("api_pe_rota", "gap-equilibrio" in api_urls)

    check("fn_aplicar", "def aplicar_cmv_modos_no_resumo" in util)
    check("fn_recalc", "def recalc_resumo_cmv" in util)
    check("fn_pack", "def pack_resumo_cmv_js" in util)
    check("fn_fundir", "def fundir_cmv_modos_grupo" in util)
    check("fn_cmv_vendida", "def custo_mercadoria_vendida" in vendas)
    check("flag_default_false", "anexar_cmv_modos: bool = False" in resumo)
    check("empresa_chama_aplicar", "aplicar_cmv_modos_no_resumo" in resumo)
    check("grupo_chama_fundir", "fundir_cmv_modos_grupo" in resumo)

    n_api = api.count("anexar_cmv_modos=True")
    check("api_anexa_4", n_api >= 4, str(n_api))
    check("ind_nao_anexa", "anexar_cmv_modos=True" not in ind)
    check("ind_caixa_sem_pdv", "usar_receita_pdv=False" in ind)

    aplicar = util.split("def aplicar_cmv_modos_no_resumo")[1].split("def fundir_cmv_modos_grupo")[0]
    check("raiz_fica_paga", 'out = dict(core)' in aplicar and 'out["cmv_paga"]' in aplicar)
    check("aplicar_nao_grava_caixa", 'out["geracao_caixa"]' not in aplicar)
    pack_fields = util.split("_RESUMO_CMV_JS_FIELDS")[1].split("def recalc_resumo_cmv")[0]
    check("pack_sem_caixa", '"geracao_caixa"' not in pack_fields)

    check("html_js", "agro_resumo_gerencial.js" in html)
    check("html_css", "agro_resumo_gerencial.css" in html)
    check("chip_vendida", 'data-dre-cmv="vendida"' in html)
    check("chip_paga", 'data-dre-cmv="paga"' in html)
    check("hint_id", 'id="rg-cmv-hint"' in html)
    check("ajuda_cmv", "CMV vendida" in html and "CMV paga" in html)
    check("ajuda_caixa", "Saldo final" in html and "empréstimos" in html)
    check("css_chip", ".rg-chip" in css and ".rg-chip.is-active" in css)

    check("js_key", "agro_dre_cmv_modo_v1" in js)
    check("ind_mesma_key", "agro_dre_cmv_modo_v1" in ind_html)
    check("js_apply", "function aplicarCmvNoCore" in js)
    check("js_core_payload", "function coreDoPayload" in js)
    check("js_fallback_paga", 'ok_vendida === false' in js)
    check("js_default_vendida", 'return "vendida"' in js)
    check("js_fetch_resumo", "/api/financeiro/resumo-operacional" in js)
    check("js_pintar_pe", "function pintarEquilibrio" in js)
    check("js_chip_click", 'querySelectorAll("[data-dre-cmv]")' in js)
    check("js_kpi_caixa", 'title: "Geração de caixa"' in js or "Geração de caixa" in js)
    check("js_markup_card", "pctJa(c.markup_pct)" in js)
    check("js_grupo_fallback", "data.consolidado || data" in js)

    dep = util.split("def deposito_pdv_por_empresa_nome")[1].split("def deposito_pdv_por_empresa_id")[0]
    check("map_centro", '"centro"' in dep)
    check("map_vila", '"vila"' in dep)


def test_urls() -> None:
    print("== URL ==")
    from django.urls import reverse

    p = reverse("resumo_financeiro_gerencial")
    check("reverse_resumo", p.rstrip("/").endswith("financeiro/resumo-gerencial"), p)
    a = reverse("financeiro-resumo-operacional")
    check("reverse_api", "resumo-operacional" in a, a)
    g = reverse("financeiro-gap-equilibrio")
    check("reverse_pe", "gap-equilibrio" in g, g)


def test_math_e_modos() -> None:
    print("== Math + modos ==")
    from financeiro.api.jsonutil import json_safe
    from financeiro.services.equilibrio import EquilibrioFinanceiroService
    from financeiro.services.receita_pdv_util import (
        aplicar_cmv_modos_no_resumo,
        deposito_pdv_por_empresa_nome,
        fundir_cmv_modos_grupo,
        pack_resumo_cmv_js,
        recalc_resumo_cmv,
    )

    check("dep_centro", deposito_pdv_por_empresa_nome("Agro Mais Centro") == "centro")
    check("dep_vila", deposito_pdv_por_empresa_nome("Agro Mais Vila Elias") == "vila")
    check("dep_grupo", deposito_pdv_por_empresa_nome("Grupo GM") is None)

    rec = Decimal("100455.29")
    cmv_paga = Decimal("60339.00")
    cmv_vend = Decimal("67016.09")
    df = Decimal("18356.83")
    dv = Decimal("2894.58")
    dfin = Decimal("3096.78")
    core = {
        "receita_operacional": rec,
        "cmv": cmv_paga,
        "despesas_fixas": df,
        "despesas_variaveis": dv,
        "despesas_financeiras": dfin,
        "lucro_bruto": rec - cmv_paga,
        "resultado_operacional": rec - cmv_paga - df - dv,
        "resultado_liquido_gerencial": rec - cmv_paga - df - dv - dfin,
        "geracao_caixa": Decimal("-134536.33"),
        "receita_fonte": "pdv",
    }
    caixa = core["geracao_caixa"]
    snap_v = recalc_resumo_cmv(core, cmv_vend, dias_periodo=31)
    snap_p = recalc_resumo_cmv(core, cmv_paga, dias_periodo=31)
    check("lucro_vendida", snap_v["lucro_bruto"] == rec - cmv_vend, str(snap_v["lucro_bruto"]))
    check("lucro_paga", snap_p["lucro_bruto"] == rec - cmv_paga, str(snap_p["lucro_bruto"]))
    check("caixa_recalc", snap_v["geracao_caixa"] == caixa)
    markup = ((rec / cmv_vend) - Decimal("1")) * Decimal("100")
    check("markup_vendida", abs(snap_v["markup_pct"] - markup) < Decimal("0.02"), str(snap_v["markup_pct"]))
    eq = EquilibrioFinanceiroService().calcular(rec, cmv_vend, df, dv, dias_periodo=31)
    check("pe_ratio", snap_v["margem_contribuicao_pct"] == eq["margem_contribuicao_pct"])
    pack = pack_resumo_cmv_js(snap_v)
    check("pack_cmv", pack["cmv"] == float(cmv_vend))
    check("pack_sem_caixa", "geracao_caixa" not in pack)

    with patch(
        "produtos.relatorios_vendas_util.custo_mercadoria_vendida",
        return_value={
            "ok": True,
            "total": cmv_vend,
            "skus_com_custo": 698,
            "skus_sem_custo": 3,
        },
    ) as mock_cmv:
        out = aplicar_cmv_modos_no_resumo(
            core, date(2026, 7, 1), date(2026, 7, 31), empresa_nome="Agro Mais Centro"
        )
    check("raiz_cmv_paga", out["cmv"] == cmv_paga)
    check("raiz_lucro_paga", out["lucro_bruto"] == rec - cmv_paga)
    check("raiz_caixa", out["geracao_caixa"] == caixa)
    check("modo_sugerido", out["cmv_modo"] == "vendida")
    check("ok_vendida", out["cmv_modos"]["ok_vendida"] is True)
    check("json_v_cmv", out["cmv_modos"]["vendida"]["cmv"] == float(cmv_vend))
    check("json_p_cmv", out["cmv_modos"]["paga"]["cmv"] == float(cmv_paga))
    check("json_v_lucro", abs(out["cmv_modos"]["vendida"]["lucro_bruto"] - float(rec - cmv_vend)) < 0.02)
    check("skus_sem", out["cmv_skus_sem_custo"] == 3)
    check("dep_chamado", mock_cmv.call_args.kwargs.get("deposito") == "centro")
    check("snap_sem_caixa", "geracao_caixa" not in out["cmv_modos"]["vendida"])

    safe = json_safe(out["cmv_modos"])
    check("json_safe_ok", safe["ok_vendida"] is True and isinstance(safe["vendida"]["cmv"], float))

    with patch(
        "produtos.relatorios_vendas_util.custo_mercadoria_vendida",
        side_effect=RuntimeError("mongo down"),
    ):
        fb = aplicar_cmv_modos_no_resumo(
            core, date(2026, 7, 1), date(2026, 7, 31), empresa_nome="Agro Mais Centro"
        )
    check("fallback_modo_paga", fb["cmv_modo"] == "paga")
    check("fallback_ok_false", fb["cmv_modos"]["ok_vendida"] is False)
    check("fallback_caixa", fb["geracao_caixa"] == caixa)
    check("fallback_raiz", fb["cmv"] == cmv_paga)

    erro = {"erro": "Empresa não encontrada", "cmv": Decimal("1")}
    check(
        "erro_intacto",
        aplicar_cmv_modos_no_resumo(erro, date(2026, 7, 1), date(2026, 7, 31))["erro"]
        == "Empresa não encontrada",
    )

    consolidado = {
        "receita_operacional": Decimal("39000"),
        "cmv": Decimal("2900"),
        "despesas_fixas": Decimal("4300"),
        "despesas_variaveis": Decimal("680"),
        "despesas_financeiras": Decimal("1050"),
        "geracao_caixa": Decimal("-80"),
    }
    fused = fundir_cmv_modos_grupo(
        consolidado,
        [
            {
                "cmv_paga": Decimal("2000"),
                "cmv_vendida": Decimal("1500"),
                "cmv_modos": {"ok_vendida": True},
                "cmv_skus_sem_custo": 2,
                "cmv_skus_com_custo": 10,
            },
            {
                "cmv_paga": Decimal("900"),
                "cmv_vendida": Decimal("700"),
                "cmv_modos": {"ok_vendida": True},
                "cmv_skus_sem_custo": 1,
                "cmv_skus_com_custo": 5,
            },
        ],
        dias_periodo=31,
    )
    check("grupo_raiz_paga", fused["cmv"] == Decimal("2900"))
    check("grupo_vendida", fused["cmv_vendida"] == Decimal("2200"))
    check("grupo_caixa", fused["geracao_caixa"] == Decimal("-80"))
    check("grupo_ok", fused["cmv_modos"]["ok_vendida"] is True)


def test_js_syntax() -> None:
    print("== JS ==")
    js = ROOT / "static" / "js" / "agro_resumo_gerencial.js"
    r = subprocess.run(["node", "--check", str(js)], capture_output=True, text=True)
    check("node_check", r.returncode == 0, (r.stderr or r.stdout or "").strip()[:120])


def test_pagina_local() -> None:
    print("== Pagina local (se runserver) ==")
    try:
        import urllib.request

        req = urllib.request.Request(
            "http://127.0.0.1:8000/financeiro/resumo-gerencial/",
            headers={"User-Agent": "rg-cmv-verify"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            code = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
        check("http_resumo", code in (200, 302), str(code))
        login = "Acessar" in body or "login" in body.lower()
        if code == 200 and not login:
            check("html_tem_chip", 'data-dre-cmv="vendida"' in body)
            check("html_tem_js", "agro_resumo_gerencial.js" in body)
        else:
            check("login_ou_redirect", True, "login" if login else str(code))
    except Exception as exc:
        check("runserver_opcional", True, f"sem local ({type(exc).__name__})")


def main() -> int:
    test_arquivos()
    test_urls()
    test_math_e_modos()
    test_js_syntax()
    test_pagina_local()
    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
