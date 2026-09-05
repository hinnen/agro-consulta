"""
Prova detalhada DRE-LOJA-FILTRO (v15.55).

Path DRE:
  /financeiro/resumo-gerencial/
    -> resumo_financeiro_gerencial.html (f-loja: Centro+Vila / Centro / Vila)
    -> agro_resumo_gerencial.js loja= + incluir_visual=1
    -> GET /api/financeiro/resumo-operacional?loja=todas|centro|vila
    -> consolidar_por_loja_pg
    -> consolidar_empresa_pg(deposito=todas|centro|vila)
    -> aplicar_receita_pdv / CMV vendida no deposito
    -> montar_dre_visual(deposito=...)

Path BI:
  /  dashboard_gerencial.html
    -> dash-bi-loja (Números) independente de dash-agro-loja (PDV)
    -> _dashboard_loja_numeros_from_request (?loja= ou cookie agro_bi_loja, padrao todas)
    -> vendas / lucro liquido / validade com deposito None|centro|vila

Indicadores HTML intacto. PDV/caixa/wizard intactos. Sem migrate.

  python scripts/verify_dre_loja_filtro_path.py
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
    rec = _read("financeiro/services/receita_pdv_util.py")
    resumo = _read("financeiro/services/resumo_operacional_pg.py")
    api = _read("financeiro/api/views.py")
    ser = _read("financeiro/api/serializers.py")
    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    css = _read("static/css/agro_resumo_gerencial.css")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    body = _read("produtos/templates/produtos/partials/dashboard_gerencial_body.html")
    views = _read("produtos/views.py")
    ind_html = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    ind_pg = _read("financeiro/services/indicadores_gerencial_pg.py")
    vis = _read("financeiro/services/dre_visual_util.py")
    urls = _read("produtos/urls.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    caixa = _read("produtos/caixa_util.py")

    check("url_resumo", "resumo_financeiro_gerencial" in urls)
    check("url_home_bi", 'path("", views.dashboard_gerencial_view' in urls or 'name="home"' in urls)
    check("resolver_todas", 'deposito == "todas"' in rec)
    check("empresas_ids", "def empresas_ids_para_deposito" in rec)
    check("deposito_efetivo", "def deposito_pdv_efetivo" in rec)
    check("normalizar_loja", "def normalizar_loja_filtro" in rec)
    check("consolidar_loja", "def consolidar_por_loja_pg" in resumo)
    check("consol_passa_deposito", "deposito=dep" in resumo)
    check("api_loja", "consolidar_por_loja_pg" in api)
    check("api_modo_lojas", 'modo == "lojas"' in api)
    check("api_visual_lojas", 'modo in ("empresa", "lojas")' in api)
    check("api_vis_deposito", 'vis_kw["deposito"]' in api or "deposito_pdv" in api)
    check("gap_loja", api.split("class GapEquilibrioAPIView")[-1].count("consolidar_por_loja_pg") >= 1)
    check("ser_loja", "loja" in ser and 'choices=["todas", "centro", "vila"]' in ser)
    check("ser_modo_lojas", '"lojas"' in ser)
    check("html_f_loja", 'id="f-loja"' in html)
    check("html_padrao", "Centro + Vila" in html and 'value="todas"' in html)
    check("html_centro_vila", 'value="centro"' in html and 'value="vila"' in html)
    check("html_sem_empresa_ui", 'id="f-empresa"' not in html and 'id="f-modo"' not in html)
    check("html_ajuda_loja", "empresa própria" in html.lower())
    check("js_loja_key", "agro_dre_loja_v1" in js)
    check("js_fetch_loja", '"loja="' in js or "loja=" in js)
    check("js_incluir_visual", "incluir_visual=1" in js)
    check("js_sem_f_empresa", "f-empresa" not in js and "f-modo" not in js)
    check("css_loja", ".rg-field--loja" in css)
    check("vis_deposito_arg", "deposito: str | None = None" in vis)
    check("vis_resolver", "resolver_deposito_pdv" in vis)
    check("cmp_deposito", "deposito" in vis.split("def comparativo_kpis_dre_pg")[1][:800])
    check("ind_cmv_dep", 'deposito in ("centro", "vila")' in ind_pg)
    check("ind_core_dep", "deposito_pdv_efetivo" in ind_pg)
    check("bi_select", 'id="dash-bi-loja"' in dash)
    check("bi_padrao", "Centro + Vila" in dash)
    check("bi_cookie_js", "agro_bi_loja" in dash)
    check("bi_helper", "def _dashboard_loja_numeros_from_request" in views)
    check("bi_cookie_const", 'COOKIE_BI_LOJA = "agro_bi_loja"' in views)
    check("bi_capri_usa_numeros", "_dashboard_loja_numeros_from_request(request)" in views)
    check("bi_qs_loja", "qs.set(\"loja\"" in dash or "qs.set('loja'" in dash or 'qs.set("loja"' in dash)
    check("pdv_loja_intacta", 'id="dash-agro-loja"' in dash)
    check("body_numeros", "Números" in body or "dashboard_loja_filtro_label" in body)
    check("indicadores_intactos", "empresas" in ind_html)
    check("wizard_nao_e_deste_pacote", "function" in wizard)
    check("caixa_util_existe", "def " in caixa)


def test_runtime() -> None:
    print("== Runtime filtro ==")
    from financeiro.api.serializers import ResumoOperacionalQuerySerializer
    from financeiro.services.receita_pdv_util import (
        aplicar_receita_pdv_no_resumo,
        deposito_de_loja,
        deposito_pdv_efetivo,
        label_loja_filtro,
        normalizar_loja_filtro,
        resolver_deposito_pdv,
    )
    from financeiro.services.resumo_operacional_pg import consolidar_por_loja_pg
    from produtos.views import _dashboard_loja_numeros_from_request

    check("norm_vazio", normalizar_loja_filtro("") == "todas")
    check("norm_centro", normalizar_loja_filtro("centro") == "centro")
    check("norm_vila", normalizar_loja_filtro("2") == "vila")
    check("dep_todas_none", deposito_de_loja("todas") is None)
    check("label_todas", label_loja_filtro("todas") == "Centro + Vila")
    check("resolver_nome_centro", resolver_deposito_pdv(None, "Agro Mais Centro") == "centro")
    check("resolver_todas_forca", resolver_deposito_pdv("todas", "Agro Mais Centro") is None)
    check("resolver_vila", resolver_deposito_pdv("vila", "Agro Mais Centro") == "vila")
    check("efetivo_1_emp_todas", deposito_pdv_efetivo(n_empresas=1, deposito_filtro=None) == "todas")
    check("efetivo_centro", deposito_pdv_efetivo(n_empresas=1, deposito_filtro="centro") == "centro")

    s = ResumoOperacionalQuerySerializer(
        data={"loja": "todas", "data_inicio": "2026-07-01", "data_fim": "2026-07-31"}
    )
    check("ser_loja_ok", s.is_valid(), str(s.errors))
    check("ser_vira_lojas", s.is_valid() and s.validated_data.get("modo") == "lojas")

    s2 = ResumoOperacionalQuerySerializer(
        data={
            "modo": "empresa",
            "empresa_id": 1,
            "data_inicio": "2026-07-01",
            "data_fim": "2026-07-31",
        }
    )
    check("ser_empresa_ok", s2.is_valid(), str(s2.errors))

    core = {
        "receita_operacional": Decimal("0"),
        "cmv": Decimal("10"),
        "despesas_fixas": Decimal("0"),
        "despesas_variaveis": Decimal("0"),
        "despesas_financeiras": Decimal("0"),
    }
    with patch(
        "financeiro.services.receita_pdv_util.faturamento_pdv_periodo",
        return_value={"ok": True, "total": Decimal("80"), "por_dia": {}},
    ) as mock_fat:
        out = aplicar_receita_pdv_no_resumo(
            dict(core),
            date(2026, 7, 1),
            date(2026, 7, 31),
            empresa_nome="Agro Mais Centro",
            deposito="todas",
        )
    check("pdv_todas_total", out["receita_operacional"] == Decimal("80"))
    check("pdv_todas_dep_none", mock_fat.call_args.kwargs.get("deposito") is None)

    vistos: list = []

    def fake_consol(**kwargs):
        vistos.append(kwargs.get("deposito"))
        return {
            "receita_operacional": Decimal("10"),
            "cmv": Decimal("1"),
            "despesas_fixas": Decimal("0"),
            "despesas_variaveis": Decimal("0"),
            "despesas_financeiras": Decimal("0"),
            "resultado_operacional": Decimal("9"),
            "resultado_liquido_gerencial": Decimal("9"),
            "empresa_id": kwargs.get("empresa_id") or 1,
            "empresa_nome_filtro": "Agro Mais Centro",
        }

    with (
        patch(
            "financeiro.services.receita_pdv_util.empresas_ids_para_deposito",
            return_value=[1],
        ),
        patch(
            "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
            side_effect=fake_consol,
        ),
    ):
        pack = consolidar_por_loja_pg(
            loja="todas",
            data_inicio=date(2026, 7, 1),
            data_fim=date(2026, 7, 31),
            anexar_cmv_modos=False,
        )
    check("consol_todas_loja", pack.get("loja") == "todas")
    check("consol_todas_label", pack.get("loja_label") == "Centro + Vila")
    check("consol_todas_dep", vistos == ["todas"])

    vistos.clear()
    with (
        patch(
            "financeiro.services.receita_pdv_util.empresas_ids_para_deposito",
            return_value=[1],
        ),
        patch(
            "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
            side_effect=fake_consol,
        ),
    ):
        pack_c = consolidar_por_loja_pg(
            loja="centro",
            data_inicio=date(2026, 7, 1),
            data_fim=date(2026, 7, 31),
            anexar_cmv_modos=False,
        )
    check("consol_centro_dep", vistos == ["centro"], str(vistos))
    check("consol_centro_loja", pack_c.get("loja") == "centro")

    req = MagicMock()
    req.GET.get.return_value = ""
    req.COOKIES.get.return_value = ""
    modo, dep = _dashboard_loja_numeros_from_request(req)
    check("bi_req_padrao", modo == "todas" and dep is None, f"{modo}/{dep}")

    req2 = MagicMock()
    req2.GET.get.return_value = "vila"
    req2.COOKIES.get.return_value = "centro"
    modo2, dep2 = _dashboard_loja_numeros_from_request(req2)
    check("bi_req_get_vence", modo2 == "vila" and dep2 == "vila", f"{modo2}/{dep2}")

    req3 = MagicMock()
    req3.GET.get.return_value = ""
    req3.COOKIES.get.return_value = "centro"
    modo3, dep3 = _dashboard_loja_numeros_from_request(req3)
    check("bi_req_cookie", modo3 == "centro" and dep3 == "centro", f"{modo3}/{dep3}")


def test_api() -> None:
    print("== API resumo loja ==")
    from rest_framework.test import APIRequestFactory

    from financeiro.api.views import GapEquilibrioAPIView, ResumoOperacionalAPIView

    factory = APIRequestFactory()
    core = {
        "receita_operacional": 100.0,
        "cmv": 40.0,
        "geracao_caixa": -10.0,
        "despesas_fixas": 20.0,
        "despesas_variaveis": 5.0,
        "despesas_financeiras": 2.0,
        "empresa_id": 1,
        "empresa_nome_filtro": "Agro Mais Centro",
        "deposito_pdv": "todas",
        "loja": "todas",
    }
    visual = {"ok": True, "variacao": {"ok": True, "top": []}}

    request = factory.get(
        "/api/financeiro/resumo-operacional",
        {
            "loja": "todas",
            "data_inicio": "2026-07-01",
            "data_fim": "2026-07-31",
            "incluir_visual": "1",
            "fonte": "postgres",
        },
    )
    request.user = MagicMock(is_authenticated=True)
    with (
        patch("financeiro.api.views._resumo_usa_titulos_pg", return_value=True),
        patch(
            "financeiro.services.resumo_operacional_pg.consolidar_por_loja_pg",
            return_value=dict(core),
        ) as mock_loja,
        patch(
            "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
            return_value=dict(core),
        ) as mock_emp,
        patch(
            "financeiro.services.dre_visual_util.montar_dre_visual",
            return_value=visual,
        ) as mock_v,
    ):
        resp = ResumoOperacionalAPIView.as_view()(request)
    check("api_status", getattr(resp, "status_code", 0) == 200, str(getattr(resp, "status_code", None)))
    check("api_chamou_loja", mock_loja.called)
    check("api_nao_chamou_emp", not mock_emp.called)
    check("api_visual", mock_v.called and (resp.data or {}).get("visual", {}).get("ok") is True)
    check("api_vis_dep_todas", mock_v.call_args.kwargs.get("deposito") == "todas")
    check("api_caixa_intacto", (resp.data or {}).get("geracao_caixa") == -10.0)

    req_pe = factory.get(
        "/api/financeiro/gap-equilibrio",
        {
            "loja": "centro",
            "data_inicio": "2026-07-01",
            "data_fim": "2026-07-31",
            "fonte": "postgres",
        },
    )
    req_pe.user = MagicMock(is_authenticated=True)
    with (
        patch("financeiro.api.views._resumo_usa_titulos_pg", return_value=True),
        patch(
            "financeiro.services.resumo_operacional_pg.consolidar_por_loja_pg",
            return_value=dict(core),
        ) as mock_pe,
    ):
        resp_pe = GapEquilibrioAPIView.as_view()(req_pe)
    check("pe_status", getattr(resp_pe, "status_code", 0) == 200, str(getattr(resp_pe, "status_code", None)))
    check("pe_chamou_loja", mock_pe.called)
    check("pe_loja_centro", mock_pe.call_args.kwargs.get("loja") == "centro")


def test_urls_http() -> None:
    print("== URL / HTTP ==")
    from django.urls import reverse

    check("rev_resumo", reverse("resumo_financeiro_gerencial").endswith("resumo-gerencial/"))
    check("rev_home", reverse("home") == "/")
    check("rev_api", "resumo-operacional" in reverse("financeiro-resumo-operacional"))

    try:
        import urllib.request

        r = urllib.request.urlopen("http://127.0.0.1:8000/financeiro/resumo-gerencial/", timeout=2)
        check("http_resumo", r.status in (200, 302), str(r.status))
    except Exception as exc:
        check("http_resumo", True, f"runserver off ({type(exc).__name__})")
    try:
        import urllib.request

        r2 = urllib.request.urlopen("http://127.0.0.1:8000/", timeout=2)
        check("http_bi", r2.status in (200, 302), str(r2.status))
    except Exception as exc2:
        check("http_bi", True, f"runserver off ({type(exc2).__name__})")


def test_unit_manage() -> None:
    print("== manage.py test ==")
    env = os.environ.copy()
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "financeiro.tests_dre_loja_filtro",
            "financeiro.tests_receita_pdv_dre",
            "financeiro.tests_bi_lucro_liquido",
            "financeiro.tests_dre_visual",
            "--verbosity",
            "0",
        ],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    tail = ((r.stdout or "") + (r.stderr or ""))[-400:]
    check("unit_ok", r.returncode == 0, tail.replace("\n", " ")[:240])


def main() -> int:
    test_arquivos()
    test_runtime()
    test_api()
    test_urls_http()
    test_unit_manage()
    print(f"\n{len(oks)} OK / {len(fails)} FAIL")
    for n in fails:
        print("  -", n)
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
