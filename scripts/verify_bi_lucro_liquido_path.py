"""
Prova detalhada BI-LUCRO-LIQUIDO.

Path:
  /  (home)
    -> dashboard_gerencial_view / dashboard_gerencial_conteudo
    -> _dashboard_capri_context
    -> ThreadPool: _dashboard_lucro_liquido_vencimento
    -> lucro_liquido_vencimento_bruto_pago
         por=vencimento · valor=bruto + realizado
         consolidar_empresa_pg (PDV) + CMV vendida + receita nao op
    -> KPI variant lucro_liquido_duplo (Bruto / Pago)
  Vila sem empresa propria -> Agro Mais Centro (senao zerava).
  Bruto do card = Liquido do Resumo/Indicadores (DATA=Vencimento · VALOR=Bruto · CMV vendida).

  python scripts/verify_bi_lucro_liquido_path.py
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
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _q(n) -> Decimal:
    return Decimal(str(n or 0)).quantize(Decimal("0.01"))


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views.py")
    body = _read("produtos/templates/produtos/partials/dashboard_gerencial_body.html")
    top = _read("produtos/templates/produtos/dashboard_gerencial.html")
    ind = _read("financeiro/services/indicadores_gerencial_pg.py")
    resumo = _read("financeiro/services/resumo_operacional_pg.py")
    rec = _read("financeiro/services/receita_pdv_util.py")
    rel = _read("produtos/relatorios_vendas_util.py")

    check("url_home", 'path("", views.dashboard_gerencial_view, name="home")' in urls)
    check("url_conteudo", "dashboard_gerencial_conteudo" in urls)
    check("url_resumo", "resumo_financeiro_gerencial" in urls)

    check("view_home", "def dashboard_gerencial_view" in views)
    check("view_conteudo", "def dashboard_gerencial_conteudo" in views)
    check("ctx_capri", "_dashboard_capri_context(request)" in views)
    check("worker_fn", "def _dashboard_lucro_liquido_vencimento" in views)
    check("fut_lucro", 'fut["lucro_liq"]' in views)
    check("worker_pool", "_dashboard_worker" in views.split('fut["lucro_liq"]')[1][:400])
    check("kpi_label", '"label": "Lucro Líquido"' in views)
    check("kpi_variant", '"variant": "lucro_liquido_duplo"' in views)
    check("kpi_bruto_fmt", '"value_bruto"' in views and '"value_pago"' in views)
    check("sem_novos_kpi", '"label": "Novos Clientes"' not in views)
    check("sem_novos_worker", "def _dashboard_novos_clientes_no_dia" not in views)

    check("fn_lucro", "def lucro_liquido_vencimento_bruto_pago" in ind)
    check("por_vencimento", 'por="vencimento"' in ind)
    check("valor_bruto", 'valor="bruto"' in ind or '", "bruto"' in ind or '"bruto"' in ind)
    check("valor_realizado", '"realizado"' in ind)
    check("consolida_pg", "consolidar_empresa_pg" in ind)
    check("filtro_resultado", 'filtro_contas="resultado"' in ind)
    check("pdv_on", "usar_receita_pdv=True" in ind)
    check("cmv_vendida", "custo_mercadoria_vendida" in ind and "def custo_mercadoria_vendida" in rel)
    check("recalc_cmv", "recalc_resumo_cmv" in ind and "def recalc_resumo_cmv" in rec)
    check("liq_mais_nao_op", "receita_nao_operacional" in ind)
    check("fallback_vila", "Vila sem cadastro próprio" in ind)
    check("fallback_lojas", "lojas or qualquer" in ind)
    check("resumo_pg", "def consolidar_empresa_pg" in resumo)

    check("label_html", "Lucro Líquido" in body)
    check("variant_html", 'kpi.variant == "lucro_liquido_duplo"' in body)
    check("lbl_bruto", ">Bruto<" in body)
    check("lbl_pago", ">Pago<" in body)
    check("link_resumo", "resumo_financeiro_gerencial" in body)
    check("sem_novos_html", "Novos Clientes" not in body)
    check("css_liq", "dash-kpi-liq-grid" in top)
    check("cor_inline", "kpi.color_bruto" in body and "kpi.color_pago" in body)


def test_url_reverse() -> None:
    print("== URL ==")
    from django.urls import reverse

    check("reverse_home", reverse("home") == "/")
    check(
        "reverse_conteudo",
        reverse("dashboard_gerencial_conteudo").rstrip("/").endswith("dashboard/gerencial/conteudo")
        or "conteudo" in reverse("dashboard_gerencial_conteudo"),
    )
    check(
        "reverse_resumo",
        reverse("resumo_financeiro_gerencial").rstrip("/").endswith("financeiro/resumo-gerencial"),
    )


def test_math_unitario() -> None:
    print("== Math unitario ==")
    rec, cmv_v, df, dv, dfin = 1000, 300, 100, 50, 20
    bruto = rec - cmv_v - df - dv - dfin
    rec2, df2, dv2, dfin2 = 1000, 80, 40, 10
    pago = rec2 - cmv_v - df2 - dv2 - dfin2
    check("math_bruto_530", bruto == 530, str(bruto))
    check("math_pago_570", pago == 570, str(pago))

    from financeiro.services.receita_pdv_util import recalc_resumo_cmv, _dec

    core = {
        "receita_operacional": Decimal("1000"),
        "receita_nao_operacional": Decimal("15"),
        "cmv": Decimal("400"),
        "despesas_fixas": Decimal("100"),
        "despesas_variaveis": Decimal("50"),
        "despesas_financeiras": Decimal("20"),
        "resultado_operacional": Decimal("450"),
        "resultado_liquido_gerencial": Decimal("430"),
    }
    out = recalc_resumo_cmv(core, Decimal("300"), dias_periodo=9)
    liq = _dec(out["resultado_liquido_gerencial"]) + _dec(out["receita_nao_operacional"])
    check("recalc_mais_nao_op", liq == Decimal("545"), str(liq))

    from produtos.views import _dashboard_lucro_liquido_cor, _format_moeda_br

    check("fmt_neg", _format_moeda_br(Decimal("-2480.17")) == "-2.480,17")
    check("fmt_pos", _format_moeda_br(Decimal("1.29")) == "1,29")
    check("cor_neg", _dashboard_lucro_liquido_cor(Decimal("-1")) == "#be123c")
    check("cor_pos", _dashboard_lucro_liquido_cor(Decimal("1")) == "#065f46")
    check("cor_zero", _dashboard_lucro_liquido_cor(Decimal("0")) == "#334155")


def test_fallback_e_wrapper() -> None:
    print("== Fallback Vila + wrapper ==")
    from datetime import date as d
    from financeiro.services.indicadores_gerencial_pg import (
        _empresas_ids_para_deposito_bi,
        lucro_liquido_vencimento_bruto_pago,
    )
    from produtos.views import _dashboard_lucro_liquido_vencimento

    class _E:
        def __init__(self, pk, nome):
            self.pk = pk
            self.nome_fantasia = nome

    with patch("base.models.Empresa.objects") as mock_emp:
        mock_emp.filter.return_value.only.return_value = [_E(1, "Agro Mais Centro")]
        eids_v = _empresas_ids_para_deposito_bi("vila")
        eids_c = _empresas_ids_para_deposito_bi("centro")
        eids_t = _empresas_ids_para_deposito_bi(None)
    check("eids_vila_fallback", eids_v == [1], str(eids_v))
    check("eids_centro", eids_c == [1], str(eids_c))
    check("eids_total", eids_t == [1], str(eids_t))

    with patch("base.models.Empresa.objects") as mock_emp:
        mock_emp.filter.return_value.only.return_value = []
        eids_vazio = _empresas_ids_para_deposito_bi("vila")
    check("eids_vazio", eids_vazio == [], str(eids_vazio))

    with patch(
        "financeiro.services.indicadores_gerencial_pg.lucro_liquido_vencimento_bruto_pago",
        side_effect=RuntimeError("boom"),
    ):
        wrapped = _dashboard_lucro_liquido_vencimento(d(2026, 8, 1), d(2026, 8, 9), "vila")
    check("wrapper_nao_quebra", wrapped.get("ok") is False)
    check("wrapper_zero", wrapped.get("bruto") == Decimal("0") and wrapped.get("pago") == Decimal("0"))


def test_template_kpi() -> None:
    print("== Template KPI ==")
    from django.template.loader import render_to_string

    html = render_to_string(
        "produtos/partials/dashboard_gerencial_body.html",
        {
            "kpis": [
                {
                    "label": "Lucro Líquido",
                    "variant": "lucro_liquido_duplo",
                    "trend": "Por vencimento",
                    "trend_short": "Venc.",
                    "trend_class": "text-slate-700 bg-slate-100",
                    "value_bruto": "R$ -2.480,17",
                    "value_pago": "R$ 1,29",
                    "color_bruto": "#be123c",
                    "color_pago": "#065f46",
                    "context_lines": ["Por vencimento."],
                }
            ],
            "periodo_label": "teste",
            "periodo_key": "mes_ate_hoje",
            "chart_labels": "[]",
            "chart_weekday_initials": "[]",
            "chart_data": "[]",
            "chart_compare_data": "[]",
            "chart_total_periodo": "0,00",
            "ticket_por_dia": "[]",
            "top_produtos": [],
            "ranking_vendedores": [],
            "top_clientes_mes_anterior": [],
            "vendas_por_loja": [],
            "stores_chart_json": "{}",
            "contas_receber": [],
            "contas_pagar": [],
            "dashboard_gastos_plano_ativo": False,
            "gastos_plano_cache_json": "{}",
            "relatorios_validade_url": "/relatorios/validade/",
            "entregas_painel_url": "/entregas/",
        },
    )
    check("tpl_titulo", "Lucro Líquido" in html)
    check("tpl_bruto_val", "-2.480,17" in html)
    check("tpl_pago_val", "1,29" in html)
    check("tpl_venc_badge", "Venc." in html)
    check("tpl_sem_novos", "Novos Clientes" not in html)
    check("tpl_link_resumo", "/financeiro/resumo-gerencial/" in html)


def test_live_pg_vs_indicadores() -> None:
    print("== Live PG vs Indicadores/Resumo ==")
    from base.models import Empresa
    from financeiro.services.indicadores_gerencial_pg import (
        get_indicadores_gerencial_pg,
        lucro_liquido_vencimento_bruto_pago,
    )
    from financeiro.services.receita_pdv_util import deposito_pdv_por_empresa_nome
    from produtos.views import _dashboard_lucro_liquido_vencimento, _format_moeda_br

    empresas = list(Empresa.objects.filter(ativo=True).only("id", "nome_fantasia"))
    check("tem_empresa", bool(empresas), str(len(empresas)))
    if not empresas:
        return
    nomes = [(e.pk, e.nome_fantasia, deposito_pdv_por_empresa_nome(e.nome_fantasia)) for e in empresas]
    check(
        "so_centro_hoje",
        any(m == "centro" for _, _, m in nomes) and not any(m == "vila" for _, _, m in nomes),
        str(nomes),
    )
    eid = next((e.pk for e, *_ in [(e, deposito_pdv_por_empresa_nome(e.nome_fantasia)) for e in empresas] if deposito_pdv_por_empresa_nome(e.nome_fantasia) == "centro"), empresas[0].pk)

    di, df = date(2026, 7, 12), date(2026, 8, 10)
    card = lucro_liquido_vencimento_bruto_pago(di, df, deposito="centro")
    card_v = lucro_liquido_vencimento_bruto_pago(di, df, deposito="vila")
    wrap = _dashboard_lucro_liquido_vencimento(di, df, "vila")
    check("card_ok", bool(card.get("ok")), str(card))
    check("card_por_venc", card.get("por") == "vencimento")
    check("card_cmv_vendida", card.get("cmv_modo") == "vendida", str(card.get("cmv_modo")))
    check("vila_igual_centro", _q(card_v.get("bruto")) == _q(card.get("bruto")) and _q(card_v.get("pago")) == _q(card.get("pago")))
    check("wrapper_igual", _q(wrap.get("bruto")) == _q(card_v.get("bruto")))

    ind_b = get_indicadores_gerencial_pg(eid, di, df, por="vencimento", valor="bruto")
    ind_p = get_indicadores_gerencial_pg(eid, di, df, por="vencimento", valor="realizado")
    liq_b = _q(ind_b["atual"]["resultado_liquido"])
    liq_p = _q(ind_p["atual"]["resultado_liquido"])
    check("vs_ind_bruto", _q(card["bruto"]) == liq_b, f"card={card['bruto']} ind={liq_b}")
    check("vs_ind_pago", _q(card["pago"]) == liq_p, f"card={card['pago']} ind={liq_p}")
    check("screenshot_bruto", _q(card["bruto"]) == Decimal("-2480.17"), str(card["bruto"]))
    check("fmt_card_bruto", f"R$ {_format_moeda_br(card['bruto'])}" == "R$ -2.480,17")

    di_ano, df_ano = date(2026, 1, 1), date(2026, 8, 9)
    ano = lucro_liquido_vencimento_bruto_pago(di_ano, df_ano, deposito="vila")
    ind_ano = get_indicadores_gerencial_pg(eid, di_ano, df_ano, por="vencimento", valor="bruto")
    check("ano_ok", bool(ano.get("ok")) and _q(ano["bruto"]) != Decimal("0"))
    check(
        "ano_vs_ind",
        _q(ano["bruto"]) == _q(ind_ano["atual"]["resultado_liquido"]),
        f"card={ano['bruto']} ind={ind_ano['atual']['resultado_liquido']}",
    )


def test_django_check_e_tests() -> None:
    print("== manage.py check + tests ==")
    py = sys.executable
    r1 = subprocess.run(
        [py, "manage.py", "check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    check("manage_check", r1.returncode == 0, (r1.stderr or r1.stdout)[-200:])
    r2 = subprocess.run(
        [py, "manage.py", "test", "financeiro.tests_bi_lucro_liquido", "--verbosity=1"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    out = (r2.stdout or "") + (r2.stderr or "")
    check("django_tests", r2.returncode == 0 and "OK" in out, out[-300:])


def main() -> int:
    test_arquivos()
    test_url_reverse()
    test_math_unitario()
    test_fallback_e_wrapper()
    test_template_kpi()
    test_live_pg_vs_indicadores()
    test_django_check_e_tests()
    print()
    print(f"{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("FAIL:", ", ".join(fails))
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
