"""
Prova da tela /vendas/lojas/ (VENDAS-LOJAS-RESUMO).

Path:
  GET /vendas/lojas/?periodo=hoje|semana|mes|ano
    -> vendas_lojas_resumo
    -> vendas_lojas_periodo_bounds (padrão hoje)
    -> vendas_lojas_totais (VendaAgro, sem cache, sem Mongo)
    -> template: Centro + Vila + total

  python scripts/verify_vendas_lojas_resumo_path.py
"""
from __future__ import annotations

import ast
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

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


def _fn_src(path: str, fn: str) -> str:
    tree = ast.parse(_read(path))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == fn:
            return ast.get_source_segment(_read(path), node) or ""
    return ""


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views.py")
    tpl = _read("produtos/templates/produtos/vendas_lojas_resumo.html")
    hub = _read("produtos/templates/produtos/relatorios_hub.html")
    util = _read("produtos/vendas_lojas_util.py")
    view_fn = _fn_src("produtos/views.py", "vendas_lojas_resumo")

    check("url_rota", "path('vendas/lojas/', views.vendas_lojas_resumo" in urls)
    check("url_antes_lista", urls.find("vendas/lojas/") < urls.find("path('vendas/', views.vendas_lista"))
    check("view_def", "def vendas_lojas_resumo" in views)
    check("view_login", "@login_required" in views.split("def vendas_lojas_resumo")[0][-400:])
    check("view_get", "@require_GET" in views.split("def vendas_lojas_resumo")[0][-400:])
    check("view_nocache", "@never_cache" in views.split("def vendas_lojas_resumo")[0][-400:])
    check("view_usa_totais", "vendas_lojas_totais" in view_fn)
    check("view_sem_bi_loja", "_dashboard_vendas_por_loja" not in view_fn)
    check("view_sem_mongo_serie", "_dashboard_mongo_vendas_serie" not in view_fn)
    check("hub_link", "vendas_lojas_resumo" in hub)
    check("tpl_centro", "Centro" in tpl and "centro_fmt" in tpl)
    check("tpl_vila", "Vila Elias" in tpl and "vila_fmt" in tpl)
    check("tpl_total", "Total das duas lojas" in tpl and "total_fmt" in tpl)
    check("tpl_filtro_dia", 'value="hoje"' in tpl)
    check("tpl_filtro_semana", 'value="semana"' in tpl)
    check("tpl_filtro_mes", 'value="mes"' in tpl)
    check("tpl_filtro_ano", 'value="ano"' in tpl)
    check("tpl_voltar", "vl-voltar" in tpl and "relatorios_hub" in tpl)
    check("tpl_mobile_viewport", "viewport-fit=cover" in tpl)
    check("tpl_mobile_dvh", "100dvh" in tpl)
    check("tpl_mobile_safe", "safe-area-inset" in tpl)
    check("tpl_numeros_grandes", "clamp(2.15rem" in tpl and "clamp(2.45rem" in tpl)
    check("tpl_filtro_toque", "min-height: 3rem" in tpl)
    check("tpl_grade_4", "repeat(4, minmax(0, 1fr))" in tpl)
    check("tpl_sem_scale", "agro_display_scale" not in tpl and "_agro_consulta_ui" not in tpl)
    check("tpl_sem_fa", "font-awesome" not in tpl.lower())
    check("tpl_standalone", "<!DOCTYPE html>" in tpl and "extends" not in tpl)
    check("util_sem_cache", "cache." not in util)
    check("util_sem_mongo", "obter_conexao_mongo" not in util and "DtoVenda" not in util)
    check("util_exclui_devolvida", "devolvida_em__isnull=True" in util)
    check("util_vila_iexact", 'deposito__iexact="vila"' in util)
    check("util_centro_exclude_vila", "exclude(deposito__iexact=" in util)
    check("util_soma_total", "centro + vila" in util)


def test_periodo() -> None:
    print("== Período ==")
    from produtos.vendas_lojas_util import vendas_lojas_periodo_bounds

    sab = date(2026, 8, 15)
    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, None)
    check("padrao_hoje", key == "hoje" and ini == fim == sab, f"{ini} {fim} {key}")
    check("padrao_label", "15/08/2026" in label)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(sab, "")
    check("vazio_hoje", key == "hoje" and ini == sab)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(sab, "lixo")
    check("invalido_hoje", key == "hoje" and ini == sab)

    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, "hoje")
    check("hoje", key == "hoje" and ini == fim == sab)

    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, "semana")
    check("semana_seg_hoje", key == "semana" and ini == date(2026, 8, 10) and fim == sab, f"{ini}→{fim}")
    check("semana_sem_domingo_futuro", fim <= sab)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(date(2026, 8, 17), "semana")
    check("semana_segunda", ini == fim == date(2026, 8, 17))

    ini, fim, _l, key = vendas_lojas_periodo_bounds(sab, "mes")
    check("mes_ate_hoje", key == "mes" and ini == date(2026, 8, 1) and fim == sab)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(sab, "ano")
    check("ano_ate_hoje", key == "ano" and ini == date(2026, 1, 1) and fim == sab)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(date(2026, 1, 1), "mes")
    check("dia1_mes", ini == fim == date(2026, 1, 1))


def test_totais_mock() -> None:
    print("== Soma Centro + Vila ==")
    from produtos.vendas_lojas_util import _q2

    c = _q2("1234.50")
    v = _q2("800.25")
    t = (c + v).quantize(Decimal("0.01"))
    check("q2_centro", str(c) == "1234.50")
    check("q2_none", str(_q2(None)) == "0.00")
    check("soma_duas_lojas", t == Decimal("2034.75"))
    check("soma_zero", (_q2(0) + _q2(0)).quantize(Decimal("0.01")) == Decimal("0.00"))


def test_versao() -> None:
    print("== Versão ==")
    ver = _read("VERSION").strip()
    check("version_bump", ver >= "16.46", ver)


def main() -> int:
    print("VERIFY VENDAS-LOJAS-RESUMO")
    test_arquivos()
    test_periodo()
    test_totais_mock()
    test_versao()
    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
