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
    check("tpl_filtro_ontem", 'value="ontem"' in tpl)
    check("tpl_filtro_mes_ant", 'value="mes_ant"' in tpl)
    check("tpl_calendario", "vl-abrir-cal" in tpl and "Calendário" in tpl)
    check("tpl_cal_sheet", "vl-sheet" in tpl and "vl-cal-grid" in tpl)
    check("tpl_pwa_manifest", "vendas_lojas_manifest" in tpl)
    check("tpl_pwa_sw", "serviceWorker" in tpl and "vendas_lojas_sw" in tpl)
    check("url_manifest", "vendas_lojas_manifest" in urls)
    check("url_sw", "vendas_lojas_sw" in urls)
    check("icon_192", (ROOT / "produtos/static/produtos/pwa/vendas-lojas-192.png").is_file())
    check("icon_512", (ROOT / "produtos/static/produtos/pwa/vendas-lojas-512.png").is_file())
    check("view_manifest", "def vendas_lojas_manifest" in views)
    check("view_sw", "def vendas_lojas_sw" in views)
    check("view_pass_data", 'request.GET.get("data")' in views)
    check("tpl_voltar", "vl-voltar" in tpl and "relatorios_hub" in tpl)
    check("tpl_mobile_viewport", "viewport-fit=cover" in tpl)
    check("tpl_mobile_dvh", "100dvh" in tpl)
    check("tpl_mobile_safe", "safe-area-inset" in tpl)
    check("tpl_numeros_grandes", "clamp(2.45rem" in tpl and "clamp(2.75rem" in tpl)
    check("tpl_filtro_toque", "min-height: 3rem" in tpl)
    check("tpl_grade_4", "repeat(4, minmax(0, 1fr))" in tpl)
    check("tpl_coluna_celular", "max-width: 28rem" in tpl)
    check("tpl_sem_scale", "agro_display_scale" not in tpl and "_agro_consulta_ui" not in tpl)
    check("tpl_sem_fa", "font-awesome" not in tpl.lower())
    check("tpl_standalone", "<!DOCTYPE html>" in tpl and "extends" not in tpl)
    check("util_sem_cache", "cache." not in util)
    check("util_sem_mongo", "obter_conexao_mongo" not in util and "DtoVenda" not in util)
    check("util_exclui_devolvida", "devolvida_em__isnull=True" in util)
    check("util_vila_iexact", 'deposito__iexact="vila"' in util)
    check("util_centro_exclude_vila", "exclude(deposito__iexact=" in util)
    check("util_soma_total", "centro + vila" in util)
    check("view_usa_meta", "vendas_lojas_meta_c_modos" in view_fn)
    check("view_meta_centro", '"centro"' in view_fn or "'centro'" in view_fn)
    check("view_meta_vila", '"vila"' in view_fn or "'vila'" in view_fn)
    check("view_pass_sentido", "centro_sentido" in view_fn and "vila_sentido" in view_fn)
    check("view_pass_esp", "centro_esp_fmt" in view_fn and "total_esp_fmt" in view_fn)
    check("view_pass_esp_dia", "centro_esp_dia_fmt" in view_fn and "mostra_toggle_media" in view_fn)
    check("tpl_card_tap", "data-media" in tpl and "data-sentido" in tpl)
    check("tpl_hint_toque", "Toque para a média" in tpl)
    check("tpl_sheet_media", "vl-sheet-media" in tpl and "vl-media-esperado" in tpl)
    check("tpl_media_pct", "acima" in tpl and "abaixo" in tpl)
    check("tpl_media_toggle", "data-media-modo" in tpl and "Até agora" in tpl and "Dia todo" in tpl)
    check("tpl_media_padrao_agora", "O que já deveria ter saído até agora" in tpl)
    check("tpl_media_dia_todo", "Média do período inteiro" in tpl)
    check("tpl_ls_modo", "vl_media_modo_v1" in tpl)
    check("util_cmp_meta", "def vendas_lojas_cmp_meta" in util)
    check("util_cmp_agora", "def vendas_lojas_cmp_meta_agora" in util)
    check("util_meta_soma", "def vendas_lojas_meta_c_soma" in util)
    check("util_meta_modos", "def vendas_lojas_meta_c_modos" in util)
    check("util_fracao", "def vendas_lojas_fracao_expediente" in util)
    check("util_expediente", "VL_EXPEDIENTE_INI" in util and "VL_EXPEDIENTE_FIM" in util)
    check("util_meta_bi", "_dashboard_serie_meta_c_vendas" in util)
    meta_fn = _fn_src("produtos/views.py", "_dashboard_serie_meta_c_vendas")
    hist_fn = _fn_src("produtos/views.py", "_dashboard_vendas_serie_meta_historico")
    meses_fn = _fn_src("produtos/views.py", "_dashboard_meta_c_meses_por_dia")
    check("meta_c_param_deposito", "deposito" in meta_fn and "dep_key" in meta_fn)
    check("meta_c_cache_loja", "dash:metac:v1:" in meta_fn)
    check("hist_param_deposito", "deposito" in hist_fn and "dashboard_vendas_serie_meta_merged" in hist_fn)
    check("meses_cache_loja", "dep_key" in meses_fn and "(fp, lp, dep_key)" in meses_fn)


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

    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, "ontem")
    check("ontem", key == "ontem" and ini == fim == date(2026, 8, 14), f"{ini} {label}")

    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, "mes_ant")
    check("mes_ant", key == "mes_ant" and ini == date(2026, 7, 1) and fim == date(2026, 7, 31), f"{ini}→{fim}")

    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, "dia", "2026-08-10")
    check("dia_cal", key == "dia" and ini == fim == date(2026, 8, 10) and "10/08/2026" in label)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(sab, "dia", "2026-12-31")
    check("dia_futuro_corta", key == "dia" and ini == sab)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(date(2026, 3, 5), "mes_ant")
    check("mes_ant_fev", ini == date(2026, 2, 1) and fim == date(2026, 2, 28))


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


def test_cmp_meta() -> None:
    print("== Média esperada vs vendido ==")
    from produtos.vendas_lojas_util import vendas_lojas_cmp_meta

    acima = vendas_lojas_cmp_meta("120.00", "100.00")
    check("cmp_acima", acima["sentido"] == "acima" and acima["diff"] == Decimal("20.00"))
    check("cmp_acima_pct", acima["pct"] == Decimal("20.0"))

    abaixo = vendas_lojas_cmp_meta("80.00", "100.00")
    check("cmp_abaixo", abaixo["sentido"] == "abaixo" and abaixo["diff"] == Decimal("-20.00"))
    check("cmp_abaixo_pct", abaixo["pct"] == Decimal("20.0"))

    igual = vendas_lojas_cmp_meta("100.00", "100.00")
    check("cmp_igual", igual["sentido"] == "igual" and igual["diff"] == Decimal("0.00"))

    sem = vendas_lojas_cmp_meta("50.00", "0")
    check("cmp_sem_media", sem["sentido"] == "sem" and sem["diff"] is None)

    zero_vendido = vendas_lojas_cmp_meta("0", "100")
    check("cmp_zero_vendido_abaixo", zero_vendido["sentido"] == "abaixo" and zero_vendido["pct"] == Decimal("100.0"))

    from produtos.vendas_lojas_util import vendas_lojas_cmp_meta_agora

    cedo_zero = vendas_lojas_cmp_meta_agora("0", "0", "1000")
    check("agora_antes_abrir_igual", cedo_zero["sentido"] == "igual")
    cedo_vendeu = vendas_lojas_cmp_meta_agora("50", "0", "1000")
    check("agora_antes_abrir_acima", cedo_vendeu["sentido"] == "acima" and cedo_vendeu["pct"] is None)
    agora_meio = vendas_lojas_cmp_meta_agora("600", "500", "1000")
    check("agora_meio_acima", agora_meio["sentido"] == "acima" and agora_meio["diff"] == Decimal("100.00"))
    sem_base = vendas_lojas_cmp_meta_agora("10", "0", "0")
    check("agora_sem_base", sem_base["sentido"] == "sem")


def test_fracao_expediente() -> None:
    print("== Fração expediente (tempo real) ==")
    from datetime import datetime

    from produtos.vendas_lojas_util import vendas_lojas_fracao_expediente

    check("antes_abrir", vendas_lojas_fracao_expediente(datetime(2026, 8, 15, 6, 50)) == Decimal("0"))
    check("abre_730", vendas_lojas_fracao_expediente(datetime(2026, 8, 15, 7, 30)) == Decimal("0"))
    meio = vendas_lojas_fracao_expediente(datetime(2026, 8, 15, 13, 0))
    check("meio_1300", meio == Decimal("0.5000"), str(meio))
    manha = vendas_lojas_fracao_expediente(datetime(2026, 8, 15, 10, 15))
    check("manha_lt_meio", manha > 0 and manha < Decimal("0.5"), str(manha))
    check("fecha_1830", vendas_lojas_fracao_expediente(datetime(2026, 8, 15, 18, 30)) == Decimal("1"))
    check("depois_fechar", vendas_lojas_fracao_expediente(datetime(2026, 8, 15, 21, 0)) == Decimal("1"))
    dia = Decimal("1000.00")
    ate = (dia * manha).quantize(Decimal("0.01"))
    check("manha_nao_e_dia_todo", ate < dia)


def test_versao() -> None:
    print("== Versão ==")
    ver = _read("VERSION").strip()
    check("version_bump", ver >= "16.53", ver)


def main() -> int:
    print("VERIFY VENDAS-LOJAS-RESUMO")
    test_arquivos()
    test_periodo()
    test_totais_mock()
    test_cmp_meta()
    test_fracao_expediente()
    test_versao()
    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
