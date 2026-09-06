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
        print(f"  OK  {name}" + (f" -- {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" -- {detail}" if detail else ""))


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
    check("tpl_prev_card", "vl-abrir-prev" in tpl and "Previsão mês" in tpl)
    check("tpl_prev_sheet", "vl-sheet-prev" in tpl and "prev_total_fmt" in tpl)
    check("tpl_prev_lojas", "prev_centro_fmt" in tpl and "prev_vila_fmt" in tpl)
    check("tpl_prev_aviso", "prev_aviso_cedo" in tpl and "Ainda é cedo" in tpl)
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
    check("util_abate_devolucao", "abatimento_devolucoes_totais_loja" in util)
    check("util_sem_fiado", "vendas_lojas_sem_fiado_totais" in util)
    check("util_fiado_baixa", "vendas_lojas_fiado_baixas_periodo" in util)
    check("util_intervalo", "intervalo" in util and "dia_fim_iso" in util)
    check("util_vila_iexact", 'deposito__iexact="vila"' in util)
    check("util_centro_exclude_vila", "exclude(deposito__iexact=" in util)
    check("util_soma_total", "centro + vila" in util)
    check("view_pass_fiado", "sem_fiado_fmt" in view_fn and "sem_fiado_quit_fmt" in view_fn)
    check("view_pass_data_fim", "data_fim_iso" in view_fn or "data_fim" in views)
    check("tpl_fiado_tag", "vl-fiado-tag" in tpl and "Sem fiado" in tpl)
    check("tpl_fiado_quit_label", "c/ fiado quitado" in tpl)
    check("tpl_fiado_sheet", "vl-sheet-fiado" in tpl)
    check("tpl_intervalo_form", "vl-goto-intervalo" in tpl and "data_fim" in tpl)
    check("view_usa_meta", "vendas_lojas_meta_c_modos" in view_fn)
    check("view_usa_prev", "vendas_lojas_previsao_mes_lojas" in view_fn)
    check("view_pass_prev", "prev_total_fmt" in view_fn and "prev_mes_lbl" in view_fn)
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
    check("util_prev_mes", "def vendas_lojas_previsao_mes" in util)
    check("util_prev_lojas", "def vendas_lojas_previsao_mes_lojas" in util)
    check("util_prev_aviso", "def vendas_lojas_previsao_aviso_cedo" in util)
    check("util_ultimo_dia", "def vendas_lojas_ultimo_dia_mes" in util)
    check("util_fracao", "def vendas_lojas_fracao_expediente" in util)
    check("util_expediente", "VL_EXPEDIENTE_INI" in util and "VL_EXPEDIENTE_FIM" in util)
    check("util_meta_bi", "_dashboard_serie_meta_c_vendas" in util)
    meta_fn = _fn_src("produtos/views.py", "_dashboard_serie_meta_c_vendas")
    hist_fn = _fn_src("produtos/views.py", "_dashboard_vendas_serie_meta_historico")
    meses_fn = _fn_src("produtos/views.py", "_dashboard_meta_c_meses_por_dia")
    check("meta_c_param_deposito", "deposito" in meta_fn)
    check("meta_c_cache_loja", "dash:metac:v2:" in meta_fn or "dash:metac:v3:" in meta_fn)
    check("meta_c_todas_soma", "todas-soma" in meta_fn)
    abert_fn = _fn_src("produtos/views.py", "_dashboard_meta_c_vila_abertura")
    um_mes_fn = _fn_src("produtos/views.py", "_dashboard_meta_c_um_mes")
    valor_fn = _fn_src("produtos/views.py", "_dashboard_vendas_meta_c_valor")
    check("meta_c_abertura_20260720", "2026-07-20" in abert_fn)
    check("meta_c_um_mes_data_min", "data_min" in um_mes_fn)
    check("meta_c_valor_soma", '"centro"' in valor_fn and '"vila"' in valor_fn and "round(" in valor_fn)
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
    check("semana_seg_hoje", key == "semana" and ini == date(2026, 8, 10) and fim == sab, f"{ini}->{fim}")
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
    check("mes_ant", key == "mes_ant" and ini == date(2026, 7, 1) and fim == date(2026, 7, 31), f"{ini}->{fim}")

    ini, fim, label, key = vendas_lojas_periodo_bounds(sab, "dia", "2026-08-10")
    check("dia_cal", key == "dia" and ini == fim == date(2026, 8, 10) and "10/08/2026" in label)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(sab, "dia", "2026-12-31")
    check("dia_futuro_corta", key == "dia" and ini == sab)

    ini, fim, _l, key = vendas_lojas_periodo_bounds(date(2026, 3, 5), "mes_ant")
    check("mes_ant_fev", ini == date(2026, 2, 1) and fim == date(2026, 2, 28))

    ini, fim, label, key = vendas_lojas_periodo_bounds(
        date(2026, 9, 10), "intervalo", "2026-09-01", "2026-09-10"
    )
    check(
        "intervalo_cal",
        key == "intervalo"
        and ini == date(2026, 9, 1)
        and fim == date(2026, 9, 10)
        and "01/09/2026" in label
        and "10/09/2026" in label,
        f"{ini}->{fim} {key}",
    )

    ini, fim, _l, key = vendas_lojas_periodo_bounds(
        date(2026, 9, 10), "intervalo", "2026-09-10", "2026-09-01"
    )
    check("intervalo_troca", key == "intervalo" and ini == date(2026, 9, 1) and fim == date(2026, 9, 10))

    ini, fim, _l, key = vendas_lojas_periodo_bounds(
        date(2026, 9, 1), "intervalo", "2026-09-01", "2026-09-10"
    )
    check("intervalo_futuro_corta", key == "dia" and ini == fim == date(2026, 9, 1))


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


def test_previsao_mes() -> None:
    print("== Previsão do mês (tempo real) ==")
    from datetime import date, datetime
    from decimal import Decimal
    from unittest.mock import patch

    from produtos.vendas_lojas_util import (
        vendas_lojas_previsao_mes,
        vendas_lojas_previsao_mes_lojas,
        vendas_lojas_ultimo_dia_mes,
    )

    check("ultimo_jan", vendas_lojas_ultimo_dia_mes(date(2026, 1, 15)) == date(2026, 1, 31))
    check("ultimo_fev", vendas_lojas_ultimo_dia_mes(date(2026, 2, 1)) == date(2026, 2, 28))
    check("ultimo_dez", vendas_lojas_ultimo_dia_mes(date(2026, 12, 10)) == date(2026, 12, 31))

    hoje = date(2026, 9, 6)
    agora = datetime(2026, 9, 6, 13, 0)

    def _fake_total(ini, fim, deposito=None):
        return Decimal("50000.00") if deposito != "vila" else Decimal("20000.00")

    def _fake_meta_soma(ini, fim, deposito=None):
        return Decimal("300000.00") if deposito != "vila" else Decimal("120000.00")

    def _fake_modos(ini, fim, deposito, *, hoje, agora):
        base = Decimal("100000.00") if deposito != "vila" else Decimal("40000.00")
        return base, base, True

    with (
        patch("produtos.vendas_lojas_util.vendas_lojas_total_deposito", side_effect=_fake_total),
        patch("produtos.vendas_lojas_util.vendas_lojas_meta_c_soma", side_effect=_fake_meta_soma),
        patch("produtos.vendas_lojas_util.vendas_lojas_meta_c_modos", side_effect=_fake_modos),
    ):
        c = vendas_lojas_previsao_mes(hoje=hoje, agora=agora, deposito="centro")
        check("prev_fonte_ritmo", c["fonte"] == "ritmo")
        check("prev_ritmo_50", c["ritmo"] == Decimal("0.5000"), str(c["ritmo"]))
        check("prev_valor", c["previsao"] == Decimal("150000.00"), str(c["previsao"]))
        lojas = vendas_lojas_previsao_mes_lojas(hoje=hoje, agora=agora)
        check(
            "prev_total_soma",
            lojas["total"]["previsao"] == Decimal("210000.00"),
            str(lojas["total"]["previsao"]),
        )

    def _fake_modos_cedo(ini, fim, deposito, *, hoje, agora):
        return Decimal("300000.00"), Decimal("1000.00"), True

    with (
        patch("produtos.vendas_lojas_util.vendas_lojas_total_deposito", return_value=Decimal("800.00")),
        patch("produtos.vendas_lojas_util.vendas_lojas_meta_c_soma", return_value=Decimal("300000.00")),
        patch("produtos.vendas_lojas_util.vendas_lojas_meta_c_modos", side_effect=_fake_modos_cedo),
    ):
        cedo = vendas_lojas_previsao_mes(hoje=hoje, agora=agora, deposito="centro")
        check("prev_cedo_media", cedo["fonte"] == "media" and cedo["previsao"] == Decimal("300000.00"))

    def _fake_modos_fim(ini, fim, deposito, *, hoje, agora):
        return Decimal("300000.00"), Decimal("300000.00"), False

    with (
        patch("produtos.vendas_lojas_util.vendas_lojas_total_deposito", return_value=Decimal("280000.00")),
        patch("produtos.vendas_lojas_util.vendas_lojas_meta_c_soma", return_value=Decimal("300000.00")),
        patch("produtos.vendas_lojas_util.vendas_lojas_meta_c_modos", side_effect=_fake_modos_fim),
    ):
        fim = vendas_lojas_previsao_mes(
            hoje=date(2026, 9, 30), agora=datetime(2026, 9, 30, 19, 0), deposito="centro"
        )
        check("prev_mes_fechado", fim["fonte"] == "fechado" and fim["previsao"] == Decimal("280000.00"))

    from produtos.vendas_lojas_util import vendas_lojas_previsao_aviso_cedo

    check(
        "aviso_manha",
        vendas_lojas_previsao_aviso_cedo(agora=datetime(2026, 9, 6, 9, 0), fonte_total="ritmo") is True,
    )
    check(
        "aviso_tarde_off",
        vendas_lojas_previsao_aviso_cedo(agora=datetime(2026, 9, 6, 14, 0), fonte_total="ritmo") is False,
    )
    check(
        "aviso_fonte_media",
        vendas_lojas_previsao_aviso_cedo(agora=datetime(2026, 9, 6, 14, 0), fonte_total="media") is True,
    )


def test_fiado_math() -> None:
    print("== Fiado (DB local) ==")
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from datetime import date
    from decimal import Decimal

    from produtos.caixa_util import _perfil_usuario_por_pin, validar_pin_operador
    from produtos.vendas_lojas_util import (
        vendas_lojas_fiado_baixas_periodo,
        vendas_lojas_sem_fiado_mais_quitacoes_totais,
        vendas_lojas_sem_fiado_totais,
        vendas_lojas_soma_fiado_vendas_periodo,
        vendas_lojas_totais,
    )

    pin_ok, pin_msg = validar_pin_operador("9973")
    check("pin_9973", pin_ok, pin_msg[:40] if pin_msg else "")
    check("pin_9973_perfil", _perfil_usuario_por_pin("9973") is not None)

    h = date.today()
    _, _, total = vendas_lojas_totais(h, h)
    _, _, sem_fiado = vendas_lojas_sem_fiado_totais(h, h)
    _, _, quit = vendas_lojas_sem_fiado_mais_quitacoes_totais(h, h)
    fc, fv = vendas_lojas_soma_fiado_vendas_periodo(h, h)
    _, _, baixas = vendas_lojas_fiado_baixas_periodo(h, h)
    fiado = (fc + fv).quantize(Decimal("0.01"))
    esperado_quit = (sem_fiado + baixas).quantize(Decimal("0.01"))
    check("sem_fiado_le_total", sem_fiado <= total, f"{sem_fiado} <= {total}")
    check("quit_ge_sem_fiado", quit >= sem_fiado, f"{quit} >= {sem_fiado}")
    check("quit_formula", quit == esperado_quit, f"{quit} vs {esperado_quit}")
    check(
        "total_fiado_parte",
        (sem_fiado + fiado).quantize(Decimal("0.01")) >= total - Decimal("0.02"),
        f"sf+fi={sem_fiado + fiado} total={total}",
    )


def test_http() -> None:
    print("== HTTP Django ==")
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if not user:
        check("http_user", False, "sem usuario")
        return
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()
        c.force_login(user)
        r = c.get("/vendas/lojas/", follow=True)
        body = r.content.decode("utf-8", "replace")
        check("http_lojas_200", r.status_code == 200, str(r.status_code))
        check("http_fiado_tag", "vl-fiado-tag" in body)
        check("http_label_quit", "c/ fiado quitado" in body)
        check("http_intervalo_form", "vl-goto-intervalo" in body)
        r2 = c.get(
            "/vendas/lojas/?periodo=intervalo&data=2026-08-01&data_fim=2026-08-15",
            follow=True,
        )
        b2 = r2.content.decode("utf-8", "replace")
        check("http_intervalo_200", r2.status_code == 200, str(r2.status_code))
        check("http_intervalo_label", "01/08/2026" in b2 and "15/08/2026" in b2)
        check("http_prev_card", "vl-abrir-prev" in body and "Previsão mês" in body)
        check("http_prev_sheet", "vl-sheet-prev" in body)
        hub = c.get(reverse("relatorios_hub"), follow=True)
        check("http_hub_link", "/vendas/lojas/" in hub.content.decode("utf-8", "replace"))


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
    test_previsao_mes()
    test_fiado_math()
    test_http()
    test_versao()
    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
