# -*- coding: utf-8 -*-
"""
Prova detalhada — mostruário META (`META-MOSTRUARIO`).

Path:
  Menu GESTÃO → META → /meta/
  GET  /api/meta/resumo/?competencia=YYYY-MM
  GET/POST /api/meta/faixas/  (CRUD faixas PG)
  Copiar foto (PNG canvas) · Copiar texto Zap

  set AGRO_PIN_TESTE=9973
  python scripts/verify_meta_mostruario_path.py

CRUD de prova usa savepoint+rollback (não suja a loja).
HTTP: Django test Client (force_login — PIN só p/ alinhar env).
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.db import transaction
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from produtos.meta_vendas_util import (
    META_VENDA_PADRAO_FAIXAS,
    _progresso_meta,
    meta_bonus_label,
    meta_competencia_bounds,
    meta_competencia_iso,
    meta_ensure_faixas_padrao,
    meta_excluir_faixa,
    meta_fmt_moeda,
    meta_listar_faixas,
    meta_montar_mostruario,
    meta_parse_competencia,
    meta_parse_decimal,
    meta_salvar_faixa,
    meta_texto_zap,
)
from produtos.models import MetaVendaFaixaAgro
from produtos.vendas_lojas_util import (
    vendas_lojas_cmp_meta,
    vendas_lojas_total_deposito,
)

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
oks: list[str] = []
fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos_rotas() -> None:
    print("== 1) Arquivos + rotas + menu ==")
    util = ROOT / "produtos/meta_vendas_util.py"
    views = ROOT / "produtos/views_meta_vendas.py"
    tpl = ROOT / "produtos/templates/produtos/meta_vendas.html"
    mig = ROOT / "produtos/migrations/0138_metavendafaixaagro.py"
    urls = _read("produtos/urls.py")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    models = _read("produtos/models.py")

    check("arquivo_util", util.is_file())
    check("arquivo_views", views.is_file())
    check("arquivo_template", tpl.is_file())
    check("arquivo_migration_0138", mig.is_file())
    check("modelo_MetaVendaFaixaAgro", "class MetaVendaFaixaAgro" in models)
    check("url_painel", "path('meta/', views_meta_vendas.meta_vendas_painel" in urls)
    check("url_api_resumo", "api/meta/resumo/" in urls)
    check("url_api_faixas", "api/meta/faixas/" in urls)
    check("import_views_meta", "from . import views_meta_vendas" in urls)
    check("menu_botao_META", "meta_vendas_painel" in dash and ">META<" in dash)
    check("menu_abaixo_config", dash.find("Configuração") < dash.find("meta_vendas_painel"))

    tpl_txt = tpl.read_text(encoding="utf-8")
    check("tpl_copiar_foto", 'id="meta-copiar-foto"' in tpl_txt)
    check("tpl_desenhar_foto", "desenharFotoMeta" in tpl_txt)
    check("tpl_clipboard_png", "image/png" in tpl_txt and "ClipboardItem" in tpl_txt)
    check("tpl_cabecalho_amarelo", "#facc15" in tpl_txt or "facc15" in tpl_txt)
    check("tpl_cols_venda_bonus", "VENDA (meta)" in tpl_txt and "BÔNUS" in tpl_txt)
    check("tpl_modo_agora", "data-modo=\"agora\"" in tpl_txt and "Até agora" in tpl_txt)
    check("tpl_modo_mes", "data-modo=\"mes\"" in tpl_txt and "Meta do mês" in tpl_txt)
    check("tpl_loja_tres", 'data-loja="todas"' in tpl_txt and 'data-loja="centro"' in tpl_txt and 'data-loja="vila"' in tpl_txt)
    check("tpl_setLoja_refetch", "function setLoja" in tpl_txt and "carregar();" in tpl_txt)

    mig_txt = mig.read_text(encoding="utf-8")
    check("mig_seed_padrao", "105000" in mig_txt and "moleton" in mig_txt)
    check("mig_CreateModel", "CreateModel" in mig_txt and "MetaVendaFaixaAgro" in mig_txt)

    # Sem escrita Mongo no path META
    util_src = util.read_text(encoding="utf-8")
    views_src = views.read_text(encoding="utf-8")
    check("util_sem_mongo", "obter_conexao_mongo" not in util_src and "DtoVenda" not in util_src)
    check("views_sem_mongo", "obter_conexao_mongo" not in views_src)


def test_math_e_format() -> None:
    print("== 2) Matemática / formatação ==")
    check("comp_iso", meta_competencia_iso(date(2026, 10, 5)) == "2026-10")
    check("parse_ok", meta_parse_competencia("2026-10") == "2026-10")
    check("parse_lixo", meta_parse_competencia("xx", date(2026, 3, 1)) == "2026-03")
    ini, fim = meta_competencia_bounds("2026-10")
    check("bounds_out", ini == date(2026, 10, 1) and fim == date(2026, 10, 31))

    check("parse_br", meta_parse_decimal("105.000,00") == Decimal("105000.00"))
    check("parse_dot", meta_parse_decimal("110000") == Decimal("110000.00"))
    check("bonus_so_rs", meta_bonus_label(100, "") == "R$ 100,00")
    check(
        "bonus_moleton",
        meta_bonus_label(Decimal("100"), "moleton") == "R$ 100,00 + moleton",
    )
    check("fmt_moeda", meta_fmt_moeda(105000) == "R$ 105.000,00")

    p = _progresso_meta(Decimal("52500"), Decimal("105000"))
    check("prog_pct", p["pct"] == Decimal("50.0"), str(p["pct"]))
    check("prog_falta", p["falta"] == Decimal("52500.00"), str(p["falta"]))
    check("prog_nao_batida", p["atingida"] is False)

    p2 = _progresso_meta(Decimal("120000"), Decimal("105000"))
    check("prog_batida", p2["atingida"] is True)
    check("prog_excedente", p2["excedente"] == Decimal("15000.00"))

    cmp_ = vendas_lojas_cmp_meta(Decimal("110"), Decimal("100"))
    check("cmp_acima", cmp_["sentido"] == "acima" and cmp_["pct"] == Decimal("10.0"))
    cmp_b = vendas_lojas_cmp_meta(Decimal("90"), Decimal("100"))
    check("cmp_abaixo", cmp_b["sentido"] == "abaixo")


def test_seed_e_crud() -> None:
    print("== 3) Seed + CRUD (rollback) ==")
    check("padrao_5_faixas", len(META_VENDA_PADRAO_FAIXAS) == 5)
    check("padrao_115_moleton", META_VENDA_PADRAO_FAIXAS[2][2] == "moleton")
    check("padrao_130_500", META_VENDA_PADRAO_FAIXAS[4] == (Decimal("130000.00"), Decimal("500.00"), ""))

    # competência fictícia só desta prova
    comp = "2099-01"
    with transaction.atomic():
        sid = transaction.savepoint()
        try:
            MetaVendaFaixaAgro.objects.filter(competencia=comp).delete()
            n = meta_ensure_faixas_padrao(comp)
            check("seed_cria_5", n == 5, f"n={n}")
            n2 = meta_ensure_faixas_padrao(comp)
            check("seed_idempotente", n2 == 0)

            faixas = meta_listar_faixas(comp)
            check("listar_5", len(faixas) == 5)
            check(
                "listar_ordem",
                [f["valor_meta"] for f in faixas]
                == [105000.0, 110000.0, 115000.0, 120000.0, 130000.0],
            )
            check("listar_bonus_moleton", faixas[2]["bonus_label"] == "R$ 100,00 + moleton")

            saved = meta_salvar_faixa(
                competencia=comp,
                valor_meta="140000",
                bonus_valor="750",
                bonus_extra="jaqueta",
            )
            check("criar_faixa", saved["valor_meta"] == 140000.0)
            check("criar_bonus_extra", saved["bonus_extra"] == "jaqueta")
            faixas2 = meta_listar_faixas(comp)
            check("listar_6", len(faixas2) == 6)

            edited = meta_salvar_faixa(
                competencia=comp,
                faixa_id=saved["id"],
                valor_meta="145000",
                bonus_valor="800",
                bonus_extra="",
            )
            check("editar_faixa", edited["valor_meta"] == 145000.0 and edited["bonus_extra"] == "")

            meta_excluir_faixa(competencia=comp, faixa_id=saved["id"])
            check("excluir_faixa", len(meta_listar_faixas(comp)) == 5)

            # rejeita meta inválida
            bad = False
            try:
                meta_salvar_faixa(competencia=comp, valor_meta="0")
            except ValueError:
                bad = True
            check("rejeita_meta_zero", bad)
        finally:
            transaction.savepoint_rollback(sid)

    check(
        "prova_nao_deixou_2099",
        not MetaVendaFaixaAgro.objects.filter(competencia="2099-01").exists(),
    )


def test_mostruario_consistencia() -> None:
    print("== 4) Mostruário × vendas reais (mês atual) ==")
    hoje = timezone.localdate()
    agora = timezone.localtime()
    comp = meta_competencia_iso(hoje)
    mes_ini, mes_fim = meta_competencia_bounds(comp)

    # garante seed do mês corrente (produção/local)
    meta_ensure_faixas_padrao(comp)
    m = meta_montar_mostruario(competencia=comp, hoje=hoje, agora=agora)

    check("mostr_comp", m["competencia"] == comp)
    check("mostr_mes_atual", m["mes_atual"] is True)
    check("mostr_hoje_ativo", m["hoje"].get("ativo") is True)
    check("mostr_faixas_ge_1", len(m.get("faixas") or []) >= 1)

    vendido_util = float(vendas_lojas_total_deposito(mes_ini, min(hoje, mes_fim), None))
    check(
        "vendido_bate_util",
        abs(float(m["vendido_mes"]) - vendido_util) < 0.02,
        f"mostr={m['vendido_mes']} util={vendido_util}",
    )

    # faixas ordenadas e progresso coerente
    prev = -1.0
    for f in m["faixas"]:
        check(f"faixa_ordem_{f['id']}", float(f["valor_meta"]) >= prev)
        prev = float(f["valor_meta"])
        esperado_pct = (
            float(m["vendido_mes"]) / float(f["valor_meta"]) * 100.0
            if float(f["valor_meta"]) > 0
            else 0
        )
        check(
            f"faixa_pct_{f['id']}",
            abs(float(f["pct"]) - round(esperado_pct, 1)) < 0.15,
            f"got={f['pct']} exp≈{round(esperado_pct,1)}",
        )
        if float(m["vendido_mes"]) >= float(f["valor_meta"]):
            check(f"faixa_batida_{f['id']}", f["atingida"] is True)
        else:
            check(f"faixa_falta_{f['id']}", f["atingida"] is False and float(f["falta"]) > 0)

    # média esperada presente (Meta C pode ser 0 em staging vazio — só exige chave)
    check("vs_media_keys", "vs_media_mes" in m and "sentido" in m["vs_media_mes"])
    check("media_ref_fmt", bool(m.get("media_mes_ref_fmt")))
    check("fracao_ate_agora", "fracao_ate_agora" in m and 0 <= float(m["fracao_ate_agora"]) <= 1)
    f0 = (m.get("faixas") or [None])[0]
    check("faixa_tem_agora", isinstance(f0, dict) and isinstance(f0.get("agora"), dict))
    check("faixa_meta_agora", isinstance(f0, dict) and "meta_agora_fmt" in f0)

    # ritmo: parcela = meta × fração
    if f0 and float(m.get("media_mes_cheia") or 0) > 0:
        esp = float(f0["valor_meta"]) * float(m["fracao_ate_agora"])
        check(
            "parcela_bate_fracao",
            abs(float(f0["meta_agora"]) - esp) < 0.05,
            f"got={f0['meta_agora']} exp≈{esp:.2f}",
        )
        # progresso «agora» coerente com parcela
        a = f0.get("agora") or {}
        if float(f0["meta_agora"]) > 0:
            pct_a = float(m["vendido_mes"]) / float(f0["meta_agora"]) * 100.0
            check(
                "agora_pct_coerente",
                abs(float(a.get("pct") or 0) - round(pct_a, 1)) < 0.2,
                f"got={a.get('pct')} exp≈{round(pct_a,1)}",
            )
            if float(m["vendido_mes"]) >= float(f0["meta_agora"]):
                check("agora_atingida_ok", a.get("atingida") is True)
            else:
                check(
                    "agora_falta_ok",
                    a.get("atingida") is False and float(a.get("falta") or 0) > 0,
                )

    # fração = media_ref / media_cheia
    if float(m.get("media_mes_cheia") or 0) > 0:
        frac_esp = float(m["media_mes_ref"]) / float(m["media_mes_cheia"])
        check(
            "fracao_bate_medias",
            abs(float(m["fracao_ate_agora"]) - frac_esp) < 0.0002,
            f"got={m['fracao_ate_agora']} exp≈{frac_esp:.4f}",
        )

    zap = meta_texto_zap(m)
    check("zap_tem_meta", "META" in zap)
    check("zap_tem_vendido", "Vendido" in zap)
    check("zap_tem_faixa", "105" in zap or "Meta" in zap or "meta" in zap.lower())
    zap_a = meta_texto_zap(m, modo="agora")
    check("zap_modo_agora", "ATÉ AGORA" in zap_a or "até agora" in zap_a.lower())
    check("zap_modo_ritmo", "ritmo" in zap_a.lower() or "parcela" in zap_a.lower())
    check("zap_loja_todas", "Centro + Vila" in zap)
    check("deposito_label_todas", m.get("deposito_label") == "Centro + Vila")

    m_c = meta_montar_mostruario(
        competencia=comp, hoje=hoje, agora=agora, deposito="centro"
    )
    m_v = meta_montar_mostruario(
        competencia=comp, hoje=hoje, agora=agora, deposito="vila"
    )
    check("deposito_centro", m_c.get("deposito") == "centro" and m_c.get("deposito_label") == "Centro")
    check("deposito_vila", m_v.get("deposito") == "vila" and m_v.get("deposito_label") == "Vila")
    check(
        "soma_lojas",
        abs(float(m["vendido_mes"]) - (float(m_c["vendido_mes"]) + float(m_v["vendido_mes"]))) < 0.05,
        f"todas={m['vendido_mes']} c={m_c['vendido_mes']} v={m_v['vendido_mes']}",
    )
    zap_c = meta_texto_zap(m_c)
    check("zap_loja_centro", "Centro" in zap_c and "Centro + Vila" not in zap_c)
    zap_v = meta_texto_zap(m_v, modo="agora")
    check("zap_loja_vila", "· Vila" in zap_v and "Centro + Vila" not in zap_v and "Centro" not in zap_v)
    check("zap_vila_agora", "até agora" in zap_v.lower() or "ATÉ AGORA" in zap_v)

    ids_todas = [int(f["id"]) for f in (m.get("faixas") or [])]
    ids_c = [int(f["id"]) for f in (m_c.get("faixas") or [])]
    ids_v = [int(f["id"]) for f in (m_v.get("faixas") or [])]
    check("faixas_mesma_lista", ids_todas == ids_c == ids_v and len(ids_todas) >= 1, str(ids_todas))
    metas_ok = True
    for a, b, cfx in zip(m.get("faixas") or [], m_c.get("faixas") or [], m_v.get("faixas") or []):
        if float(a["valor_meta"]) != float(b["valor_meta"]) or float(a["valor_meta"]) != float(cfx["valor_meta"]):
            metas_ok = False
        if (a.get("bonus_label") or "") != (b.get("bonus_label") or "") or (a.get("bonus_label") or "") != (cfx.get("bonus_label") or ""):
            metas_ok = False
    check("meta_bonus_iguais", metas_ok)

    check(
        "soma_hoje",
        abs(
            float((m.get("hoje") or {}).get("vendido") or 0)
            - (
                float((m_c.get("hoje") or {}).get("vendido") or 0)
                + float((m_v.get("hoje") or {}).get("vendido") or 0)
            )
        )
        < 0.05,
    )
    check(
        "soma_media_ref",
        abs(float(m["media_mes_ref"]) - (float(m_c["media_mes_ref"]) + float(m_v["media_mes_ref"]))) < 0.05,
        f"todas={m['media_mes_ref']} c={m_c['media_mes_ref']} v={m_v['media_mes_ref']}",
    )
    # lixo de loja cai em Centro + Vila
    m_lixo = meta_montar_mostruario(
        competencia=comp, hoje=hoje, agora=agora, deposito="xyz"
    )
    check("deposito_lixo_vira_todas", m_lixo.get("deposito") == "todas")
    check("sem_coluna_loja_na_meta", not any(
        f.name == "deposito" for f in MetaVendaFaixaAgro._meta.fields
    ))

    # mês fechado (competência prova — rollback)
    with transaction.atomic():
        sid = transaction.savepoint()
        try:
            MetaVendaFaixaAgro.objects.filter(competencia="2097-03").delete()
            meta_ensure_faixas_padrao("2097-03")
            m_p = meta_montar_mostruario(
                competencia="2097-03", hoje=hoje, agora=agora
            )
            check("passado_nao_atual", m_p["mes_atual"] is False)
            check("passado_hoje_off", m_p["hoje"].get("ativo") is False)
            check("passado_fechado", m_p["mes_fechado"] is True)
        finally:
            transaction.savepoint_rollback(sid)


def test_http() -> None:
    print("== 5) HTTP (Client + force_login) ==")
    check("pin_env", PIN == "9973" or len(PIN) >= 4, f"PIN={PIN!r}")

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if user is None:
        user = User.objects.create_user(username="meta_prova", password="x")
    check("user_login", user is not None, getattr(user, "username", ""))

    c = Client(HTTP_HOST="127.0.0.1")
    # anônimo
    r0 = c.get(reverse("meta_vendas_painel"))
    check("anon_redirect", r0.status_code in (302, 301), str(r0.status_code))

    c.force_login(user)
    hoje = timezone.localdate()
    comp = meta_competencia_iso(hoje)

    r = c.get(reverse("meta_vendas_painel"))
    check("painel_200", r.status_code == 200, str(r.status_code))
    body = r.content.decode("utf-8", errors="replace")
    check("painel_titulo", "META" in body)
    check("painel_copiar_foto", "meta-copiar-foto" in body)
    check("painel_cfg", "meta-cfg-lista" in body)
    check("painel_modo_agora_btn", 'data-modo="agora"' in body and "Até agora" in body)
    check("painel_modo_mes_btn", 'data-modo="mes"' in body and "Meta do mês" in body)
    check("painel_js_setModo", "function setModo" in body or "setModo(" in body)
    check("painel_js_progAtivo", "progAtivo" in body)
    check("painel_loja_centro", 'data-loja="centro"' in body and "Só Centro" in body)
    check("painel_loja_vila", 'data-loja="vila"' in body and "Só Vila" in body)
    check("painel_loja_todas", 'data-loja="todas"' in body and "Centro + Vila" in body)
    check("painel_js_setLoja", "function setLoja" in body and "carregar()" in body)
    check("painel_js_deposito_query", "deposito=" in body and "agro_meta_loja_v1" in body)
    check("painel_foto_loja", "deposito_label" in body)

    r2 = c.get(reverse("api_meta_vendas_resumo"), {"competencia": comp})
    check("api_resumo_200", r2.status_code == 200, str(r2.status_code))
    data = r2.json()
    check("api_resumo_ok", data.get("ok") is True)
    check("api_resumo_mostr", isinstance(data.get("mostruario"), dict))
    check("api_resumo_zap", isinstance(data.get("texto_zap"), str) and len(data["texto_zap"]) > 20)
    # JSON serializável (sem Decimal cru)
    try:
        json.dumps(data)
        check("api_resumo_jsonable", True)
    except TypeError as exc:
        check("api_resumo_jsonable", False, str(exc))

    r2a = c.get(
        reverse("api_meta_vendas_resumo"),
        {"competencia": comp, "modo": "agora"},
    )
    check("api_resumo_agora_200", r2a.status_code == 200)
    d2a = r2a.json()
    check("api_resumo_agora_ok", d2a.get("ok") is True and d2a.get("modo") == "agora")
    zap_http = d2a.get("texto_zap") or ""
    check(
        "api_resumo_agora_zap",
        "até agora" in zap_http.lower() or "ATÉ AGORA" in zap_http or "ritmo" in zap_http.lower(),
    )
    m_http = d2a.get("mostruario") or {}
    check("api_resumo_agora_fracao", "fracao_ate_agora" in m_http)
    faixas_http = m_http.get("faixas") or []
    if faixas_http:
        a0 = faixas_http[0].get("agora") or {}
        check("api_resumo_agora_faixa", "pct" in a0 and "falta_fmt" in a0)
        # pct modo agora = vendido / meta_agora
        ma = float(faixas_http[0].get("meta_agora") or 0)
        vd = float(m_http.get("vendido_mes") or 0)
        if ma > 0:
            exp_pct = round(vd / ma * 100.0, 1)
            check(
                "api_resumo_agora_pct",
                abs(float(a0.get("pct") or 0) - exp_pct) < 0.2,
                f"got={a0.get('pct')} exp≈{exp_pct}",
            )

    r_c = c.get(reverse("api_meta_vendas_resumo"), {"competencia": comp, "deposito": "centro"})
    d_c = r_c.json() if r_c.status_code == 200 else {}
    mc = (d_c.get("mostruario") or {})
    check("api_centro_ok", r_c.status_code == 200 and mc.get("deposito") == "centro")
    check("api_centro_zap", "Centro" in (d_c.get("texto_zap") or "") and "Centro + Vila" not in (d_c.get("texto_zap") or ""))
    faixas_api = (data.get("mostruario") or {}).get("faixas") or []
    faixas_c = mc.get("faixas") or []
    check(
        "api_meta_igual_centro",
        [f.get("id") for f in faixas_api] == [f.get("id") for f in faixas_c]
        and [f.get("valor_meta") for f in faixas_api] == [f.get("valor_meta") for f in faixas_c],
    )

    r_v = c.get(
        reverse("api_meta_vendas_resumo"),
        {"competencia": comp, "deposito": "vila", "modo": "agora"},
    )
    d_v = r_v.json() if r_v.status_code == 200 else {}
    mv = d_v.get("mostruario") or {}
    check("api_vila_agora_ok", r_v.status_code == 200 and mv.get("deposito") == "vila" and d_v.get("modo") == "agora")
    check("api_vila_zap", "Vila" in (d_v.get("texto_zap") or "") and "Centro + Vila" not in (d_v.get("texto_zap") or ""))
    vend_todas = float((data.get("mostruario") or {}).get("vendido_mes") or 0)
    vend_c = float(mc.get("vendido_mes") or 0)
    vend_v = float(mv.get("vendido_mes") or 0)
    check("api_soma_lojas", abs(vend_todas - (vend_c + vend_v)) < 0.05, f"todas={vend_todas} c={vend_c} v={vend_v}")

    r_lixo = c.get(reverse("api_meta_vendas_resumo"), {"competencia": comp, "deposito": "xyz"})
    d_lixo = r_lixo.json() if r_lixo.status_code == 200 else {}
    check("api_deposito_lixo", (d_lixo.get("mostruario") or {}).get("deposito") == "todas")


    r3 = c.get(reverse("api_meta_vendas_faixas"), {"competencia": comp})
    check("api_faixas_200", r3.status_code == 200)
    data3 = r3.json()
    check("api_faixas_ok", data3.get("ok") is True and len(data3.get("faixas") or []) >= 1)

    # CRUD HTTP em competência prova + rollback
    comp_h = "2098-06"
    with transaction.atomic():
        sid = transaction.savepoint()
        try:
            MetaVendaFaixaAgro.objects.filter(competencia=comp_h).delete()
            r4 = c.post(
                reverse("api_meta_vendas_faixas"),
                data=json.dumps(
                    {
                        "competencia": comp_h,
                        "valor_meta": "99000",
                        "bonus_valor": "50",
                        "bonus_extra": "prova",
                    }
                ),
                content_type="application/json",
            )
            check("api_criar_200", r4.status_code == 200, str(r4.status_code))
            d4 = r4.json()
            check("api_criar_ok", d4.get("ok") is True)
            fid = (d4.get("faixa") or {}).get("id")
            check("api_criar_id", bool(fid))

            r5 = c.post(
                reverse("api_meta_vendas_faixas"),
                data=json.dumps(
                    {"acao": "excluir", "competencia": comp_h, "id": fid}
                ),
                content_type="application/json",
            )
            check("api_excluir_ok", r5.status_code == 200 and r5.json().get("ok") is True)
        finally:
            transaction.savepoint_rollback(sid)

    check(
        "http_nao_deixou_2098",
        not MetaVendaFaixaAgro.objects.filter(competencia="2098-06").exists(),
    )


def test_isolamento_pdv() -> None:
    print("== 6) Isolamento (não mexe PDV/caixa) ==")
    util = _read("produtos/meta_vendas_util.py")
    views = _read("produtos/views_meta_vendas.py")
    for nome, src in (("util", util), ("views", views)):
        check(f"{nome}_sem_caixa", "CaixaSessao" not in src and "abrir_caixa" not in src)
        check(f"{nome}_sem_venda_write", "VendaAgro.objects.create" not in src)
        check(f"{nome}_sem_nfce", "Nfce" not in src)


def main() -> int:
    print(f"META-MOSTRUARIO path · PIN={PIN!r}")
    print(f"hoje local={timezone.localdate()} tz={timezone.get_current_timezone()}")
    test_arquivos_rotas()
    test_math_e_format()
    test_seed_e_crud()
    test_mostruario_consistencia()
    test_http()
    test_isolamento_pdv()
    print()
    print(f"RESULTADO: {len(oks)}/{len(oks)+len(fails)} OK")
    if fails:
        print("FALHAS:")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("TUDO OK — path META pronto.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
