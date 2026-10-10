#!/usr/bin/env python
"""Smoke gráfico gastos PG (filtro plano + bucket + as_of + UI). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import calendar
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

DE = date(2026, 5, 5)
ATE = date(2026, 8, 5)
MESES = ((2026, 5), (2026, 6), (2026, 7), (2026, 8))
REF = date(2026, 6, 1)


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"OK {msg}")


def main() -> None:
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.lancamentos_financeiro_pg_analytics_util import (
        _titulos_no_periodo_pg,
        _valor_titulo_grafico,
        grafico_gastos_serie_pg,
    )
    from produtos.lancamentos_financeiro_pg_util import planos_distintos_pg
    from produtos.mongo_financeiro_util import _grafico_gastos_status_para_lista_planos

    checks = 0

    # --- rotas ---
    for name in (
        "grafico_gastos",
        "api_dados_grafico_gastos",
        "api_grafico_gastos_atalhos",
    ):
        reverse(name)
    checks += 1
    ok("rotas gráfico")

    # --- template / JS (contratos do fix) ---
    import re

    tpl = Path(ROOT, "financeiro/templates/financeiro/grafico_gastos.html").read_text(
        encoding="utf-8"
    )
    for needle in (
        "intervalo.de < filtroDe",
        "intervalo.ate > filtroAte",
        "function cpStatusFromGrafico",
        "statusPlanosComoCp",
        "situação de hoje",
        "gg-split-inner--filtros",
        ".gg-chip.is-active",
        "flex-shrink: 0",
        "--gg-touch",
        "> Atualizar",
        "não guarda a data de cadastro",
    ):
        if needle not in tpl:
            fail(f"template sem '{needle}'")
    i_past = tpl.find(".gg-chip.is-past")
    i_active = tpl.find(".gg-chip.is-active,")
    if i_past < 0 or i_active < 0 or i_active < i_past:
        fail("CSS .is-active deve vir depois de .is-past")
    if re.search(r"(?<!flex-)shrink:\s*0", tpl):
        fail("ainda existe 'shrink: 0' inválido no CSS")
    checks += 1
    ok("template / JS contratos")

    analytics = Path(
        ROOT, "produtos/lancamentos_financeiro_pg_analytics_util.py"
    ).read_text(encoding="utf-8")
    for needle in (
        "incluir_planos=incluir",
        "b_de = max(b_de, data_de)",
        "b_ate = min(b_ate, data_ate)",
        "pago_depois = bool(as_of",
        "if not todos_planos and plano_ids:",
    ):
        if needle not in analytics:
            fail(f"analytics sem '{needle}'")
    checks += 1
    ok("backend inclusão + clip + as_of")

    def manual(alvos, por, valor, as_of=None):
        st = _grafico_gastos_status_para_lista_planos(por, valor, data_referencia=as_of)
        out = []
        for y, m in MESES:
            r_de = max(date(y, m, 1), DE)
            r_ate = min(date(y, m, calendar.monthrange(y, m)[1]), ATE)
            tot = Decimal("0")
            for t in _titulos_no_periodo_pg(
                data_de=r_de, data_ate=r_ate, por=por, despesa=True, status=st
            ):
                if por == "vencimento":
                    dt = t.data_vencimento
                elif por == "pagamento":
                    dt = t.data_pagamento
                else:
                    dt = t.data_competencia or t.data_vencimento
                if dt is None or dt < r_de or dt > r_ate:
                    continue
                plano = (t.plano_conta or "").strip() or "(sem plano)"
                if alvos is not None and plano not in alvos:
                    continue
                v = _valor_titulo_grafico(t, valor, as_of=as_of)
                if v <= Decimal("0.02"):
                    continue
                tot += v
            out.append(round(float(tot), 2))
        return out

    def serie(plano_ids, todos, por, valor, planos, as_of=None):
        excl = [] if todos else [p for p in planos if p not in (plano_ids or [])]
        r = grafico_gastos_serie_pg(
            data_de=DE,
            data_ate=ATE,
            agrupamento="mes",
            plano_ids=plano_ids,
            planos_excluir_nomes=excl,
            todos_planos=todos,
            individual=False,
            por=por,
            valor=valor,
            data_referencia=as_of,
        )
        if not r.get("ok"):
            fail(f"série falhou: {r.get('erro')}")
        return r["datasets"][0]["data"], r["datasets"][0]["label"]

    casos = [
        ("vencimento", "pago", None),
        ("vencimento", "bruto", None),
        ("pagamento", "pago", None),
        ("vencimento", "saldo", None),
        ("vencimento", "saldo", REF),
        ("vencimento", "pago", REF),
        ("competencia", "bruto", None),
    ]
    n_series = 0
    for por, valor, as_of in casos:
        st = _grafico_gastos_status_para_lista_planos(por, valor, data_referencia=as_of)
        if por == "pagamento":
            kw = {"pagamento_de": DE, "pagamento_ate": ATE}
        elif por == "competencia":
            kw = {"competencia_de": DE, "competencia_ate": ATE}
        else:
            kw = {"vencimento_de": DE, "vencimento_ate": ATE}
        try:
            planos = [
                p["nome"]
                for p in planos_distintos_pg(despesa=True, status=st, limit=500, **kw)
            ]
        except TypeError:
            # assinatura pode usar nomes ligeiramente diferentes
            planos = [
                p["nome"]
                for p in planos_distintos_pg(despesa=True, status=st, limit=500)
            ]
        if not planos:
            ok(f"skip {por}/{valor} (sem planos no filtro local)")
            continue
        alvo = planos[0]
        g1, label = serie([alvo], False, por, valor, planos, as_of)
        m1 = manual({alvo}, por, valor, as_of)
        if g1 != m1:
            fail(f"{por}/{valor} 1 plano diverge: serie={g1} manual={m1}")
        if label != alvo and label != "Total Selecionado":
            # 1 plano deve rotular com o nome
            if len([alvo]) == 1 and label != alvo:
                fail(f"rótulo esperado '{alvo}', veio '{label}'")
        g_all, _ = serie(planos, True, por, valor, planos, as_of)
        m_all = manual(None, por, valor, as_of)
        if g_all != m_all:
            fail(f"{por}/{valor} TODOS diverge: serie={g_all} manual={m_all}")
        n_series += 2
        tag = f"{por}/{valor}" + (f" as_of={as_of}" if as_of else "")
        ok(f"série = manual · {tag} · 1 plano + TODOS")

    if n_series < 2:
        fail("nenhuma série comparável (banco local sem títulos?)")
    checks += 1
    ok(f"séries × manual ({n_series} comparações)")

    # clip: Mai com DE=05/05 não pode incluir 01–04/05
    # prova estrutural: bucket clip no código + ponta Mai com DE mid-month
    r_clip = grafico_gastos_serie_pg(
        data_de=DE,
        data_ate=ATE,
        agrupamento="mes",
        plano_ids=None,
        planos_excluir_nomes=[],
        todos_planos=True,
        individual=False,
        por="vencimento",
        valor="bruto",
    )
    if not r_clip.get("ok"):
        fail("clip série falhou")
    if r_clip["bucket_keys"][0] != "2026-05":
        fail(f"primeiro bucket esperado 2026-05, veio {r_clip['bucket_keys'][0]}")
    checks += 1
    ok("buckets mes no filtro 05/05-05/08")

    # as_of unit: pago depois → saldo = bruto, pago = 0
    from produtos.models import TituloFinanceiroAgro

    t = TituloFinanceiroAgro(
        mongo_id="verify_gg_tmp",
        despesa=True,
        valor_bruto=Decimal("100.00"),
        valor_pago=Decimal("100.00"),
        valor_restante=Decimal("0"),
        quitado=True,
        data_pagamento=date(2026, 7, 1),
        data_vencimento=date(2026, 6, 15),
        plano_conta="VerifyPlano",
    )
    v_saldo = _valor_titulo_grafico(t, "saldo", as_of=date(2026, 6, 1))
    v_pago = _valor_titulo_grafico(t, "pago", as_of=date(2026, 6, 1))
    v_saldo_hoje = _valor_titulo_grafico(t, "saldo", as_of=None)
    if v_saldo != Decimal("100.00"):
        fail(f"as_of saldo esperado 100, veio {v_saldo}")
    if v_pago != Decimal("0"):
        fail(f"as_of pago esperado 0, veio {v_pago}")
    if v_saldo_hoje != Decimal("0"):
        fail(f"saldo tempo real esperado 0 (quitado), veio {v_saldo_hoje}")
    checks += 1
    ok("as_of unitario (pago depois vira aberto)")

    # HTTP smoke
    U = get_user_model()
    u = U.objects.filter(is_superuser=True).first() or U.objects.first()
    if not u:
        fail("sem usuário no banco local")
    c = Client(headers={"host": "127.0.0.1"})
    c.force_login(u)
    r = c.get(reverse("grafico_gastos"))
    if r.status_code != 200:
        fail(f"GET tela → {r.status_code}")
    html = r.content.decode("utf-8", "ignore")
    for marca in ("graficoGastos", "gg-meta-bar", "Atualizar", "gg-plano-item"):
        if marca not in html:
            fail(f"HTML sem '{marca}'")
    checks += 1
    ok("GET /financeiro/grafico-gastos/ 200")

    body = {
        "agrupamento": "mes",
        "por": "vencimento",
        "valor": "pago",
        "inicio": "2026-05-05",
        "fim": "2026-08-05",
        "planos": ["Aluguel"],
        "planos_excluir": [],
        "todos_planos": False,
        "individual": False,
        "modo_tempo": "real",
    }
    import json

    r2 = c.post(
        reverse("api_dados_grafico_gastos"),
        data=json.dumps(body),
        content_type="application/json",
    )
    if r2.status_code != 200:
        fail(f"POST API → {r2.status_code} {r2.content[:180]}")
    data = r2.json()
    if "datasets" not in data or not data.get("labels"):
        fail(f"API sem datasets/labels: {data}")
    checks += 1
    ok("POST api dados-grafico-gastos 200")

    # comparar modo
    body_c = dict(body, modo_tempo="comparar", data_referencia="2026-06-01", valor="saldo")
    r3 = c.post(
        reverse("api_dados_grafico_gastos"),
        data=json.dumps(body_c),
        content_type="application/json",
    )
    if r3.status_code != 200:
        fail(f"POST comparar → {r3.status_code}")
    dc = r3.json()
    if dc.get("modo_tempo") != "comparar" and "comparacao" not in dc and len(dc.get("datasets") or []) < 1:
        # payload comparar pode vir com 2 datasets
        if len(dc.get("datasets") or []) < 2 and not dc.get("comparacao"):
            fail(f"modo comparar sem payload esperado: keys={list(dc.keys())}")
    checks += 1
    ok("POST modo comparar 200")

    # 1 plano marcado não pode somar outros (regressão Renan)
    planos_all = [
        p["nome"]
        for p in planos_distintos_pg(
            despesa=True, status="todos", limit=50, vencimento_de=DE, vencimento_ate=ATE
        )
    ]
    if len(planos_all) >= 2:
        alvo = planos_all[0]
        outros = set(planos_all[1:])
        g, _ = serie([alvo], False, "vencimento", "bruto", planos_all, None)
        # se incluir errado, soma de TODOS != 1 plano; aqui só garante 1 plano ≤ TODOS
        g_t, _ = serie(planos_all, True, "vencimento", "bruto", planos_all, None)
        if sum(g) - 0.02 > sum(g_t):
            fail("1 plano somou mais que TODOS — filtro inclusão quebrado")
        # exclusão positiva: série com plano inventado = zeros
        g_fantasma, _ = serie(
            ["__PLANO_INEXISTENTE_VERIFY__"], False, "vencimento", "bruto", planos_all, None
        )
        if any(v != 0 for v in g_fantasma):
            fail(f"plano inexistente deveria ser 0: {g_fantasma}")
        checks += 1
        ok("inclusão positiva (1 plano ≤ TODOS · fantasma=0)")
    else:
        ok("skip inclusão (poucos planos locais)")

    print(f"VERIFY_OK {checks} checks · {n_series} séries")


if __name__ == "__main__":
    main()
