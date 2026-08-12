#!/usr/bin/env python
"""Prova CP-CAL-PG — calendário contas a pagar no Postgres. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import calendar as cal_mod
import json
import os
import re
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8").replace("\r\n", "\n")


def _fn_body(src: str, name: str) -> str:
    m = re.search(rf"^def {re.escape(name)}\b", src, re.M)
    if not m:
        return ""
    rest = src[m.start() :]
    body_start = re.search(r"\)\s*(?:->[^:]+)?:\n", rest)
    if not body_start:
        return ""
    after = rest[body_start.end() :]
    nxt = re.search(r"^def \w+", after, re.M)
    end = body_start.end() + (nxt.start() if nxt else len(after))
    return rest[:end]


def _grid(ano: int, mes: int) -> tuple[date, date, date, date]:
    ultimo = cal_mod.monthrange(ano, mes)[1]
    mes_ini = date(ano, mes, 1)
    mes_fim = date(ano, mes, ultimo)
    grid_ini = mes_ini - timedelta(days=(mes_ini.weekday() + 1) % 7)
    grid_fim = mes_fim + timedelta(days=(6 - ((mes_fim.weekday() + 1) % 7)) % 7)
    return mes_ini, mes_fim, grid_ini, grid_fim


def main() -> None:
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.utils import timezone

    from produtos.agro_fonte_config import agro_financeiro_usa_postgres, agro_mongo_erp_desligado
    from produtos.lancamentos_financeiro_pg_analytics_util import (
        financeiro_calendario_contas_pagar_dias_pg,
        lancamentos_contas_pagar_totais_diarios_pg,
    )
    from produtos.lancamentos_financeiro_pg_util import (
        _dec2,
        contas_pagar_buscar_pagina_pg,
        contas_pagar_montar_qs,
        dedup_titulos,
    )

    print("== Markers path ==")
    views = _read("produtos/views.py")
    analytics = _read("produtos/lancamentos_financeiro_pg_analytics_util.py")
    urls = _read("produtos/urls.py")
    html = _read("produtos/templates/produtos/lancamentos_contas_pagar_calendario.html")

    api_body = _fn_body(views, "api_lancamentos_contas_pagar_calendario")
    pg_fn = _fn_body(analytics, "financeiro_calendario_contas_pagar_dias_pg")
    tot_fn = _fn_body(analytics, "lancamentos_contas_pagar_totais_diarios_pg")

    check("fn_api_calendario", bool(api_body), f"chars={len(api_body)}")
    check("api_despacha_pg", "agro_financeiro_usa_postgres()" in api_body)
    check(
        "api_chama_pg_fn",
        "financeiro_calendario_contas_pagar_dias_pg" in api_body,
    )
    check(
        "api_mongo_so_else",
        "else:" in api_body and "obter_conexao_mongo" in api_body,
    )
    check("fn_calendario_pg", bool(pg_fn), f"chars={len(pg_fn)}")
    check("fn_totais_diarios_pg", bool(tot_fn), f"chars={len(tot_fn)}")
    check("pg_usa_dedup", "dedup_titulos" in tot_fn and "contas_pagar_montar_qs" in tot_fn)
    check("pg_status_abertos", 'status="abertos"' in tot_fn)
    check(
        "url_api",
        "api/lancamentos/contas-pagar/calendario/" in urls
        and "api_lancamentos_contas_pagar_calendario" in urls,
    )
    check(
        "url_pagina",
        "lancamentos/contas-pagar/calendario/" in urls
        and "lancamentos_contas_pagar_calendario" in urls,
    )
    check("ui_api_path", 'API = "/api/lancamentos/contas-pagar/calendario/"' in html)
    check("ui_mostra_erro", "sv-cal-erro" in html and "showErro" in html)
    check("ui_metric_pagar", "sv-cal-m-pagar" in html and "r.pagar" in html)

    print("== Fonte financeiro ==")
    check("financeiro_pg_ativo", agro_financeiro_usa_postgres() is True)
    check("mongo_erp_desligado_ou_pg", agro_mongo_erp_desligado() or agro_financeiro_usa_postgres())

    hoje = timezone.localdate()
    ano, mes = hoje.year, hoje.month
    mes_ini, mes_fim, grid_ini, grid_fim = _grid(ano, mes)

    print(f"== Runtime grade {ano}-{mes:02d} ({grid_ini}..{grid_fim}) ==")
    out = financeiro_calendario_contas_pagar_dias_pg(
        grid_ini=grid_ini, grid_fim=grid_fim, dias_media_vendas=30
    )
    dias = out.get("dias") or {}
    totais = out.get("totais") or {}
    meta = out.get("meta") or {}

    check("sem_erro", not out.get("erro"), str(out.get("erro") or ""))
    check("meta_fonte_postgres", meta.get("fonte") == "postgres")
    n_dias = (grid_fim - grid_ini).days + 1
    check("dias_completos", len(dias) == n_dias, f"{len(dias)}/{n_dias}")
    check("totais_igual_pagar", totais == {k: v.get("pagar") for k, v in dias.items()})

    # Independente: somar abertos por vencimento (mesma query da lista)
    esperado: dict[str, float] = {}
    titulos = dedup_titulos(
        list(
            contas_pagar_montar_qs(
                status="abertos",
                vencimento_de=grid_ini,
                vencimento_ate=grid_fim,
            )
        )
    )
    for t in titulos:
        dkey = t.data_vencimento
        if dkey is None or dkey < grid_ini or dkey > grid_fim:
            continue
        rest = float(_dec2(t.valor_restante))
        if rest <= 0.02:
            continue
        k = dkey.isoformat()
        esperado[k] = round(esperado.get(k, 0.0) + rest, 2)

    mapa = lancamentos_contas_pagar_totais_diarios_pg(
        vencimento_de=grid_ini, vencimento_ate=grid_fim
    )
    check(
        "mapa_igual_lista_dedup",
        {k: round(v, 2) for k, v in mapa.items()} == esperado,
        f"mapa={len(mapa)} esperado={len(esperado)}",
    )

    diffs = []
    for k, exp in esperado.items():
        got = round(float((dias.get(k) or {}).get("pagar") or 0), 2)
        if abs(got - exp) > 0.02:
            diffs.append(f"{k}: cal={got} lista={exp}")
    for k, row in dias.items():
        if k in esperado:
            continue
        got = round(float(row.get("pagar") or 0), 2)
        if got > 0.02:
            diffs.append(f"{k}: cal={got} lista=0")
    check("calendario_bate_lista_por_dia", not diffs, "; ".join(diffs[:5]))

    # Mês civil (só dias do mês visível) vs página lista
    soma_mes_cal = 0.0
    for d_ord in range(mes_ini.toordinal(), mes_fim.toordinal() + 1):
        k = date.fromordinal(d_ord).isoformat()
        soma_mes_cal += float((dias.get(k) or {}).get("pagar") or 0)
    soma_mes_cal = round(soma_mes_cal, 2)

    _rows, _total, tot_lista = contas_pagar_buscar_pagina_pg(
        status="abertos",
        vencimento_de=mes_ini,
        vencimento_ate=mes_fim,
        page=1,
        page_size=1,
        skip_totais=False,
        limite_max=25000,
    )
    saldo_lista = round(float((tot_lista or {}).get("saldo_aberto") or 0), 2)
    check(
        "mes_civil_bate_lista_saldo",
        abs(soma_mes_cal - saldo_lista) <= 0.05,
        f"cal={soma_mes_cal} lista={saldo_lista}",
    )
    dias_com_pagar = sum(1 for v in dias.values() if float(v.get("pagar") or 0) > 0.02)
    check("tem_dias_com_pagar", dias_com_pagar > 0, f"n={dias_com_pagar}")

    # Contrato de saldo do dia (vendas/previsão − pagar)
    sample_ok = True
    for k, row in list(dias.items())[:10]:
        pagar = Decimal(str(row.get("pagar") or 0))
        if row.get("vendas") is not None:
            ent = Decimal(str(row.get("vendas") or 0))
        else:
            ent = Decimal(str(row.get("previsao_vendas") or 0))
        esperado_liq = float((ent - pagar).quantize(Decimal("0.01")))
        got_liq = float(row.get("liquido_dia") or 0)
        if abs(esperado_liq - got_liq) > 0.02:
            sample_ok = False
            break
    check("liquido_dia_formula", sample_ok)

    print("== HTTP API (Django Client) ==")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).order_by("id").first()
    if user is None:
        user = User.objects.filter(is_staff=True).order_by("id").first()
    check("usuario_teste", user is not None, str(getattr(user, "username", None) or ""))
    if user is not None:
        c = Client()
        c.force_login(user)
        r = c.get(
            f"/api/lancamentos/contas-pagar/calendario/?ano={ano}&mes={mes}",
            HTTP_HOST="127.0.0.1",
        )
        check("api_status_200", r.status_code == 200, f"status={r.status_code}")
        try:
            payload = r.json()
        except Exception:
            payload = {}
            try:
                payload = json.loads(r.content.decode("utf-8"))
            except Exception:
                payload = {}
        check("api_sem_erro", not payload.get("erro"), str(payload.get("erro") or ""))
        api_dias = payload.get("dias") or {}
        check("api_tem_dias", len(api_dias) == n_dias, f"{len(api_dias)}/{n_dias}")
        api_pagar = sum(float((v or {}).get("pagar") or 0) for v in api_dias.values())
        check("api_soma_pagar_gt0", api_pagar > 0, f"soma={round(api_pagar, 2)}")
        # Não pode ser o banner de legado / Mongo
        body_txt = (r.content or b"").decode("utf-8", errors="replace").lower()
        check(
            "api_sem_mongo_indisponivel",
            "mongo indispon" not in body_txt and "serviço legado" not in body_txt,
        )
        page = c.get("/lancamentos/contas-pagar/calendario/", HTTP_HOST="127.0.0.1")
        check("pagina_status_200", page.status_code == 200, f"status={page.status_code}")

    total = OK + len(FAIL)
    print()
    if FAIL:
        print("FAILS:")
        for f in FAIL:
            print(f"  - {f}")
        print(f"VERIFY_FAIL {OK}/{total}")
        sys.exit(1)
    print(f"VERIFY_OK {OK}/{total}")


if __name__ == "__main__":
    main()
