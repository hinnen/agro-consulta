# -*- coding: utf-8 -*-
"""Prova detalhada path BI-DEVOL-DIA + BI-DEVOL-PLANILHA.

Cobre: qs PDV, abatimento no dia do evento, merge planilha (PDV manda),
cache v7/v10, PIN 9973, HTTP home/BI, healthz se o runserver estiver no ar.

  python scripts/verify_bi_devolucao_dia.py
"""
from __future__ import annotations

import os
import sys
import urllib.request
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global ok, fail
    msg = name + ((" -- " + detail) if detail else "")
    safe = msg.encode("ascii", "replace").decode("ascii")
    if cond:
        ok += 1
        print("  OK ", safe)
    else:
        fail += 1
        print(" FAIL", safe)


def main() -> None:
    print("=== BI-DEVOL-PLANILHA / BI-DEVOL-DIA detalhado ===")

    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    lojas = (ROOT / "produtos/vendas_lojas_util.py").read_text(encoding="utf-8")
    util = (ROOT / "produtos/dashboard_pdv_devolucao_util.py").read_text(encoding="utf-8")
    hist = (ROOT / "produtos/dashboard_vendas_historico_util.py").read_text(encoding="utf-8")

    i_qs = views.find("def _dashboard_vendas_qs_pdv_periodo")
    i_ser = views.find("def _dashboard_vendas_serie_pdv")
    bloco_qs = views[i_qs:i_ser]
    check("qs_nao_some_devolvida", "devolvida_em__isnull=True" not in bloco_qs)
    check("cache_pdv_v7", "dash:mvs:v7:pdv:" in views)
    check("serie_abate", "abatimento_devolucoes_por_dia" in views)
    check("lojas_abate", "abatimento_devolucoes_totais_loja" in lojas)
    check("util_evento", "DevolucaoVendaAgro" in util)
    check("util_legado", "devolvida_em__isnull=False" in util)
    check("sem_max_planilha", "por_dia[k] = round(max(vp, vd), 2)" not in hist)
    check("fn_merge", "def merge_planilha_pdv_por_dia" in hist)
    check("usa_merge", "por_dia = merge_planilha_pdv_por_dia(plan, pdv_por_dia)" in hist)
    check("cache_meta_v10", "dash:mvs:v10:meta:" in hist)
    check("views_chama_merged", "dashboard_vendas_serie_meta_merged" in views)
    check("hoje_pdv_usa_lojas", "vendas_lojas_total_deposito" in views[views.find("def _dashboard_vendas_hoje_pdv"):views.find("def _dashboard_devolucoes_periodo")])
    check("dia_agro_usa_lojas", "vendas_lojas_total_deposito" in views[views.find("def _dashboard_mongo_total_por_dia_vendas_agro"):views.find("def _dashboard_invalidar_cache_vendas_serie")])
    check("periodo_usa_lojas", "vendas_lojas_total_deposito(data_ini, data_fim, deposito_filtro)" in views)
    check("lojas_helper", "def vendas_lojas_total_deposito" in lojas)

    import django

    django.setup()

    from produtos.dashboard_pdv_devolucao_util import aplicar_abatimento_por_dia
    from produtos.dashboard_vendas_historico_util import merge_planilha_pdv_por_dia

    ab = aplicar_abatimento_por_dia({"2026-09-01": 25.0}, {"2026-09-01": Decimal("40.00")})
    check("math_hoje_menos_dev", ab["2026-09-01"] == -15.0)
    ab2 = aplicar_abatimento_por_dia({"2026-08-31": 140.0}, {})
    check("math_dia_venda_intacto", ab2["2026-08-31"] == 140.0)
    ab3 = aplicar_abatimento_por_dia({"2026-09-01": 1431.26}, {"2026-09-01": Decimal("360.00")})
    check("math_caso_loja_centro", abs(ab3["2026-09-01"] - 1071.26) < 0.001)

    m = merge_planilha_pdv_por_dia({"2026-09-01": 1431.26}, {"2026-09-01": 1071.26})
    check("merge_pdv_vence_planilha", m["2026-09-01"] == 1071.26)
    m2 = merge_planilha_pdv_por_dia({"2026-01-10": 50.0}, {})
    check("merge_planilha_so_buraco", m2["2026-01-10"] == 50.0)
    m3 = merge_planilha_pdv_por_dia({}, {"2026-09-01": 1196.56})
    check("merge_so_pdv", m3["2026-09-01"] == 1196.56)
    m4 = merge_planilha_pdv_por_dia({"2026-09-01": 0.0}, {"2026-09-01": -15.0})
    check("merge_pdv_negativo", m4["2026-09-01"] == -15.0)

    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.urls import reverse
    from produtos.caixa_util import validar_pin_operador, _perfil_usuario_por_pin

    pin_ok, pin_msg = validar_pin_operador("9973")
    perfil = _perfil_usuario_por_pin("9973")
    check("pin_9973_valido", pin_ok, pin_msg)
    check("pin_9973_tem_perfil", perfil is not None)

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    if user:
        with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
            c = Client()
            c.force_login(user)
            home = c.get(reverse("home"), follow=True)
            dash = c.get(reverse("dashboard_gerencial"), follow=True)
            vl = c.get("/vendas/lojas/", follow=True)
        bh = home.content.decode("utf-8", "replace")
        bd = dash.content.decode("utf-8", "replace")
        check("http_home_200", home.status_code == 200, str(home.status_code))
        check("http_dash_200", dash.status_code == 200, str(dash.status_code))
        check("http_lojas_200", vl.status_code == 200, str(vl.status_code))
        check("http_home_bi", "dashboard" in bh.lower() or "kpi" in bh.lower() or "vendas" in bh.lower())
        check("http_nao_login_home", 'name="username"' not in bh or "dashboard" in bh.lower())
    else:
        check("http_user", False, "sem usuario Django")

    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=4) as resp:
            hz = resp.status
        check("live_healthz", hz == 200, str(hz))
        with urllib.request.urlopen("http://127.0.0.1:8000/", timeout=8) as resp:
            live = resp.read().decode("utf-8", "replace")
        check("live_home_ou_login", resp.status in (200, 302) or "html" in live.lower())
    except Exception as exc:
        check("live_healthz", False, str(exc)[:80])

    print("")
    print("%s OK / %s FAIL" % (ok, fail))
    raise SystemExit(1 if fail else 0)


if __name__ == "__main__":
    main()
