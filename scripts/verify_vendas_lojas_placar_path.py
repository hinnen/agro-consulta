"""
Prova — placar de vendas Centro × Vila.

Path:
  /vendas/lojas/
    -> vendas_lojas_placar_view
    -> vendas_placar_util.resolver_periodo (padrão = dia atual)
    -> _dashboard_mongo_vendas_serie (mesma fonte do BI)
    -> Centro + Vila + soma

  python scripts/verify_vendas_lojas_placar_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

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


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views.py")
    tpl = _read("produtos/templates/produtos/vendas_lojas_placar.html")
    hub = _read("produtos/templates/produtos/relatorios_hub.html")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    util = _read("produtos/vendas_placar_util.py")
    vmod = _read("produtos/views_vendas_placar.py")

    check("url", "vendas_lojas_placar" in urls and "vendas/lojas/" in urls)
    check("view", "def vendas_lojas_placar_view" in vmod)
    check("util_padrao_dia", 'return _ALIAS.get(key, "dia")' in util)
    check("tpl_centro", "Centro" in tpl)
    check("tpl_vila", "Vila Elias" in tpl)
    check("tpl_soma", "Soma das duas lojas" in tpl)
    check("tpl_filtros", ">Dia<" in tpl and ">Semana<" in tpl and ">Mês<" in tpl and ">Ano<" in tpl)
    check("home_atalho", "Vendas Centro × Vila" in views)
    check("bi_menu", "vendas_lojas_placar" in dash)
    check("relatorios", "vendas_lojas_placar" in hub)
    check("fonte_bi", "_dashboard_mongo_vendas_serie" in vmod)


def test_periodo() -> None:
    print("== Período ==")
    from produtos.vendas_placar_util import resolver_periodo

    hoje = date(2026, 8, 15)
    dia = resolver_periodo(None, None, hoje)
    check("padrao_dia", dia["periodo"] == "dia")
    check("padrao_hoje", dia["data_ini"] == hoje and dia["data_fim"] == hoje)
    check("label_hoje", "Hoje" in dia["label"])

    sem = resolver_periodo("semana", hoje, hoje)
    check("semana_seg", sem["data_ini"] == date(2026, 8, 10))
    check("semana_dom", sem["data_fim"] == date(2026, 8, 16))

    mes = resolver_periodo("mes", hoje, hoje)
    check("mes_ini", mes["data_ini"] == date(2026, 8, 1))
    check("mes_fim", mes["data_fim"] == date(2026, 8, 31))

    ano = resolver_periodo("ano", hoje, hoje)
    check("ano_ini", ano["data_ini"] == date(2026, 1, 1))
    check("ano_fim", ano["data_fim"] == date(2026, 12, 31))

    ontem = resolver_periodo("dia", hoje - timedelta(days=1), hoje)
    check("ontem", "Ontem" in ontem["label"])
    check("ontem_avanca", ontem["pode_avancar"] is True)
    check("hoje_nao_avanca", dia["pode_avancar"] is False)


def test_runtime() -> None:
    print("== Runtime Django ==")
    from unittest.mock import patch

    import django

    django.setup()
    from django.template.loader import get_template
    from django.test import RequestFactory
    from django.urls import reverse

    from produtos.views_vendas_placar import json_placar, montar_contexto_placar

    check("reverse", reverse("vendas_lojas_placar").rstrip("/").endswith("vendas/lojas"))
    check("template", bool(get_template("produtos/vendas_lojas_placar.html")))

    fake = {
        "vendas_por_loja": [
            {"loja": "Centro", "total": 100.50},
            {"loja": "Vila Elias", "total": 40.25},
        ]
    }
    factory = RequestFactory()
    with patch("produtos.views_vendas_placar._dashboard_mongo_vendas_serie", return_value=fake):
        ctx = montar_contexto_placar(factory.get("/vendas/lojas/"))
    check("ctx_periodo_dia", ctx["periodo"] == "dia")
    check("ctx_centro", ctx["centro"] == Decimal("100.50"))
    check("ctx_vila", ctx["vila"] == Decimal("40.25"))
    check("ctx_soma", ctx["soma"] == Decimal("140.75"))
    check("fmt_br", ctx["soma_fmt"] == "140,75")
    payload = json_placar(ctx)
    check("json_ok", payload.get("ok") is True and payload.get("soma") == "140.75")


def main() -> int:
    test_arquivos()
    test_periodo()
    try:
        test_runtime()
    except Exception as exc:
        check("runtime", False, str(exc))
    print()
    print(f"OK {len(oks)}  FAIL {len(fails)}")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
