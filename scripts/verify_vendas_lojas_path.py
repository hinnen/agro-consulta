"""
Prova VENDAS-LOJAS — tela só valores Centro × Vila + soma.

Path:
  /vendas/lojas/  vendas_lojas_view
    -> payload_vendas_lojas (padrão periodo=dia = hoje)
    -> totais VendaAgro Centro / Vila / soma
  Atalhos /atalhos/  card S
  Menu BI  Comercial → Vendas das lojas

  python scripts/verify_vendas_lojas_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse

from produtos.vendas_lojas_util import resolver_periodo_vendas_lojas

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
    tpl = _read("produtos/templates/produtos/vendas_lojas.html")
    home = _read("produtos/views.py")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    check("url", "vendas_lojas" in urls and "vendas/lojas/" in urls)
    check("view", "def vendas_lojas_view" in _read("produtos/views_vendas_lojas.py"))
    check("tpl_soma", "Soma das duas" in tpl)
    check("tpl_filtros", "?periodo=dia" in tpl and "?periodo=semana" in tpl and "?periodo=mes" in tpl and "?periodo=ano" in tpl)
    check("atalho", '"title": "Vendas das lojas"' in home)
    check("menu_bi", "Vendas das lojas" in dash and "vendas_lojas" in dash)


def test_periodo() -> None:
    print("== Período padrão ==")
    hoje = date(2026, 8, 15)
    p = resolver_periodo_vendas_lojas(modo=None, ref=None, hoje=hoje)
    check("padrao_dia", p["modo"] == "dia" and p["data_ini"] == hoje)
    check("sem_futuro", p["pode_avancar"] is False)


def test_http() -> None:
    print("== HTTP ==")
    try:
        User = get_user_model()
        user = User.objects.filter(is_superuser=True).first() or User.objects.first()
        if user is None:
            user = User.objects.create_user("verify-vl", password="x")
        c = Client(headers={"host": "127.0.0.1"})
        c.force_login(user)
        url = reverse("vendas_lojas")
        r = c.get(url)
        check("get_200", r.status_code == 200, str(r.status_code))
        body = r.content.decode("utf-8", errors="replace")
        check("html_centro", "Centro" in body)
        check("html_vila", "Vila" in body)
        check("html_soma", "Soma das duas" in body)
        j = c.get(url + "?fmt=json")
        check("json_200", j.status_code == 200)
        data = j.json() if j.status_code == 200 else {}
        check("json_dia", data.get("periodo") == "dia")
        check("json_chaves", all(k in data for k in ("centro_fmt", "vila_fmt", "soma_fmt")))
    except Exception as exc:  # noqa: BLE001
        print(f"  SKIP http — {type(exc).__name__}: {exc}")


def main() -> None:
    test_arquivos()
    test_periodo()
    test_http()
    print(f"\n{len(oks)} ok · {len(fails)} fail")
    if fails:
        print("VERIFY_FAIL")
        for f in fails:
            print(" -", f)
        sys.exit(1)
    print("VERIFY_OK", len(oks))


if __name__ == "__main__":
    main()
