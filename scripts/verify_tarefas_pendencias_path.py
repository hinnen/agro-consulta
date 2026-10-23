"""Prova rápida — hub Vendas/Tarefas + app tarefas."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OK = 0
FAIL = 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f"  FAIL {label}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("VERIFY TAREFAS-PENDENCIAS")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    settings = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    check("app_installed", "tarefas.apps.TarefasConfig" in settings)
    check("url_hub", "vendas_lojas_hub" in urls)
    check("url_painel", "vendas/lojas/painel/" in urls)
    check("url_tarefas", "vendas/lojas/tarefas/" in urls and "include('tarefas.urls')" in urls)
    check("models", (ROOT / "tarefas/models.py").is_file())
    check("seed_cmd", (ROOT / "tarefas/management/commands/seed_tarefas_agro_mais.py").is_file())
    check("tpl_hub", (ROOT / "produtos/templates/produtos/vendas_lojas_hub.html").is_file())
    check("tpl_lista", (ROOT / "tarefas/templates/tarefas/lista.html").is_file())
    check("tpl_pin", (ROOT / "tarefas/templates/tarefas/pin.html").is_file())

    import os
    import django

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    django.setup()
    from django.test import Client, override_settings
    from django.urls import reverse
    from tarefas.models import TarefaAgro

    check("seed_count", TarefaAgro.objects.filter(seed_key__gt="").count() >= 8)
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()
        r = c.get("/vendas/lojas/")
        body = r.content.decode("utf-8", "replace")
        check("hub_200", r.status_code == 200, str(r.status_code))
        check("hub_vendas_btn", "Vendas" in body and reverse("vendas_lojas_resumo") in body)
        check("hub_tarefas_btn", "Tarefas" in body and "/vendas/lojas/tarefas/" in body)
        r2 = c.get("/vendas/lojas/tarefas/")
        check("tarefas_pede_pin", r2.status_code in (302, 301), str(r2.status_code))
        r3 = c.get("/vendas/lojas/tarefas/pin/")
        check("pin_200", r3.status_code == 200, str(r3.status_code))
    print(f"\nVERIFY {'OK' if FAIL == 0 else 'FAIL'} {OK}/{OK + FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
