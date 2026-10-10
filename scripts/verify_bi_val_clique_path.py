"""
Verificacao BI-VAL-CLIQUE — clique do card Validade abre Todas + vencidos.
  .venv\\Scripts\\python.exe scripts/verify_bi_val_clique_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import RequestFactory
from django.urls import reverse

PASS = FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    (ok if cond else bad)(msg)


def _option_selected(html: str, select_name: str, value: str) -> bool:
    """Detecta <option value="X" ... selected> dentro do <select name=...>."""
    m = re.search(
        rf'<select[^>]*name="{re.escape(select_name)}"[^>]*>(.*?)</select>',
        html,
        re.I | re.S,
    )
    if not m:
        return False
    block = m.group(1)
    for opt in re.finditer(r"<option\b([^>]*)>", block, re.I):
        attrs = opt.group(1)
        vm = re.search(r'\bvalue="([^"]*)"', attrs, re.I)
        if not vm or vm.group(1) != value:
            continue
        if re.search(r"\bselected\b", attrs, re.I):
            return True
    return False


def _checkbox_checked(html: str, name: str) -> bool:
    return bool(
        re.search(
            rf'<input[^>]*name="{re.escape(name)}"[^>]*\bchecked\b',
            html,
            re.I,
        )
    )


def test_fonte_dashboard() -> None:
    print("\n== Link no dashboard ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    block = src.split('"relatorios_validade_url":', 1)[-1][:500]
    check("+ \"?loja=todas\"" in block or '+ "?loja=todas"' in block, "URL com loja=todas")
    check("validade_vencidos_n > 0" in block, "status=vencido quando ha vencidos")
    check("deposito_filtro" not in block.split("relatorios_validade_url")[0][-200:], "nao usa deposito_filtro no link")
    check("deposito_filtro" not in block[:280], "link nao concatena loja do filtro Numeros")


def test_relatorio_todas_vencido() -> None:
    print("\n== Relatorio loja=todas + status=vencido ==")
    from produtos.views import relatorios_validade

    rf = RequestFactory()
    url = reverse("relatorios_validade") + "?loja=todas&status=vencido"
    req = rf.get(url, HTTP_HOST="127.0.0.1")
    resp = relatorios_validade(req)
    check(resp.status_code == 200, f"HTTP 200 ({resp.status_code})")
    html = resp.content.decode("utf-8", errors="replace")
    check(_option_selected(html, "loja", "todas"), "select loja=Todas (C+V)")
    check(_option_selected(html, "status", "vencido"), "select status=Vencidos")
    check(not _checkbox_checked(html, "somente_com_estoque"), "checkbox estoque desmarcado (Todas)")


def test_relatorio_centro_forca_estoque() -> None:
    print("\n== Relatorio loja=centro (contraste) ==")
    from produtos.views import relatorios_validade

    rf = RequestFactory()
    req = rf.get(
        reverse("relatorios_validade") + "?loja=centro&status=vencido",
        HTTP_HOST="127.0.0.1",
    )
    resp = relatorios_validade(req)
    html = resp.content.decode("utf-8", errors="replace")
    check(_option_selected(html, "loja", "centro"), "select loja=Centro")
    check(_checkbox_checked(html, "somente_com_estoque"), "centro forca estoque da loja")


def main() -> int:
    print("VERIFY BI-VAL-CLIQUE path")
    test_fonte_dashboard()
    test_relatorio_todas_vencido()
    test_relatorio_centro_forca_estoque()
    print(f"\n== RESULTADO {PASS}/{PASS + FAIL} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK bi_val_clique")
    return 0


if __name__ == "__main__":
    sys.exit(main())
