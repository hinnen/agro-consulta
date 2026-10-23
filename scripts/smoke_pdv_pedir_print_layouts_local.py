"""Smoke PDV-PEDIR-PRINT-3 — página PDV + modal 3 layouts (PIN 9973).

  set AGRO_PIN_TESTE=9973
  python scripts/smoke_pdv_pedir_print_layouts_local.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from produtos.caixa_util import operador_label_de_pin

PIN = os.environ.get("AGRO_PIN_TESTE", "9973")
ok_n = 0
fail_n = 0


def check(cond: bool, msg: str) -> None:
    global ok_n, fail_n
    if cond:
        ok_n += 1
        print("  OK ", msg)
    else:
        fail_n += 1
        print("  FAIL", msg)


def main() -> int:
    print("SMOKE PDV-PEDIR-PRINT-3 local")
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("-is_superuser", "id").first()
    check(u is not None, f"user Django ativo ({getattr(u, 'username', None)})")

    label = None
    try:
        label = operador_label_de_pin(PIN)
    except Exception as e:
        print("  note pin:", e)
    check(
        bool(label and (label[0] if isinstance(label, tuple) else label)),
        f"PIN {PIN} resolve ({label})",
    )

    js = (ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js").read_text(encoding="utf-8")
    check("function abrirEscolhaImpressao" in js, "JS tem popup escolha")
    check("montarHtmlCupomPedidos" in js and "size:80mm auto" in js, "cupom 80mm")
    check("montarHtmlA4Separacao" in js and "size:A4" in js, "folha A4")
    check("montarHtmlEtiquetas40x40" in js and "size:40mm 40mm" in js, "etiqueta 40×40")
    check("LINHAS_POR_ETQ = 3" in js, "3 produtos/etiqueta")
    check("Etiquetas 53" not in js and "imprimirEtiquetasSeparacao53" not in js, "sem legado 53")
    check("executarImpressaoEscolhida" in js, "executa layout escolhido")

    c = Client(HTTP_HOST="127.0.0.1")
    if u:
        c.force_login(u)

    r = c.get("/pdv/")
    body = r.content.decode("utf-8", "replace")
    check(r.status_code == 200, f"PDV HTTP {r.status_code}")
    check("pdv-pedir-loja-overlay" in body, "overlay Pedir loja na página")
    check("pdv-pedir-loja-print" in body, "modal Como imprimir? na página")
    check('data-pl-print="cupom80"' in body, "opção cupom80 no HTML")
    check('data-pl-print="a4"' in body, "opção a4 no HTML")
    check('data-pl-print="etq40"' in body, "opção etq40 no HTML")
    check("Cupom 80 mm" in body, "rótulo Cupom 80 mm")
    check("Folha A4" in body, "rótulo Folha A4")
    check("Etiqueta 40" in body, "rótulo Etiqueta 40")
    check("pdv_pedir_loja.js" in body, "script Pedir loja carregado")
    check("pdv-pedir-loja-imprimir-todos" in body, "botão Imprimir todos")

    # gera HTML dos 3 layouts em memória (sem print real)
    # simulamos contrato mínimo das funções via trechos
    check("SEPARAÇÃO" in js and "PEDIR LOJA #" in js, "cupom tem cabeçalho separação")
    check("Separação · Pedir loja" in js or "Separação A4" in js, "A4 tem título")
    check("Etiquetas 40" in js or "40×40" in js, "40×40 no título/status")

    print()
    print(f"SMOKE PRINT-3 {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


if __name__ == "__main__":
    sys.exit(main())
