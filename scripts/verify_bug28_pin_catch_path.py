# -*- coding: utf-8 -*-
"""Prova BUG-28-PIN-CATCH: Point já cobrou → PIN reabre e tenta gravar de novo.

  set AGRO_PIN_TESTE=9973
  python scripts/verify_bug28_pin_catch_path.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("AGRO_PIN_TESTE", "9973")

fails = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global fails
    mark = "OK" if cond else "FAIL"
    if not cond:
        fails += 1
    extra = f" — {detail}" if detail else ""
    print(f"  {mark}  {name}{extra}")


def main() -> int:
    print("== BUG-28-PIN-CATCH ==")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    pin_html = (ROOT / "produtos/templates/produtos/_screensaver_pin.html").read_text(encoding="utf-8")
    caixa = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")

    fin = wiz.split("function confirmSaleFinalizarMpPointOrders", 1)[1]
    nxt = fin.find("\n    function ")
    fin = fin[:nxt] if nxt > 0 else fin
    check("marca precisa_pin na falha do finalizar", "eFin.precisaPin = !!(finRes.data && finRes.data.precisa_pin);" in fin)
    check("marca pagamento já efetivado", "eFin.pagamentoEfetivado = !!(finRes.data && finRes.data.pagamento_efetivado);" in fin)
    check("abre PIN e retenta gravar o Point", "confirmSaleFinalizarMpPointOrders(withPrint, opts);" in fin)
    check("título: máquina já cobrou", "máquina já cobrou" in fin)
    check("não chama cobrança nova no catch", "confirmSaleMercadoPagoPoint(" not in fin)
    check("toast de PIN da venda comum retenta confirmar", "confirmSaleProsseguir(withPrint);" in wiz)
    check("aviso Point não cobra de novo", "não envie outro valor" in wiz)
    check("API finalizar devolve precisa_pin", '"precisa_pin": True' in views)
    check("API finalizar devolve pagamento_efetivado", '"pagamento_efetivado": True' in views)
    msg_m = re.search(r'MSG_PIN_OPERADOR_OBRIGATORIO\s*=\s*\(\s*"([^"]+)"', caixa, re.S)
    if not msg_m:
        msg_m = re.search(r'MSG_PIN_OPERADOR_OBRIGATORIO\s*=\s*"([^"]+)"', caixa)
    msg = msg_m.group(1) if msg_m else ""
    check("mensagem de PIN existe", bool(msg), msg[:80])
    pede = bool(
        re.search(r"Identifique-se com o PIN", msg, re.I)
        or (re.search(r"modo descanso", msg, re.I) and re.search(r"PIN", msg))
    )
    check("mensagem casa no teclado do PIN", pede, msg[:90])
    check("teclado só abre se a frase é de PIN", "gmSspinErroPedePin" in pin_html and "openLock(true)" in pin_html)

    os.environ.setdefault("SECRET_KEY", "verify-bug28-pin-catch")
    os.environ.setdefault("DEBUG", "True")
    import django

    django.setup()
    from django.test import RequestFactory

    from produtos.caixa_util import PinOperadorObrigatorioError
    from produtos.views_mp_point import api_pdv_mp_point_finalizar

    req = RequestFactory().post("/api/pdv/mp-point/finalizar/", data=b"{}", content_type="application/json")
    req.session = {}
    with patch(
        "produtos.views_mp_point._api_pdv_mp_point_finalizar_impl",
        side_effect=PinOperadorObrigatorioError("Identifique-se com o PIN (modo descanso) antes de continuar."),
    ):
        resp = api_pdv_mp_point_finalizar(req)
    data = json.loads(resp.content.decode("utf-8"))
    check("HTTP 403 quando falta PIN", resp.status_code == 403, str(resp.status_code))
    check("JSON precisa_pin", data.get("precisa_pin") is True)

    print()
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK 14/14")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
