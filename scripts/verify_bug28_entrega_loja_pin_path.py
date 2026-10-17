# -*- coding: utf-8 -*-
"""Prova BUG-28 entrega+pagamento na loja: venda não zera PIN antes de registrar entrega.

  python scripts/verify_bug28_entrega_loja_pin_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

fails = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global fails
    mark = "OK" if cond else "FAIL"
    if not cond:
        fails += 1
    extra = f" — {detail}" if detail else ""
    print(f"  {mark}  {name}{extra}")


def main() -> int:
    print("== BUG-28 entrega loja PIN ==")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    mp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    resp_i = views.find("def _resposta_venda")
    check("tem _resposta_venda", resp_i >= 0)
    resp = views[resp_i : resp_i + 900] if resp_i >= 0 else ""
    check(
        "ERP não zera PIN na resposta da venda",
        "expirar_operador_pdv_fresco" not in resp,
    )
    check(
        "comentário bug #28 no ERP",
        "Bug #28" in resp or "entrega+pagamento" in resp or "api_entrega_registrar" in resp,
    )

    # api_entrega_registrar ainda exige PIN (operador na entrega), mas fresco segue vivo.
    ent_i = views.find("op_label, err_pin_op = exigir_operador_pin_request(request, body")
    check("entrega registrar exige operador", ent_i > 0)
    ent_bloco = views[ent_i : ent_i + 280] if ent_i > 0 else ""
    check("entrega 403 marca precisa_pin", '"precisa_pin": True' in ent_bloco)

    erp_pin = views.find("_, err_pin_op = exigir_operador_pin_request(request, data)")
    erp_bloco = views[erp_pin : erp_pin + 220] if erp_pin > 0 else ""
    check("ERP 403 marca precisa_pin", '"precisa_pin": True' in erp_bloco)

    helper = mp.split("def _mp_point_expirar_pin_apos_venda", 1)[-1][:500]
    check("Point helper não chama expirar", "expirar_operador_pdv_fresco(request)" not in helper)

    fin_mp = wiz.split("function confirmSaleFinalizarMpPointOrders", 1)[-1]
    nxt = fin_mp.find("\n    function ")
    fin_mp = fin_mp[:nxt] if nxt > 0 else fin_mp
    check(
        "Point success expira PIN no fim (cliente)",
        "gmSspinExpirarFrescoAposVenda" in fin_mp,
    )
    check(
        "venda comum ainda registra entrega após ERP",
        "apiEntregaRegistrar" in wiz and "state.entrega.ativa" in wiz,
    )
    check(
        "pagamento na loja libera frete e vai pro pagamento",
        "entregaFreteLiberadoPagamento: true" in wiz
        and "PIN para seguir com a entrega" in wiz,
    )

    print()
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK 10/10")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
