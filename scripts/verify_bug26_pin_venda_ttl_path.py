#!/usr/bin/env python
"""Bug #26 — PIN venda: TTL 45s na confirmação; após gravar, próxima pede PIN de novo."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {msg}")
    else:
        fail += 1
        print(f"  FAIL {msg}")


def main() -> int:
    transf = (ROOT / "produtos" / "pdv_transf_loja_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    mp = (ROOT / "produtos" / "views_mp_point.py").read_text(encoding="utf-8")
    sspin = (ROOT / "produtos" / "templates" / "produtos" / "_screensaver_pin.html").read_text(
        encoding="utf-8"
    )
    wiz = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js").read_text(encoding="utf-8")
    consulta = (ROOT / "produtos" / "static" / "produtos" / "js" / "consulta_produtos.js").read_text(
        encoding="utf-8"
    )

    print("--- bug #26 PIN venda TTL + expirar pós-venda ---")
    check("PDV_OPERADOR_FRESCO_VENDA_TTL_S = 45" in transf, "constante venda = 45")
    check("PDV_OPERADOR_FRESCO_VENDA_TTL_S = 10" not in transf, "sem constante venda 10")
    check("def expirar_operador_pdv_fresco" in transf, "util expirar_operador_pdv_fresco")

    idx = wiz.find("PIN para confirmar a venda")
    check(idx > 0, "wizard tem titulo confirmar venda")
    bloco = wiz[idx : idx + 320]
    check("maxFrescoS: frescoEntrega ? 120 : 45" in bloco, "wizard bloco maxFrescoS 45")
    check("120 : 10" not in bloco and ": 10\n" not in bloco, "wizard bloco sem fallback 10")
    check("gmSspinExpirarFrescoAposVenda" in wiz, "wizard chama expirar após venda")

    idx_c = consulta.find("PIN para confirmar a venda")
    check(idx_c > 0, "consulta tem titulo confirmar venda")
    bloco_c = consulta[idx_c : idx_c + 120]
    check("maxFrescoS: 45" in bloco_c, "consulta Confirmar maxFrescoS 45")
    check("maxFrescoS: 10" not in bloco_c, "consulta sem maxFrescoS 10 na venda")
    check("gmSspinExpirarFrescoAposVenda" in consulta, "consulta chama expirar após venda")

    check("gmSspinExpirarFrescoAposVenda" in sspin, "sspin helper expirar")
    check("expirar_fresco" in views, "API operador aceita expirar_fresco")
    check("expirar_operador_pdv_fresco(request)" in views, "enviar ERP expira fresco na resposta")
    check("_mp_point_expirar_pin_apos_venda" in mp, "Point expira fresco pós-venda")

    check("PDV_OPERADOR_FRESCO_TTL_S = 45" in transf, "TTL geral 45 no util")
    check(
        "PDV_OPERADOR_FRESCO_VENDA_TTL_S = 45" in transf
        and "PDV_OPERADOR_FRESCO_TTL_S = 45" in transf,
        "venda alinhada ao TTL geral (ambos 45)",
    )

    total = ok + fail
    print(f"\nVERIFY_OK {ok}/{total}" if fail == 0 else f"\nVERIFY_FAIL {ok}/{total}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
