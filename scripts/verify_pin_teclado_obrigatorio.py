# -*- coding: utf-8 -*-
"""PIN-TECLADO-OBRIG: erro «precisa PIN» sem linha para digitar — varredura."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []


def check(cond: bool, label: str) -> None:
    if cond:
        print(f"  OK  {label}")
    else:
        print(f"FAIL  {label}")
        fails.append(label)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def main() -> int:
    print("=== PIN-TECLADO-OBRIG ===")
    sspin = read("produtos/templates/produtos/_screensaver_pin.html")
    entrada = read("produtos/templates/produtos/entrada_nota.html")
    gestao = read("produtos/templates/produtos/produtos_gestao.html")
    cad = read("produtos/templates/produtos/produtos_cadastro_erp.html")
    pdv = read("produtos/static/produtos/js/pdv_wizard.js")

    check("sspin-input" in sspin and "sspin-numpad" in sspin, "teclado PIN no partial")
    check("gmSspinErroPedePin" in sspin, "helper detecta erro PIN")
    check("gmSspinAbrirSeErroPin" in sspin, "helper abre teclado no erro")
    check("forcarVisibilidadeLock" in sspin, "força tela PIN visível")
    check("__GM_SSPIN_INIT__" in sspin, "não inicia 2 teclados")
    check("input.focus" in sspin, "foca campo ao abrir")
    check("display: flex !important" in sspin or "display:flex !important" in sspin.replace(" ", ""), "CSS lock visível")

    check('_screensaver_pin.html' in entrada, "Entrada NF inclui teclado PIN")
    check("gmSspinGarantirOperador" in entrada and "PIN para registrar estoque" in entrada, "estoque pede PIN antes")
    check("gmSspinAbrirSeErroPin" in entrada, "toast Entrada NF abre PIN")
    check("r.status === 403" in entrada and "gmSspinErroPedePin" in entrada, "403 estoque reabre PIN")

    check('_screensaver_pin.html' in gestao, "Gestão inclui teclado PIN")
    check("gmSspinAbrirSeErroPin" in gestao, "banner Gestão abre PIN")
    check('_screensaver_pin.html' in cad, "Cadastro ERP inclui teclado PIN")

    check("gmSspinAbrirSeErroPin" in pdv, "PDV toast abre PIN")
    check('_screensaver_pin.html' in read("produtos/templates/produtos/pdv_wizard.html"), "PDV wizard tem sspin")
    check('_screensaver_pin.html' in read("produtos/templates/produtos/consulta_produtos.html"), "consulta tem sspin")

    msg = "Identifique-se com o PIN (modo descanso) antes de continuar."
    check(msg in read("produtos/caixa_util.py"), "mensagem canônica no backend")

    print(f"\nResultado: {len(fails)} falha(s)")
    for f in fails:
        print(f"  - {f}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
