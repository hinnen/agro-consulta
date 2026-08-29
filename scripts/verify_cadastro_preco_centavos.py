#!/usr/bin/env python3
"""Prova: cadastro — 82,90 nao vira 829,00 no modal de precos por forma.

Causa: parseMoeda apagava todo ponto. Numero JS 82.9 virava texto '82.9',
perdia o ponto e gravava 829.
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODAL = ROOT / "produtos" / "templates" / "produtos" / "_modal_editar_produto_cadastro_erp.inc.html"

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    safe = msg.encode("ascii", "replace").decode("ascii")
    if ok:
        PASS += 1
        print(f"  OK  {safe}")
    else:
        FAIL += 1
        print(f" FAIL {safe}")


def parse_moeda_espelho(s):
    """Espelho da regra nova (numero JS fica numero; virgula = pt-BR)."""
    if s is None or s == "":
        return 0.0
    if isinstance(s, bool):
        return 0.0
    if isinstance(s, (int, float)):
        return float(s) if math.isfinite(s) else 0.0
    t = str(s).replace(" ", "").replace("\u00a0", "")
    if not t:
        return 0.0
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        n = float(t)
    except ValueError:
        return 0.0
    return n if math.isfinite(n) else 0.0


def parse_moeda_antigo(s):
    t = str(s).replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return 0.0


def main() -> int:
    print("CAD-PRECO-CENTAVOS")
    html = MODAL.read_text(encoding="utf-8")

    check(MODAL.is_file(), "modal de cadastro existe")
    check("function _parseMoedaTexto" in html, "helper _parseMoedaTexto no modal")
    check("typeof s === 'number'" in html, "parseMoeda protege numero JS")
    check("t.indexOf(',') >= 0" in html, "ponto de milhar so quando ha virgula")
    check(
        ".replace(/\\s/g, '').replace(/\\./g, '').replace(',', '.')" not in html,
        "parser antigo (apaga todo ponto) saiu do modal",
    )
    check("commitPrecoGrupoCampo" in html, "grupo A/B formata no blur")
    check("agroCadastroObterPrecosPorFormaPayload" in html, "payload por forma ainda existe")

    check(abs(parse_moeda_antigo(82.9) - 829.0) < 1e-9, "bug antigo: 82.9 numero virava 829")
    check(abs(parse_moeda_espelho(82.9) - 82.9) < 1e-9, "fix: 82.9 numero continua 82.9")
    check(abs(parse_moeda_espelho("82,90") - 82.9) < 1e-9, "fix: '82,90' continua 82.9")
    check(abs(parse_moeda_espelho("82.90") - 82.9) < 1e-9, "fix: '82.90' continua 82.9")
    check(abs(parse_moeda_espelho("1.234,56") - 1234.56) < 1e-9, "fix: '1.234,56' = 1234.56")
    check(parse_moeda_espelho("") == 0.0, "vazio = 0")
    check(abs(parse_moeda_espelho(92) - 92.0) < 1e-9, "inteiro 92 continua 92")

    # Ida e volta: digita -> guarda numero -> reparse (troca de aba / salvar)
    n = parse_moeda_espelho("82,90")
    n2 = parse_moeda_espelho(n)
    check(abs(n2 - 82.9) < 1e-9, "ida e volta 82,90 nao vira 829")
    check(abs(n2 - 829.0) > 1, "ida e volta nao cai em 829")

    blob = re.search(
        r"function _parseMoedaTexto\(t\) \{.*?function parseMoedaStrict\(s\) \{.*?\n  \}",
        html,
        re.S,
    )
    check(blob is not None, "bloco parseMoeda extraivel")
    if blob:
        body = blob.group(0)
        check("typeof s === 'number'" in body, "guarda de numero esta no bloco parse")
        check("indexOf(',') >= 0" in body, "virgula decide milhar no bloco parse")

    print(f"\nRESULTADO {PASS} ok / {FAIL} falha")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
