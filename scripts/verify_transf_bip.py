#!/usr/bin/env python
"""Smoke: TRANSF-BIP — bip volta na busca; digitar nome/GM foca qtd. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

CHECKS: list[str] = []


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    for c in CHECKS:
        print(f"  ok até: {c}")
    sys.exit(1)


def ok(msg: str) -> None:
    CHECKS.append(msg)
    print(f"  OK {msg}")


def read(rel: str) -> str:
    path = os.path.join(ROOT, rel.replace("/", os.sep))
    if not os.path.isfile(path):
        fail(f"arquivo ausente: {rel}")
    return open(path, encoding="utf-8").read()


def tf_termo_parece_bip(termo: str) -> bool:
    """Espelho de tfTermoPareceBip no template."""
    q = str(termo or "").strip().replace(" ", "").replace("\t", "")
    q = re.sub(r"\s+", "", str(termo or "").strip())
    return bool(re.match(r"^\d{8,}$", q))


def simular_add(carrinho: list, produto: dict, from_bip: bool) -> tuple[list, str]:
    """
    Espelho reduzido: bip → +1; digitar → 1 se novo / mantém se já existe.
    Retorna (carrinho, foco) com foco in ('busca', 'qtd').
    """
    pid = str(produto["produto_id"])
    idx = next((i for i, x in enumerate(carrinho) if str(x["produto_id"]) == pid), -1)
    if from_bip:
        if idx >= 0:
            carrinho[idx]["quantidade"] = float(carrinho[idx]["quantidade"] or 0) + 1
        else:
            carrinho.append({**produto, "quantidade": 1})
        return carrinho, "busca"
    # manual
    if idx < 0:
        carrinho.append({**produto, "quantidade": 1})
        idx = len(carrinho) - 1
    return carrinho, "qtd"


def check_static() -> None:
    html = read("produtos/templates/produtos/transferencias.html")

    must = [
        "function tfTermoPareceBip",
        "fromBip: isBarcodeLike",
        "tfEscolherResultado(tfHighlightIdx, { fromBip })",
        "tfAddOuSomar(tfResultados[idx], { quantidade: 1 })",
        'onclick="tfEscolherResultado(${i})"',
        "tfVoltarBusca()",
        "tfFocarQtd(cartIdx)",
    ]
    for needle in must:
        if needle not in html:
            fail(f"ausente no template: {needle}")
    if "\\d{8,}" not in html:
        fail("padrão bip 8+ dígitos ausente")
    # Ramo: fromBip → busca; senão → qtd
    if not re.search(
        r"if\s*\(\s*fromBip\s*\)\s*\{\s*tfVoltarBusca\(\)\s*;\s*\}\s*else\s*\{\s*tfFocarQtd\(cartIdx\)",
        html,
        re.S,
    ):
        fail("ramo fromBip→busca / else→qtd ausente ou alterado")
    ok("template: wiring bip/manual presente")


def check_matriz() -> None:
    cases_bip = [
        ("7891234567890", True),
        ("78912345", True),  # 8 digitos
        ("  7891234567890  ", True),
        ("GM1546", False),
        ("gm1546-5s", False),
        ("racao cao", False),
        ("1234567", False),  # 7 digitos — não bip
        ("", False),
        ("12 34567890123", True),  # espaços removidos → 13 digitos
    ]
    for termo, esperado in cases_bip:
        got = tf_termo_parece_bip(termo)
        if got != esperado:
            fail(f"tfTermoPareceBip({termo!r}) = {got}, esperado {esperado}")
    ok(f"matriz tfTermoPareceBip ({len(cases_bip)} casos)")

    p = {"produto_id": "1", "nome": "Teste"}
    cart: list = []
    cart, foco = simular_add(cart, p, from_bip=True)
    if cart[0]["quantidade"] != 1 or foco != "busca":
        fail("1º bip: qtd!=1 ou foco!=busca")
    cart, foco = simular_add(cart, p, from_bip=True)
    if cart[0]["quantidade"] != 2 or foco != "busca":
        fail("2º bip mesmo produto: deveria +1 e busca")
    cart2: list = []
    cart2, foco = simular_add(cart2, p, from_bip=False)
    if cart2[0]["quantidade"] != 1 or foco != "qtd":
        fail("digitar novo: qtd!=1 ou foco!=qtd")
    cart2, foco = simular_add(cart2, p, from_bip=False)
    if cart2[0]["quantidade"] != 1 or foco != "qtd":
        fail("digitar repetido: deve manter qtd e focar qtd")
    ok("simulação carrinho: bip +1/busca · manual qtd")


def check_decisao_auto_add() -> None:
    """Espelho do if em tfExecutarBusca (1 resultado + match código ou barcode)."""

    def decide(q: str, codes: list[str]) -> str | None:
        """None = não auto-add; 'bip' | 'manual'."""
        q_digits = re.sub(r"\s+", "", q)
        is_barcode = tf_termo_parece_bip(q_digits)
        codes_l = [c.strip().lower() for c in codes if c]
        if q_digits.lower() in codes_l or is_barcode:
            return "bip" if is_barcode else "manual"
        return None

    # EAN único → bip
    if decide("7891234567890", ["GM1", "7891234567890"]) != "bip":
        fail("EAN match deveria auto-add bip")
    # GM digitado match → manual (foco qtd)
    if decide("GM1546", ["GM1546", "789"]) != "manual":
        fail("GM exact match deveria auto-add manual")
    # Nome sem match de código → sem auto-add
    if decide("racao", ["GM1", "789"]) is not None:
        fail("nome parcial não deve auto-add")
    # Barcode-like mesmo sem código na lista (1 resultado) → bip
    if decide("7891234567890", ["GM999"]) != "bip":
        fail("8+ dígitos com 1 resultado → bip mesmo sem match código")
    ok("decisão auto-add: EAN=bip · GM=manual · nome=lista")


def main() -> None:
    print("verify_transf_bip…")
    check_static()
    check_matriz()
    check_decisao_auto_add()
    print(f"VERIFY_OK: {len(CHECKS)}/{len(CHECKS)}")


if __name__ == "__main__":
    main()
