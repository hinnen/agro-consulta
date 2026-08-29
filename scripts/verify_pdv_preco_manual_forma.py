#!/usr/bin/env python3
"""Prova estatica do path PDV-PRECO-MANUAL-FORMA.

Simula: lista 25 -> digita 32 -> recalc com forma (debito) -> preco continua 32.
Nao sobe servidor — le JS + espelha a regra critica.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS_PROMO = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_promocoes.js"
JS_STATE = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_state.js"
JS_CAMP = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_campanha.js"

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


def to_num(v, fallback=0.0) -> float:
    try:
        n = float(v)
        return n if n == n else float(fallback)
    except (TypeError, ValueError):
        return float(fallback)


def aplicar_no_item_espelho(item: dict, forma: str) -> None:
    """Espelho da regra fix: preco_manual nao restaura de preco_pos_promo."""
    if item.get("preco_manual"):
        item["preco_pos_promo"] = to_num(item.get("preco"), 0)
        item["preco_base_forma"] = to_num(item.get("preco"), 0)
        return
    # caminho legado (nao entra no teste principal)
    if item.get("preco_pos_promo") is not None:
        item["preco"] = to_num(item["preco_pos_promo"], item.get("preco"))
    item["preco_base_forma"] = to_num(item.get("preco"), 0)
    _ = forma


def update_item_price_espelho(item: dict, preco: float) -> None:
    """Espelho updateItemPrice apos o fix."""
    item["preco"] = float(preco)
    item["preco_manual"] = True
    item["preco_pos_promo"] = float(preco)
    item["preco_base_forma"] = float(preco)


def aplicar_campanha_espelho(itens: list) -> None:
    for item in itens:
        if not item:
            continue
        if item.get("preco_manual"):
            item["preco_pos_promo"] = to_num(item.get("preco"), 0)
            item.pop("campanha_id", None)
            item.pop("campanha_pct", None)
            item.pop("campanha_usou", None)
            continue
        # sem campanha ativa: so garante cache
        if item.get("preco_pos_promo") is None:
            item["preco_pos_promo"] = to_num(item.get("preco"), 0)


def recalc_com_forma_espelho(itens: list, forma: str) -> None:
    for item in itens:
        if not item:
            continue
        aplicar_no_item_espelho(item, forma)
    for item in itens:
        if not item:
            continue
        item.pop("campanha_id", None)
        item.pop("campanha_pct", None)
        item.pop("campanha_usou", None)
        item.pop("preco_pos_promo", None)
    aplicar_campanha_espelho(itens)


def main() -> int:
    promo = JS_PROMO.read_text(encoding="utf-8")
    state = JS_STATE.read_text(encoding="utf-8")
    camp = JS_CAMP.read_text(encoding="utf-8")

    print("== Fonte JS ==")
    check(
        "if (item.preco_manual)" in promo and "preco_pos_promo = toNum(item.preco" in promo,
        "pdv_promocoes: guard preco_manual em aplicarNoItem",
    )
    # Nao pode restaurar preco de pos_promo quando manual (bug antigo)
    bad = re.search(
        r"preco_manual[\s\S]{0,200}preco\s*=\s*toNum\(item\.preco_pos_promo",
        promo,
    )
    check(bad is None, "pdv_promocoes: nao restaura preco de preco_pos_promo se manual")
    check(
        "preco_pos_promo: p" in state and "preco_manual: true" in state,
        "pdv_state: updateItemPrice grava preco_pos_promo + preco_manual",
    )
    check(
        "if (item.preco_manual)" in camp and "precoEnvioItem" in camp,
        "pdv_campanha: pula manual + precoEnvioItem usa item.preco",
    )
    check(
        "if (item.preco_manual) return toNum(item.preco, 0)" in camp,
        "pdv_campanha: precoEnvioItem retorna item.preco se manual",
    )

    print("== Simulacao path (lista 25 -> digita 32 -> forma debito) ==")
    item = {
        "id": "p1",
        "qtd": 1,
        "preco": 25.0,
        "preco_pos_promo": 25.0,
        "preco_base_forma": 25.0,
    }
    update_item_price_espelho(item, 32.0)
    check(item["preco"] == 32.0 and item["preco_manual"] is True, "apos digitar: preco=32 e flag manual")
    check(
        item["preco_pos_promo"] == 32.0 and item["preco_base_forma"] == 32.0,
        "apos digitar: caches alinhados em 32",
    )

    # simula "como pagar" ainda com 32
    total_antes = item["preco"] * item["qtd"]
    check(abs(total_antes - 32.0) < 0.001, "modal como pagar: total 32")

    recalc_com_forma_espelho([item], "debito")
    check(abs(item["preco"] - 32.0) < 0.001, "apos escolher forma: preco continua 32 (nao volta 25)")
    check(item.get("preco_manual") is True, "apos forma: preco_manual permanece")
    check(
        abs(to_num(item.get("preco_pos_promo"), 0) - 32.0) < 0.001,
        "apos forma+campanha: cache pos_promo = 32",
    )

    print("== Contraste bug antigo ==")
    item_bug = {
        "id": "p2",
        "qtd": 1,
        "preco": 32.0,
        "preco_manual": True,
        "preco_pos_promo": 25.0,  # cache velho (bug)
        "preco_base_forma": 25.0,
    }

    def aplicar_bug_antigo(it: dict) -> None:
        # comportamento pre-fix: restaurava sempre
        if it.get("preco_pos_promo") is not None:
            it["preco"] = to_num(it["preco_pos_promo"], it.get("preco"))
        it["preco_base_forma"] = to_num(it.get("preco"), 0)

    aplicar_bug_antigo(item_bug)
    check(abs(item_bug["preco"] - 25.0) < 0.001, "bug antigo: voltaria para 25 (regressao documentada)")

    item_fix = {
        "id": "p3",
        "qtd": 1,
        "preco": 32.0,
        "preco_manual": True,
        "preco_pos_promo": 25.0,
        "preco_base_forma": 25.0,
    }
    aplicar_no_item_espelho(item_fix, "pix")
    check(abs(item_fix["preco"] - 32.0) < 0.001, "fix: mesmo cache velho, preco fica 32")
    check(abs(item_fix["preco_pos_promo"] - 32.0) < 0.001, "fix: cache reescrito para 32")

    print(f"\nResultado: {PASS} ok, {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
