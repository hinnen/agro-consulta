"""Prova: dinheiro sem troco nao imprime LEVAR MAQUINA."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAIL = 0


def check(cond, msg):
    global FAIL
    if cond:
        print("OK", msg)
    else:
        FAIL += 1
        print("FAIL", msg)


def main():
    js = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js").read_text(
        encoding="utf-8"
    )
    html = (ROOT / "produtos" / "templates" / "produtos" / "entregas_painel.html").read_text(
        encoding="utf-8"
    )
    check("function linhaObsMaquininhaEntrega" in js, "nao grava Maquininha: nao na obs")
    check("if (dinheiro) cartao = false;" in js, "PDV: dinheiro nao vira maquina")
    check("if (dinheiro) cartao = false;" in html, "painel: dinheiro nao vira maquina")
    check("COBRAR DINHEIRO" in js and "COBRAR DINHEIRO" in html, "faixa cobrar dinheiro")
    check("maquininha ? 'Maquininha:" not in js, "obs antiga Maquininha: + val removida")
    if FAIL:
        print(f"\n{FAIL} falha(s)")
        return 1
    print("\nOK verify_ent_via_dinheiro_sem_maquina")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
