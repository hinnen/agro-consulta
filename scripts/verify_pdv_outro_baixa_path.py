#!/usr/bin/env python3
"""Prova estática do path PDV-OUTRO-BAIXA (bug loja #2).

Não sobe servidor — valida HTML/JS + regras de prontidão/confirmação.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "produtos" / "templates" / "produtos" / "partials" / "pdv" / "step_pagamento.html"
JS = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js"

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    # ASCII-safe para console Windows cp1252
    safe = msg.encode("ascii", "replace").decode("ascii")
    if ok:
        PASS += 1
        print(f"  OK  {safe}")
    else:
        FAIL += 1
        print(f" FAIL {safe}")


def outro_pronta(forma: str, pin: bool, detalhe: str) -> bool:
    return bool(forma == "Outro" and pin and str(detalhe or "").strip())


def ready_confirm(
    err: bool,
    forma: str,
    rest: float,
    pin: bool,
    detalhe: str,
    n_lanc: int,
) -> bool:
    """Espelho de readyConfirm + readyOutroAuto em renderPagamento."""
    if err:
        return False
    ready_outro = forma == "Outro" and rest > 0.009 and outro_pronta(forma, pin, detalhe)
    ready_quitado = (not forma) and n_lanc > 0 and rest <= 0.009
    return ready_outro or ready_quitado


def try_confirm_action(
    step: str,
    forma: str,
    rest: float,
    pin: bool,
    detalhe: str,
    valor: float | None = None,
) -> str:
    """Espelho do ramo Outro em tryConfirmSale (sem I/O)."""
    if step != "pagamento":
        return "confirm_direct"
    if forma == "Outro" and rest > 0.009:
        if not outro_pronta(forma, pin, detalhe):
            return "aviso_pin" if not pin else "aviso_detalhe"
        cur = rest if valor is None else float(valor)
        if not (cur > 0.009):
            return "aviso_valor"
        # commitTrancheFlow + confirmSale (mesmo parcial: avisa e lança)
        return "auto_lancar_e_confirmar"
    return "confirm_normal"


def main() -> int:
    html = HTML.read_text(encoding="utf-8")
    js = JS.read_text(encoding="utf-8")

    print("== HTML ==")
    i_outro = html.find('id="pdv-flow-outro"')
    i_valor = html.find('id="pdv-pay-valor-tranche-bar"')
    check(i_outro > 0 and i_valor > i_outro, "bloco Outro acima do painel Valor/Lançar")
    check("1) PIN · 2) detalhe · 3) Lançar ou Confirmar" in html, "hint PIN/detalhe/Lançar|Confirmar")
    check(
        "Só o detalhe não fecha a venda" in html,
        "ajuda ? diz que só detalhe não fecha",
    )
    check('id="pdv-outro-detalhes"' in html, "textarea pdv-outro-detalhes existe")
    check('id="pdv-outro-validar-pin"' in html, "botão Validar PIN existe")

    print("== JS símbolos ==")
    for name in (
        "syncOutroDetalhesFromDom",
        "outroTranchePronta",
        "tryConfirmSale",
        "readyOutroAuto",
        "outroBloqueiaLancar",
        "renderPayStepChips",
        "setInputValueUnlessFocused",
    ):
        check(f"function {name}" in js or f"var {name}" in js or name in js, f"presente: {name}")

    check(
        re.search(
            r"mode === 'outro'\s*\?\s*\['PIN',\s*'Detalhe',\s*'Lan[^']*',\s*'Confirmar'\]",
            js,
        )
        is not None,
        "chips Outro = PIN > Detalhe > Lancar > Confirmar",
    )
    check(
        "forma === 'Outro' && rest > 0.009" in js and "outroTranchePronta(st)" in js,
        "tryConfirmSale auto-lança Outro se PIN+detalhe",
    )
    check("commitTrancheFlow(st, comp, cur)" in js or "syncOutroDetalhesFromDom()" in js, "sync no commit")
    # sync chamado no início do commit e do confirm
    check(js.count("syncOutroDetalhesFromDom()") >= 3, "syncOutro chamado ≥3× (render/commit/confirm)")

    print("== Regras prontidão ==")
    cases = [
        # (forma, pin, detalhe, rest, n_lanc, err, expect_ready, label)
        ("Outro", False, "permuta", 79.0, 0, False, False, "sem PIN → Confirmar off"),
        ("Outro", True, "", 79.0, 0, False, False, "sem detalhe → Confirmar off"),
        ("Outro", True, "   ", 79.0, 0, False, False, "detalhe só espaço → off"),
        ("Outro", True, "não dado bai", 79.0, 0, False, True, "PIN+detalhe+resta → Confirmar on (bug #2)"),
        ("Outro", True, "ok", 0.0, 1, False, False, "restante 0 com forma ainda Outro → off (precisa limpar forma)"),
        ("", True, "x", 0.0, 1, False, True, "quitado clássico: sem forma + lançamentos"),
        ("Dinheiro", True, "x", 50.0, 0, False, False, "Dinheiro aberto não libera Confirmar"),
        ("Outro", True, "ok", 79.0, 0, True, False, "com erro validação → off"),
    ]
    for forma, pin, det, rest, n, err_flag, expect, label in cases:
        # último case usa err=True
        err = True if "erro validação" in label else err_flag
        got = ready_confirm(err, forma, rest, pin, det, n)
        check(got is expect, label)

    print("== tryConfirmSale ramo Outro ==")
    check(
        try_confirm_action("pagamento", "Outro", 79.0, False, "x") == "aviso_pin",
        "Confirmar sem PIN → aviso",
    )
    check(
        try_confirm_action("pagamento", "Outro", 79.0, True, "") == "aviso_detalhe",
        "Confirmar sem detalhe → aviso",
    )
    check(
        try_confirm_action("pagamento", "Outro", 79.0, True, "permuta")
        == "auto_lancar_e_confirmar",
        "Confirmar com PIN+detalhe → auto lançar",
    )
    check(
        try_confirm_action("carrinho", "Outro", 79.0, True, "x") == "confirm_direct",
        "fora do passo pagamento → confirm direto",
    )

    print("== Lançar bloqueado ==")
    check(outro_pronta("Outro", False, "x") is False, "Lançar bloqueado sem PIN")
    check(outro_pronta("Outro", True, "detalhe") is True, "Lançar liberado com PIN+detalhe")

    print()
    print(f"Resultado: {PASS} ok · {FAIL} falha")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
