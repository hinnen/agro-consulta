#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Path: PDV-FECHAR-CTA-QUITADO — CTA central + aviso sair + idle pulse."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js"
HTML_STEP = ROOT / "produtos" / "templates" / "produtos" / "partials" / "pdv" / "step_pagamento.html"
HTML_WIZ = ROOT / "produtos" / "templates" / "produtos" / "pdv_wizard.html"

checks: list[tuple[str, bool]] = []


def ok(name: str, cond: bool) -> None:
    checks.append((name, bool(cond)))


def main() -> int:
    js = JS.read_text(encoding="utf-8")
    step = HTML_STEP.read_text(encoding="utf-8")
    wiz = HTML_WIZ.read_text(encoding="utf-8")

    ok("hero html id", 'id="pdv-payment-fechar-hero"' in step)
    ok("hero btn no-print", 'id="pdv-fechar-hero-no-print"' in step)
    ok("hero btn print", 'id="pdv-fechar-hero-print"' in step)
    ok("hero title text", "Pode fechar a venda" in step)
    ok("css idle pulse class", "pdv-quitado-idle-pulse" in wiz)
    ok("css keyframes", "pdv-quitado-confirm-pulse" in wiz)
    ok("js vendaQuitadaSemFechar", "function vendaQuitadaSemFechar" in js)
    ok("js confirmarSaida", "function confirmarSaidaComVendaQuitada" in js)
    ok("js focarFechar", "function focarFecharVendaQuitada" in js)
    ok("js schedule idle", "function scheduleQuitadoIdlePulse" in js)
    ok("js idle ms 45s", "PDV_QUITADO_IDLE_MS = 45000" in js)
    ok("js hide forma wrap quitado", "paymentFormaAtualWrap" in js and "quitadoPay" in js)
    ok("js show fechar hero", "paymentFecharHero" in js)
    ok("js openForma bloqueia quitado", re.search(
        r"function openPaymentFormaModal\(\)\s*\{[\s\S]{0,400}?vendaQuitadaSemFechar",
        js,
    ) is not None)
    ok("js nova venda guarda quitado", re.search(
        r"function solicitarNovaVenda\(\)\s*\{[\s\S]{0,2500}?confirmarSaidaComVendaQuitada",
        js,
    ) is not None)
    ok("js btnPrev guarda", "Não, voltar" in js and "confirmarSaidaComVendaQuitada" in js)
    ok("js hero click no-print", re.search(
        r"fecharHeroNoPrint[\s\S]{0,200}?tryConfirmSale\(false\)",
        js,
    ) is not None)
    ok("js hero click print", re.search(
        r"fecharHeroPrint[\s\S]{0,200}?tryConfirmSale\(true\)",
        js,
    ) is not None)
    ok("js setConfirmButtonsBusy hero", "fecharHeroNoPrint" in js and "Confirmando…" in js)
    ok("js bump idle activity", "function bumpQuitadoIdleActivity" in js)
    ok("js pointerdown bump", "bumpQuitadoIdleActivity" in js and "pointerdown" in js)
    ok("msg fechar agora", "Tem venda paga sem fechar" in js)
    ok("ajuda wizard texto", "Pode fechar a venda" in wiz)

    failed = [n for n, c in checks if not c]
    for n, c in checks:
        print(f"{'OK' if c else 'FAIL':4} {n}")
    print(f"\n{len(checks) - len(failed)}/{len(checks)}")
    if failed:
        print("FAILED:", ", ".join(failed))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
