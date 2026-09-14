#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Path: PDV-FECHAR-CTA-QUITADO — popup Fechar + aviso sair + idle pulse."""
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

    ok("modal html id", 'id="pdv-payment-fechar-modal"' in step)
    ok("modal backdrop", 'id="pdv-payment-fechar-modal-backdrop"' in step)
    ok("btn voltar pagamento", 'id="pdv-payment-fechar-voltar"' in step)
    ok("voltar label", "Voltar ao pagamento" in step)
    ok("hero btn no-print", 'id="pdv-fechar-hero-no-print"' in step)
    ok("hero btn print", 'id="pdv-fechar-hero-print"' in step)
    ok("title text", "Pode fechar a venda" in step)
    ok("sem hero inline", 'id="pdv-payment-fechar-hero"' not in step)
    ok("reabrir bar", 'id="pdv-payment-reabrir-fechar"' in step)
    ok("css idle pulse", "pdv-quitado-idle-pulse" in wiz)
    ok("backdrop opaco", "bg-slate-950/80" in step)
    ok("js openFechar", "function openFecharVendaModal" in js)
    ok("js closeFechar", "function closeFecharVendaModal" in js)
    ok("js syncFechar", "function syncFecharVendaModalUi" in js)
    ok("js dismissed flag", "pdvFecharModalDismissed" in js)
    ok("js vendaQuitada", "function vendaQuitadaSemFechar" in js)
    ok("js confirmarSaida", "function confirmarSaidaComVendaQuitada" in js)
    ok("js idle reopen modal", re.search(
        r"scheduleQuitadoIdlePulse[\s\S]{0,800}?openFecharVendaModal\(true\)",
        js,
    ) is not None)
    ok("js voltar click", re.search(
        r"paymentFecharVoltar[\s\S]{0,200}?closeFecharVendaModal\(true\)",
        js,
    ) is not None)
    ok("js esc fecha modal", re.search(
        r"isFecharVendaModalOpen\(\)[\s\S]{0,120}?closeFecharVendaModal\(true\)",
        js,
    ) is not None)
    ok("js nova venda guarda", "confirmarSaidaComVendaQuitada" in js and "Não, descartar" in js)
    ok("msg fechar agora", "Tem venda paga sem fechar" in js)
    ok("js fecha modal no confirm", re.search(
        r"function tryConfirmSale\([\s\S]{0,280}?closeFecharVendaModal\(false\)",
        js,
    ) is not None)
    ok("js reabre se cancela impressao", re.search(
        r"abrirModalEscolhaImpressao[\s\S]{0,400}?openFecharVendaModal\(true\)",
        js,
    ) is not None)
    ok("html escolha impressao z alto", 'id="modal-pdv-escolha-impressao"' in wiz and "z-[360]" in wiz)
    ok("html nfce cpf z alto", 'id="modal-pdv-nfce-cpf"' in wiz and "z-[370]" in wiz)

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
