# -*- coding: utf-8 -*-
"""Prova PDV-ORC-IMPRIMIR — Salvar | Imprimir no card de orçamentos.

  python scripts/verify_pdv_orc_imprimir_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def main() -> int:
    html = read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    check('id="pdv-step1-salvar-orcamento-btn"' in html, "HTML tem botao Salvar")
    check('id="pdv-step1-imprimir-orcamento-btn"' in html, "HTML tem botao Imprimir")
    check(">Salvar<" in html or ">Salvar\n" in html or ">\n              Salvar\n" in html, "HTML rotulo Salvar")
    check("Imprimir" in html and "Salvar orçamento" not in html, "HTML sem rotulo longo antigo")
    check("grid-cols-2" in html, "HTML grade dois botoes")

    wiz = read("produtos/static/produtos/js/pdv_wizard.js")
    check("step1ImprimirOrcamentoBtn" in wiz, "JS dom Imprimir")
    check("function imprimirOrcamentoWizard" in wiz, "JS funcao imprimir")
    check("fromImpressao" in wiz, "JS flag fromImpressao")
    check("origem: 'impressao'" in wiz or "'impressao'" in wiz, "JS origem impressao")
    check("function budgetEhImpressao" in wiz, "JS detecta origem impressao")
    check("function budgetImpressaoIconHtml" in wiz, "JS icone impressora")
    check("budgetOrigemIconHtml" in wiz, "JS icone origem unificado")
    check("imprimirOrcamentoWizard" in wiz and "addEventListener('click', imprimirOrcamentoWizard)" in wiz, "JS click Imprimir")
    check(
        "wizardImprimirPacoteEntrega(orcId, { sep: false, ent: false, cup: true })" in wiz,
        "JS imprime so cupom orcamento",
    )
    idx = wiz.find("function salvarOrcamentoWizard")
    save = wiz[idx : idx + 3500]
    check("fromWhatsapp" in save and "fromImpressao" in save, "JS salvar aceita WhatsApp e impressao")
    check("setOrcamentoActionBtnsDisabled" in wiz, "JS desliga Salvar+Imprimir juntos")

    print()
    print(f"Resultado: {OKS} ok, {len(FAILS)} fail")
    if FAILS:
        for f in FAILS:
            print(" -", f.encode("ascii", "replace").decode("ascii"))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
