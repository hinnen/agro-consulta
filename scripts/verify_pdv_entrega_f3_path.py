#!/usr/bin/env python
"""Prova path PDV-ENTREGA-F3 — botão Entrega (F3) não pula para pagamento.

  python scripts/verify_pdv_entrega_f3_path.py
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = ""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fail += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main():
    print("=== PDV-ENTREGA-F3 ===")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
        encoding="utf-8"
    )

    check("btn_id", 'id="pdv-step1-advance"' in html)
    check("btn_label", ">Entrega<" in html)
    check("btn_f3", "F3" in html.split("pdv-step1-advance", 1)[1][:800])
    check("pagar_separado", 'id="pdv-step1-payment"' in html)

    i = js.find("function prepararEntregaAoSairDeProdutos")
    check("fn_preparar", i > 0)
    bloco = js[i : i + 900] if i > 0 else ""
    check("reset_local_pag", "localPagamento: ''" in bloco or 'localPagamento: ""' in bloco)
    check("reset_endereco_passo", "enderecoPassoConcluido: false" in bloco)
    check("modo_entrega", "modoRetiradaEntrega: 'entrega'" in bloco)

    j = js.find("dom.step1Advance.addEventListener")
    check("bind_advance", j > 0)
    click = js[j : j + 700] if j > 0 else ""
    check("click_prepara", "prepararEntregaAoSairDeProdutos()" in click)
    check("click_vai_entrega", "State.setCurrentStep('entrega')" in click)
    check(
        "click_nao_nextstep",
        "nextStep(state, computed)" not in click,
        "nao usa fluxo velho (retirada -> pagamento)",
    )
    check("pagar_fn", "function irParaPagamentoFromProdutos" in js)

    print(f"\n{ok} OK · {fail} FAIL")
    raise SystemExit(1 if fail else 0)


if __name__ == "__main__":
    main()
