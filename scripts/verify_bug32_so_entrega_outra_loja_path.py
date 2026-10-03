# -*- coding: utf-8 -*-
"""Prova BUG #32 — só-entrega outra loja + pagar aqui segue pro pagamento (não trava).

  python scripts/verify_bug32_so_entrega_outra_loja_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

fails = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global fails
    mark = "OK" if cond else "FAIL"
    if not cond:
        fails += 1
    extra = f" — {detail}" if detail else ""
    print(f"  {mark}  {name}{extra}")


def main() -> int:
    print("== BUG-32 só-entrega outra loja ==")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    conf = js.split("function confirmarLojaSaidaComEscopo", 1)[-1]
    nxt = conf.find("\n    function ")
    conf = conf[:nxt] if nxt > 0 else conf
    check(
        "escopo entrega: caixa fica no aparelho",
        "escopo === 'entrega'" in conf and "lojaPag = atual === 'vila'" in conf,
    )
    check(
        "escopo pagamento: saída fica no aparelho",
        "escopo === 'pagamento'" in conf and "lojaEnt = atual === 'vila'" in conf,
    )

    pros = js.split("function tryProsseguirEntregaStep", 1)[-1]
    nxt = pros.find("\n    function ")
    pros = pros[:nxt] if nxt > 0 else pros
    check(
        "prosseguir: pagar outra loja vai painel",
        "lojaPagamentoEntregaAtual(state) !== depositoPdvAtivo()" in pros,
    )
    check("prosseguir: pagar aqui vai pagamento", "wizardIrParaPagamentoComImpressao()" in pros)
    check("prosseguir: não desvia só por saída", "entregaVaiParaOutraLoja" not in pros)

    pag = js.split("function wizardIrParaPagamentoComImpressao", 1)[-1]
    nxt = pag.find("\n    function ")
    pag = pag[:nxt] if nxt > 0 else pag
    check(
        "ir pagamento: não manda painel por saída (bug #32)",
        "entregaVaiParaOutraLoja" not in pag and "wizardEnviarEntregaPainel" not in pag,
    )
    check("ir pagamento: abre passo pagamento", "setCurrentStep('pagamento')" in pag)
    check("comentário bug #32 no wizard", "Bug #32" in pag or "bug #32" in pag.lower())

    print()
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK 8/8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
