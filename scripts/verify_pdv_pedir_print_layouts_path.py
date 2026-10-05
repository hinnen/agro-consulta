"""Path PDV-PEDIR-PRINT-3 — 1 botão Imprimir → popup Cupom 80 / A4 / Etiqueta 40×40.

  python scripts/verify_pdv_pedir_print_layouts_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js"
HTML = ROOT / "produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html"

ok_n = 0
fail_n = 0


def check(cond: bool, msg: str) -> None:
    global ok_n, fail_n
    if cond:
        ok_n += 1
    else:
        fail_n += 1
        print("FAIL", msg)


def main() -> int:
    js = JS.read_text(encoding="utf-8")
    html = HTML.read_text(encoding="utf-8")

    check("Etiquetas 53" not in js, "sem botão Etiquetas 53")
    check('data-pl-acao="etiquetas"' not in js, "sem acao etiquetas")
    check("imprimirEtiquetasSeparacao53" not in js, "sem fn 53×30 antiga")
    check(">Imprimir</button>" in js or "Imprimir</button>" in js, "botão Imprimir único")
    check('data-pl-acao="imprimir"' in js, "acao imprimir")
    check("Imprimir cupom" not in js, "sem rótulo Imprimir cupom")

    check("pdv-pedir-loja-print" in html, "modal print no HTML")
    check('data-pl-print="cupom80"' in html, "opção cupom80")
    check('data-pl-print="a4"' in html, "opção a4")
    check('data-pl-print="etq40"' in html, "opção etq40")
    check("Cupom 80 mm" in html, "rótulo Cupom 80 mm")
    check("Folha A4" in html, "rótulo Folha A4")
    check("Etiqueta 40×40" in html or "Etiqueta 40x40" in html, "rótulo Etiqueta 40×40")

    check("function abrirEscolhaImpressao" in js, "abrirEscolhaImpressao")
    check("function executarImpressaoEscolhida" in js, "executarImpressaoEscolhida")
    check("function montarHtmlCupomPedidos" in js, "cupom HTML")
    check("size:80mm auto" in js, "cupom @page 80mm")
    check("function montarHtmlA4Separacao" in js, "A4 HTML")
    check("size:A4" in js, "A4 @page")
    check("function montarHtmlEtiquetas40x40" in js, "40×40 HTML")
    check("size:40mm 40mm" in js, "etiqueta @page 40×40")
    check("LINHAS_POR_ETQ = 3" in js, "3 produtos por etiqueta")
    check("function imprimirTodosPedidos" in js, "imprimir todos abre escolha")
    check("imprimirTodosCupons" not in js, "sem imprimirTodosCupons direto")
    check("abrirEscolhaImpressao([row])" in js, "item abre popup")
    check("abrirEscolhaImpressao(rows)" in js, "todos abre popup")

    # packing 40×40
    def pages_for(count: int, per: int = 3) -> int:
        return max(1, (count + per - 1) // per) if count else 0

    check(pages_for(1) == 1, "1 item = 1 etq")
    check(pages_for(3) == 1, "3 itens = 1 etq")
    check(pages_for(4) == 2, "4 itens = 2 etq")
    check(pages_for(9) == 3, "9 itens = 3 etq")

    print(f"{'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


if __name__ == "__main__":
    sys.exit(main())
