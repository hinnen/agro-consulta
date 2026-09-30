"""Dispenser — 4 A6 na A4 em pé e 2 A5 em pé na A4 deitada.

Cada espaço da folha pede um impresso diferente.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "produtos" / "templates" / "produtos" / "dispenser_a6_studio.html"
CSS = ROOT / "produtos" / "static" / "produtos" / "dispenser-a6" / "dispenser.css"


def main():
    html = HTML.read_text(encoding="utf-8")
    css = CSS.read_text(encoding="utf-8")
    checks = []

    def ok(cond, msg):
        checks.append((bool(cond), msg))

    ok('id="dspPrint"' in html and "Imprimir 1 A6" in html, "botão 1 A6")
    ok('id="dspPrintA4v"' in html and "4 A6 na A4 em pé" in html, "botão 4 A6")
    ok('id="dspPrintA4h"' in html and "2 A5 na A4 deitada" in html, "botão 2 A5")
    ok('id="dspPrintSheet"' in html and 'id="dspPrintModal"' in html, "folha e escolha")
    ok("__preview__" in html and "Cada espaço precisa de uma folha diferente." in html, "espaços diferentes")
    ok("210mm 297mm" in html and "297mm 210mm" in html, "página A4 em pé e deitada")
    ok("Cima, esquerda" in html and "Esquerda (em pé)" in html, "lugares na folha")
    ok("grid-template-columns: 105mm 105mm" in css, "grade 2×2 A6")
    ok("grid-template-rows: 148mm 148mm" in css, "altura A6")
    ok("grid-template-columns: 148mm 148mm" in css, "dois A5 lado a lado")
    ok("grid-template-rows: 210mm" in css, "altura A5")
    ok("zoom: 1.40952381" in css, "A6 aumenta até a largura A5")
    ok("min-height: 0 !important" in css and "#sspin-root" in css, "nao sobra folha em branco nem PIN")
    ok("body.sspin-locked #sspin-root" in css and "pointer-events: auto" in css, "PIN do descanso cobre a tela")
    ok("size: 210mm 297mm" in css and "size: 297mm 210mm" in css, "papel A4")
    ok("page: dspA4v" in css and "page: dspA4h" in css, "modo de página")

    # 2× A6 = largura A4; 2× A6 altura cabe na A4 em pé (297)
    ok(105 * 2 == 210 and 148 * 2 <= 297, "4 A6 cabem na A4 em pé")
    # 2× A5 largura cabe na A4 deitada (297); A5 é em pé (148×210)
    ok(148 * 2 <= 297 and 210 <= 210, "2 A5 em pé cabem na A4 deitada")
    scaled_h = 148 * (148 / 105)
    ok(scaled_h <= 210, "cartão ampliado não passa de 210 mm")

    failed = [msg for cond, msg in checks if not cond]
    print(f"{len(checks) - len(failed)}/{len(checks)}")
    for cond, msg in checks:
        print(("OK  " if cond else "FALHA  ") + msg)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
