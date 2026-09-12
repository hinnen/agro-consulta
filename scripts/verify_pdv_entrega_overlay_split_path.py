#!/usr/bin/env python3
"""PDV-ENT-OVERLAY-SPLIT — overlay maior, duas colunas, concluir pagas."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def main() -> int:
    html = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    util = (ROOT / "produtos/entrega_pdv_pendente_util.py").read_text(encoding="utf-8")
    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    boot = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
    mig = ROOT / "produtos/migrations/0130_pedido_entrega_pdv_lista_concluida.py"

    print("=== overlay split ===")
    check("76rem" in html, "overlay mais largo")
    check("pdv-entregas-split" in html, "divisão no meio")
    check('id="pdv-entregas-list-pagar"' in html and 'id="pdv-entregas-list-pagas"' in html, "duas listas")
    check("overflow-y: auto" in html, "scroll nas colunas")
    check("pdv-entrega-card" in html, "cards compactos")
    check("function htmlEntregaPendenteCard" in js, "JS monta as duas colunas")
    check("pdv-entrega-concluir" in js, "botão Concluir")
    check("function concluirEntregaPagaOverlay" in js, "JS conclui")
    check("pdv_lista_concluida" in models, "campo no Postgres")
    check(mig.is_file(), "migrate 0130")
    check("pdv_lista_concluida=False" in util, "lista pagas ignora concluídas")
    check("HORAS_PAGAS_LOJA_PDV = 24" in util, "24 h permanece")
    check("concluir-overlay/" in urls, "rota concluir")
    check("def api_pdv_entrega_pendente_concluir_overlay" in views, "API concluir")
    check("apiPdvEntregaPendenteConcluirOverlay" in boot, "URL no PDV")
    check("Concluir" in html or "concluir" in html.lower(), "ajuda menciona concluir")

    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("FAILED")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
