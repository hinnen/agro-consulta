#!/usr/bin/env python
"""Smoke: UX Composição recolhimento Saco/Kit. VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODAL = os.path.join(
    ROOT,
    "produtos",
    "templates",
    "produtos",
    "_modal_editar_produto_cadastro_erp.inc.html",
)


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def main() -> None:
    html = open(MODAL, encoding="utf-8").read()
    if "\u251c" in html:
        fail("mojibake ├ no modal")

    for need in (
        'id="edit-cf-toggle-corpo"',
        'id="edit-kit-toggle-corpo"',
        'id="edit-cf-corpo"',
        'id="edit-kit-corpo"',
        'id="edit-cf-ativo"',
        'id="edit-kit-ativo"',
        'id="edit-cf-baixa-estoque"',
        "coletarCustoFamiliaPayload",
        "atualizarCfCorpoVisivel",
        "atualizarKitCamposVisivel",
        "modalEdit._cfCorpoAberto",
        "modalEdit._kitCorpoAberto",
        "#tab-composicao:not(.hidden)",
        "overflow-y: auto !important",
        "baixa_estoque_saco",
        "comp-btn-buscar",
    ):
        if need not in html:
            fail(f"falta {need}")

    # texto longo fora do ?
    bad_out = (
        "Bloco 1 (saco):",
        "outra ferramenta",
        "Usar neste produto",
        "Com o kit ativo, a baixa",
        "Baixa por unidade vendida =",
    )
    for b in bad_out:
        if b in html:
            fail(f"texto longo/legado ainda na tela: {b!r}")

    # ? ainda tem a ajuda
    if "Saco → pacote/granel" not in html and "Saco &rarr; pacote" not in html:
        if "Saco" not in html or "pacote/granel" not in html:
            fail("ajuda ? do saco sumiu")
    if "Kit / combo" not in html and "vários insumos" not in html:
        fail("ajuda ? do kit sumiu")

    # uma definição de cada função
    if html.count("function atualizarKitCamposVisivel") != 1:
        fail("atualizarKitCamposVisivel duplicada ou ausente")
    if html.count("function atualizarCfCorpoVisivel") != 1:
        fail("atualizarCfCorpoVisivel duplicada ou ausente")
    if html.count("function atualizarCustoFamiliaPreview") != 1:
        fail("atualizarCustoFamiliaPreview duplicada ou ausente")

    # payload save ainda manda custo_familia + composicao
    if "custo_familia:" not in html or "composicao: compPayload" not in html:
        fail("salvar sem custo_familia/composicao")

    # ids únicos
    for eid in (
        "edit-cf-toggle-corpo",
        "edit-kit-toggle-corpo",
        "edit-cf-corpo",
        "edit-kit-corpo",
        "edit-cf-ativo",
        "edit-kit-ativo",
    ):
        n = len(re.findall(rf'id="{eid}"', html))
        if n != 1:
            fail(f"id {eid} aparece {n}x")

    # CF util + verify legado
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    import django

    django.setup()
    from produtos.custo_familia_util import calcular_custo_filho, qtd_baixa_saco_por_unidade
    from decimal import Decimal

    if calcular_custo_filho(Decimal("38"), Decimal("24"), Decimal("5")) != Decimal("7.92"):
        fail("calc 24kg→5kg (caso da tela)")
    if qtd_baixa_saco_por_unidade(24, 5) is None:
        fail("qtd baixa")

    print("VERIFY_OK")


if __name__ == "__main__":
    main()
