#!/usr/bin/env python3
"""Verifica transferência forçada UX (popup direção + layout invertido C→Vila)."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HTML = ROOT / "produtos/templates/produtos/transferencias.html"


def main() -> int:
    text = HTML.read_text(encoding="utf-8")
    errors: list[str] = []

    required = [
        'id="modal-transfer-forcada-direcao"',
        "tfIniciarTransferForcada('vila_centro')",
        "tfIniciarTransferForcada('centro_vila')",
        "abrirModal('modal-transfer-forcada-direcao')",
        "tf-layout-invertido",
        'id="tf-painel-busca"',
        'id="tf-painel-carrinho"',
        "modal-transfer-forcada-direcao', 'modal-transfer-forcada'",
        "direcao: tfDirecao",
    ]
    for needle in required:
        if needle not in text:
            errors.append(f"faltando: {needle!r}")

    stale = ["tf-dir-vc", "tf-dir-cv", "getElementById('tf-dir-"]
    for needle in stale:
        if needle in text:
            errors.append(f"referência obsoleta: {needle!r}")

    if "onclick=\"tfSetDirecao(" in text:
        errors.append("toggle tfSetDirecao no HTML (deveria ser só popup)")

    # tfSetDirecao deve alternar layout só para centro_vila
    m = re.search(
        r"function tfSetDirecao\(dir\) \{.*?modalTf\.classList\.toggle\('tf-layout-invertido', tfDirecao === 'centro_vila'\)",
        text,
        re.S,
    )
    if not m:
        errors.append("tfSetDirecao não aplica tf-layout-invertido para centro_vila")

    # tfIniciarTransferForcada zera carrinho
    m2 = re.search(r"function tfIniciarTransferForcada\(dir\) \{.*?tfCarrinho = \[\]", text, re.S)
    if not m2:
        errors.append("tfIniciarTransferForcada não zera tfCarrinho")

    if errors:
        print("FALHOU verify_transf_forcada_ux:")
        for e in errors:
            print(" -", e)
        return 1

    print("OK verify_transf_forcada_ux — popup direção + layout invertido + API direcao intacta")
    return 0


if __name__ == "__main__":
    sys.exit(main())
