#!/usr/bin/env python3
"""Path check: arquivos e símbolos do pacote NF forn unif + ajuste manual."""
from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main() -> int:
    checks = [
        ROOT / "produtos/fornecedor_entrada_nfe_util.py",
        ROOT / "produtos/migrations/0149_fornecedor_entrada_nfe_alias_agro.py",
        ROOT / "scripts/verify_fornecedor_entrada_nfe_alias_path.py",
        ROOT / "scripts/verify_nf_ajuste_manual_rateio_path.py",
    ]
    for p in checks:
        if not p.is_file():
            print(f"MISSING {p}")
            return 1
    html = (ROOT / "produtos/templates/produtos/entrada_nota.html").read_text(encoding="utf-8")
    needles = [
        "URL_FORN_UNIF",
        "modal-unificar-forn",
        "nfe-ajuste-manual-aplicar",
        "entradaNfeCalcularVUnitLinhaRateios",
        "entradaNfeLinhaQtdCustoUnit",
        "entradaNfeLinhaValorLinhaRateio",
        "manual_ajuste_custo_tipo",
        "api_entrada_nota_fornecedores_unificar",
    ]
    for n in needles:
        if n not in html:
            print(f"MISSING in entrada_nota.html: {n}")
            return 1
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    if "fornecedores/unificar" not in urls:
        print("MISSING url fornecedores/unificar")
        return 1
    print("VERIFY_OK nf_forn_unif_ajuste_path")
    return 0


if __name__ == "__main__":
    sys.exit(main())
