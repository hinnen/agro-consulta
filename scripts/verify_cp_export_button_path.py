#!/usr/bin/env python3
"""Smoke estático: botão Exportar na lista CP nova."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TPL = ROOT / "produtos/templates/produtos/lancamentos_contas_pagar_teste.html"


def main() -> int:
    text = TPL.read_text(encoding="utf-8")
    required = [
        "sv-btn-export",
        "sv-export-csv",
        "sv-export-xlsx",
        "sv-export-pdf",
        "API_EXPORT_CSV",
        "API_EXPORT_XLSX",
        "API_EXPORT_PDF",
        "montarParamsExport",
        "baixarExportLancamentos",
        "initExportCp",
        "excluir_plano",
        "montarParamsBaseLista",
        "credentials: 'same-origin'",
    ]
    missing = [s for s in required if s not in text]
    if missing:
        print("FAIL missing:", ", ".join(missing))
        return 1
    if "montarParamsExport()" not in text.replace("function montarParamsExport", ""):
        print("FAIL montarParamsExport not used in download")
        return 1
    print("OK verify_cp_export_button_path")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
