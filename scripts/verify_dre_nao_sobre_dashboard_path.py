#!/usr/bin/env python
"""Prova: DRE/links do BI nao navegam o iframe do Dashboard nem a janela Gestao."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise SystemExit(f"FAIL {name}: {detail or 'assertion'}")
    print(f"OK {name}")


def main() -> None:
    dual = (ROOT / "produtos/static/produtos/js/agro_dual_window.js").read_text(
        encoding="utf-8"
    )
    check("openGestao_shell", "Nunca navegar a janela Gestão" in dual or "só shell" in dual)
    check("openGestao_empty_name", "openNamed(GESTAO_NAME, '')" in dual)
    check("boot_pending", "bootGestaoPendingFocus" in dual)
    check("isGestaoHost_addTab", "isGestaoHost()" in dual and "__agroInAppAddTab" in dual)

    dash = (
        ROOT / "produtos/templates/produtos/dashboard_gerencial.html"
    ).read_text(encoding="utf-8")
    check("wire_all_links", "wireDashLinksToInAppTabs" in dash)
    check("no_old_wire_only_cp", "wireLancamentosInAppFromBi" not in dash)
    check("path_is_dashboard", "pathIsDashboard" in dash)

    body = (
        ROOT / "produtos/templates/produtos/partials/dashboard_gerencial_body.html"
    ).read_text(encoding="utf-8")
    check(
        "kpi_btn_not_href",
        "dashLaunchpadAbrir('Resumo gerencial'" in body
        or 'dashLaunchpadAbrir("Resumo gerencial"' in body,
    )
    check(
        "kpi_no_a_href_resumo",
        'href="{% url \'resumo_financeiro_gerencial\' %}"' not in body
        and "resumo_financeiro_gerencial' %}\" class=\"block focus" not in body,
    )

    print("ALL OK")


if __name__ == "__main__":
    main()
