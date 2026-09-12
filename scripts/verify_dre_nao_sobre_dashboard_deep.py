#!/usr/bin/env python
"""Prova profunda: DRE/Resumo nao sobe em cima do Dashboard (path completo)."""
from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise SystemExit(f"FAIL {name}: {detail or 'assertion'}")
    print(f"OK {name}")


def main() -> None:
    dual = (ROOT / "produtos/static/produtos/js/agro_dual_window.js").read_text(
        encoding="utf-8"
    )
    dash = (
        ROOT / "produtos/templates/produtos/dashboard_gerencial.html"
    ).read_text(encoding="utf-8")
    body = (
        ROOT / "produtos/templates/produtos/partials/dashboard_gerencial_body.html"
    ).read_text(encoding="utf-8")
    open_ext = (
        ROOT / "produtos/templates/produtos/_agro_open_external.html"
    ).read_text(encoding="utf-8")

    # --- openGestao: never window.open(deepUrl, GESTAO) ---
    m = re.search(r"function openGestao\(url\)\s*\{(.*?)\n  function navigateGestao", dual, re.S)
    check("openGestao_block", bool(m))
    og = m.group(1) if m else ""
    check("og_shell_comment", "Nunca navegar" in og or "só shell" in og)
    check("og_open_empty", "openNamed(GESTAO_NAME, '')" in og)
    check("og_fallback_shell", "openNamed(GESTAO_NAME, shellHref)" in og)
    check("og_no_open_deep", "openNamed(GESTAO_NAME, href)" not in og)
    check("og_host_addtab", "__agroInAppAddTab(deepHref)" in og)
    check("boot_pending", "bootGestaoPendingFocus" in dual)

    # --- navigateGestao in embed → postMessage tab ---
    check("nav_embed_tab", "agro-open-inapp-tab" in dual)
    check("nav_gestao_host_tab", "isGestaoHost()" in dual and "__agroInAppAddTab(url)" in dual)

    # --- Dashboard: intercept ALL outbound links ---
    wire = dash.split("wireDashLinksToInAppTabs", 1)[1] if "wireDashLinksToInAppTabs" in dash else ""
    check("wire_fn", "wireDashLinksToInAppTabs" in dash)
    check("wire_prevent", "e.preventDefault()" in wire[:2500])
    check("wire_post", "agro-open-inapp-tab" in wire[:2500])
    check("path_dash_guard", "pathIsDashboard" in dash)
    check("no_legacy_cp_only", "wireLancamentosInAppFromBi" not in dash)

    # --- launchpad F8/F9 → openInShellTab first ---
    lp = dash.split("function dashLaunchpadAbrir", 1)[1][:4000]
    check("lp_shell_first", "openInShellTab(url)" in lp)
    check("lp_return_if_shell", "if (openInShellTab(url)) return" in lp)

    # --- KPI Lucro: button + launchpad, not <a href> ---
    check("kpi_btn", "dashLaunchpadAbrir('Resumo gerencial'" in body)
    check("kpi_not_a_href", re.search(r"<a[^>]+resumo_financeiro_gerencial", body) is None)

    # --- Parent shell: escape dash iframe → restore + tab ---
    check("escape_restore", "ensureDashboardHome" in open_ext)
    check("escape_occupy", "occupyUserSlotWithUrl(curHref" in open_ext)
    check("escape_comment", "escapando" in open_ext.lower() or "Dashboard fixo" in open_ext)

    # --- Django URLs alive ---
    from django.urls import reverse

    r_res = reverse("resumo_financeiro_gerencial")
    r_dre = reverse("lancamentos_dre")
    check("url_resumo", r_res.rstrip("/").endswith("financeiro/resumo-gerencial"), r_res)
    check("url_dre", "dre" in r_dre.lower(), r_dre)

    # --- HTTP smoke via Client + force_login (PIN loja = operador; login Django = user) ---
    from django.contrib.auth import get_user_model
    from django.test import Client

    User = get_user_model()
    user = (
        User.objects.filter(username__iexact="Renan", is_active=True).first()
        or User.objects.filter(is_superuser=True, is_active=True).first()
    )
    check("user_login", user is not None, "sem user Django")
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)

    # PIN 9973 = operador PDV (não login Django) — só conferimos que existe no cadastro
    from base.models import PerfilUsuario

    pin_ok = PerfilUsuario.objects.filter(senha_rapida="9973").exists()
    check("pin_9973_cadastrado", pin_ok, "PIN 9973 nao achado em PerfilUsuario")

    r1 = c.get(r_res)
    check("get_resumo_auth", r1.status_code == 200, str(r1.status_code))
    html_r = r1.content.decode("utf-8", errors="ignore")
    check("resumo_title", "DRE" in html_r or "gerencial" in html_r.lower())

    r2 = c.get("/dashboard/gerencial/")
    check("get_dash_auth", r2.status_code == 200, str(r2.status_code))
    html_d = r2.content.decode("utf-8", errors="ignore")
    check("dash_wire", "wireDashLinksToInAppTabs" in html_d)
    check("dash_launchpad", "dashLaunchpadAbrir" in html_d)
    check("dash_kpi_btn", "Resumo gerencial" in html_d)

    # dual JS syntax size
    check("dual_size", len(dual) > 5000)

    for rel in (
        "scripts/verify_dre_nao_sobre_dashboard_path.py",
        "scripts/verify_extravio_conferencia_auto_path.py",
    ):
        ast.parse((ROOT / rel).read_text(encoding="utf-8"))
    check("ast_scripts", True)

    print("ALL OK deep")


if __name__ == "__main__":
    main()
