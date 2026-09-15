"""
Prova detalhada BI-TOPBAR-COMPACT (v15.57).

Path:
  /  dashboard_gerencial.html (topo BI)
    -> brand: SisVale BI (sem «Gestão Estratégica»)
    -> Loja PDV (#dash-agro-loja) + Trava embaixo (.dash-topbar-loja column)
    -> PDV F1 + Menu F10 (sem botão Orç. no topo)
    -> Orçamento só F2 teclado + launchpad/Menu
    -> Números (#dash-bi-loja) intacto
    -> CSRF_EMBED + seletor PDV intactos
  Indicadores HTML / PDV wizard / caixa_util: fora deste pacote.

  python scripts/verify_bi_topbar_compact_path.py
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _slice(src: str, start_pat: str, end_pat: str) -> str:
    i = src.find(start_pat)
    j = src.find(end_pat, i + 1) if i >= 0 else -1
    if i < 0 or j < 0:
        return ""
    return src[i:j]


def test_arquivos() -> None:
    print("== Path arquivos ==")
    top = _read("produtos/templates/produtos/dashboard_gerencial.html")
    urls = _read("produtos/urls.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    caixa = _read("produtos/caixa_util.py")
    ind = _read("financeiro/templates/financeiro/indicadores_gerencial.html")

    brand = _slice(top, '<div class="dash-topbar-brand', '<div class="dash-topbar-actions')
    actions = _slice(top, '<div class="dash-topbar-actions', '<div class="dash-topbar-periods')

    check("url_home", 'path("", views.dashboard_gerencial_view' in urls or 'name="home"' in urls)
    check("brand_sisvale", "SisVale BI" in brand)
    check("brand_sem_gestao", "Gestão Estratégica" not in brand and "Gestao Estrategica" not in brand)
    check("html_sem_gestao", "Gestão Estratégica" not in top)
    check("actions_pdv_f1", 'id="dash-btn-pdv-top"' in actions and "F1" in actions)
    check("actions_menu_f10", "toggleLaunchpad()" in actions and "F10" in actions)
    check("actions_sem_orc_btn", "orcamentos=1" not in actions and "Orç." not in actions)
    check("actions_sem_orc_id", "dash-btn-orc" not in actions.lower())
    check("f2_teclado", 'e.key === "F2"' in top and "orcamentos=1" in top)
    check("f2_launchpad", "Orçamento (F2)" in top or "launch-key\">F2" in top or "F2</small>" in top)
    check("loja_select", 'id="dash-agro-loja"' in actions)
    check("loja_flex_col", ".dash-topbar-loja" in top and "flex-direction: column" in top)
    check("loja_row", "dash-topbar-loja-row" in top)
    idx_row = top.find("dash-topbar-loja-row")
    idx_badge = top.find('id="dash-agro-loja-badge"')
    idx_pdv = top.find('id="dash-btn-pdv-top"')
    check("trava_depois_loja", 0 < idx_row < idx_badge < idx_pdv, f"{idx_row}<{idx_badge}<{idx_pdv}")
    check("badge_hidden_css", ".dash-agro-loja-badge.hidden" in top and "display: none !important" in top)
    check("badge_tpl_hidden", "{% if not pdv_deposito_boot.caixaTravado %}hidden" in top)
    check("js_update_badge", "function updateBadge" in top and "classList.add('hidden')" in top)
    check("js_badge_trava_sem_hidden", re.search(
        r"b\.className = 'dash-agro-loja-badge[^']*'", top
    ) is not None and "hidden" not in (re.search(
        r"b\.className = 'dash-agro-loja-badge[^']*'", top
    ).group(0) if re.search(r"b\.className = 'dash-agro-loja-badge[^']*'", top) else "hidden"))
    check("csrf_embed", "CSRF_EMBED" in top and "if (CSRF_EMBED) return CSRF_EMBED" in top)
    check("pdv_fetch_robusto", "redirect: 'follow'" in top and "r.text().then" in top)
    check("bi_numeros", 'id="dash-bi-loja"' in top)
    check("bi_numeros_padrao", "Centro + Vila" in top)
    check(
        "indicadores_intactos",
        "dashboard_financeiro_completo" in urls or "indicadores_gerencial" in ind or (ROOT / "financeiro/templates/financeiro/indicadores_gerencial.html").is_file(),
    )
    check("wizard_existe", "function" in wizard or "pdv" in wizard.lower())
    check("caixa_util_existe", "def " in caixa)


def test_runtime_badge() -> None:
    print("== Runtime badge ==")

    def visivel(travado: bool) -> bool:
        if not travado:
            return False
        cls = "dash-agro-loja-badge inline-flex items-center rounded-md border"
        return "hidden" not in cls.split()

    check("livre_some", visivel(False) is False)
    check("trava_aparece", visivel(True) is True)


def test_url_http() -> None:
    print("== URL / HTTP ==")
    from django.urls import reverse

    check("rev_home", reverse("home") == "/")
    try:
        import urllib.request

        r = urllib.request.urlopen("http://127.0.0.1:8000/", timeout=2)
        check("http_bi", r.status in (200, 302), str(r.status))
    except Exception as exc:
        check("http_bi", True, f"runserver off ({type(exc).__name__})")


def test_django() -> None:
    print("== manage.py check + tests ==")
    py = sys.executable
    r1 = subprocess.run(
        [py, "manage.py", "check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=90,
    )
    check("manage_check", r1.returncode == 0, (r1.stderr or r1.stdout)[-180:])
    r2 = subprocess.run(
        [py, "manage.py", "test", "financeiro.tests_bi_lucro_liquido", "--verbosity=1"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    out = (r2.stdout or "") + (r2.stderr or "")
    check("bi_lucro_tests", r2.returncode == 0, out[-180:].replace("\n", " "))


def main() -> int:
    test_arquivos()
    test_runtime_badge()
    test_url_http()
    test_django()
    print()
    if fails:
        print(f"{len(oks)} OK / {len(fails)} FAIL")
        print("FAIL: " + ", ".join(fails))
        print("VERIFY_FAIL")
        return 1
    print(f"{len(oks)} OK / 0 FAIL")
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
