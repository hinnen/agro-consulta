"""
Prova DRE-VISUAL-PREVIA.
Path: /financeiro/resumo-gerencial/ (nova visual)
      -> agro_resumo_gerencial.js incluir_visual=1
      -> GET /api/financeiro/resumo-operacional
      -> montar_dre_visual -> gastos_variacao_pg
Indicadores /financeiro/dashboard-gerencial/ permanece intacto.

  python scripts/verify_dre_visual_path.py
"""
from __future__ import annotations

import os
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


def test_arquivos() -> None:
    print("== Path arquivos ==")
    urls = _read("produtos/urls.py")
    api = _read("financeiro/api/views.py")
    ser = _read("financeiro/api/serializers.py")
    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    css = _read("static/css/agro_resumo_gerencial.css")
    util = _read("financeiro/services/dre_visual_util.py")
    ind_html = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    ind_pg = _read("financeiro/services/indicadores_gerencial_pg.py")

    check("url_resumo", "financeiro/resumo-gerencial/" in urls)
    check("url_indicadores", "financeiro/dashboard-gerencial/" in urls)
    check("fn_montar", "def montar_dre_visual" in util)
    check("ser_incluir_visual", "incluir_visual" in ser)
    check("api_chama_montar", "montar_dre_visual" in api)
    check("html_painel_visual", 'id="painel-visual"' in html)
    check("html_indicadores_link", "dashboard_financeiro_completo" in html)
    check("html_titulo_dre", "DRE gerencial" in html)
    check("html_numeros_completos", 'id="rg-mais-numeros"' in html)
    check("js_incluir_visual", "incluir_visual=1" in js)
    check("js_render_visual", "function renderVisualBoard" in js)
    check("js_despesas_categoria", "Despesas por categoria" in js)
    check("css_board", ".rg-board" in css)
    check("css_gauge", ".rg-gauge" in css)
    check("css_donut", ".rg-donut" in css)
    check("ind_titulo_intact", ">Indicadores</span>" in ind_html or "Indicadores</span>" in ind_html)
    check("ind_aba_financeiro", "Financeiro" in ind_html and "Estoque" in ind_html)
    check("ind_despesas_categoria", "Despesas por categoria" in ind_html)
    check("ind_pg_get", "def get_indicadores_gerencial_pg" in ind_pg)


def test_montar() -> None:
    print("== montar_dre_visual ==")
    from unittest.mock import patch

    from financeiro.services.dre_visual_util import montar_dre_visual

    fake = {
        "ok": True,
        "buckets": [],
        "resumo_grupos": [{"key": "fixa", "label": "Fixas", "ultimo": 50}],
        "total_ultimo_periodo": 50,
        "linhas": [
            {"plano": "Aluguel", "valores": [40, 50], "delta_abs": 10, "tendencia": "up"}
        ],
    }
    with patch(
        "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
        return_value=fake,
    ):
        out = montar_dre_visual(empresa_id=9, por="competencia")
    check("montar_ok", out.get("ok") is True)
    check("montar_top", (out.get("variacao") or {}).get("top", [{}])[0].get("ultimo") == 50.0)


def test_indicadores_nao_redirect() -> None:
    print("== Indicadores nao redirecionado ==")
    views = _read("financeiro/views.py")
    check(
        "view_indicadores_template",
        "indicadores_gerencial.html" in views and "dashboard_financeiro_completo" in views,
    )
    resumo_views = _read("produtos/views.py")
    check(
        "resumo_nao_redirect_indicadores",
        "HttpResponseRedirect" not in resumo_views.split("def resumo_financeiro_gerencial_view")[1][:800]
        if "def resumo_financeiro_gerencial_view" in resumo_views
        else False,
    )


def test_node_check() -> None:
    print("== node --check ==")
    r = subprocess.run(
        ["node", "--check", str(ROOT / "static/js/agro_resumo_gerencial.js")],
        capture_output=True,
        text=True,
    )
    check("js_syntax", r.returncode == 0, (r.stderr or r.stdout or "").strip()[:120])


def main() -> int:
    test_arquivos()
    test_montar()
    test_indicadores_nao_redirect()
    test_node_check()
    print("")
    print(f"OK {len(oks)} / FAIL {len(fails)}")
    if fails:
        print("FALHAS:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
