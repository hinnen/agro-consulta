"""Prova do filtro Centro + Vila / Centro / Vila no DRE e no BI."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

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


def main() -> int:
    rec = _read("financeiro/services/receita_pdv_util.py")
    resumo = _read("financeiro/services/resumo_operacional_pg.py")
    api = _read("financeiro/api/views.py")
    ser = _read("financeiro/api/serializers.py")
    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    views = _read("produtos/views.py")
    ind = _read("financeiro/templates/financeiro/indicadores_gerencial.html")

    check("resolver_todas", 'deposito == "todas"' in rec)
    check("empresas_ids", "def empresas_ids_para_deposito" in rec)
    check("consolidar_loja", "def consolidar_por_loja_pg" in resumo)
    check("api_loja", "consolidar_por_loja_pg" in api)
    check("ser_loja", "loja" in ser and "todas" in ser)
    check("dre_select", 'id="f-loja"' in html)
    check("dre_padrao", "Centro + Vila" in html)
    check("dre_centro", 'value="centro"' in html)
    check("dre_vila", 'value="vila"' in html)
    check("dre_sem_empresa_ui", 'id="f-empresa"' not in html and 'id="f-modo"' not in html)
    check("js_loja_key", "agro_dre_loja_v1" in js)
    check("js_fetch_loja", '"loja="' in js or "loja=" in js)
    check("bi_select", 'id="dash-bi-loja"' in dash)
    check("bi_padrao", "Centro + Vila" in dash)
    check("bi_cookie", "agro_bi_loja" in dash and "COOKIE_BI_LOJA" in views)
    check("bi_helper", "def _dashboard_loja_numeros_from_request" in views)
    check("bi_nao_usa_pdv_numeros", "loja_numeros, deposito_filtro = _dashboard_loja_numeros_from_request" in views)
    check("pdv_loja_intacta", 'id="dash-agro-loja"' in dash)
    check("indicadores_intactos", 'id="f-empresa"' in ind or "empresas" in ind)
    check("ajuda_dre_loja", "Centro + Vila" in html and "empresa própria" in html.lower())

    print(f"\n{len(oks)} OK / {len(fails)} FAIL")
    for n in fails:
        print("  -", n)
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
