"""Prova estática — gráfico saldo dia a dia DRE gerencial."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _read(rel: str) -> str:
    p = ROOT / rel.replace("/", "\\") if "\\" not in rel else ROOT / rel
    return p.read_text(encoding="utf-8")


def main() -> int:
    fails: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        if ok:
            print(f"  OK  {name}")
        else:
            fails.append(name + (f" — {detail}" if detail else ""))
            print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))

    html = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")
    css = _read("static/css/agro_resumo_gerencial.css")
    urls = _read("financeiro/api/urls.py")
    views = _read("financeiro/api/views.py")
    util = _read("financeiro/services/dre_saldo_diario_util.py")

    check("html_sec_saldo", "sec-saldo-diario" in html)
    check("html_apex", "apexcharts" in html.lower())
    check("html_chart_host", "rg-saldo-chart" in html)
    check("js_fetch", "/api/financeiro/saldo-diario-mes" in js)
    check("js_render", "renderRgSaldoDiario" in js)
    check("js_apex", "ApexCharts" in js)
    check("css_saldo", "rg-saldo-diario" in css)
    check("api_url", "saldo-diario-mes" in urls)
    check("api_view", "SaldoDiarioMesAPIView" in views)
    check("util_fn", "def dre_saldo_diario_mes_pg" in util)
    check("util_lucro", "metrica" in util and "_lucro_maps" in util)
    check("util_uma_passada", "lucro_liquido_vencimento_bruto_pago" not in util)
    check("util_planos", "planos_incluir" in util)
    check("html_lucro_titulo", "Lucro líquido" in html)
    check("html_btn_planos", "btn-planos-gasto" in html)
    check("js_planos", "planosGastoQueryParam" in js)
    check("filtro_util", "filtrar_linhas_dre_planos" in _read("financeiro/services/dre_planos_filtro_util.py"))

    if fails:
        print("\nFalhas:", len(fails))
        for f in fails:
            print(" -", f)
        return 1
    print("\nTudo OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
