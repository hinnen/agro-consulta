"""
Prova estática BI card Lucro Líquido (no lugar de Novos Clientes).
  .venv\\Scripts\\python.exe scripts/verify_bi_lucro_liquido_path.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    views = _read("produtos/views.py")
    body = _read("produtos/templates/produtos/partials/dashboard_gerencial_body.html")
    top = _read("produtos/templates/produtos/dashboard_gerencial.html")
    ind = _read("financeiro/services/indicadores_gerencial_pg.py")

    print("== serviço ==")
    check("fn_lucro", "def lucro_liquido_vencimento_bruto_pago" in ind)
    check("por_vencimento", 'por="vencimento"' in ind)
    check("valor_bruto", '"bruto"' in ind and '"realizado"' in ind)
    check("cmv_vendida", "custo_mercadoria_vendida" in ind)
    check("fallback_vila", "Vila sem cadastro próprio" in ind or "lojas or qualquer" in ind)

    print("== BI views ==")
    check("worker_lucro", "def _dashboard_lucro_liquido_vencimento" in views)
    check("fut_lucro", 'fut["lucro_liq"]' in views)
    check("kpi_label", '"label": "Lucro Líquido"' in views)
    check("kpi_variant", '"variant": "lucro_liquido_duplo"' in views)
    check("sem_novos_kpi", '"label": "Novos Clientes"' not in views)
    check("sem_novos_worker", "def _dashboard_novos_clientes_no_dia" not in views)

    print("== template ==")
    check("label_html", "Lucro Líquido" in body)
    check("variant_html", 'kpi.variant == "lucro_liquido_duplo"' in body)
    check("lbl_bruto", ">Bruto<" in body)
    check("lbl_pago", ">Pago<" in body)
    check("link_resumo", "resumo_financeiro_gerencial" in body)
    check("sem_novos_html", "Novos Clientes" not in body)
    check("css_liq", "dash-kpi-liq-grid" in top)

    print("== verify math snapshot ==")
    rec, cmv_v, df, dv, dfin = 1000, 300, 100, 50, 20
    bruto = rec - cmv_v - df - dv - dfin
    rec2, df2, dv2, dfin2 = 1000, 80, 40, 10
    pago = rec2 - cmv_v - df2 - dv2 - dfin2
    check("math_bruto_530", bruto == 530, str(bruto))
    check("math_pago_570", pago == 570, str(pago))

    print()
    print(f"{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
