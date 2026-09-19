"""
Prova estática DRE-PDV-RECEITA + RG-AJUDA-MODAL.
  python scripts/verify_dre_pdv_receita.py
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

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
    return open(os.path.join(_ROOT, rel), encoding="utf-8").read()


def main() -> int:
    util = _read("financeiro/services/receita_pdv_util.py")
    resumo = _read("financeiro/services/resumo_operacional_pg.py")
    ind = _read("financeiro/services/indicadores_gerencial_pg.py")
    html_ind = _read("financeiro/templates/financeiro/indicadores_gerencial.html")
    html_rg = _read("produtos/templates/produtos/resumo_financeiro_gerencial.html")
    js = _read("static/js/agro_resumo_gerencial.js")

    print("== util PDV -> DRE ==")
    check("aplicar_receita_pdv", "def aplicar_receita_pdv_no_resumo" in util)
    check("somar_grupo", "def somar_resumos_dre_empresas" in util)
    check("map_centro", '"centro"' in util and "vila" in util)
    aplicar_body = util.split("def aplicar_receita_pdv_no_resumo")[1].split("def somar_resumos_dre_empresas")[0]
    check("overlay_nao_grava_caixa", 'core["geracao_caixa"]' not in aplicar_body)

    print("== consolidação ==")
    check("empresa_aplica_pdv", "aplicar_receita_pdv_no_resumo" in resumo)
    check("flag_usar_receita_pdv", "usar_receita_pdv: bool = True" in resumo)
    check("grupo_soma_empresas", "somar_resumos_dre_empresas" in resumo)
    check("grupo_nao_deposito_none", "deposito=None" not in resumo)

    print("== indicadores ==")
    check("caixa_sem_pdv", "usar_receita_pdv=False" in ind)
    check("card_pdv_deposito", "deposito_pdv_por_empresa_id" in ind)
    check("label_receita_pdv", "Receita operacional (PDV)" in html_ind)
    check("dre_badge_pdv", "receita_fonte == 'pdv'" in html_ind)
    check("ajuda_pdv", "vendas do PDV" in html_ind.lower() or "vendas do PDV" in html_ind)
    check("recalc_cmv", "def recalc_indicadores_cmv" in ind)
    check("cmv_modos_json", '"cmv_modos"' in ind or "cmv_modos" in ind)
    check("chip_vendida", 'data-dre-cmv="vendida"' in html_ind)
    check("chip_paga", 'data-dre-cmv="paga"' in html_ind)
    check("js_toggle_cmv", "agro_dre_cmv_modo_v1" in html_ind)
    check("ajuda_cmv_dois", "CMV vendida" in html_ind and "CMV paga" in html_ind)

    util_vendas = _read("produtos/relatorios_vendas_util.py")
    print("== CMV vendida ==")
    check("fn_cmv_vendida", "def custo_mercadoria_vendida" in util_vendas)
    check("fn_cmv_rows", "def cmv_vendida_de_rows" in util_vendas)
    check("qs_deposito", "deposito: str | None = None" in util_vendas)

    print("== resumo gerencial ==")
    check("modal_hidden_css", ".rg-modal-backdrop.hidden { display: none !important; }" in html_rg)
    check("js_subtitulo_pdv", 'c.receita_fonte === "pdv"' in js)
    check("fn_cmv_modos_resumo", "def aplicar_cmv_modos_no_resumo" in util)
    check("fn_recalc_resumo_cmv", "def recalc_resumo_cmv" in util)
    check("empresa_flag_cmv", "anexar_cmv_modos" in resumo)
    check("chip_vendida_rg", 'data-dre-cmv="vendida"' in html_rg)
    check("chip_paga_rg", 'data-dre-cmv="paga"' in html_rg)
    check("js_cmv_key", "agro_dre_cmv_modo_v1" in js)
    check("js_apply_cmv", "function aplicarCmvNoCore" in js)
    check("js_caixa_intacto", "geracao_caixa" in js)
    check("ajuda_cmv_rg", "CMV vendida" in html_rg and "Saldo final" in html_rg)

    print(f"\n{len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
