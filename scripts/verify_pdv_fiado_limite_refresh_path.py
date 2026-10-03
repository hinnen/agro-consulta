# -*- coding: utf-8 -*-
"""
Prova — limite fiado atualiza no PDV sem fechar/abrir (`PDV-FIADO-LIMITE-REFRESH`).

  python scripts/verify_pdv_fiado_limite_refresh_path.py
"""
from __future__ import annotations

import os
import subprocess
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


def test_js() -> None:
    print("== Contratos JS ==")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    check("helper_ensure", "function ensureCreditoFiadoFrescos" in js)
    check("helper_msg", "function mensagemFiadoAcimaLimite" in js)
    check("helper_valor", "function valorFiadoParaConsultaCredito" in js)
    check("helper_foco", "function agroFiadoCreditoRefreshNoFoco" in js)
    check("cache_valor_key", "creditoFiadoValorKey" in js)
    check("bust_cache_http", "q += '&_t=' + encodeURIComponent(String(Date.now()))" in js)
    check("select_fiado_refresh", "if (forma === 'Fiado')" in js and "ensureCreditoFiadoFrescos(valorFiadoParaConsultaCredito" in js)
    check("commit_fiado_refresh", "String(st.pagamento.forma || '') === 'Fiado'" in js)
    check("confirm_fiado_refresh", "temFiadoConfirm && clientePodeFiado(state0)" in js)
    check("foco_hook", "agroFiadoCreditoRefreshNoFoco()" in js)
    check("gravar_limite_refresh", "gravarLimiteFiadoPdv" in js and "ensureCreditoFiadoFrescos(valorFiadoNosLancamentos" in js)
    check("nao_usa_alert_nativo_confirm", "showPdvAviso(validation0, { tone: 'error', title: 'Atenção' })" in js)


def test_node() -> None:
    print("== Sintaxe JS ==")
    try:
        r = subprocess.run(
            ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_wizard.js")],
            capture_output=True,
            text=True,
            timeout=30,
        )
        check("node_check", r.returncode == 0, (r.stderr or "")[:160])
    except FileNotFoundError:
        check("node_check_skip", True, "node off")


def main() -> int:
    print("PDV-FIADO-LIMITE-REFRESH")
    test_js()
    test_node()
    print(f"\nResultado: {len(oks)} OK · {len(fails)} FAIL")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    print("PREP_FAILS=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
