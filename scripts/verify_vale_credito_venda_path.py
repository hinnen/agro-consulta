"""
Prova PDV-VALE-USADO — pagar com vale crédito baixa o saldo (bug loja #16).

  python scripts/verify_vale_credito_venda_path.py
"""
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
    util = _read("produtos/vale_credito_venda_util.py")
    views = _read("produtos/views.py")
    models = _read("produtos/models.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    check("util_usado", "def valor_vale_credito_usado_no_payload" in util)
    check("util_aplicar", "def aplicar_movimento_vale_credito_venda" in util)
    check("util_skip_compra", "payload_e_compra_vale_credito" in util)
    check("model_vale_usado", 'VALE_USADO = "vale_usado"' in models)
    check("view_validar", "validar_vale_credito_payload" in views)
    check("view_aplicar", "aplicar_movimento_vale_credito_venda" in views)
    check("view_saldos_resp", "cliente_saldos" in views)
    check("view_devolucao", "creditar_vale_devolucao" in views)
    check("js_aplicar", "aplicarSaldoClienteNoPdv" in wizard)
    check("js_resp", "cliente_saldos" in wizard)
    check("js_cache", "loadWizardClientesCache(true)" in wizard)
    print(f"\n{len(oks)} ok / {len(fails)} fail")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
