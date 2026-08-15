"""
Prova Pedir loja no PDV (celular + APIs + rollback).

  python scripts/verify_pdv_pedir_loja.py
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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


def _fn_src(path: str, fn: str) -> str:
    tree = ast.parse(_read(path))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == fn:
            return ast.get_source_segment(_read(path), node) or ""
    return ""


def main() -> int:
    print("VERIFY PDV-PEDIR-LOJA")
    urls = _read("produtos/urls.py")
    html = _read("produtos/templates/produtos/partials/pdv/pedir_loja_overlay.html")
    js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    util = _read("produtos/pdv_transf_loja_util.py")
    views = _read("produtos/views_pdv_transf_loja.py")
    boot = _read("pdv/views.py")
    rollback = _read("docs/ROLLBACK-PDV-PEDIR-LOJA.md")
    tests = _read("produtos/tests_pdv_transf_loja.py")
    mig = ROOT / "estoque/migrations/0018_solicitacao_transferencia_pdv.py"

    check("url_resumo", "api_pdv_transf_loja_resumo" in urls)
    check("url_criar", "api_pdv_transf_loja_criar" in urls)
    check("url_acao", "api_pdv_transf_loja_acao" in urls)
    check("url_lista", "api_pdv_transf_loja_lista" in urls)
    check("wizard_botao", "pdv-topbar-pedir-loja-btn" in wiz)
    check("wizard_include", "pedir_loja_overlay.html" in wiz)
    check("wizard_js", "pdv_pedir_loja.js" in wiz)
    check("boot_urls", "apiPdvTransfLojaCriar" in boot and "apiPdvTransfLojaResumo" in boot)
    check("overlay_id", 'id="pdv-pedir-loja-overlay"' in html)
    check("overlay_safe_area", "safe-area-inset" in html)
    check("overlay_coluna_celular", "28rem" in html)
    check("overlay_qty_grande", "clamp(2.45rem" in html)
    check("overlay_stock_grande", "clamp(1.35rem" in html)
    check("overlay_toque_qty", "3.5rem" in html)
    check("overlay_busca_alta", "3.4rem" in html)
    check("overlay_obs_recolhida", "Observação (opcional)" in html)
    check("js_status_sem_estoque", "transferir" in js and "aceitar" in js)
    check("js_pin_sessao", "precisa_pin" in js)
    check("js_qty_class", "pl-qty" in js)
    check("util_a_mais_b", "STATUS_ACEITO" in util and "concluir_transferencia" in util)
    check("util_sem_reserva", "pular_validacao_pin" in _fn_src("produtos/pdv_transf_loja_util.py", "concluir_transferencia"))
    check("view_criar", "def api_pdv_transf_loja_criar" in views)
    check("migrate_0018", mig.is_file())
    check("tests_existem", "PodeAgirTests" in tests and "ApiViewsTests" in tests)
    check("logistica_intocada", "sugestao_transferencia" in tests)
    check("rollback_doc", "7f7b8022" in rollback and "rollback/pre-pdv-pedir-loja" in rollback)
    check("rollback_migrate", "0018_solicitacao_transferencia_pdv" in rollback)
    ver = _read("VERSION").strip()
    check("version_bump", ver >= "16.54", ver)

    print()
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhou: " + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
