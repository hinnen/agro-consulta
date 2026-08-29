"""
Prova Pedir loja no PDV (layout PC primeiro + APIs + rollback).

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
    check("url_saldos", "api_pdv_transf_loja_saldos" in urls)
    check("url_criar", "api_pdv_transf_loja_criar" in urls)
    check("url_acao", "api_pdv_transf_loja_acao" in urls)
    check("url_lista", "api_pdv_transf_loja_lista" in urls)
    check("url_ajustar", "api_pdv_transf_loja_ajustar" in urls)
    check("wizard_botao", "pdv-topbar-pedir-loja-btn" in wiz)
    check("wizard_include", "pedir_loja_overlay.html" in wiz)
    check("wizard_js", "pdv_pedir_loja.js" in wiz)
    check("boot_urls", "apiPdvTransfLojaCriar" in boot and "apiPdvTransfLojaSaldos" in boot)
    check("boot_ajustar", "apiPdvTransfLojaAjustar" in boot)
    check("overlay_id", 'id="pdv-pedir-loja-overlay"' in html)
    check("overlay_pc_sem_safe_area", "safe-area-inset" not in html)
    check("overlay_btn_transf_rosa", "pl-btn--transf" in html and "pl-btn--transf" in js)
    check("overlay_pc_tela_cheia", "width: 100%" in html and "height: 100%" in html)
    check("overlay_altura_unica", "h-[min(92dvh" not in html)
    check("overlay_grid_pc", "pl-pedir-grid" in html and "pl-col--pedido" in html)
    check("overlay_hits_tabela", "pl-table" in html and "table-layout: fixed" in html)
    check("overlay_col_gm", "Código GM" in html and "pl-td-gm" in js)
    check("overlay_fonte_lista", "font-size: 1.15rem" in html)
    check("overlay_nome_legivel", "pl-td-nome" in html and "overflow-wrap: anywhere" in html)
    check("js_hits_tr", "<tr class=\"pl-hit\"" in js or "<tr class='pl-hit'" in js or 'class="pl-hit"' in js)
    check("overlay_sem_maxh_hits", "max-h-[11rem]" not in html)
    check("js_saldos_agro", "apiPdvTransfLojaSaldos" in js and "fmtSaldo" in js)
    check("js_busca_seq", "buscaSeq" in js)
    check("view_saldos", "def api_pdv_transf_loja_saldos" in views)
    check("tests_saldos", "test_saldos_agro" in tests)
    check("overlay_saldo_pills", "Saldo Centro" in js and "Saldo Vila" in js)
    check("overlay_abas_completas", "Recebidos" in html and "Histórico" in html)
    check("overlay_obs_visivel", 'id="pdv-pedir-loja-obs"' in html)
    check("js_btn_rosa", "pdv-wiz-topbar-btn--rose" in js)
    check("overlay_ajuda_larga", "Ajuda" in html)
    check("js_tirar_x", 'aria-label="Tirar da lista"' in js)
    check("js_status_sem_estoque", "transferir" in js and "aceitar" in js)
    check("js_pin_sessao", "precisa_pin" in js)
    check("js_confirm_modal", "abrirConfirm" in js and "estoque_furado" in js)
    check("js_bip_pendente", "syncBeepPendentes" in js and "60000" in js)
    check("js_sem_window_confirm", "window.confirm" not in js)
    check("js_ajuste_busca", "abrirAjuste" in js and "apiPdvTransfLojaAjustar" in js and "data-pl-aj" in js)
    check("js_aviso_pos_pin", "abrirTemPedido" in js and "aposPin" in js)
    check("js_cupom_80mm", "imprimirCupomSeparacao" in js and "SEPARAÇÃO" in js)
    check("js_qtd_envio", "lerQtdsDoCard" in js and "pl-item-qtd" in js and "podeEditarQtd" in js)
    check("overlay_confirm_furado", "pdv-pedir-loja-confirm-furado" in html)
    check("overlay_ajuste_modal", "pdv-pedir-loja-ajuste" in html and "pl-btn-aj" in html)
    check("overlay_aviso_pin", "pdv-pedir-loja-tem-pedido" in html and "Enter também fecha" in html)
    check("overlay_btn_print", "pl-btn--print" in html or "Imprimir cupom" in js)
    check("view_ajustar", "def api_pdv_transf_loja_ajustar" in views)
    check("util_ajuste_furado", "qtd_decimal_ou_zero" in util and "_aplicar_ajuste_absoluto_origem" in util)
    check("util_a_mais_b", "STATUS_ACEITO" in util and "concluir_transferencia" in util)
    check("util_qtds_envio", "_resolver_qtds_envio" in util and "quantidade_pedida" in util)
    check(
        "util_sem_reserva",
        "pular_validacao_pin"
        in _fn_src("produtos/pdv_transf_loja_util.py", "concluir_transferencia"),
    )
    check("view_criar", "def api_pdv_transf_loja_criar" in views)
    check("view_qtds_acao", "quantidades_envio" in views)
    check("migrate_0018", mig.is_file())
    mig20 = ROOT / "estoque/migrations/0020_solicitacao_item_quantidade_pedida.py"
    check("migrate_0020_pedida", mig20.is_file())
    check("model_pedida", "quantidade_pedida" in _read("estoque/models.py"))
    check("tests_existem", "PodeAgirTests" in tests and "ApiViewsTests" in tests)
    check("tests_qtds", "ResolverQtdsEnvioTests" in tests)
    check("logistica_intocada", "sugestao_transferencia" in tests)
    check("rollback_doc", "7f7b8022" in rollback and "rollback/pre-pdv-pedir-loja" in rollback)
    check("rollback_migrate", "0018_solicitacao_transferencia_pdv" in rollback)
    path_script = ROOT / "scripts/verify_pdv_pedir_cupom_qtd_path.py"
    check("path_cupom_qtd_script", path_script.is_file())
    ver = _read("VERSION").strip()
    check("version_bump", ver >= "19.01", ver)

    print()
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhou: " + ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
