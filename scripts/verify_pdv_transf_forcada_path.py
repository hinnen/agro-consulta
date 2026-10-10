#!/usr/bin/env python3
"""
Prova detalhada path PDV-TRANSF-FORCADA.

Cobre: escolha Pedir×Forçada · direção (hero+Enter) · overlay 2 cards ·
PIN · Esc · APIs estoque · anti-Gestão · Pedir loja intacto · Logística intacta.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    ok = 0
    fail = 0
    errors: list[str] = []

    def check(name: str, cond: bool) -> None:
        nonlocal ok, fail
        if cond:
            ok += 1
            print(f"  OK  {name}")
        else:
            fail += 1
            errors.append(name)
            print(f"  FAIL {name}")

    print("VERIFY PDV-TRANSF-FORCADA")

    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    escolha = _read("produtos/templates/produtos/partials/pdv/pedir_loja_escolha_overlay.html")
    tf_html = _read("produtos/templates/produtos/partials/pdv/transf_forcada_overlay.html")
    pedir_js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    tf_js = _read("produtos/static/produtos/js/pdv_transf_forcada.js")
    urls = _read("config/urls.py")
    estoque_views = _read("estoque/views.py")
    logistica = _read("produtos/templates/produtos/transferencias.html")
    version = _read("VERSION").strip()

    # —— Wizard wiring ——
    check("wizard_include_escolha", "pedir_loja_escolha_overlay.html" in wiz)
    check("wizard_include_tf", "transf_forcada_overlay.html" in wiz)
    check("wizard_include_pedir", "pedir_loja_overlay.html" in wiz)
    check("wizard_js_pedir", "pdv_pedir_loja.js" in wiz)
    check("wizard_js_tf", "pdv_transf_forcada.js" in wiz)
    check("wizard_js_ordem_tf_depois_pedir", wiz.find("pdv_pedir_loja.js") < wiz.find("pdv_transf_forcada.js"))
    check("badge_pedir_topbar", "pdv-topbar-pedir-loja-count" in wiz)
    check("badge_nao_na_forcada", "pdv-topbar-pedir-loja-count" not in tf_html)

    # —— Escolha overlay ——
    check("escolha_id", 'id="pdv-pedir-loja-escolha"' in escolha)
    check("escolha_pedir", 'id="pdv-pedir-loja-escolha-pedir"' in escolha)
    check("escolha_forcada", 'id="pdv-pedir-loja-escolha-forcada"' in escolha)
    check("escolha_fechar", 'id="pdv-pedir-loja-escolha-fechar"' in escolha)
    el = escolha.lower()
    check("escolha_sub_pedir", "não tira estoque agora" in el or "nao tira estoque agora" in el)
    check("escolha_sub_forcada", "tira estoque agora" in el)

    # —— Pedir JS: topbar → escolha; Pedir → abrir(); Forçada → API ——
    check("pedir_js_abrirEscolha", "function abrirEscolha" in pedir_js or "abrirEscolha()" in pedir_js)
    check("pedir_js_btn_escolha", "addEventListener('click', abrirEscolha)" in pedir_js)
    check("pedir_js_nao_abre_pedir_direto", "btnOpen.addEventListener('click', abrir)" not in pedir_js)
    check("pedir_js_api_escolha", "PdvPedirLojaEscolha" in pedir_js)
    check("pedir_js_api_pedir", "PdvPedirLoja" in pedir_js)
    check("pedir_js_forcada_call", "PdvTransfForcada.abrirDirecao" in pedir_js)
    check("pedir_js_esc_escolha", "escolhaAberta" in pedir_js)

    # —— Direção popup ——
    check("tf_direcao", 'id="pdv-tf-direcao"' in tf_html)
    check("tf_dir_vc", 'id="pdv-tf-dir-vc"' in tf_html)
    check("tf_dir_cv", 'id="pdv-tf-dir-cv"' in tf_html)
    check("tf_dir_cancel", 'id="pdv-tf-dir-cancelar"' in tf_html)
    check("tf_html_hero_css", "pdv-tf-dir-btn--hero" in tf_html)
    check("tf_html_sec_css", "pdv-tf-dir-btn--sec" in tf_html)
    check("tf_html_atalho_enter", "pdv-tf-dir-atalho" in tf_html and "Enter" in tf_html)
    check("tf_js_destaque", "aplicarDestaqueDirecao" in tf_js)
    check("tf_js_deposito", "depositoAtual" in tf_js and "pdvDeposito" in tf_js)
    check("tf_js_hero_centro", "dirCv" in tf_js and "centro" in tf_js)
    check("tf_js_hero_vila", "dirVc" in tf_js and "vila" in tf_js)
    check("tf_js_enter_hero", "hero.click" in tf_js)
    check(
        "tf_js_enter_so_dir",
        "isDirOpen()" in tf_js and "isOverlayOpen()" in tf_js,
    )

    # —— Overlay principal: 2 cards + seta ——
    check("tf_overlay", 'id="pdv-tf-overlay"' in tf_html)
    check("tf_card_origem", 'id="pdv-tf-card-origem"' in tf_html)
    check("tf_card_destino", 'id="pdv-tf-card-destino"' in tf_html)
    check("tf_card_vila_css", "pdv-tf-loja-card--vila" in tf_html)
    check("tf_card_centro_css", "pdv-tf-loja-card--centro" in tf_html)
    check("tf_seta", "pdv-tf-dir-seta" in tf_html)
    check("tf_js_seta_cards", "cardOrigem" in tf_js and "cardDestino" in tf_js)
    check("tf_js_setDirecao_troca_cards", "pdv-tf-loja-card--centro" in tf_js and "pdv-tf-loja-card--vila" in tf_js)
    check("tf_sem_titulo_antigo_faixa", "Transferência forçada</h3>" not in tf_html.split("pdv-tf-overlay")[1][:800] if "pdv-tf-overlay" in tf_html else False)
    # Header do overlay não deve ter badge pill antigo
    overlay_part = tf_html.split('id="pdv-tf-overlay"', 1)[-1]
    check("tf_sem_badge_pill", 'id="pdv-tf-direcao-badge"' not in overlay_part)
    check("tf_layout_invertido_css", "tf-layout-invertido" in tf_html)
    check("tf_js_invertido", "tf-layout-invertido" in tf_js)

    # —— Busca / carrinho / bip ——
    check("tf_busca", 'id="pdv-tf-busca"' in tf_html)
    check("tf_carrinho", 'id="pdv-tf-carrinho"' in tf_html)
    check("tf_btn_transferir", 'id="pdv-tf-btn-transferir"' in tf_html)
    check("tf_colar", 'id="pdv-tf-colar"' in tf_html)
    check("tf_js_bip", "termoPareceBip" in tf_js)
    check("tf_js_bip_regex", r"/^\d{8,}$/" in tf_js or r"/^\d{8,}$/" in tf_js.replace("\\", ""))
    check("tf_js_buscar_api", "/api/buscar/?q=" in tf_js)
    check("tf_js_fromBip", "fromBip" in tf_js)

    # —— PIN sempre ——
    check("tf_pin", 'id="pdv-tf-pin"' in tf_html)
    check("tf_pin_input", 'id="pdv-tf-pin-input"' in tf_html)
    check("tf_js_pin", "pedirPin" in tf_js)
    check("tf_js_pin_no_body", "pin: pin" in tf_js or "pin:pin" in tf_js.replace(" ", ""))
    check("tf_js_confirm_antes_pin", "confirm(msgConfirm)" in tf_js)

    # —— Esc volta escolha ——
    check("tf_js_voltar_escolha", "voltarEscolha" in tf_js)
    check("tf_js_esc", "Escape" in tf_js)
    check("tf_js_reopen_escolha", "PdvPedirLojaEscolha" in tf_js)

    # —— Anti Gestão / anti redirect ——
    check("tf_sem_gestao_url", "/produtos/gestao" not in tf_html and "/gestao/" not in tf_html)
    check("tf_js_sem_gestao", "/produtos/gestao" not in tf_js)
    check("tf_js_sem_window_open", "window.open" not in tf_js)
    check("tf_sem_iframe_gestao", "iframe" not in tf_html.lower() or "gestao" not in tf_html.lower())
    check("tf_sem_nav_transferencias", "/transferencias/" not in tf_html)
    check("tf_js_sem_location_href_transf", "location.href" not in tf_js and "location.assign" not in tf_js)

    # —— APIs backend (existem e forçadas) ——
    check("url_api_transferir", "api_transferir_forcado_vila_para_centro" in urls)
    check("url_api_resolver", "api_resolver_codigos_transferencia_forcada" in urls)
    check("view_transferir", "def api_transferir_forcado_vila_para_centro" in estoque_views)
    check("view_resolver", "def api_resolver_codigos_transferencia_forcada" in estoque_views)
    check("view_pin_obrigatorio", 'data.get("pin")' in estoque_views or "data.get('pin')" in estoque_views)
    check("view_direcao_cv", "centro_vila" in estoque_views)
    check("view_origem_forcado", '"forçado"' in estoque_views or "'forçado'" in estoque_views)
    check("tf_js_api_url", "api_transferir_forcado_vila_para_centro" in tf_js)
    check("tf_js_resolver", "api_resolver_codigos_transferencia_forcada" in tf_js)

    # —— Logística intocada (ainda tem forçada própria) ——
    check("logistica_forcada_btn", "abrirModalTransferForcada" in logistica)
    check("logistica_modal_dir", 'id="modal-transfer-forcada-direcao"' in logistica)
    check("logistica_modal_tf", 'id="modal-transfer-forcada"' in logistica)
    check("logistica_sem_pdv_tf", "pdv-tf-overlay" not in logistica)

    # —— API pública JS ——
    check("tf_js_api_global", "window.PdvTransfForcada" in tf_js)
    check("tf_js_abrirDirecao", "abrirDirecao:" in tf_js or "abrirDirecao: abrirDirecao" in tf_js)

    # —— Sem migrate (só front + APIs existentes) ——
    mig_dir = ROOT / "estoque" / "migrations"
    recent = sorted(mig_dir.glob("*.py"), key=lambda p: p.name)[-3:]
    check(
        "sem_migrate_nova_forcada",
        not any("forcad" in p.name.lower() or "transf_forcada" in p.name.lower() for p in recent),
    )

    # —— Version ——
    check("version_semver", bool(re.match(r"^\d+\.\d+(\.\d+)?$", version)))
    parts = version.split(".")
    check("version_ge_19_80", len(parts) >= 2 and (int(parts[0]), int(parts[1])) >= (19, 80))

    # —— IIFE / strict ——
    check("tf_js_iife", "(function ()" in tf_js and "'use strict'" in tf_js)
    check("pedir_js_iife", "(function ()" in pedir_js)

    print()
    if errors:
        print(f"FALHOU verify_pdv_transf_forcada_path: {fail} falha(s), {ok} ok")
        for e in errors:
            print(f" - {e}")
        return 1

    print(f"OK verify_pdv_transf_forcada_path — {ok}/{ok} · escolha · hero · cards · PIN · Esc · APIs · Logística OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
