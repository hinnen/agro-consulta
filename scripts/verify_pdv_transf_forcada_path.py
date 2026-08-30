#!/usr/bin/env python3
"""Prova path PDV-TRANSF-FORCADA: escolha Pedir × Forçada + overlay nativo no PDV."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def check(name: str, cond: bool, errors: list[str]) -> None:
    if not cond:
        errors.append(name)


def main() -> int:
    errors: list[str] = []
    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    escolha = _read("produtos/templates/produtos/partials/pdv/pedir_loja_escolha_overlay.html")
    tf_html = _read("produtos/templates/produtos/partials/pdv/transf_forcada_overlay.html")
    pedir_js = _read("produtos/static/produtos/js/pdv_pedir_loja.js")
    tf_js = _read("produtos/static/produtos/js/pdv_transf_forcada.js")

    check("wizard_include_escolha", "pedir_loja_escolha_overlay.html" in wiz, errors)
    check("wizard_include_tf", "transf_forcada_overlay.html" in wiz, errors)
    check("wizard_js_tf", "pdv_transf_forcada.js" in wiz, errors)

    check("escolha_id", 'id="pdv-pedir-loja-escolha"' in escolha, errors)
    check("escolha_pedir", 'id="pdv-pedir-loja-escolha-pedir"' in escolha, errors)
    check("escolha_forcada", 'id="pdv-pedir-loja-escolha-forcada"' in escolha, errors)
    check(
        "escolha_sub_pedir",
        "não tira estoque agora" in escolha.lower() or "nao tira estoque agora" in escolha.lower(),
        errors,
    )
    check(
        "escolha_sub_forcada",
        "tira estoque agora" in escolha.lower(),
        errors,
    )

    check("tf_direcao", 'id="pdv-tf-direcao"' in tf_html, errors)
    check("tf_overlay", 'id="pdv-tf-overlay"' in tf_html, errors)
    check("tf_pin", 'id="pdv-tf-pin"' in tf_html, errors)
    check("tf_layout_css", "tf-layout-invertido" in tf_html, errors)
    check("tf_sem_gestao_url", "/produtos/gestao" not in tf_html and "/gestao/" not in tf_html, errors)
    check("tf_sem_transferencias_nav", "/transferencias/" not in tf_html, errors)
    check("tf_js_sem_gestao", "/produtos/gestao" not in tf_js and "window.open" not in tf_js, errors)

    check("pedir_js_escolha", "abrirEscolha" in pedir_js, errors)
    check("pedir_js_btn_escolha", "addEventListener('click', abrirEscolha)" in pedir_js, errors)
    check("pedir_js_api_escolha", "PdvPedirLojaEscolha" in pedir_js, errors)
    check("pedir_js_forcada_call", "PdvTransfForcada.abrirDirecao" in pedir_js, errors)
    check("pedir_js_esc_escolha", "escolhaAberta" in pedir_js, errors)

    check("tf_js_api", "window.PdvTransfForcada" in tf_js, errors)
    check("tf_js_api_url", "api_transferir_forcado_vila_para_centro" in tf_js, errors)
    check("tf_js_resolver", "api_resolver_codigos_transferencia_forcada" in tf_js, errors)
    check("tf_js_pin", "pedirPin" in tf_js, errors)
    check("tf_js_invertido", "tf-layout-invertido" in tf_js, errors)
    check("tf_js_bip", "termoPareceBip" in tf_js, errors)
    check("tf_js_voltar_escolha", "voltarEscolha" in tf_js, errors)
    check("tf_js_esc", "Escape" in tf_js, errors)
    check("tf_js_destaque_loja", "aplicarDestaqueDirecao" in tf_js and "pdv-tf-dir-btn--hero" in tf_js, errors)
    check("tf_html_hero_css", "pdv-tf-dir-btn--hero" in tf_html, errors)
    check("tf_js_enter_hero", "Enter" in tf_js and "pdv-tf-dir-btn--hero" in tf_js and "hero.click" in tf_js, errors)

    # Badge ainda no topbar pedir (não na forçada)
    check("badge_pedir", "pdv-topbar-pedir-loja-count" in wiz, errors)

    if errors:
        print("FALHOU verify_pdv_transf_forcada_path:")
        for e in errors:
            print(" -", e)
        return 1

    print("OK verify_pdv_transf_forcada_path — escolha + forçada PDV + PIN + Esc")
    return 0


if __name__ == "__main__":
    sys.exit(main())
