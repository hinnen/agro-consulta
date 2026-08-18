#!/usr/bin/env python
"""Verify paths/strings for Repasse Vila → Centro."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def check(path: str, *needles: str) -> None:
    global oks
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fails.append(f"{path} missing {n!r}")
        else:
            oks += 1


check("produtos/caixa_util.py", "filtrar_maquininhas_por_loja", "filtrar_maquininhas_pdv_sem_mp")
check("pdv/views.py", "mp_vila", "pix_mp_vila", "pix_sicoob_chave", "filtrar_maquininhas_por_loja", "lojas")
check("produtos/repasse_vila_util.py", "ja_eletronico", "falta_dinheiro", "_ja_eletronico_vila", "validar_data_ref_repasse", "lucro_ficou_vila", "_receita_e_cmv_vila_periodo", "partir_despesas_centro_vila", "planos_desconto_centro", "acumulado_anterior", "listar_acumulado_detalhe", "RepasseVilaAcumuladoAjusteAgro", "RepasseVilaDeltaDiaAgro", "quitar_acumulado_zerar", "_quitar_acumulado_no_repasse")
check("produtos/views_repasse_vila.py", "repasse_vila_view", "api_repasse_vila_confirmar", "formas_pagamento", "validar_data_ref_repasse", "salvar_planos_desconto_centro", "api_repasse_vila_acumulado", "api_repasse_vila_acumulado_zerar", "_skip_acumulado")
check("produtos/urls.py", "repasse_vila", "api/repasse-vila/confirmar/", "api/repasse-vila/acumulado/", "api/repasse-vila/acumulado/zerar/")
check("produtos/models.py", "RepasseVilaCentroAgro", "RepasseVilaConfigAgro", "planos_desconto_centro", "RepasseVilaAcumuladoAjusteAgro", "RepasseVilaDeltaDiaAgro")
check("produtos/migrations/0093_repasse_vila_acumulado_ajuste.py", "RepasseVilaAcumuladoAjusteAgro")
check("produtos/migrations/0094_repasse_vila_delta_cache.py", "RepasseVilaDeltaDiaAgro")
check("produtos/templates/produtos/repasse_vila.html", "Transferir", "rv-data", "rv-day", "rv-lucro-ficou", "Enviado ao Centro", "Lucro ficou na Vila", "rv-btn-planos", "rv-planos-modal", "rv-acum", "rv-btn-acum", "rv-acum-zerar")
check("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "pdv-repasse-overlay", "pdv-rp-forma-grid", "pdv-rp-data", "pdv-rp-desp-hint", "pdv-rp-acum", "pdv-rp-acumulado", "pdv-rp-acum-modal", "pdv-rp-acum-zerar")
check("produtos/static/produtos/js/pdv_repasse_vila.js", "api/repasse-vila/confirmar/", "forma_pagamento: formaPag", "data_ref: dataRef()", "despesas_centro_dia", "incluir_acumulado", "api/repasse-vila/acumulado/", "api/repasse-vila/acumulado/zerar/")
check("produtos/templates/produtos/pdv_wizard.html", "pdv_repasse_vila.js", "repasse_vila_overlay")
check("produtos/templates/produtos/dashboard_gerencial.html", "repasse_vila")
check("produtos/templates/produtos/caixa_retiradas_historico.html", "crh-btn-repasse", "pdv_repasse_vila.js")
check("produtos/templates/produtos/includes/repasse_help_agents.html", "O que é este repasse", "hoje ou que passou", "Planos:", "Acumulado")
check("produtos/templates/produtos/includes/repasse_aviso_abertura.html", "Repasse da Vila")
check("produtos/views.py", "aplicar_repasses_pendentes_centro", "repasse_aviso_abertura")
check("scripts/verify_repasse_vila_deep.py", "VERIFY_DEEP_OK", "confirmar_repasse", "forma PIX", "confirmar ontem")
check("scripts/verify_repasse_planos_path.py", "VERIFY_PLANOS_OK", "planos_desconto_centro")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_OK")
