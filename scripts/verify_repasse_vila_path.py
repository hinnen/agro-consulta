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


def forbid(path: str, *needles: str) -> None:
    global oks
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n in text:
            fails.append(f"{path} still has {n!r}")
        else:
            oks += 1


check("produtos/caixa_util.py", "filtrar_maquininhas_por_loja", "filtrar_maquininhas_pdv_sem_mp")
check("pdv/views.py", "mp_vila", "pix_mp_vila", "pix_sicoob_chave", "filtrar_maquininhas_por_loja", "lojas")
check("produtos/repasse_vila_util.py", "ja_eletronico", "falta_dinheiro", "_ja_eletronico_vila", "validar_data_ref_repasse", "lucro_ficou_vila", "_receita_e_cmv_vila_periodo", "partir_despesas_centro_vila", "planos_desconto_centro", "acumulado_anterior", "listar_acumulado_detalhe", "RepasseVilaAcumuladoAjusteAgro", "RepasseVilaDeltaDiaAgro", "quitar_acumulado_zerar", "abater_extras_do_acumulado", "_extra_do_calc", "_extra_enviado_apos", "acumulado_bruto", 'acum = _dec(calc.get("acumulado_anterior"))', "salvar_reserva_vila", "reserva_vila", "lucro_penultimo", "listar_log_reserva", "reserva_aplicada_no_dia")
forbid("produtos/repasse_vila_util.py", "_quitar_acumulado_no_repasse", "quitacao = max")
check("produtos/views_repasse_vila.py", "repasse_vila_view", "api_repasse_vila_confirmar", "formas_pagamento", "validar_data_ref_repasse", "salvar_planos_desconto_centro", "api_repasse_vila_acumulado", "api_repasse_vila_acumulado_zerar", "_skip_acumulado", "salvar_reserva_vila", "api_repasse_vila_reserva_log")
check("produtos/urls.py", "repasse_vila", "api/repasse-vila/confirmar/", "api/repasse-vila/acumulado/", "api/repasse-vila/acumulado/zerar/", "api/repasse-vila/reserva-log/", "api/repasse-vila/cofrinho/", "api/repasse-vila/cofrinho/separar/", "api/repasse-vila/cofrinho/movimento/", "api/repasse-vila/cofrinho/estornar/")
check("produtos/models.py", "RepasseVilaCentroAgro", "RepasseVilaConfigAgro", "planos_desconto_centro", "RepasseVilaAcumuladoAjusteAgro", "RepasseVilaDeltaDiaAgro", "reserva_vila", "reserva_vila_desde", "saldo_reserva_vila", "RepasseVilaReservaLogAgro", "RepasseVilaReservaMovimentoAgro", "lucro_penultimo_dia")
check("produtos/migrations/0093_repasse_vila_acumulado_ajuste.py", "RepasseVilaAcumuladoAjusteAgro")
check("produtos/migrations/0094_repasse_vila_delta_cache.py", "RepasseVilaDeltaDiaAgro")
check("produtos/migrations/0095_repasse_vila_reserva.py", "reserva_vila")
check("produtos/migrations/0097_repasse_reserva_lucro_log.py", "reserva_vila_desde", "RepasseVilaReservaLogAgro")
check("produtos/migrations/0100_repasse_vila_cofrinho.py", "saldo_reserva_vila", "RepasseVilaReservaMovimentoAgro", "idempotencia_chave")
check("produtos/templates/produtos/repasse_vila.html", "Transferir", "rv-data", "rv-day", "rv-lucro-ficou", "Enviado ao Centro", "Lucro ficou na Vila", "rv-btn-planos", "rv-planos-modal", "rv-acum", "rv-btn-acum", "rv-acum-zerar", "acumulado já coberto por envio", "acumulado_bruto", "tot + acum", "rv-reserva", "rv-fold", "Dias 1 a 15", "Dias 16 a 31", "rv-log-lista", "penúltimo")
check("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "pdv-repasse-overlay", "pdv-rp-forma-grid", "pdv-rp-data", "pdv-rp-desp-hint", "pdv-rp-acum", "pdv-rp-acumulado", "pdv-rp-acum-modal", "pdv-rp-acum-zerar", "pdv-rp-reserva")
check("produtos/static/produtos/js/pdv_repasse_vila.js", "api/repasse-vila/confirmar/", "forma_pagamento: formaPag", "data_ref: dataRef()", "despesas_centro_dia", "incluir_acumulado", "api/repasse-vila/acumulado/", "api/repasse-vila/acumulado/zerar/", "reservaAtual", "notifyParentFecharAtualizar", "agro-caixa-fechar-atualizar")
forbid("produtos/static/produtos/js/pdv_repasse_vila.js", "tot - reservaAtual()")
forbid("produtos/templates/produtos/repasse_vila.html", "tot - reserva")
check("produtos/templates/produtos/pdv_wizard.html", "pdv_repasse_vila.js", "repasse_vila_overlay")
check("produtos/templates/produtos/dashboard_gerencial.html", "repasse_vila")
check("produtos/templates/produtos/caixa_retiradas_historico.html", "crh-btn-repasse", "pdv_repasse_vila.js")
check("produtos/templates/produtos/includes/repasse_help_agents.html", "O que é este repasse", "hoje ou que passou", "Planos:", "Acumulado", "abate sozinho", "Reserva diária do cofrinho")
check("produtos/templates/produtos/includes/repasse_aviso_abertura.html", "Repasse da Vila")
check("produtos/views.py", "aplicar_repasses_pendentes_centro", "repasse_aviso_abertura")
check("scripts/verify_repasse_vila_deep.py", "VERIFY_DEEP_OK", "confirmar_repasse", "forma PIX", "confirmar ontem", "envio extra zera acum do dia")
check("scripts/verify_repasse_planos_path.py", "VERIFY_PLANOS_OK", "planos_desconto_centro")
check("scripts/verify_repasse_acum_net.py", "VERIFY_ACUM_NET_OK", "print 18/08", "abater_extras_do_acumulado")
check("scripts/verify_repasse_reserva.py", "VERIFY_RESERVA_OK", "salvar_reserva_vila", "lucro_penultimo", "listar_log_reserva", "reserva_aplicada_no_dia")
check("scripts/verify_caixa_fechar_repasse_path.py", "VERIFY_FECHAR_REPASSE_OK", "escopo=loja", "notifyParentFecharAtualizar")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_OK")
