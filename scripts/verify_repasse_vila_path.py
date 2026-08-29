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
check("produtos/repasse_vila_util.py", "ja_eletronico", "falta_dinheiro", "_ja_eletronico_vila", "validar_data_ref_repasse", "lucro_ficou_vila", "_receita_e_cmv_vila_periodo", "partir_despesas_centro_vila", "planos_desconto_centro", "acumulado_anterior", "listar_acumulado_detalhe", "RepasseVilaAcumuladoAjusteAgro", "RepasseVilaDeltaDiaAgro", "quitar_acumulado_zerar", "abater_extras_do_acumulado", "_extra_do_calc", "_extra_enviado_apos", "acumulado_bruto", 'acum = _dec(calc.get("acumulado_anterior"))', "salvar_reserva_vila", "reserva_vila", "lucro_penultimo", "listar_log_reserva", "reserva_aplicada_no_dia", "pendente_reserva_cofrinho_ate", "obrigacao_reserva_cofrinho_ate", "credito_reserva_cofrinho_ate", "registrar_saldo_inicial_cofrinho", "obrigacao_acumulada", "adiantado", "parte_vila_elias", "parte_salario", "COFRE_VILA_ELIAS", "saldo_cofre_vila_elias", "PRECISA_FORCAR_MANUAL", "forcar_manual_zerado")
forbid("produtos/repasse_vila_util.py", "_quitar_acumulado_no_repasse", "quitacao = max", "Não há valor disponível nas linhas marcadas para o valor manual.")
check("produtos/views_repasse_vila.py", "repasse_vila_view", "api_repasse_vila_confirmar", "formas_pagamento", "validar_data_ref_repasse", "salvar_planos_desconto_centro", "api_repasse_vila_acumulado", "api_repasse_vila_acumulado_zerar", "_skip_acumulado", "salvar_reserva_vila", "api_repasse_vila_reserva_log", "registrar_saldo_inicial_cofrinho", 'tipo == "saldo_inicial"', "precisa_forcar_manual", "forcar_manual_zerado")
check("produtos/urls.py", "repasse_vila", "api/repasse-vila/confirmar/", "api/repasse-vila/acumulado/", "api/repasse-vila/acumulado/zerar/", "api/repasse-vila/reserva-log/", "api/repasse-vila/cofrinho/", "api/repasse-vila/cofrinho/separar/", "api/repasse-vila/cofrinho/movimento/", "api/repasse-vila/cofrinho/estornar/")
check("produtos/models.py", "RepasseVilaCentroAgro", "RepasseVilaConfigAgro", "planos_desconto_centro", "RepasseVilaAcumuladoAjusteAgro", "RepasseVilaDeltaDiaAgro", "reserva_vila", "reserva_vila_desde", "saldo_reserva_vila", "saldo_cofre_vila_elias", "RepasseVilaReservaLogAgro", "RepasseVilaReservaMovimentoAgro", "lucro_penultimo_dia", 'SALDO_INICIAL = "saldo_inicial"', "cofre")
check("produtos/migrations/0093_repasse_vila_acumulado_ajuste.py", "RepasseVilaAcumuladoAjusteAgro")
check("produtos/migrations/0094_repasse_vila_delta_cache.py", "RepasseVilaDeltaDiaAgro")
check("produtos/migrations/0095_repasse_vila_reserva.py", "reserva_vila")
check("produtos/migrations/0097_repasse_reserva_lucro_log.py", "reserva_vila_desde", "RepasseVilaReservaLogAgro")
check("produtos/migrations/0100_repasse_vila_cofrinho.py", "saldo_reserva_vila", "RepasseVilaReservaMovimentoAgro", "idempotencia_chave")
check("produtos/migrations/0103_dois_cofrinhos_vila.py", "saldo_cofre_vila_elias", "vila_elias", "salario")
check("produtos/templates/produtos/repasse_vila.html", "Transferir", "rv-data", "rv-day", "rv-lucro-ficou", "Enviado ao Centro", "Lucro ficou na Vila", "rv-btn-planos", "rv-planos-modal", "rv-acum", "rv-btn-acum", "rv-acum-zerar", "acumulado já coberto por envio", "acumulado_bruto", "tot + acum", "rv-reserva", "rv-fold", "Dias 1 a 15", "Dias 16 a 31", "rv-log-lista", "penúltimo", "Ainda separar", "rv-cofre-saldo-inicial", "Saldo inicial", 'value="saldo_inicial"', "obrigacao_acumulada", "Não separar hoje soma amanhã", "Cofrinho Salário funcionário", "rv-cofre-ve-card", "Cofre Vila Elias")
check("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "pdv-repasse-overlay", "pdv-rp-forma-grid", "pdv-rp-data", "pdv-rp-desp-hint", "pdv-rp-acum", "pdv-rp-acumulado", "pdv-rp-acum-modal", "pdv-rp-acum-zerar", "pdv-rp-reserva", "pdv-rp-cofre-aviso", "NÃO levar", "pdv-rp-hero-cofre", "pdv-rp-hero-cofre-ve", "pdv-rp-quem-modal", "pdv-rp-pin-modal", "pdv-rp-forma-modal", "Levar ao Centro", "Cofrinho Salário funcionário", "Cofre Vila Elias", "rp-popup", "grid-template-columns", "pdv-rp-forcar-manual-modal", "PIN de novo")
forbid("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "pdv-rp-btn-quem", "pdv-rp-btn-forma", "pdv-rp-btn-pin", "rp-chip", "Forma de pagamento")
check("produtos/static/produtos/js/pdv_repasse_vila.js", "api/repasse-vila/confirmar/", "forma_pagamento: formaPag", "data_ref: dataRef()", "despesas_centro_dia", "incluir_acumulado", "api/repasse-vila/acumulado/", "api/repasse-vila/acumulado/zerar/", "reservaAtual", "notifyParentFecharAtualizar", "agro-caixa-fechar-atualizar", "pendente_dia", "openCofreConfirmModal", "enviarConfirmacao", "formaPag = 'Dinheiro'", "openQuemModal", "openPinModal", "tryConfirmarFlow", "focusSoon", "Inclui ", "api/repasse-vila/historico/", "cofre_vila_elias", "pdv-rp-hero-cofre-ve", "openForcarManualModal", "forcar_manual_zerado")
check("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "pdv-rp-cofre-confirm-modal", "pdv-rp-cofre-confirm-valor-ve", "NÃO coloque esses valores", "Confirma o repasse", "80vw", "80dvh")
forbid("produtos/static/produtos/js/pdv_repasse_vila.js", "tot - reservaAtual()", "if (!dom.reserva.value)", "pdv-rp-btn-quem", "openFormaModal", "updateChips")
forbid("produtos/templates/produtos/repasse_vila.html", "tot - reserva")
check("produtos/templates/produtos/pdv_wizard.html", "pdv_repasse_vila.js", "repasse_vila_overlay", "pdv-topbar-repasse-btn")
check("produtos/static/produtos/js/pdv_balanca.js", "#pdv-repasse-overlay")
forbid("produtos/static/produtos/js/pdv_balanca.js", "#pdv-repasse-vila-overlay")
check("produtos/templates/produtos/dashboard_gerencial.html", "repasse_vila")
check("produtos/templates/produtos/caixa_retiradas_historico.html", "crh-btn-repasse", "pdv_repasse_vila.js")
check("produtos/templates/produtos/includes/repasse_help_agents.html", "O que é este repasse", "hoje ou que passou", "Planos:", "Acumulado", "abate sozinho", "Cofre Vila Elias", "Cofrinho Salário", "Saldo inicial")
check("produtos/templates/produtos/includes/repasse_aviso_abertura.html", "Repasse da Vila")
check("produtos/views.py", "aplicar_repasses_pendentes_centro", "repasse_aviso_abertura")
check("scripts/verify_repasse_vila_deep.py", "VERIFY_DEEP_OK", "confirmar_repasse", "forma PIX", "confirmar ontem", "envio extra zera acum do dia")
check("scripts/verify_repasse_planos_path.py", "VERIFY_PLANOS_OK", "planos_desconto_centro")
check("scripts/verify_repasse_acum_net.py", "VERIFY_ACUM_NET_OK", "print 18/08", "abater_extras_do_acumulado")
check("scripts/verify_repasse_reserva.py", "VERIFY_RESERVA_OK", "salvar_reserva_vila", "lucro_penultimo", "listar_log_reserva", "reserva_aplicada_no_dia")
check("scripts/verify_caixa_fechar_repasse_path.py", "VERIFY_FECHAR_REPASSE_OK", "escopo=loja", "notifyParentFecharAtualizar")
check("scripts/verify_repasse_cofrinho.py", "VERIFY_REPASSE_COFRINHO_OK", "registrar_saldo_inicial_cofrinho", "hoje acumula ontem+hoje = 200", "adiantar/separar a mais abate próximo dia", "fórmula lucro 200 → VE 100")
check("scripts/verify_repasse_pdv_overlay_path.py", "VERIFY_REPASSE_PDV_OVERLAY_OK", "focusSoon", "pdv-rp-quem-modal", "Levar ao Centro", "tryConfirmarFlow", "formaPag = 'Dinheiro'", "grid-template-columns")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_OK")
