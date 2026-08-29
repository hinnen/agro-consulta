#!/usr/bin/env python
"""Prova detalhada do overlay PDV (REPASSE-PDV-OVERLAY-POPUP).

Contrato loja:
- Quem / PIN só no popup (fluxo Confirmar)
- Forma oculta (= Dinheiro)
- Sem chips na tela principal
- Hero enxuto + detalhes recolhidos
"""
from __future__ import annotations

import re
import subprocess
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


def check_order(path: str, *needles: str) -> None:
    """Garante que needles aparecem em ordem no arquivo."""
    global oks
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    pos = -1
    for n in needles:
        i = text.find(n, pos + 1)
        if i < 0:
            fails.append(f"{path} order missing {n!r} after pos={pos}")
            return
        pos = i
        oks += 1


def count_ok(path: str, pattern: str, min_n: int, label: str) -> None:
    global oks
    p = ROOT / path
    text = p.read_text(encoding="utf-8", errors="replace")
    n = len(re.findall(pattern, text))
    if n < min_n:
        fails.append(f"{path} {label}: found {n} < {min_n}")
    else:
        oks += 1


HTML = "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
JS = "produtos/static/produtos/js/pdv_repasse_vila.js"

# —— Shell + hero ——
check(HTML, "pdv-repasse-overlay", "rp-shell", "min(98rem", "96dvh", "rp-hero", "rp-hero-cofre")
check(HTML, "pdv-rp-hero-cofre", "pdv-rp-hero-cofre-ve", "pdv-rp-hero-cofre-saldo", "pdv-rp-hero-cofre-ve-saldo", "pdv-rp-hero-cofre-hoje", "pdv-rp-hero-cofre-ve-hoje", "A separar", "Separado hoje", "Levado ao Centro hoje", "pdv-rp-levado-hoje", "Cofrinho Salário funcionário", "Cofre Vila Elias", "Levar ao Centro", "pdv-rp-total", "pdv-rp-total-acum", "pdv-rp-manual")
check(HTML, "pdv-rp-caixa-din", "Caixa Vila · dinheiro agora")
check(HTML, "pdv-rp-mes-dinheiro", "pdv-rp-card-cofre", "pdv-rp-mes-lucro-ficou", "pdv-rp-dia-todas")
check(HTML, "pdv-rp-hero-mes", "pdv-rp-hero-geral", "Enviado no mês", "Total geral", "pdv-rp-totais-linha", "rp-hero-totais-item", "rp-hero-edit", "rp-hero-mute")
count_ok(HTML, r">A separar<", 3, "A separar nos 3 cards (>=3)")
check(HTML, "rp-fold", "Detalhes do dia", "% lucro e opções", "pdv-rp-receita", "pdv-rp-acumulado")
check(HTML, "Separar · Salário", "Separar · Vila Elias", "pdv-rp-input-cofre-sal", "pdv-rp-input-cofre-ve", "Separar junto", "pdv-rp-separar-reserva", "pdv-repasse-confirmar")
check(JS, "pdv-rp-hero-mes", "pdv-rp-hero-geral", "total_geral", "renderMesCards", "pdv-rp-levado-hoje", "Total a levar")

# —— Sem chips / forma na tela ——
forbid(
    HTML,
    "pdv-rp-btn-quem",
    "pdv-rp-btn-forma",
    "pdv-rp-btn-pin",
    "rp-chip",
    "Toque para escolher",
    "Toque para digitar",
    "Forma de pagamento",
    "Digitado manda",
)

# —— Popups: quem + PIN; forma escondida ——
check(HTML, "pdv-rp-quem-modal", "pdv-rp-pin-modal", "pdv-rp-forma-modal", "rp-popup")
check(HTML, "pdv-rp-quem-grid", "pdv-rp-forma-grid", "pdv-rp-pin", "pdv-rp-quem-ok", "pdv-rp-pin-ok")
check(HTML, "pdv-rp-enviar-hint", "pdv-rp-cofre-aviso", "Quem levou", "PIN do operador")
check(JS, "NÃO levar no envelope")
check(HTML, "pdv-rp-cofre-confirm-modal", "pdv-rp-cofre-confirm-valor", "pdv-rp-cofre-confirm-valor-ve", "pdv-rp-cofre-confirm-valor-levar", "pdv-rp-cofre-confirm-ok", "rp-cofre-confirm-panel")
check(HTML, "Confirma a transferência", "96vw", "92dvh", "sm:grid-cols-3")
check(HTML, "pdv-rp-cofre-check-modal", "pdv-rp-cofre-check-ok", "pdv-rp-cofre-check-pergunta", "Já separei")
check(JS, "cofre Vila Elias", "levado para o Centro", "cofrinho do funcionário")
forbid(HTML, "pdv-rp-cofre-confirm-input-sal", "pdv-rp-cofre-confirm-input-ve", "pdv-rp-cofre-confirm-input-levar", "edite os 3 valores")
check(HTML, "pdv-rp-forcar-manual-modal", "pdv-rp-forcar-manual-pin", "Atenção — valor forçado", "PIN de novo")
check(HTML, 'id="pdv-rp-forma-modal"', "hidden", "grid-template-columns", "rp-quem-btn")
# Popups depois do shell (não atrás dos chips)
check(HTML, "pdv-rp-aviso-modal", "pdv-rp-aviso-ok", "pdv-rp-aviso-msg", "Entendi", "Atenção")
check(JS, "openAvisoModal", "closeAvisoModal")
check_order(HTML, "pdv-repasse-confirmar", "pdv-rp-quem-modal", "pdv-rp-pin-modal", "pdv-rp-cofre-confirm-modal", "pdv-rp-aviso-modal")
# Forma modal tem attribute hidden
check(HTML, 'id="pdv-rp-forma-modal" class="rp-popup hidden')

# —— JS contrato ——
check(JS, "focusSoon", "openQuemModal", "openPinModal", "tryConfirmarFlow")
forbid(JS, "openFormaModal", "pdv-rp-btn-quem", "pdv-rp-btn-forma", "pdv-rp-btn-pin", "updateChips")
check(JS, "ev.key === 'Enter'", "closeQuemModal", "closePinModal")
check(JS, "formaPag = 'Dinheiro'", "pendingConfirmar", "Escape", "pdv-rp-hero-cofre", "pdv-rp-hero-cofre-ve", "cofre_vila_elias")
check(JS, "openCofreConfirmModal", "closeCofreConfirmModal", "enviarConfirmacao", "notifyParentFecharAtualizar", "valor_cofre_salario", "parseMoneyInput", "inputCofreSal", "pdv-rp-input-cofre-sal", "Preencha os 3 valores", "openCofreCheckSequence", "advanceCofreCheck", "COFRE_CHECK_PASSOS", "pointerEvents", "forcar_manual_zerado = true")
check(JS, "openForcarManualModal", "forcar_manual_zerado", "precisa_forcar_manual", "autoLinhasZeradas", "submitForcarManual")
check(JS, "pdv-rp-caixa-din", "caixa_vila", "saldo_dinheiro", "manualDirty", "fmtManualNum", "syncManualFromAuto", "realizada_dia", "renderCofreHero")
forbid(JS, "window.confirm(msgCofre)", "msgCofre")
forbid(JS, "Não há valor disponível nas linhas marcadas")
check(JS, "api/repasse-vila/confirmar/", "incluir_acumulado", "separar_reserva")
check(JS, "pdv-rp-total-acum", "diaAuto", "Total a levar", "fetchHistoricoMes", "pickQuem", "renderQuem")
# Fluxo Confirmar: quem → pin → confirmar (ordem no tryConfirmarFlow)
check_order(JS, "function tryConfirmarFlow", "openQuemModal", "openPinModal", "confirmar()")
count_ok(JS, r"formaPag = 'Dinheiro'", 3, "força Dinheiro (>=3)")

# —— Wiring PDV + Retiradas ——
check("produtos/templates/produtos/pdv_wizard.html", "pdv_repasse_vila.js", "repasse_vila_overlay", "pdv-topbar-repasse-btn")
check("produtos/templates/produtos/caixa_retiradas_historico.html", "crh-btn-repasse", "pdv_repasse_vila.js", "repasse_vila_overlay")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)

r = subprocess.run(
    ["node", "--check", str(ROOT / JS)],
    capture_output=True,
    text=True,
)
if r.returncode != 0:
    print("FAIL node --check", r.stderr or r.stdout)
    sys.exit(1)
oks += 1
print("OK node --check")

print(f"checks_ok={oks} fails=0")
print("VERIFY_REPASSE_PDV_OVERLAY_OK")
