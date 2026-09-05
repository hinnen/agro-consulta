# -*- coding: utf-8 -*-
"""
LANC-PIN-TECLADO — prova do path.

Bug: Finalizar / baixar / editar em Lançamentos mostrava alert nativo
«Identifique-se com o PIN (modo descanso)» sem teclado.

Path: helper em lancamentos_pin_entrada · Novo lançamento · CP · CR.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def main() -> int:
    pin = read("produtos/templates/produtos/includes/lancamentos_pin_entrada.html")
    manual = read("produtos/templates/produtos/lancamentos_manual.html")
    cp = read("produtos/templates/produtos/lancamentos_contas_pagar_teste.html")
    cr = read("produtos/templates/produtos/lancamentos_financeiros.html")
    caixa = read("produtos/caixa_util.py")

    check("gmLancamentosTratarErroPin" in pin, "helper tratar erro PIN")
    check("gmLancamentosComOperador" in pin, "helper com operador")
    check("gmSspinAbrirSeErroPin" in pin, "usa sspin abrir")
    check("gmSspinGarantirOperador" in pin, "usa garantir operador")

    check("gmLancamentosComOperador" in manual, "Novo lançamento pede PIN antes")
    check("gmLancamentosTratarErroPin" in manual, "Novo lançamento trata 403 PIN")
    check("gravarLoteManual" in manual, "retry após PIN")

    check("alertOuPinLanc" in cp, "CP tem alertOuPin")
    check("comOperadorLanc" in cp, "CP tem comOperador")
    check("alertOuPinLanc" in cp and "enviarBaixa" in cp, "CP baixa usa PIN")
    check("alertOuPinLanc" in cp and "enviarEditar" in cp, "CP editar usa PIN")
    check("alertOuPinLanc" in cp and "enviarParcial" in cp, "CP parcial usa PIN")

    check("alertOuPinLanc" in cr, "CR tem alertOuPin")
    check("comOperadorLanc" in cr, "CR tem comOperador")
    check("alertOuPinLanc" in cr and "enviarBaixaParcial" in cr, "CR parcial usa PIN")
    check("alertOuPinLanc" in cr and "enviarEditar" in cr, "CR editar usa PIN")

    msg = "Identifique-se com o PIN (modo descanso) antes de continuar."
    check(msg in caixa, "MSG canônica no servidor")

    # Não pode voltar ao alert puro da falha de PIN no Finalizar
    idx_falha = manual.find("Falha ao gravar")
    chunk = manual[max(0, idx_falha - 400) : idx_falha + 200] if idx_falha >= 0 else ""
    check("gmLancamentosTratarErroPin" in chunk, "Finalizar: PIN antes do alert")

    print("---")
    print(f"VERIFY_{'OK' if not FAILS else 'FAIL'} {OKS}/{OKS + len(FAILS)}")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
