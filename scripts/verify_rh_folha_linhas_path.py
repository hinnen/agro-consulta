#!/usr/bin/env python3
"""Path RH-FOLHA-LINHAS — estático, sem banco e sem a loja."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("VERIFY RH-FOLHA-LINHAS")
    fech = read("rh/services/fechamento.py")
    models = read("rh/models.py")
    forms = read("rh/forms.py")
    views = read("rh/views.py")
    mig = read("rh/migrations/0007_ajustemanualfolha.py")
    tpl = read("rh/templates/rh/fechamento_detalhe.html")

    if "class AjusteManualFolha" in models and "ajustes_manuais_inicializados" in models:
        ok("modelo da linha")
    else:
        fail("modelo")

    if "select_for_update" in fech and "garantir_linhas_dos_totais_antigos" in fech:
        ok("copia dos totais trancada")
    else:
        fail("copia sem trava")

    if "PagamentoSalarioFuncionario" in fech and "total_pagamentos_salario_fechamento" in fech:
        ok("pagamento de salário continua no recálculo")
    else:
        fail("pagamento de salário sumiu do recálculo")

    if re.search(r"fields = \(\s*\"observacoes\", \"valor_pago\", \"status\"\s*\)", forms):
        ok("formulário não grava mais os dois totais à mão")
    else:
        fail("formulário ainda edita outros_descontos direto")

    if "AjusteManualFormSet" in views and "ajustes_formset.save()" in views:
        ok("salvar grava as linhas")
    else:
        fail("view de salvar")

    if "Outros descontos" in mig and "Outros proventos" in mig and "ajustes_manuais_inicializados = True" in mig:
        ok("migração copia os dois totais e marca a folha")
    else:
        fail("migração")

    if "Adicionar linha" in tpl and "Remover esta linha" in tpl:
        ok("tela com linha nova e remover")
    else:
        fail("template")

    if fails:
        print(f"VERIFY_FAIL {len(fails)}")
        return 1
    print(f"VERIFY_OK {oks}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
