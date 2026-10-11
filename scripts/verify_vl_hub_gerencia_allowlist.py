#!/usr/bin/env python3
"""Prova — allowlist gerência Vendas/Tarefas (código # + nomes, não PIN pessoal)."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import override_settings

from produtos.vendas_lojas_hub_acesso_util import (
    _norm_nome,
    nomes_vendas_tarefas_hub,
    operador_liberado_vendas_tarefas,
    pins_vendas_tarefas_hub,
)

OK = FAIL = 0


def check(label: str, cond: bool) -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f"  FAIL {label}")


def main() -> int:
    print("VERIFY VL-HUB GERENCIA ALLOWLIST")
    nomes = nomes_vendas_tarefas_hub()
    for n in (
        "geraldinho",
        "geraldo hinnen",
        "renan hinnen",
        "isabela cugler",
        "zuleide hinnen",
    ):
        check(f"nome_padrao_{n.replace(' ', '_')}", n in nomes)

    pins = pins_vendas_tarefas_hub()
    check("codigos_padrao", all(c in pins for c in ("1", "2", "3", "4", "8")))

    with override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="99"):
        with patch("produtos.vendas_lojas_hub_acesso_util._perfil_por_pin") as mp:
            mp.return_value = MagicMock(codigo_vendedor="6")
            with patch("base.operador_util.nome_exibicao_perfil", return_value="Nathan Hinnen"):
                check("nathan_bloqueado", not operador_liberado_vendas_tarefas("1234"))

    with override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="99"):
        with patch("produtos.vendas_lojas_hub_acesso_util._perfil_por_pin") as mp:
            mp.return_value = MagicMock(codigo_vendedor="9999")
            with patch("base.operador_util.nome_exibicao_perfil", return_value="Renan Hinnen"):
                check("renan_9973_por_nome", operador_liberado_vendas_tarefas("9973"))

    with patch("produtos.vendas_lojas_hub_acesso_util._perfil_por_pin") as mp:
        mp.return_value = MagicMock(codigo_vendedor="1")
        with patch("base.operador_util.nome_exibicao_perfil", return_value="Renan Hinnen"):
            check("renan_por_codigo_1", operador_liberado_vendas_tarefas("9973"))

    check("norm_nome", _norm_nome("Renan  Hinnen") == "renan hinnen")

    print(f"\nTOTAL OK={OK} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
