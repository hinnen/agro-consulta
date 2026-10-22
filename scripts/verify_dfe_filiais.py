#!/usr/bin/env python
"""Prova Dist DF-e Centro + Vila Elias (sem bater na SEFAZ)."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.dfe_inbox_util import dfe_executar_consulta_e_gravar
from produtos.sefaz_dfe_client import dfe_cnpjs_interessados

CNPJ_CENTRO = "48900774000103"
CNPJ_VILA = "48900774000286"
PASS = 0
FAIL = 0


def _ok(nome: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  OK  {nome}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f" FAIL {nome}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("=== verify_dfe_filiais ===")
    with patch(
        "produtos.sefaz_dfe_client._cfg_dist_dfe",
        return_value={"cnpj": CNPJ_CENTRO},
    ):
        cnpjs = dfe_cnpjs_interessados()
    _ok("lista inclui matriz", CNPJ_CENTRO in cnpjs, str(cnpjs))
    _ok("lista inclui Vila Elias", CNPJ_VILA in cnpjs, str(cnpjs))

    chamados: list[str] = []

    def interesse(_ult, cnpj=None):
        chamados.append(str(cnpj or ""))
        return {
            "ok": True,
            "c_stat": 137,
            "x_motivo": "Nenhum documento",
            "ult_nsu": "000000000000001",
            "max_nsu": "000000000000001",
            "notas_xml": [],
        }

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch(
            "produtos.sefaz_dfe_client._cfg_dist_dfe",
            return_value={"cnpj": CNPJ_CENTRO},
        ),
        patch(
            "produtos.sefaz_dfe_client.nfe_distribuicao_dfe_interesse",
            side_effect=interesse,
        ),
        patch("produtos.nfe_entrada_util.obter_ult_nsu", return_value="0"),
        patch("produtos.nfe_entrada_util.gravar_ult_nsu"),
        patch("produtos.dfe_inbox_util.dfe_completar_resumos_pendentes", return_value={}),
        patch(
            "produtos.dfe_inbox_util.dfe_inbox_upsert_xmls",
            return_value={"novas": 0, "atualizadas": 0, "resumos": 0},
        ),
        patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
    ):
        out = dfe_executar_consulta_e_gravar(origem="manual")
    _ok("consulta ok", bool(out.get("ok")))
    _ok("pediu matriz e filial", chamados == [CNPJ_CENTRO, CNPJ_VILA], str(chamados))

    print(f"PASS={PASS} FAIL={FAIL}")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
