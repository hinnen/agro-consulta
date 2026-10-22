#!/usr/bin/env python
"""Prova Dist DF-e multi-CNPJ (Centro + Vila) sem SEFAZ."""
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

from produtos import dfe_inbox_util as inbox
from produtos.sefaz_dfe_client import dist_dfe_cnpjs_destino, dist_dfe_rotulo_cnpj

CENTRO = "48900774000103"
VILA = "48900774000286"
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
    print("=== verify_dfe_multi_cnpj ===")

    with patch(
        "produtos.sefaz_dfe_client._cfg_dist_dfe",
        return_value={"cnpj": CENTRO, "uf": "SP", "tp_amb": 1},
    ):
        cnpjs = dist_dfe_cnpjs_destino()
    _ok("lista inclui Centro", CENTRO in cnpjs, str(cnpjs))
    _ok("lista inclui Vila", VILA in cnpjs)
    _ok("rotulo Vila", dist_dfe_rotulo_cnpj(VILA) == "Vila Elias")
    _ok("rotulo Centro", dist_dfe_rotulo_cnpj(CENTRO) == "Centro")

    chamadas: list[str] = []
    gravados: list[tuple[str, str]] = []

    def _interesse(ult, *, cnpj_dest=None):
        chamadas.append(str(cnpj_dest or ""))
        return {
            "ok": True,
            "c_stat": 137,
            "x_motivo": "ok",
            "ult_nsu": "000000000000100",
            "max_nsu": "000000000000100",
            "notas_xml": [],
        }

    def _gravar(_db, cnpj, ult):
        gravados.append((str(cnpj), str(ult)))

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CENTRO}),
        patch("produtos.sefaz_dfe_client.dist_dfe_cnpjs_destino", return_value=[CENTRO, VILA]),
        patch("produtos.sefaz_dfe_client.nfe_distribuicao_dfe_interesse", side_effect=_interesse),
        patch("produtos.nfe_entrada_util.obter_ult_nsu", return_value="000000000000099"),
        patch("produtos.nfe_entrada_util.gravar_ult_nsu", side_effect=_gravar),
        patch.object(inbox, "dfe_inbox_listar_configurados", return_value=[]),
        patch.object(inbox, "dfe_completar_resumos_pendentes", return_value={"tentadas": 0}),
        patch.object(
            inbox,
            "dfe_inbox_upsert_xmls",
            return_value={"novas": 0, "atualizadas": 0, "resumos": 0},
        ),
    ):
        out = inbox.dfe_executar_consulta_e_gravar(origem="manual")

    _ok("duas consultas SEFAZ", chamadas == [CENTRO, VILA], f"chamadas={chamadas}")
    _ok("cursor gravado por CNPJ", len(gravados) == 2 and {g[0] for g in gravados} == {CENTRO, VILA})
    _ok("ok agregado", bool(out.get("ok")))
    _ok("por_cnpj len 2", len(out.get("por_cnpj") or []) == 2)

    print(f"\nRESULTADO: {PASS} ok · {FAIL} fail")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
