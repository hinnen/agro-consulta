#!/usr/bin/env python
"""
Prova unitária Dist DF-e — cursor no 656 (sem bater na SEFAZ).

Rodar:
  .\\.venv\\Scripts\\python.exe scripts\\verify_dfe_nsu_656.py
"""
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


CNPJ = "12345678000199"
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


def _run_consulta(mock_res: dict, *, cursor_antes: str = "2086") -> tuple[dict, list[str]]:
    gravados: list[str] = []

    def _gravar(_db, _cnpj, ult):
        gravados.append(str(ult))

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch("produtos.nfe_entrada_util.obter_ult_nsu", return_value=str(cursor_antes).zfill(15)),
        patch("produtos.nfe_entrada_util.gravar_ult_nsu", side_effect=_gravar),
        patch(
            "produtos.sefaz_dfe_client.nfe_distribuicao_dfe_interesse",
            return_value=mock_res,
        ),
        patch.object(inbox, "dfe_inbox_listar", return_value=[]),
        patch.object(
            inbox,
            "dfe_inbox_upsert_xmls",
            return_value={"novas": 0, "atualizadas": 0, "resumos": 0},
        ),
    ):
        out = inbox.dfe_executar_consulta_e_gravar(origem="manual")
    return out, gravados


def main() -> int:
    print("=== verify_dfe_nsu_656 ===")

    # 1) 656 SEFAZ com ultNSU maior → grava
    out, grav = _run_consulta(
        {
            "ok": False,
            "c_stat": 656,
            "x_motivo": "Consumo Indevido",
            "erro": "Consumo Indevido",
            "ult_nsu": "000000000002090",
            "max_nsu": "000000000002095",
            "aguardar_segundos": 3600,
            "notas_xml": [],
            "bloqueio_local": False,
        },
        cursor_antes="2086",
    )
    _ok("656 SEFAZ maior grava", grav == ["2090"] or grav == ["000000000002090"], f"grav={grav} salvo={out.get('ult_nsu_salvo')}")
    _ok("656 SEFAZ maior nao usa maxNSU", "2095" not in "".join(grav) and "000000000002095" not in grav)

    # 2) 656 SEFAZ com ultNSU menor/igual → nao anda pra tras
    out2, grav2 = _run_consulta(
        {
            "ok": False,
            "c_stat": 656,
            "x_motivo": "Consumo Indevido",
            "erro": "Consumo Indevido",
            "ult_nsu": "000000000002080",
            "max_nsu": "000000000002095",
            "aguardar_segundos": 3600,
            "notas_xml": [],
        },
        cursor_antes="2086",
    )
    _ok("656 SEFAZ menor nao grava", grav2 == [], f"grav={grav2}")
    _ok("656 SEFAZ menor mantem 2086", str(out2.get("ult_nsu_salvo") or "").lstrip("0") == "2086")

    # 3) Bloqueio local (Aguarde) com eco de NSU alto → NAO grava
    out3, grav3 = _run_consulta(
        {
            "ok": False,
            "c_stat": 656,
            "erro": "Aguarde 50 min",
            "aguardar_segundos": 3000,
            "ult_nsu": None,
            "max_nsu": None,
            "notas_xml": [],
            "bloqueio_local": True,
        },
        cursor_antes="2086",
    )
    _ok("bloqueio local nao grava", grav3 == [], f"grav={grav3} out_u={out3.get('ult_nsu')}")

    # 4) 138 avanca normalmente
    out4, grav4 = _run_consulta(
        {
            "ok": True,
            "c_stat": 138,
            "x_motivo": "Documento localizado",
            "ult_nsu": "000000000002091",
            "max_nsu": "000000000002095",
            "notas_xml": ["<nfeProc/>"],
        },
        cursor_antes="2086",
    )
    _ok("138 grava ultNSU", any(str(g).lstrip("0") == "2091" or g.endswith("2091") for g in grav4), f"grav={grav4}")
    _ok("138 ok", bool(out4.get("ok")))

    # 5) 137 grava ultNSU
    out5, grav5 = _run_consulta(
        {
            "ok": True,
            "c_stat": 137,
            "x_motivo": "Nenhum documento",
            "ult_nsu": "000000000002090",
            "max_nsu": "000000000002090",
            "aguardar_segundos": 3600,
            "notas_xml": [],
        },
        cursor_antes="2086",
    )
    _ok("137 grava ultNSU", any(str(g).endswith("2090") or str(g).lstrip("0") == "2090" for g in grav5), f"grav={grav5}")

    # 6) download por chave nao chama gravar_ult_nsu
    grav_chave: list[str] = []

    def _gravar_ch(_db, _cnpj, ult):
        grav_chave.append(str(ult))

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch("produtos.nfe_entrada_util.gravar_ult_nsu", side_effect=_gravar_ch),
        patch(
            "produtos.sefaz_dfe_client.nfe_distribuicao_dfe_por_chave",
            return_value={
                "ok": True,
                "c_stat": 138,
                "notas_xml": ["<nfeProc/>"],
                "x_motivo": "ok",
            },
        ),
        patch.object(inbox, "dfe_inbox_listar", return_value=[]),
        patch.object(
            inbox,
            "dfe_inbox_upsert_xmls",
            return_value={"novas": 1, "atualizadas": 0, "resumos": 0},
        ),
    ):
        out6 = inbox.dfe_executar_download_por_chave("35240112345678000199550010000021751000021750")
    _ok("chave nao mexe cursor", grav_chave == [], f"grav={grav_chave}")
    _ok("chave ok", bool(out6.get("ok")))

    # 7) client marca bloqueio_local e zera ult_nsu
    from produtos.sefaz_dfe_client import nfe_distribuicao_dfe_interesse

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch(
            "produtos.sefaz_dfe_client._cfg_dist_dfe",
            return_value={"cnpj": CNPJ, "uf": "SP", "tp_amb": 1, "cert_path": "x", "cert_password": "y"},
        ),
        patch(
            "produtos.sefaz_dfe_client.dfe_checar_limite_consulta",
            return_value={"ok": False, "erro": "Aguarde", "aguardar_segundos": 100, "c_stat": 656},
        ),
    ):
        r7 = nfe_distribuicao_dfe_interesse("000000000002090")
    _ok("client bloqueio_local", r7.get("bloqueio_local") is True)
    _ok("client ult_nsu None no bloqueio", r7.get("ult_nsu") is None, f"ult={r7.get('ult_nsu')}")

    print(f"\nRESULTADO: {PASS} ok · {FAIL} fail")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
