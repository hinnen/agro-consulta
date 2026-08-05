#!/usr/bin/env python
"""Prova isolada da Ciência DF-e — sem Receita e sem banco real."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos import dfe_inbox_util as inbox
from produtos.sefaz_dfe_client import nfe_manifestar_ciencia_operacao

CNPJ = "48900774000103"
CHAVE = "35260848900774000103550010000012341000012345"
PASS = 0
FAIL = 0


def check(nome: str, cond: bool) -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  OK  {nome}")
    else:
        FAIL += 1
        print(f" FAIL {nome}")


def provar_cliente() -> None:
    enviado: dict[str, str] = {}

    def assinar(evento, _path, _password, _id):
        from lxml import etree

        return etree.tostring(evento, encoding="unicode"), None

    resposta = """
    <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
      <soap:Body><retEnvEvento xmlns="http://www.portalfiscal.inf.br/nfe">
        <cStat>128</cStat><xMotivo>Lote processado</xMotivo>
        <retEvento><infEvento>
          <cStat>135</cStat>
          <xMotivo>Evento registrado e vinculado a NF-e</xMotivo>
          <nProt>135260000000001</nProt>
        </infEvento></retEvento>
      </retEnvEvento></soap:Body>
    </soap:Envelope>
    """

    def postar(url, **kwargs):
        enviado["url"] = url
        enviado["xml"] = kwargs["data"].decode("utf-8")
        return SimpleNamespace(status_code=200, text=resposta)

    cfg = {
        "cert_path": "certificado.pfx",
        "cert_password": "senha",
        "cnpj": CNPJ,
        "uf": "SP",
        "tp_amb": 1,
    }
    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client.dfe_bloqueio_pc_local", return_value=None),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value=cfg),
        patch("produtos.nfce_sp_emissao_util._assinar_evento_xml", side_effect=assinar),
        patch(
            "produtos.nfce_sp_emissao_util._cert_pem_temporario",
            return_value=("cert.pem", "key.pem", []),
        ),
        patch("produtos.sefaz_dfe_client.requests.post", side_effect=postar),
    ):
        out = nfe_manifestar_ciencia_operacao(CHAVE)

    check("cStat 135 aceito", out.get("ok") and out.get("c_stat") == "135")
    check("protocolo capturado", out.get("protocolo") == "135260000000001")
    check("endpoint Ambiente Nacional", "www.nfe.fazenda.gov.br/NFeRecepcaoEvento4" in enviado["url"])
    check("evento 210210", "<tpEvento>210210</tpEvento>" in enviado["xml"])
    check("orgao 91", "<cOrgao>91</cOrgao>" in enviado["xml"])
    check("CNPJ destinatario", f"<CNPJ>{CNPJ}</CNPJ>" in enviado["xml"])
    check("chave correta", f"<chNFe>{CHAVE}</chNFe>" in enviado["xml"])


class FakeRow:
    pk = 7
    cnpj = CNPJ
    chave = CHAVE
    schema = "resumo"
    xml = "<resNFe/>"
    manifestacao_status = ""
    manifestacao_protocolo = ""
    manifestacao_mensagem = ""
    manifestacao_em = None

    def save(self, **_kwargs):
        return None

    def refresh_from_db(self):
        return None


def provar_fluxo() -> None:
    row = FakeRow()
    chamadas = {"ciencia": 0, "download": 0}

    def ciencia(_chave):
        chamadas["ciencia"] += 1
        return {
            "ok": True,
            "c_stat": "135",
            "x_motivo": "Evento registrado",
            "protocolo": "135260000000001",
        }

    def download(_chave):
        chamadas["download"] += 1
        row.schema = "nfe"
        row.xml = "<nfeProc/>"
        return {"ok": True, "c_stat": 138}

    with (
        patch("produtos.dfe_inbox_util.dfe_inbox_obter", return_value=row),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch("produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao", side_effect=ciencia),
        patch("produtos.dfe_inbox_util.dfe_executar_download_por_chave", side_effect=download),
        patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
    ):
        out = inbox.dfe_manifestar_ciencia_e_baixar(row.pk)

    check("fluxo retorna XML completo", out.get("ok") and out.get("xml_completo"))
    check("ciencia persistida", row.manifestacao_status == "ciencia")
    check("ciencia uma vez", chamadas["ciencia"] == 1)
    check("download apos ciencia", chamadas["download"] == 1)

    row.schema = "resumo"
    row.xml = "<resNFe/>"
    with (
        patch("produtos.dfe_inbox_util.dfe_inbox_obter", return_value=row),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch("produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao", side_effect=ciencia),
        patch(
            "produtos.dfe_inbox_util.dfe_executar_download_por_chave",
            return_value={"ok": False, "aguardar_segundos": 120},
        ),
        patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
    ):
        out2 = inbox.dfe_manifestar_ciencia_e_baixar(row.pk)
    check("nao repete evento", chamadas["ciencia"] == 1 and out2.get("ciencia_reutilizada"))


def main() -> int:
    print("=== verify_dfe_manifestacao ===")
    provar_cliente()
    provar_fluxo()
    print(f"\nRESULTADO: {PASS} ok · {FAIL} fail")
    print("VERIFY_OK" if not FAIL else "VERIFY_FAIL")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
