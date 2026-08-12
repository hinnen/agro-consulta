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


def _cfg() -> dict:
    return {
        "cert_path": "certificado.pfx",
        "cert_password": "senha",
        "cnpj": CNPJ,
        "uf": "SP",
        "tp_amb": 1,
    }


def _assinar(evento, _path, _password, _id):
    from lxml import etree

    return etree.tostring(evento, encoding="unicode"), None


def _soap_ret(c_stat: str, motivo: str, prot: str = "135260000000001") -> str:
    return f"""
    <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
      <soap:Body><retEnvEvento xmlns="http://www.portalfiscal.inf.br/nfe">
        <cStat>128</cStat><xMotivo>Lote processado</xMotivo>
        <retEvento><infEvento>
          <cStat>{c_stat}</cStat>
          <xMotivo>{motivo}</xMotivo>
          <nProt>{prot}</nProt>
        </infEvento></retEvento>
      </retEnvEvento></soap:Body>
    </soap:Envelope>
    """


def provar_cliente() -> None:
    enviado: dict[str, str] = {}

    def postar(url, **kwargs):
        enviado["url"] = url
        enviado["xml"] = kwargs["data"].decode("utf-8")
        return SimpleNamespace(status_code=200, text=_soap_ret("135", "Evento registrado e vinculado a NF-e"))

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client.dfe_bloqueio_pc_local", return_value=None),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value=_cfg()),
        patch("produtos.nfce_sp_emissao_util._assinar_evento_xml", side_effect=_assinar),
        patch(
            "produtos.nfce_sp_emissao_util._cert_pem_temporario",
            return_value=("cert.pem", "key.pem", []),
        ),
        patch("produtos.sefaz_dfe_client.requests.post", side_effect=postar),
    ):
        out = nfe_manifestar_ciencia_operacao(CHAVE)

    check("cStat 135 aceito (ignora lote 128)", out.get("ok") and out.get("c_stat") == "135")
    check("protocolo capturado", out.get("protocolo") == "135260000000001")
    check("endpoint Ambiente Nacional", "www.nfe.fazenda.gov.br/NFeRecepcaoEvento4" in enviado["url"])
    check("evento 210210", "<tpEvento>210210</tpEvento>" in enviado["xml"])
    check("orgao 91", "<cOrgao>91</cOrgao>" in enviado["xml"])
    check("CNPJ destinatario", f"<CNPJ>{CNPJ}</CNPJ>" in enviado["xml"])
    check("chave correta", f"<chNFe>{CHAVE}</chNFe>" in enviado["xml"])
    check("desc Ciencia", "Ciencia da Operacao" in enviado["xml"])

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch("produtos.sefaz_dfe_client.dfe_bloqueio_pc_local", return_value=None),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value=_cfg()),
        patch("produtos.nfce_sp_emissao_util._assinar_evento_xml", side_effect=_assinar),
        patch(
            "produtos.nfce_sp_emissao_util._cert_pem_temporario",
            return_value=("cert.pem", "key.pem", []),
        ),
        patch(
            "produtos.sefaz_dfe_client.requests.post",
            return_value=SimpleNamespace(
                status_code=200,
                text=_soap_ret("573", "Duplicidade de evento"),
            ),
        ),
    ):
        out573 = nfe_manifestar_ciencia_operacao(CHAVE)
    check("cStat 573 duplicidade aceito", out573.get("ok") and out573.get("c_stat") == "573")

    out_bad = nfe_manifestar_ciencia_operacao("123")
    check("chave invalida rejeitada", not out_bad.get("ok") and "44" in str(out_bad.get("erro") or ""))

    with (
        patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
        patch(
            "produtos.sefaz_dfe_client.dfe_bloqueio_pc_local",
            return_value={"ok": False, "erro": "bloqueado local", "c_stat": "LOCAL"},
        ),
    ):
        out_local = nfe_manifestar_ciencia_operacao(CHAVE)
    check("bloqueio PC local", not out_local.get("ok") and "bloqueado" in str(out_local.get("erro") or ""))


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

    row_err = FakeRow()
    with (
        patch("produtos.dfe_inbox_util.dfe_inbox_obter", return_value=row_err),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch(
            "produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao",
            return_value={"ok": False, "c_stat": "213", "x_motivo": "Rejeicao", "erro": "Rejeicao"},
        ),
        patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
    ):
        out_fail = inbox.dfe_manifestar_ciencia_e_baixar(row_err.pk)
    check("rejeicao SEFAZ propaga", not out_fail.get("ok") and row_err.manifestacao_status == "erro")

    row_ok = FakeRow()
    row_ok.schema = "nfe"
    row_ok.xml = "<nfeProc/>"
    with (
        patch("produtos.dfe_inbox_util.dfe_inbox_obter", return_value=row_ok),
        patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value={"cnpj": CNPJ}),
        patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
        patch("produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao") as mock_c,
    ):
        out_ja = inbox.dfe_manifestar_ciencia_e_baixar(row_ok.pk)
    check("XML ja completo nao chama SEFAZ", out_ja.get("xml_completo") and mock_c.call_count == 0)


def provar_cooldown_chave() -> None:
    """Aguarde da lista (137) não trava Buscar XML; 656 trava."""
    import time

    from django.core.cache import cache

    from produtos.sefaz_dfe_client import (
        dfe_aplicar_cooldown_apos_resposta,
        dfe_checar_limite_consulta,
    )

    cnpj = CNPJ
    cache.delete(f"agro_dfe_cooldown:{cnpj}")
    cache.delete(f"agro_dfe_last_call:nsu:{cnpj}")
    cache.delete(f"agro_dfe_last_call:chave:{cnpj}")

    dfe_aplicar_cooldown_apos_resposta(
        cnpj,
        c_stat=137,
        ult_nsu="1",
        max_nsu="1",
        x_motivo="Nenhum documento",
        origem="dist_nsu",
    )
    bloq_lista = dfe_checar_limite_consulta(cnpj, modo="dist_nsu")
    bloq_xml = dfe_checar_limite_consulta(cnpj, modo="cons_chave")
    check("137 trava Buscar lista", bool(bloq_lista and bloq_lista.get("aguardar_segundos")))
    check("137 nao trava Buscar XML", bloq_xml is None)

    dfe_aplicar_cooldown_apos_resposta(
        cnpj,
        c_stat=656,
        ult_nsu="1",
        max_nsu="1",
        x_motivo="Consumo indevido",
        origem="cons_chave",
    )
    bloq_xml2 = dfe_checar_limite_consulta(cnpj, modo="cons_chave")
    bloq_lista2 = dfe_checar_limite_consulta(cnpj, modo="dist_nsu")
    check("656 trava Buscar XML", bool(bloq_xml2 and bloq_xml2.get("c_stat") == 656))
    check("656 trava Buscar lista", bool(bloq_lista2))

    # cons_chave com 138/fim NSU não deve gravar cooldown de lista
    cache.delete(f"agro_dfe_cooldown:{cnpj}")
    dfe_aplicar_cooldown_apos_resposta(
        cnpj,
        c_stat=138,
        ult_nsu="99",
        max_nsu="99",
        x_motivo="Documento localizado",
        origem="cons_chave",
    )
    check(
        "138 chave nao grava Aguarde lista",
        dfe_checar_limite_consulta(cnpj, modo="dist_nsu") is None,
    )
    # limpa
    cache.delete(f"agro_dfe_cooldown:{cnpj}")
    _ = time.time()


def provar_rota_migrate() -> None:
    import importlib.util

    from django.urls import reverse

    url = reverse("api_entrada_nota_dfe_inbox_ciencia", args=[7])
    check("rota ciencia", url == "/api/entrada-nota/dfe-inbox/7/ciencia/")

    mig_path = ROOT / "produtos" / "migrations" / "0083_dfe_manifestacao_ciencia.py"
    spec = importlib.util.spec_from_file_location("mig_0083_dfe", mig_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    Migration = mod.Migration
    check("migrate 0083 depende 0082", Migration.dependencies == [("produtos", "0082_plano_conta_agro")])
    ops = [op.name for op in Migration.operations]
    check(
        "migrate campos manifestacao",
        ops == [
            "manifestacao_em",
            "manifestacao_mensagem",
            "manifestacao_protocolo",
            "manifestacao_status",
        ],
    )


def main() -> int:
    print("=== verify_dfe_manifestacao ===")
    provar_cliente()
    provar_fluxo()
    provar_cooldown_chave()
    provar_rota_migrate()
    print(f"\nRESULTADO: {PASS} ok · {FAIL} fail")
    print("VERIFY_OK" if not FAIL else "VERIFY_FAIL")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
