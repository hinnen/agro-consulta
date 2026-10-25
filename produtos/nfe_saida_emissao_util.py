"""Emissão NF-e 55 — homologação simulada (opcional) ou SOAP NFeAutorizacao4 SP."""
from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timezone
from typing import Any

from decouple import config

from produtos.nfe_saida_util import (
    NFE_SAIDA_STATUS_AUTORIZADA,
    NFE_SAIDA_STATUS_REJEITADA,
    checklist_conferencia,
    nfe_saida_emitente_uf_default,
    nfe_saida_tp_amb_default,
    total_itens,
    validar_destinatario,
    validar_itens,
    validar_pagamentos,
)

logger = logging.getLogger(__name__)


def _simular_habilitado(tp_amb: int) -> bool:
    raw = (config("NFE_SAIDA_SIMULAR_EMIT", default="") or "").strip().lower()
    if raw in ("1", "true", "sim", "yes"):
        return True
    if raw in ("0", "false", "nao", "não", "no"):
        return False
    return tp_amb == 2


def _gerar_chave_fake(cnpj: str, serie: int, nnf: int) -> str:
    c = re.sub(r"\D", "", cnpj)[:14].zfill(14)
    uf = "35"  # SP default
    aamm = datetime.now(timezone.utc).strftime("%y%m")
    mod = "55"
    ser = str(int(serie or 1)).zfill(3)[-3:]
    num = str(int(nnf or 1)).zfill(9)[-9:]
    tp_emis = "1"
    cod = str(random.randint(10000000, 99999999))
    base = f"{uf}{aamm}{c}{mod}{ser}{num}{tp_emis}{cod}"
    if len(base) < 43:
        base = base.ljust(43, "0")
    dv = str(random.randint(0, 9))
    return (base[:43] + dv)[:44]


def emitir_nfe_saida_documento(doc: dict, *, cnpj_emit: str) -> dict[str, Any]:
    """
    Valida e tenta transmitir. Retorno: ok, status, chave, protocolo, c_stat, x_motivo, simulado, erro.
    """
    from produtos.sefaz_dfe_client import distribuicao_dfe_configurada

    cert_ok = distribuicao_dfe_configurada()
    from produtos.nfe_saida_tributacao_util import erros_fiscal_emissao

    erros = (
        validar_destinatario(doc.get("destinatario") or {})
        + validar_itens(doc.get("itens") or [])
        + validar_pagamentos(doc.get("pagamento") or [], total_itens(doc.get("itens") or []))
        + erros_fiscal_emissao(doc, uf_emit=nfe_saida_emitente_uf_default())
    )
    if erros:
        return {
            "ok": False,
            "status": NFE_SAIDA_STATUS_REJEITADA,
            "erro": erros[0],
            "x_motivo": "; ".join(erros)[:500],
            "c_stat": 999,
        }
    if not cert_ok:
        return {
            "ok": False,
            "status": NFE_SAIDA_STATUS_REJEITADA,
            "erro": "Certificado A1 não configurado (NFE_DIST_DFE_* no servidor).",
            "c_stat": 280,
            "x_motivo": "Certificado não configurado.",
        }

    tp_amb = int(doc.get("tp_amb") or nfe_saida_tp_amb_default())
    fiscal = doc.get("fiscal") if isinstance(doc.get("fiscal"), dict) else {}
    serie = int(fiscal.get("serie") or doc.get("serie") or 1)
    nnf = int(doc.get("nNF") or random.randint(1, 999999))

    if _simular_habilitado(tp_amb):
        chave = _gerar_chave_fake(cnpj_emit, serie, nnf)
        xml = (
            f'<?xml version="1.0" encoding="UTF-8"?>'
            f"<nfeProc><NFe><infNFe Id=\"NFe{chave}\">"
            f"<ide><mod>55</mod><tpAmb>{tp_amb}</tpAmb></ide></infNFe></NFe>"
            f"<protNFe><infProt><cStat>100</cStat><xMotivo>Autorizado (simulado homologação)</xMotivo>"
            f"<nProt>SIM{random.randint(100000,999999)}</nProt></infProt></protNFe></nfeProc>"
        )
        return {
            "ok": True,
            "status": NFE_SAIDA_STATUS_AUTORIZADA,
            "chave": chave,
            "protocolo": f"SIM{random.randint(100000, 999999)}",
            "c_stat": 100,
            "x_motivo": "Autorizado o uso da NF-e (simulação homologação — configure NFE_SAIDA_SIMULAR_EMIT=0 para exigir SOAP real).",
            "nNF": nnf,
            "serie": serie,
            "xml_autorizado": xml,
            "simulado": True,
        }

    from produtos.nfe_saida_sp_emissao_util import emitir_nfe_saida_sefaz

    return emitir_nfe_saida_sefaz(doc, cnpj_emit=cnpj_emit)


def montar_html_danfe_previa(doc: dict, *, emitente_nome: str = "", emitente_cnpj: str = "") -> str:
    from produtos.nfe_saida_danfe_html import montar_html_danfe_previa as _montar

    return _montar(doc, emitente_nome=emitente_nome, emitente_cnpj=emitente_cnpj)
