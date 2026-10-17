"""Emissão NF-e 55 — homologação simulada ou mensagem até SOAP NFeAutorizacao4 dedicado."""
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
    erros = (
        validar_destinatario(doc.get("destinatario") or {})
        + validar_itens(doc.get("itens") or [])
        + validar_pagamentos(doc.get("pagamento") or [], total_itens(doc.get("itens") or []))
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

    return {
        "ok": False,
        "status": NFE_SAIDA_STATUS_REJEITADA,
        "erro": (
            "Emissão NF-e 55 em produção ainda não está ligada ao webservice NFeAutorizacao4. "
            "Use homologação (NFE_DIST_DFE_TP_AMB=2) com simulação automática ou aguarde integração SOAP."
        ),
        "c_stat": 0,
        "x_motivo": "Integração SEFAZ NF-e 55 pendente.",
    }


def montar_html_danfe_previa(doc: dict, *, emitente_nome: str = "", emitente_cnpj: str = "") -> str:
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    itens = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    total = total_itens(itens)
    chave = doc.get("chave") or "— (prévia — ainda não autorizada)"
    rows = ""
    for ln in itens:
        if not isinstance(ln, dict):
            continue
        rows += (
            f"<tr><td>{ln.get('descricao','')}</td>"
            f"<td class='r'>{ln.get('qtd','')}</td>"
            f"<td class='r'>{ln.get('v_unit','')}</td>"
            f"<td class='r'>{ln.get('v_total','')}</td></tr>"
        )
    amb = "HOMOLOGAÇÃO" if int(doc.get("tp_amb") or 2) == 2 else "PRODUÇÃO"
    return f"""<!DOCTYPE html><html lang="pt-br"><head><meta charset="utf-8"><title>DANFE NF-e 55</title>
<style>
body{{font-family:Arial,sans-serif;font-size:12px;color:#111;margin:16px}}
h1{{font-size:18px;margin:0 0 8px}}
.box{{border:2px solid #111;padding:8px;margin-bottom:8px}}
table{{width:100%;border-collapse:collapse}}
th,td{{border:1px solid #333;padding:4px}}
th{{background:#eee;text-align:left}}
.r{{text-align:right}}
.chave{{font-family:monospace;font-size:11px;word-break:break-all}}
.warn{{background:#ffedd5;border:2px solid #ea580c;padding:8px;font-weight:bold;margin-bottom:8px}}
</style></head><body>
<div class="warn">PRÉVIA / DANFE — modelo 55 · {amb}</div>
<h1>DANFE — Documento Auxiliar da NF-e</h1>
<div class="box"><strong>Emitente</strong><br>{emitente_nome or 'GM Agro'} · CNPJ {emitente_cnpj or '—'}</div>
<div class="box"><strong>Destinatário</strong><br>
{dest.get('nome','')}<br>
Doc: {dest.get('documento','')} · {dest.get('uf','')}/{dest.get('cidade','')}<br>
{dest.get('logradouro','')} {dest.get('numero','')}
</div>
<p class="chave">Chave: {chave}</p>
<table><thead><tr><th>Descrição</th><th>Qtd</th><th>V.Unit</th><th>Total</th></tr></thead>
<tbody>{rows}</tbody>
<tfoot><tr><td colspan="3"><strong>Total</strong></td><td class="r"><strong>R$ {total}</strong></td></tr></tfoot>
</table>
<p style="margin-top:12px;font-size:10px">Simples Nacional — CSOSN por item. Conferência interna; não substitui XML autorizado.</p>
</body></html>"""
