"""Backup completo de lançamentos (pré-corte ERP) — Excel com todos os títulos."""
from __future__ import annotations

from io import BytesIO
from typing import Any

from django.utils import timezone
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font

from produtos.mongo_financeiro_util import (
    AGRO_CONGELADO_EM,
    AGRO_FONTE_VERDADE,
    COL_DTO_LANCAMENTO,
    lancamento_para_api,
    lancamentos_buscar_pagina,
    lancamentos_montar_query_mongo,
)

_BACKUP_CAP = 120_000


def _linha_backup(doc: dict, despesa: bool) -> dict[str, Any]:
    row = lancamento_para_api(doc, despesa)
    row["tipo_label"] = "A pagar" if despesa else "A receber"
    row["fonte_agro"] = "Sim" if doc.get(AGRO_FONTE_VERDADE) else "Não"
    ce = doc.get(AGRO_CONGELADO_EM)
    row["congelado_em"] = (str(ce)[:19] if ce else "")
    row["criado_por"] = str(doc.get("CriadoPor") or "")[:200]
    row["modificado_por"] = str(doc.get("ModificadoPor") or "")[:200]
    row["id_erp"] = str(doc.get("Id") or doc.get("ID") or "")[:80]
    return row


def lancamentos_coletar_backup_completo(db) -> dict[str, Any]:
    """Todos os títulos (pagar + receber, status todos) para backup pré-corte ERP."""
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    cap = _BACKUP_CAP
    partes: dict[str, Any] = {"ok": True, "pagar": [], "receber": [], "total_pagar": 0, "total_receber": 0}

    for despesa, key in ((True, "pagar"), (False, "receber")):
        q = lancamentos_montar_query_mongo(despesa=despesa, status="todos")
        linhas, total, _ = lancamentos_buscar_pagina(
            db,
            q,
            despesa,
            page=1,
            page_size=cap,
            ordenacao="vencimento_asc",
            limite_max=cap,
        )
        partes[f"total_{key}"] = int(total)
        # Busca docs brutos em lote para carimbo Agro / ERP
        ids = [str(x.get("id") or "") for x in linhas if x.get("id")]
        docs_map: dict[str, dict] = {}
        if ids:
            try:
                from bson import ObjectId

                oids = []
                for i in ids:
                    try:
                        oids.append(ObjectId(i))
                    except Exception:
                        pass
                if oids:
                    for d in db[COL_DTO_LANCAMENTO].find({"_id": {"$in": oids}}):
                        docs_map[str(d.get("_id"))] = d
            except Exception:
                pass
        bucket = []
        for ln in linhas:
            doc = docs_map.get(str(ln.get("id") or ""))
            if doc:
                bucket.append(_linha_backup(doc, despesa))
            else:
                ln = dict(ln)
                ln["tipo_label"] = "A pagar" if despesa else "A receber"
                ln["fonte_agro"] = "?"
                ln["congelado_em"] = ""
                ln["criado_por"] = ""
                ln["modificado_por"] = ""
                ln["id_erp"] = ""
                bucket.append(ln)
        partes[key] = bucket

    partes["truncado"] = partes["total_pagar"] > len(partes["pagar"]) or partes["total_receber"] > len(
        partes["receber"]
    )
    partes["limite"] = cap
    return partes


def _escrever_aba(ws, linhas: list[dict], *, despesa: bool) -> None:
    head_font = Font(bold=True)
    label_mov = "Pago" if despesa else "Recebido"
    label_saldo = "A pagar" if despesa else "A receber"
    headers = (
        "ID SisVale",
        "ID ERP",
        "Vencimento",
        "Competência",
        "Data pagamento",
        "Cliente / favorecido",
        "Descrição",
        "Documento",
        "Parcela",
        "Plano conta",
        "Grupo",
        "Forma pagamento",
        "Banco",
        "Centro custo",
        "Empresa",
        "Valor bruto",
        label_mov,
        label_saldo,
        "Situação",
        "Observações",
        "Carimbo SisVale",
        "Congelado em",
        "Criado por",
        "Alterado por",
    )
    for col, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col, value=h)
        c.font = head_font
        c.alignment = Alignment(wrap_text=True, vertical="top")
    for ri, row in enumerate(linhas, start=2):
        vals = (
            row.get("id") or "",
            row.get("id_erp") or "",
            (row.get("data_vencimento") or "")[:10],
            (row.get("data_competencia") or "")[:10],
            (row.get("data_pagamento") or "")[:10],
            row.get("cliente") or "",
            row.get("descricao") or "",
            row.get("numero_documento") or "",
            row.get("parcela") or 0,
            row.get("plano_conta") or "",
            row.get("grupo") or "",
            row.get("forma_pagamento") or "",
            row.get("banco") or "",
            row.get("centro_custo") or "",
            row.get("empresa") or "",
            row.get("valor_bruto"),
            row.get("valor_movimentado"),
            row.get("restante"),
            "Quitado" if row.get("pago") else "Aberto",
            row.get("observacoes") or "",
            row.get("fonte_agro") or "",
            row.get("congelado_em") or "",
            row.get("criado_por") or "",
            row.get("modificado_por") or "",
        )
        for col, v in enumerate(vals, start=1):
            ws.cell(row=ri, column=col, value=v)


def montar_xlsx_backup_completo(
    dados: dict[str, Any],
    *,
    gerado_por: str = "",
) -> bytes:
    wb = Workbook()
    ws_r = wb.active
    ws_r.title = "A receber"
    _escrever_aba(ws_r, dados.get("receber") or [], despesa=False)
    ws_p = wb.create_sheet("A pagar")
    _escrever_aba(ws_p, dados.get("pagar") or [], despesa=True)
    ws_i = wb.create_sheet("Resumo")
    agora = timezone.localtime()
    bold = Font(bold=True)
    ws_i["A1"] = "Backup completo — Lançamentos SisVale"
    ws_i["A1"].font = bold
    ws_i["A2"] = "Gerado em"
    ws_i["B2"] = agora.strftime("%d/%m/%Y %H:%M")
    ws_i["A3"] = "Por"
    ws_i["B3"] = (gerado_por or "—")[:120]
    ws_i["A5"] = "Títulos a pagar (total no sistema)"
    ws_i["B5"] = dados.get("total_pagar", 0)
    ws_i["A6"] = "Títulos a receber (total no sistema)"
    ws_i["B6"] = dados.get("total_receber", 0)
    ws_i["A7"] = "Linhas exportadas (pagar)"
    ws_i["B7"] = len(dados.get("pagar") or [])
    ws_i["A8"] = "Linhas exportadas (receber)"
    ws_i["B8"] = len(dados.get("receber") or [])
    if dados.get("truncado"):
        ws_i["A10"] = "Atenção"
        ws_i["B10"] = (
            f"Lista truncada no limite de {dados.get('limite')} por aba. "
            "Avise o suporte se a loja tiver mais títulos."
        )
    ws_i["A12"] = "Uso"
    ws_i["B12"] = (
        "Guarde este arquivo no seu computador antes de congelar/cortar vínculo com o ERP. "
        "Não apaga nem altera nada no sistema."
    )
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def nome_arquivo_backup_completo() -> str:
    agora = timezone.localtime()
    return f"SisVale_Lancamentos_BACKUP_{agora.strftime('%Y-%m-%d_%H%M')}.xlsx"
