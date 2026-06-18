"""Backup completo de lançamentos (pré-corte ERP) — ZIP com CSV (leve no PC)."""
from __future__ import annotations

import csv
import zipfile
from io import BytesIO, StringIO
from typing import Any, Iterator

from django.utils import timezone

from produtos.mongo_financeiro_util import (
    AGRO_CONGELADO_EM,
    AGRO_FONTE_VERDADE,
    COL_DTO_LANCAMENTO,
    lancamento_para_api,
)

_BACKUP_CAP = 120_000


def _linha_backup(doc: dict, despesa: bool) -> dict[str, Any]:
    row = lancamento_para_api(doc, despesa)
    row["fonte_agro"] = "Sim" if doc.get(AGRO_FONTE_VERDADE) else "Não"
    ce = doc.get(AGRO_CONGELADO_EM)
    row["congelado_em"] = (str(ce)[:19] if ce else "")
    row["id_erp"] = str(doc.get("Id") or doc.get("ID") or "")[:80]
    return row


def _filtro_mongo_backup(despesa: bool) -> dict[str, Any]:
    if despesa:
        return {"Despesa": True}
    return {
        "$or": [
            {"Despesa": False},
            {"Despesa": {"$exists": False}},
            {"Despesa": None},
        ]
    }


def _headers_lancamentos(*, despesa: bool) -> tuple[str, ...]:
    label_mov = "Pago" if despesa else "Recebido"
    label_saldo = "A pagar" if despesa else "A receber"
    return (
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


def _vals_lancamentos(row: dict, *, despesa: bool) -> tuple[Any, ...]:
    return (
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


def _iter_csv_lancamentos(db, despesa: bool) -> Iterator[tuple[Any, ...]]:
    col = db[COL_DTO_LANCAMENTO]
    filtro = _filtro_mongo_backup(despesa)
    n = 0
    for doc in col.find(filtro).sort([("DataVencimento", 1), ("_id", 1)]).batch_size(400):
        if n >= _BACKUP_CAP:
            break
        yield _vals_lancamentos(_linha_backup(doc, despesa), despesa=despesa)
        n += 1


def _iter_csv_fiado() -> Iterator[tuple[Any, ...]]:
    from produtos.fiado_gestao_util import titulo_para_dict
    from produtos.models import FiadoTituloAgro

    n = 0
    qs = FiadoTituloAgro.objects.select_related("cliente_agro", "venda_agro").order_by("pk")
    for t in qs.iterator(chunk_size=300):
        if n >= _BACKUP_CAP:
            break
        row = titulo_para_dict(t)
        yield (
            row.get("id"),
            row.get("cliente_nome") or "",
            row.get("cliente_codigo") or "",
            row.get("numero_documento") or "",
            f"{row.get('parcela_num') or 0}/{row.get('parcela_total') or 0}",
            row.get("vencimento_texto") or (row.get("vencimento") or "")[:10],
            row.get("valor_bruto"),
            row.get("valor_pago"),
            row.get("saldo_aberto"),
            row.get("situacao_label") or row.get("situacao") or "",
            row.get("origem") or "",
            row.get("venda_agro_id") or "",
            row.get("descricao") or "",
        )
        n += 1


def _csv_bytes(headers: tuple[str, ...], rows: Iterator[tuple[Any, ...]]) -> tuple[bytes, int]:
    sio = StringIO()
    w = csv.writer(sio, delimiter=";", lineterminator="\r\n")
    w.writerow(headers)
    count = 0
    for vals in rows:
        w.writerow(vals)
        count += 1
    return sio.getvalue().encode("utf-8-sig"), count


def _contagem_mongo(db, despesa: bool) -> int | None:
    try:
        return int(db[COL_DTO_LANCAMENTO].count_documents(_filtro_mongo_backup(despesa)))
    except Exception:
        return None


def montar_zip_backup_completo(db, *, gerado_por: str = "") -> tuple[bytes, dict[str, Any]]:
    """ZIP com CSV separados — abre no Excel sem travar o PC (um arquivo por vez)."""
    if db is None:
        return b"", {"ok": False, "erro": "Mongo indisponível"}
    agora = timezone.localtime()
    buf = BytesIO()
    stats: dict[str, Any] = {"ok": True, "limite": _BACKUP_CAP}

    fiado_headers = (
        "ID",
        "Cliente",
        "Código cliente",
        "Documento",
        "Parcela",
        "Vencimento",
        "Valor",
        "Pago",
        "Saldo",
        "Situação",
        "Origem",
        "Venda Agro ID",
        "Descrição",
    )

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        body_p, c_p = _csv_bytes(_headers_lancamentos(despesa=True), _iter_csv_lancamentos(db, True))
        zf.writestr("01_a_pagar.csv", body_p)
        body_r, c_r = _csv_bytes(_headers_lancamentos(despesa=False), _iter_csv_lancamentos(db, False))
        zf.writestr("02_a_receber.csv", body_r)
        body_f, c_f = _csv_bytes(fiado_headers, _iter_csv_fiado())
        zf.writestr("03_fiado_pdv.csv", body_f)

        tot_p = _contagem_mongo(db, True)
        tot_r = _contagem_mongo(db, False)
        from produtos.models import FiadoTituloAgro

        tot_f = FiadoTituloAgro.objects.count()
        stats.update(
            {
                "export_pagar": c_p,
                "export_receber": c_r,
                "export_fiado": c_f,
                "total_pagar": tot_p,
                "total_receber": tot_r,
                "total_fiado": tot_f,
                "truncado": any(
                    x is not None and x > _BACKUP_CAP
                    for x in (tot_p, tot_r, tot_f)
                )
                or c_p >= _BACKUP_CAP
                or c_r >= _BACKUP_CAP
                or c_f >= _BACKUP_CAP,
            }
        )

        readme = (
            f"SisVale — backup Lançamentos\r\n"
            f"Gerado: {agora.strftime('%d/%m/%Y %H:%M')}\r\n"
            f"Por: {(gerado_por or '—')[:120]}\r\n"
            f"\r\n"
            f"Arquivos:\r\n"
            f"  01_a_pagar.csv     — {c_p} linhas (total espelho: {tot_p if tot_p is not None else '?'})\r\n"
            f"  02_a_receber.csv   — {c_r} linhas (total espelho: {tot_r if tot_r is not None else '?'})\r\n"
            f"  03_fiado_pdv.csv   — {c_f} linhas (total fiado: {tot_f})\r\n"
            f"\r\n"
            f"Como abrir: extraia o ZIP e abra UM CSV por vez no Excel.\r\n"
            f"Não abra tudo numa planilha só — anos de contas a pagar pesam demais.\r\n"
            f"Fiado PDV é módulo separado; PDV continua igual.\r\n"
        )
        if stats.get("truncado"):
            readme += f"\r\nATENÇÃO: limite de exportação {_BACKUP_CAP} linhas por arquivo.\r\n"
        zf.writestr("LEIA-ME.txt", readme.encode("utf-8"))

    return buf.getvalue(), stats


def nome_arquivo_backup_completo(ext: str = "zip") -> str:
    agora = timezone.localtime()
    return f"SisVale_Lancamentos_BACKUP_{agora.strftime('%Y-%m-%d_%H%M')}.{ext}"


# Compat — testes / chamadas antigas
def lancamentos_coletar_backup_completo(db) -> dict[str, Any]:
    _, stats = montar_zip_backup_completo(db)
    return stats


def montar_xlsx_backup_completo(dados: dict[str, Any], *, gerado_por: str = "") -> bytes:
    """Legado — preferir ZIP."""
    _ = dados, gerado_por
    return b""
