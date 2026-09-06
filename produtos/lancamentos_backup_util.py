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
    lancamentos_montar_query_mongo,
)

_BACKUP_CAP = 120_000


def _linha_backup(doc: dict, despesa: bool) -> dict[str, Any]:
    row = lancamento_para_api(doc, despesa)
    row["fonte_agro"] = "Sim" if doc.get(AGRO_FONTE_VERDADE) else "Não"
    ce = doc.get(AGRO_CONGELADO_EM)
    row["congelado_em"] = (str(ce)[:19] if ce else "")
    row["id_erp"] = str(doc.get("Id") or doc.get("ID") or "")[:80]
    return row


def _filtro_mongo_backup(despesa: bool, *, somente_abertos: bool = False) -> dict[str, Any]:
    if somente_abertos:
        return lancamentos_montar_query_mongo(despesa=despesa, status="abertos")
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


def _iter_csv_lancamentos(db, despesa: bool, *, somente_abertos: bool = False) -> Iterator[tuple[Any, ...]]:
    col = db[COL_DTO_LANCAMENTO]
    filtro = _filtro_mongo_backup(despesa, somente_abertos=somente_abertos)
    n = 0
    for doc in col.find(filtro).sort([("DataVencimento", 1), ("_id", 1)]).batch_size(400):
        if n >= _BACKUP_CAP:
            break
        yield _vals_lancamentos(_linha_backup(doc, despesa), despesa=despesa)
        n += 1


def _iter_csv_fiado(*, somente_abertos: bool = False) -> Iterator[tuple[Any, ...]]:
    from produtos.fiado_gestao_util import titulo_para_dict
    from produtos.models import FiadoTituloAgro

    n = 0
    qs = FiadoTituloAgro.objects.select_related("cliente_agro", "venda_agro").order_by("pk")
    if somente_abertos:
        qs = qs.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
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


def _contagem_mongo(db, despesa: bool, *, somente_abertos: bool = False) -> int | None:
    try:
        return int(
            db[COL_DTO_LANCAMENTO].count_documents(
                _filtro_mongo_backup(despesa, somente_abertos=somente_abertos)
            )
        )
    except Exception:
        return None


def _contagem_fiado(*, somente_abertos: bool = False) -> int:
    from produtos.models import FiadoTituloAgro

    qs = FiadoTituloAgro.objects.all()
    if somente_abertos:
        qs = qs.exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
    return qs.count()


def montar_zip_backup_completo(
    db,
    *,
    gerado_por: str = "",
    somente_abertos: bool = False,
) -> tuple[bytes, dict[str, Any]]:
    """ZIP com CSV separados — abre no Excel sem travar o PC (um arquivo por vez)."""
    if db is None:
        return b"", {"ok": False, "erro": "Mongo indisponível"}
    agora = timezone.localtime()
    buf = BytesIO()
    stats: dict[str, Any] = {"ok": True, "limite": _BACKUP_CAP, "somente_abertos": somente_abertos}

    sufixo_arq = "_em_aberto" if somente_abertos else ""
    rotulo_filtro = "Em aberto (mesmo critério da lista Lançamentos)" if somente_abertos else "Todos (abertos + quitados)"

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
        body_p, c_p = _csv_bytes(
            _headers_lancamentos(despesa=True),
            _iter_csv_lancamentos(db, True, somente_abertos=somente_abertos),
        )
        zf.writestr(f"01_a_pagar{sufixo_arq}.csv", body_p)
        body_r, c_r = _csv_bytes(
            _headers_lancamentos(despesa=False),
            _iter_csv_lancamentos(db, False, somente_abertos=somente_abertos),
        )
        zf.writestr(f"02_a_receber{sufixo_arq}.csv", body_r)
        body_f, c_f = _csv_bytes(fiado_headers, _iter_csv_fiado(somente_abertos=somente_abertos))
        zf.writestr(f"03_fiado_pdv{sufixo_arq}.csv", body_f)

        tot_p = _contagem_mongo(db, True, somente_abertos=somente_abertos)
        tot_r = _contagem_mongo(db, False, somente_abertos=somente_abertos)
        tot_f = _contagem_fiado(somente_abertos=somente_abertos)
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
            f"Filtro: {rotulo_filtro}\r\n"
            f"Gerado: {agora.strftime('%d/%m/%Y %H:%M')}\r\n"
            f"Por: {(gerado_por or '—')[:120]}\r\n"
            f"\r\n"
            f"Arquivos:\r\n"
            f"  01_a_pagar{sufixo_arq}.csv     — {c_p} linhas (total no filtro: {tot_p if tot_p is not None else '?'})\r\n"
            f"  02_a_receber{sufixo_arq}.csv   — {c_r} linhas (total no filtro: {tot_r if tot_r is not None else '?'})\r\n"
            f"  03_fiado_pdv{sufixo_arq}.csv   — {c_f} linhas (total no filtro: {tot_f})\r\n"
            f"\r\n"
            f"Como abrir: extraia o ZIP e abra UM CSV por vez no Excel.\r\n"
            f"Fiado PDV é módulo separado; PDV continua igual.\r\n"
        )
        if stats.get("truncado"):
            readme += f"\r\nATENÇÃO: limite de exportação {_BACKUP_CAP} linhas por arquivo.\r\n"
        zf.writestr("LEIA-ME.txt", readme.encode("utf-8"))

    return buf.getvalue(), stats


def _iter_csv_lancamentos_pg(despesa: bool, *, somente_abertos: bool = False) -> Iterator[tuple[Any, ...]]:
    from produtos.models import TituloFinanceiroAgro

    qs = TituloFinanceiroAgro.objects.filter(despesa=bool(despesa)).order_by(
        "data_vencimento", "id"
    )
    if somente_abertos:
        qs = qs.filter(quitado=False).exclude(valor_restante=0)
    n = 0
    for t in qs.iterator(chunk_size=400):
        if n >= _BACKUP_CAP:
            break
        row = {
            "id": t.mongo_id or str(t.pk),
            "id_erp": "",
            "data_vencimento": t.data_vencimento.isoformat() if t.data_vencimento else "",
            "data_competencia": t.data_competencia.isoformat() if t.data_competencia else "",
            "data_pagamento": t.data_pagamento.isoformat() if t.data_pagamento else "",
            "cliente": t.cliente or "",
            "descricao": t.descricao or "",
            "numero_documento": t.numero_documento or "",
            "parcela": t.parcela or 0,
            "plano_conta": t.plano_conta or "",
            "grupo": t.grupo or "",
            "forma_pagamento": t.forma_pagamento or "",
            "banco": t.banco or "",
            "centro_custo": "",
            "empresa": t.empresa or "",
            "valor_bruto": t.valor_bruto,
            "valor_movimentado": t.valor_pago,
            "restante": t.valor_restante,
            "pago": bool(t.quitado),
            "observacoes": t.observacoes or "",
            "fonte_agro": "Sim",
            "congelado_em": "",
            "criado_por": t.criado_por or "",
            "modificado_por": t.modificado_por or "",
        }
        snap = t.dados_snapshot_json if isinstance(t.dados_snapshot_json, dict) else {}
        row["id_erp"] = str(snap.get("id_erp") or "")[:80]
        yield _vals_lancamentos(row, despesa=despesa)
        n += 1


def _contagem_pg(despesa: bool, *, somente_abertos: bool = False) -> int:
    from produtos.models import TituloFinanceiroAgro

    qs = TituloFinanceiroAgro.objects.filter(despesa=bool(despesa))
    if somente_abertos:
        qs = qs.filter(quitado=False).exclude(valor_restante=0)
    return qs.count()


def montar_zip_backup_completo_pg(
    *,
    gerado_por: str = "",
    somente_abertos: bool = False,
) -> tuple[bytes, dict[str, Any]]:
    """Backup CP/CR a partir do Postgres (loja desvinculada do Mongo financeiro)."""
    agora = timezone.localtime()
    buf = BytesIO()
    stats: dict[str, Any] = {
        "ok": True,
        "gerado_em": agora.isoformat(),
        "fonte": "postgres",
        "somente_abertos": bool(somente_abertos),
    }
    sufixo_arq = "_em_aberto" if somente_abertos else ""
    rotulo_filtro = (
        "Em aberto (mesmo critério da lista Lançamentos)"
        if somente_abertos
        else "Todos (abertos + quitados)"
    )
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
        body_p, c_p = _csv_bytes(
            _headers_lancamentos(despesa=True),
            _iter_csv_lancamentos_pg(True, somente_abertos=somente_abertos),
        )
        zf.writestr(f"01_a_pagar{sufixo_arq}.csv", body_p)
        body_r, c_r = _csv_bytes(
            _headers_lancamentos(despesa=False),
            _iter_csv_lancamentos_pg(False, somente_abertos=somente_abertos),
        )
        zf.writestr(f"02_a_receber{sufixo_arq}.csv", body_r)
        body_f, c_f = _csv_bytes(fiado_headers, _iter_csv_fiado(somente_abertos=somente_abertos))
        zf.writestr(f"03_fiado_pdv{sufixo_arq}.csv", body_f)
        tot_p = _contagem_pg(True, somente_abertos=somente_abertos)
        tot_r = _contagem_pg(False, somente_abertos=somente_abertos)
        tot_f = _contagem_fiado(somente_abertos=somente_abertos)
        stats.update(
            {
                "export_pagar": c_p,
                "export_receber": c_r,
                "export_fiado": c_f,
                "total_pagar": tot_p,
                "total_receber": tot_r,
                "total_fiado": tot_f,
                "truncado": any(x > _BACKUP_CAP for x in (tot_p, tot_r, tot_f))
                or c_p >= _BACKUP_CAP
                or c_r >= _BACKUP_CAP
                or c_f >= _BACKUP_CAP,
            }
        )
        readme = (
            f"SisVale — backup Lançamentos\r\n"
            f"Filtro: {rotulo_filtro}\r\n"
            f"Fonte: Postgres (SisVale)\r\n"
            f"Gerado: {agora.strftime('%d/%m/%Y %H:%M')}\r\n"
            f"Por: {(gerado_por or '—')[:120]}\r\n"
            f"\r\n"
            f"Arquivos:\r\n"
            f"  01_a_pagar{sufixo_arq}.csv     — {c_p} linhas (total no filtro: {tot_p})\r\n"
            f"  02_a_receber{sufixo_arq}.csv   — {c_r} linhas (total no filtro: {tot_r})\r\n"
            f"  03_fiado_pdv{sufixo_arq}.csv   — {c_f} linhas (total no filtro: {tot_f})\r\n"
            f"\r\n"
            f"Como abrir: extraia o ZIP e abra UM CSV por vez no Excel.\r\n"
        )
        if stats.get("truncado"):
            readme += f"\r\nATENÇÃO: limite de exportação {_BACKUP_CAP} linhas por arquivo.\r\n"
        zf.writestr("LEIA-ME.txt", readme.encode("utf-8"))
    return buf.getvalue(), stats


def montar_zip_backup_lancamentos_dispatch(
    db,
    *,
    gerado_por: str = "",
    somente_abertos: bool = False,
) -> tuple[bytes, dict[str, Any]]:
    from produtos.agro_fonte_config import agro_financeiro_usa_postgres

    if agro_financeiro_usa_postgres():
        return montar_zip_backup_completo_pg(
            gerado_por=gerado_por, somente_abertos=somente_abertos
        )
    return montar_zip_backup_completo(
        db, gerado_por=gerado_por, somente_abertos=somente_abertos
    )


def nome_arquivo_backup_completo(ext: str = "zip", *, somente_abertos: bool = False) -> str:
    agora = timezone.localtime()
    sufixo = "_ABERTOS" if somente_abertos else ""
    return f"SisVale_Lancamentos_BACKUP{sufixo}_{agora.strftime('%Y-%m-%d_%H%M')}.{ext}"


# Compat — testes / chamadas antigas
def lancamentos_coletar_backup_completo(db) -> dict[str, Any]:
    _, stats = montar_zip_backup_completo(db)
    return stats


def montar_xlsx_backup_completo(dados: dict[str, Any], *, gerado_por: str = "") -> bytes:
    """Legado — preferir ZIP."""
    _ = dados, gerado_por
    return b""
