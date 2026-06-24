"""Resumo, planilha e ZIP mensal NFC-e para contabilidade."""
from __future__ import annotations

import csv
import io
import zipfile
from calendar import monthrange
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.db.models import Count, Max, Min, Q, Sum
from django.utils import timezone

from produtos.models import NfceDocumentoAgro, VendaAgro


def periodo_mes(ano: int, mes: int) -> tuple[date, date]:
    ultimo = monthrange(ano, mes)[1]
    return date(ano, mes, 1), date(ano, mes, ultimo)


def _qs_nfce_mes(ano: int, mes: int):
    return NfceDocumentoAgro.objects.filter(
        criado_em__year=ano,
        criado_em__month=mes,
    ).select_related("venda")


def resumo_nfce_mes(ano: int, mes: int) -> dict[str, Any]:
    qs = _qs_nfce_mes(ano, mes)
    agg = qs.aggregate(
        total=Count("pk"),
        autorizadas=Count("pk", filter=Q(status=NfceDocumentoAgro.Status.AUTORIZADA)),
        canceladas=Count("pk", filter=Q(status=NfceDocumentoAgro.Status.CANCELADA)),
        rejeitadas=Count("pk", filter=Q(status=NfceDocumentoAgro.Status.REJEITADA)),
        erros=Count("pk", filter=Q(status=NfceDocumentoAgro.Status.ERRO)),
        com_xml=Count("pk", filter=~Q(xml_autorizado="")),
        num_min=Min("numero", filter=Q(numero__gt=0)),
        num_max=Max("numero", filter=Q(numero__gt=0)),
    )
    ids_venda = list(
        qs.filter(status=NfceDocumentoAgro.Status.AUTORIZADA).values_list("venda_id", flat=True)
    )
    total_rs = Decimal("0")
    if ids_venda:
        total_rs = (
            VendaAgro.objects.filter(pk__in=ids_venda).aggregate(s=Sum("total")).get("s") or Decimal("0")
        )
    serie = (
        qs.filter(numero__gt=0)
        .order_by("-pk")
        .values_list("serie", flat=True)
        .first()
    )
    return {
        "ano": ano,
        "mes": mes,
        "total_notas": int(agg["total"] or 0),
        "autorizadas": int(agg["autorizadas"] or 0),
        "canceladas": int(agg["canceladas"] or 0),
        "rejeitadas": int(agg["rejeitadas"] or 0),
        "erros": int(agg["erros"] or 0),
        "com_xml": int(agg["com_xml"] or 0),
        "numero_min": agg["num_min"],
        "numero_max": agg["num_max"],
        "serie": serie,
        "total_autorizado_rs": float(total_rs),
    }


def linhas_planilha_nfce_mes(ano: int, mes: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    qs = (
        _qs_nfce_mes(ano, mes)
        .filter(
            status__in=(
                NfceDocumentoAgro.Status.AUTORIZADA,
                NfceDocumentoAgro.Status.CANCELADA,
            )
        )
        .order_by("numero", "pk")
    )
    for doc in qs:
        venda = doc.venda
        cpf = doc.dest_cpf or ""
        if not cpf and venda and venda.cliente_documento:
            cpf = (venda.cliente_documento or "")[:11]
        if doc.consumidor_sem_identificacao and not cpf:
            cpf = ""
        criado = timezone.localtime(doc.criado_em) if doc.criado_em else None
        rows.append(
            {
                "numero": doc.numero or "",
                "serie": doc.serie or "",
                "chave": doc.chave or "",
                "data_emissao": criado.strftime("%Y-%m-%d %H:%M:%S") if criado else "",
                "status": doc.get_status_display(),
                "status_codigo": doc.status,
                "valor": float(venda.total) if venda else 0.0,
                "cpf_consumidor": cpf,
                "sem_identificacao": bool(doc.consumidor_sem_identificacao),
                "venda_id": doc.venda_id,
                "protocolo": doc.protocolo or "",
                "tem_xml": bool((doc.xml_autorizado or "").strip()),
            }
        )
    return rows


PENDENCIAS_CABECALHO = [
    "venda_id",
    "data_emissao",
    "numero",
    "serie",
    "status",
    "valor",
    "mensagem_sefaz",
]


def linhas_pendencias_nfce_mes(ano: int, mes: int, *, limit: int | None = None) -> list[dict[str, Any]]:
    qs = (
        _qs_nfce_mes(ano, mes)
        .filter(
            status__in=(
                NfceDocumentoAgro.Status.REJEITADA,
                NfceDocumentoAgro.Status.ERRO,
            )
        )
        .order_by("-criado_em", "-pk")
    )
    if limit is not None:
        qs = qs[:limit]
    rows: list[dict[str, Any]] = []
    for doc in qs:
        venda = doc.venda
        criado = timezone.localtime(doc.criado_em) if doc.criado_em else None
        msg = (doc.mensagem_sefaz or "").strip().replace("\r\n", " ").replace("\n", " ")
        rows.append(
            {
                "venda_id": doc.venda_id,
                "data_emissao": criado.strftime("%Y-%m-%d %H:%M:%S") if criado else "",
                "numero": doc.numero or "",
                "serie": doc.serie or "",
                "status": doc.get_status_display(),
                "status_codigo": doc.status,
                "valor": float(venda.total) if venda else 0.0,
                "mensagem_sefaz": msg[:500],
            }
        )
    return rows


def pendencias_nfce_resumo_json(ano: int, mes: int, *, limit: int = 200) -> dict[str, Any]:
    total = _qs_nfce_mes(ano, mes).filter(
        status__in=(
            NfceDocumentoAgro.Status.REJEITADA,
            NfceDocumentoAgro.Status.ERRO,
        )
    ).count()
    linhas = linhas_pendencias_nfce_mes(ano, mes, limit=limit)
    return {
        "total": total,
        "mostrando": len(linhas),
        "truncado": total > len(linhas),
        "linhas": linhas,
    }


def pendencias_nfce_csv_bytes(ano: int, mes: int) -> bytes:
    buf = io.StringIO()
    buf.write("\ufeff")
    w = csv.writer(buf, delimiter=";")
    w.writerow(PENDENCIAS_CABECALHO)
    for row in linhas_pendencias_nfce_mes(ano, mes):
        w.writerow(
            [
                row["venda_id"],
                row["data_emissao"],
                row["numero"],
                row["serie"],
                row["status"],
                f"{row['valor']:.2f}".replace(".", ","),
                row["mensagem_sefaz"],
            ]
        )
    return buf.getvalue().encode("utf-8")


PLANILHA_CABECALHO = [
    "numero",
    "serie",
    "chave",
    "data_emissao",
    "status",
    "valor",
    "cpf_consumidor",
    "sem_identificacao",
    "venda_id",
    "protocolo",
    "tem_xml",
]


def planilha_nfce_csv_bytes(ano: int, mes: int) -> bytes:
    buf = io.StringIO()
    buf.write("\ufeff")
    w = csv.writer(buf, delimiter=";")
    w.writerow(PLANILHA_CABECALHO)
    for row in linhas_planilha_nfce_mes(ano, mes):
        w.writerow(
            [
                row["numero"],
                row["serie"],
                row["chave"],
                row["data_emissao"],
                row["status"],
                f"{row['valor']:.2f}".replace(".", ","),
                row["cpf_consumidor"],
                "sim" if row["sem_identificacao"] else "nao",
                row["venda_id"],
                row["protocolo"],
                "sim" if row["tem_xml"] else "nao",
            ]
        )
    return buf.getvalue().encode("utf-8")


def planilha_nfce_xlsx_bytes(ano: int, mes: int) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font

    wb = Workbook()
    ws = wb.active
    ws.title = f"NFC-e {mes:02d}-{ano}"
    ws.append(PLANILHA_CABECALHO)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for row in linhas_planilha_nfce_mes(ano, mes):
        ws.append(
            [
                row["numero"],
                row["serie"],
                row["chave"],
                row["data_emissao"],
                row["status"],
                row["valor"],
                row["cpf_consumidor"],
                "sim" if row["sem_identificacao"] else "nao",
                row["venda_id"],
                row["protocolo"],
                "sim" if row["tem_xml"] else "nao",
            ]
        )
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def montar_zip_nfce_mes(ano: int, mes: int) -> tuple[bytes, int]:
    """ZIP: index.csv + XMLs em autorizadas/ e canceladas/."""
    buf = io.BytesIO()
    count_xml = 0
    linhas = linhas_planilha_nfce_mes(ano, mes)
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        idx = io.StringIO()
        idx.write("\ufeff")
        w = csv.writer(idx, delimiter=";")
        w.writerow(PLANILHA_CABECALHO)
        for row in linhas:
            w.writerow(
                [
                    row["numero"],
                    row["serie"],
                    row["chave"],
                    row["data_emissao"],
                    row["status"],
                    f"{row['valor']:.2f}".replace(".", ","),
                    row["cpf_consumidor"],
                    "sim" if row["sem_identificacao"] else "nao",
                    row["venda_id"],
                    row["protocolo"],
                    "sim" if row["tem_xml"] else "nao",
                ]
            )
        zf.writestr(f"NFC-e/{ano}-{mes:02d}/index.csv", idx.getvalue().encode("utf-8"))

        qs = (
            _qs_nfce_mes(ano, mes)
            .filter(
                status__in=(
                    NfceDocumentoAgro.Status.AUTORIZADA,
                    NfceDocumentoAgro.Status.CANCELADA,
                )
            )
            .exclude(xml_autorizado="")
            .order_by("numero", "pk")
        )
        for doc in qs:
            chave = (doc.chave or f"venda{doc.venda_id}").strip()
            pasta = (
                "autorizadas"
                if doc.status == NfceDocumentoAgro.Status.AUTORIZADA
                else "canceladas"
            )
            nome = f"NFC-e/{ano}-{mes:02d}/{pasta}/{chave}.xml"
            zf.writestr(nome, doc.xml_autorizado.encode("utf-8"))
            count_xml += 1
    if count_xml == 0 and not linhas:
        return b"", 0
    buf.seek(0)
    return buf.getvalue(), count_xml


def urls_exportacao_mes(ano: int, mes: int) -> dict[str, str]:
    de, ate = periodo_mes(ano, mes)
    de_s = de.isoformat()
    ate_s = ate.isoformat()
    return {
        "vendas_csv": f"/vendas/exportar-csv/?de={de_s}&ate={ate_s}",
        "lancamentos_csv": (
            f"/api/lancamentos/export-csv/?tipo=pagar&status=todos"
            f"&comp_de={de_s}&comp_ate={ate_s}"
        ),
        "lancamentos_xlsx": (
            f"/api/lancamentos/export-financeiro-xlsx/?tipo=pagar&status=todos"
            f"&comp_de={de_s}&comp_ate={ate_s}"
        ),
        "lancamentos_pdf": (
            f"/api/lancamentos/export-financeiro-pdf/?tipo=pagar&status=todos"
            f"&comp_de={de_s}&comp_ate={ate_s}"
        ),
        "dre": f"/lancamentos/dre/?comp_de={de_s}&comp_ate={ate_s}",
        "resumo_gerencial": f"/financeiro/resumo-gerencial/?de={de_s}&ate={ate_s}",
    }
