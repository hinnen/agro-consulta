"""
PDF do relatório financeiro — layout próximo ao modelo de referência (tabela limpa + bloco manual).
"""
from __future__ import annotations

import os
from io import BytesIO
from typing import Any
from xml.sax.saxutils import escape

import reportlab
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle

from .lancamentos_financeiro_xlsx import (
    fmt_brl_pdf,
    fmt_venc_dd_mmm,
    ref_periodo_label,
)

_FONT_DIR = os.path.join(os.path.dirname(reportlab.__file__), "fonts")
_VERA = os.path.join(_FONT_DIR, "Vera.ttf")
_VERA_BD = os.path.join(_FONT_DIR, "VeraBd.ttf")

_registered = False


def _register_fonts() -> None:
    global _registered
    if _registered:
        return
    if os.path.isfile(_VERA):
        pdfmetrics.registerFont(TTFont("Vera", _VERA))
    if os.path.isfile(_VERA_BD):
        pdfmetrics.registerFont(TTFont("Vera-Bold", _VERA_BD))
    _registered = True


def _p(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(escape(str(text or "")).replace("\n", "<br/>"), style)


def _linha_tabela(row: dict[str, Any], styles: dict[str, Any]) -> list:
    vb = float(row.get("valor_bruto") or 0)
    vm = float(row.get("valor_movimentado") or 0)
    pago = bool(row.get("pago"))
    restante = float(row.get("restante") or 0)
    # Quitação parcial: já houve movimento e ainda há saldo em aberto.
    quita_parcial = vm > 0.01 and restante > 0.01

    c_plano = (row.get("plano_conta") or "").strip()

    if pago and abs(vm) <= 0.005 and abs(vb) > 0.005:
        mov_txt = fmt_brl_pdf(vb)
    elif pago or abs(vm) > 0.005:
        mov_txt = fmt_brl_pdf(vm)
    else:
        mov_txt = ""

    forma_pg = (row.get("forma_pagamento") or "").strip()

    if quita_parcial:
        vb_cell: list | Paragraph = [
            Paragraph(escape(fmt_brl_pdf(vb)), styles["num_lg"]),
            Paragraph(escape("Saldo: " + fmt_brl_pdf(restante)), styles["num_saldo_linha"]),
        ]
    else:
        vb_cell = _p(fmt_brl_pdf(vb), styles["num_lg"])

    return [
        _p(fmt_venc_dd_mmm(row.get("data_vencimento")), styles["cell"]),
        _p(row.get("cliente") or "", styles["cell"]),
        _p(c_plano, styles["cell"]),
        vb_cell,
        _p(mov_txt, styles["num"]),
        _p(forma_pg, styles["cell_sm"]),
        _p("", styles["cell"]),
    ]


def montar_pdf_financeiro_padrao(
    linhas: list[dict[str, Any]],
    *,
    despesa: bool,
    v_de,
    v_ate,
    status: str,
) -> bytes:
    _register_fonts()
    font = "Vera" if "Vera" in pdfmetrics.getRegisteredFontNames() else "Helvetica"
    font_bd = "Vera-Bold" if "Vera-Bold" in pdfmetrics.getRegisteredFontNames() else "Helvetica-Bold"

    base = getSampleStyleSheet()
    st_title = ParagraphStyle(
        "FinTitle",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=16,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=4,
        leading=20,
    )
    st_sub = ParagraphStyle(
        "FinSub",
        parent=base["Normal"],
        fontName=font,
        fontSize=9.5,
        textColor=colors.HexColor("#64748b"),
        spaceAfter=10,
        leading=12,
    )
    st_period = ParagraphStyle(
        "FinPeriod",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=11,
        textColor=colors.HexColor("#0f766e"),
        spaceAfter=14,
        leading=14,
    )
    cell = ParagraphStyle(
        "FinCell",
        parent=base["Normal"],
        fontName=font,
        fontSize=7.5,
        leading=9.5,
        alignment=TA_LEFT,
    )
    cell_sm = ParagraphStyle(
        "FinCellSm",
        parent=base["Normal"],
        fontName=font,
        fontSize=7,
        leading=8.5,
        alignment=TA_LEFT,
    )
    num = ParagraphStyle(
        "FinNum",
        parent=base["Normal"],
        fontName=font,
        fontSize=7.5,
        leading=9.5,
        alignment=TA_RIGHT,
    )
    num_lg = ParagraphStyle(
        "FinNumLg",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=11,
        leading=13.5,
        alignment=TA_RIGHT,
    )
    num_saldo_linha = ParagraphStyle(
        "FinNumSaldo",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=9,
        leading=11,
        alignment=TA_RIGHT,
        textColor=colors.HexColor("#0f172a"),
    )
    head = ParagraphStyle(
        "FinHead",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=7.5,
        textColor=colors.white,
        alignment=TA_CENTER,
        leading=10,
    )
    styles = {
        "cell": cell,
        "cell_sm": cell_sm,
        "num": num,
        "num_lg": num_lg,
        "num_saldo_linha": num_saldo_linha,
    }

    label_mov = "Pago" if despesa else "Recebido"
    header_cells = [
        _p("Vencimento", head),
        _p("Cliente / favorecido", head),
        _p("Plano conta", head),
        _p("Valor bruto", head),
        _p(label_mov, head),
        _p("Forma de pagamento", head),
        _p("QUAL CONTA?", head),
    ]

    data_rows: list[list] = [header_cells]
    for row in linhas:
        data_rows.append(_linha_tabela(row, styles))

    # Larguras (paisagem A4 ~ usable 269mm): valores em destaque; «Qual conta» vazio para caneta.
    col_widths = [
        22 * mm,
        64 * mm,
        44 * mm,
        34 * mm,
        34 * mm,
        38 * mm,
        33 * mm,
    ]

    tbl = Table(data_rows, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0d9488")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (3, 1), (4, -1), "RIGHT"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )

    # Bloco manual (segunda tabela)
    st_block_title = ParagraphStyle(
        "BlockT",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=9,
        textColor=colors.HexColor("#334155"),
        spaceBefore=16,
        spaceAfter=8,
    )
    man_head = ParagraphStyle(
        "ManHead",
        parent=base["Normal"],
        fontName=font_bd,
        fontSize=7.5,
        textColor=colors.white,
        alignment=TA_CENTER,
        leading=10,
    )
    man_rows = [
        [
            _p("VALOR ENTRADA", man_head),
            _p("VALOR DIVIDA", man_head),
            _p("DATA VENCIMENTO", man_head),
            _p("BENEFICIARIO", man_head),
        ],
    ]
    empty_cell = ParagraphStyle(
        "Empty",
        parent=base["Normal"],
        fontName=font,
        fontSize=8,
        leading=14,
        textColor=colors.HexColor("#94a3b8"),
    )
    for _ in range(6):
        man_rows.append([_p(" ", empty_cell), _p(" ", empty_cell), _p(" ", empty_cell), _p(" ", empty_cell)])

    man_tbl = Table(man_rows, colWidths=[52 * mm, 52 * mm, 44 * mm, 121 * mm])
    man_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#475569")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#f1f5f9"), colors.white]),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )

    bio = BytesIO()
    page = landscape(A4)
    doc = SimpleDocTemplate(
        bio,
        pagesize=page,
        leftMargin=14 * mm,
        rightMargin=14 * mm,
        topMargin=12 * mm,
        bottomMargin=14 * mm,
        title="Financeiro",
    )
    story = [
        _p("Relatório financeiro", st_title),
        _p(
            f"GM Agro Mais · Contas a {'pagar' if despesa else 'receber'} · Situação: {status}",
            st_sub,
        ),
        _p(f"Período: {ref_periodo_label(v_de, v_ate)}", st_period),
        tbl,
        _p("Anotações e conferência", st_block_title),
        man_tbl,
    ]
    doc.build(story)
    return bio.getvalue()
