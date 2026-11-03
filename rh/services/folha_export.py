"""Impressão da folha (duas vias) e Excel da lista de fechamentos."""
from __future__ import annotations

from decimal import Decimal
from io import BytesIO

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

_MENOS = {"VALE", "DESCONTO"}
_MAIS = {"SALARIO_BASE", "ACRESCIMO"}


def linhas_para_holerite(fechamento) -> list[dict]:
    """Itens que compõem o líquido. Pagamento de salário fica só no rodapé."""
    linhas = []
    for it in fechamento.itens.all():
        if it.tipo not in _MENOS and it.tipo not in _MAIS:
            continue
        sinal = Decimal("-1") if it.tipo in _MENOS else Decimal("1")
        linhas.append(
            {
                "nome": f"{it.get_tipo_display()} — {it.descricao}",
                "sinal": sinal,
                "valor": it.valor,
            }
        )
    if linhas:
        return linhas
    base = [
        ("Salário base", Decimal("1"), fechamento.salario_base_na_competencia),
        ("Vales", Decimal("-1"), fechamento.total_vales),
        ("Acréscimos", Decimal("1"), fechamento.outros_proventos),
        ("Descontos", Decimal("-1"), fechamento.outros_descontos),
    ]
    return [
        {"nome": nome, "sinal": sinal, "valor": valor}
        for nome, sinal, valor in base
        if valor and valor != 0
    ]


def xlsx_fechamentos(fechamentos) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "Fechamentos"
    headers = [
        "Funcionário",
        "Competência",
        "Empresa",
        "Status",
        "Salário base",
        "Vales",
        "Acréscimos",
        "Descontos",
        "Líquido previsto",
        "Valor pago",
        "Observações",
    ]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for f in fechamentos:
        ws.append(
            [
                f.funcionario.nome_exibicao,
                f"{f.competencia:%m/%Y}",
                getattr(f.empresa, "nome_fantasia", "") or "",
                f.get_status_display(),
                float(f.salario_base_na_competencia or 0),
                float(f.total_vales or 0),
                float(f.outros_proventos or 0),
                float(f.outros_descontos or 0),
                float(f.valor_liquido_previsto or 0),
                float(f.valor_pago or 0),
                f.observacoes or "",
            ]
        )
    for col in range(5, 11):
        for row in ws.iter_rows(min_row=2, min_col=col, max_col=col):
            for cell in row:
                cell.number_format = "#,##0.00"
    widths = [28, 14, 22, 14, 14, 12, 14, 14, 16, 14, 40]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()
