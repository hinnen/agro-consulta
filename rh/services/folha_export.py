"""Impressão da folha (duas vias) e Excel da lista de fechamentos."""
from __future__ import annotations

from decimal import Decimal
from io import BytesIO

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

_MENOS = {"VALE", "DESCONTO"}
_MAIS = {"SALARIO_BASE", "ACRESCIMO"}
_MESES = (
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
)


def brl(valor) -> str:
    q = Decimal(valor or 0).quantize(Decimal("0.01"))
    neg = q < 0
    q = abs(q)
    txt = f"{q:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"-{txt}" if neg else txt


def competencia_por_extenso(fechamento) -> str:
    d = fechamento.competencia
    return f"{_MESES[d.month - 1]}/{d.year}"


def _hora_confiavel(vale) -> str:
    """Hora só quando o registro nasceu no mesmo dia do vale. Sem hora inventada."""
    criado = getattr(vale, "criado_em", None)
    if not criado or not getattr(vale, "data", None):
        return ""
    from django.utils import timezone

    local = timezone.localtime(criado) if timezone.is_aware(criado) else criado
    if local.date() != vale.data:
        return ""
    return local.strftime(" %H:%M")


def _descricao_impressa(item, vales: dict, nome_funcionario: str) -> str:
    if item.tipo == "VALE":
        vale = vales.get(str(item.referencia_id or ""))
        if vale is not None:
            return f"Vale — {vale.data:%d/%m/%Y}{_hora_confiavel(vale)} — {nome_funcionario}"
        return f"Vale — {nome_funcionario}"
    if item.tipo == "SALARIO_BASE":
        return "Salário base"
    prefixo = "Acréscimo" if item.tipo == "ACRESCIMO" else "Desconto"
    nome = (item.descricao or "").strip()
    if nome.lower().startswith(prefixo.lower()):
        return nome
    return f"{prefixo} — {nome}" if nome else prefixo


def linhas_para_holerite(fechamento) -> list[dict]:
    """Linhas da via. Pagamento de salário não entra na soma; fica no valor pago."""
    itens = [it for it in fechamento.itens.all() if it.tipo in _MENOS or it.tipo in _MAIS]
    ids = [
        it.referencia_id
        for it in itens
        if it.tipo == "VALE" and it.referencia_tipo == "ValeFuncionario" and it.referencia_id
    ]
    vales = {}
    if ids:
        from rh.models import ValeFuncionario

        vales = {str(v.pk): v for v in ValeFuncionario.objects.filter(pk__in=ids)}
    nome = fechamento.funcionario.nome_exibicao
    linhas = []
    for it in itens:
        provento = it.valor if it.tipo in _MAIS else None
        desconto = it.valor if it.tipo in _MENOS else None
        linhas.append(
            {
                "codigo": f"{len(linhas) + 1:03d}",
                "descricao": _descricao_impressa(it, vales, nome),
                "referencia": "—",
                "provento": brl(provento) if provento is not None else "—",
                "desconto": brl(desconto) if desconto is not None else "—",
                "provento_num": provento or Decimal("0"),
                "desconto_num": desconto or Decimal("0"),
            }
        )
    if linhas:
        return linhas
    base = [
        ("Salário base", fechamento.salario_base_na_competencia, None),
        ("Acréscimos", fechamento.outros_proventos, None),
        ("Vales", None, fechamento.total_vales),
        ("Descontos", None, fechamento.outros_descontos),
    ]
    for desc, provento, desconto in base:
        valor = provento if provento is not None else desconto
        if not valor:
            continue
        linhas.append(
            {
                "codigo": f"{len(linhas) + 1:03d}",
                "descricao": desc,
                "referencia": "—",
                "provento": brl(provento) if provento else "—",
                "desconto": brl(desconto) if desconto else "—",
                "provento_num": provento or Decimal("0"),
                "desconto_num": desconto or Decimal("0"),
            }
        )
    return linhas


def totais_holerite(linhas: list[dict]) -> dict:
    proventos = sum((ln["provento_num"] for ln in linhas), Decimal("0"))
    descontos = sum((ln["desconto_num"] for ln in linhas), Decimal("0"))
    return {
        "proventos": brl(proventos),
        "descontos": brl(descontos),
        "proventos_num": proventos,
        "descontos_num": descontos,
    }


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
