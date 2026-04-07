from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date
from decimal import Decimal

from django.db import transaction
from django.db.models import Q, Sum

from rh.models import (
    FechamentoFolhaSimplificado,
    Funcionario,
    HistoricoSalarial,
    ItemFechamentoFolha,
    ValeFuncionario,
)

logger = logging.getLogger(__name__)

_Q2 = Decimal("0.01")


def money_two_decimals(value) -> Decimal:
    """SQLite pode devolver float em Sum; normaliza para Decimal com 2 casas."""
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value.quantize(_Q2)
    return Decimal(str(value)).quantize(_Q2)


def primeiro_dia_mes(d: date) -> date:
    return date(d.year, d.month, 1)


def ultimo_dia_mes(d: date) -> date:
    return date(d.year, d.month, monthrange(d.year, d.month)[1])


def salario_vigente_em(funcionario: Funcionario, em: date) -> Decimal:
    h = (
        HistoricoSalarial.objects.filter(
            funcionario=funcionario,
            data_inicio_vigencia__lte=em,
        )
        .filter(Q(data_fim_vigencia__isnull=True) | Q(data_fim_vigencia__gte=em))
        .order_by("-data_inicio_vigencia", "-id")
        .first()
    )
    return h.salario_base if h else Decimal("0")


def salario_vigente_para_competencia(funcionario: Funcionario, competencia: date) -> Decimal:
    """
    Salário aplicável ao mês de competência (folha).

    Usa o último dia do mês, não o 1º: uma faixa com início no próprio mês
    (ex.: vigência dia 15) passa a valer para aquele fechamento — antes ficava R$ 0
    ou valor antigo ao calcular só no dia 01.

    Se a regra estrita (início + fim de vigência) retornar zero mas existir histórico
    com início até o fim do mês, usa a faixa mais recente (dados legados com data_fim
    preenchida de forma que exclui o último dia do mês).
    """
    comp = primeiro_dia_mes(competencia)
    ult = ultimo_dia_mes(comp)
    sal = salario_vigente_em(funcionario, ult)
    if sal > 0:
        return sal
    legado = (
        HistoricoSalarial.objects.filter(
            funcionario=funcionario,
            data_inicio_vigencia__lte=ult,
        )
        .order_by("-data_inicio_vigencia", "-id")
        .first()
    )
    return legado.salario_base if legado else Decimal("0")


def total_vales_mes(funcionario: Funcionario, ano: int, mes: int) -> Decimal:
    ini = date(ano, mes, 1)
    fim = date(ano, mes, monthrange(ano, mes)[1])
    q = ValeFuncionario.objects.filter(
        funcionario=funcionario,
        data__gte=ini,
        data__lte=fim,
        cancelado=False,
    ).aggregate(t=Sum("valor"))
    return money_two_decimals(q["t"])


@transaction.atomic
def recalcular_fechamento(f: FechamentoFolhaSimplificado) -> FechamentoFolhaSimplificado:
    comp = f.competencia
    y, m = comp.year, comp.month
    sal = salario_vigente_para_competencia(f.funcionario, comp)
    vales = total_vales_mes(f.funcionario, y, m)
    f.salario_base_na_competencia = sal
    f.total_vales = vales
    f.valor_liquido_previsto = money_two_decimals(
        sal + f.outros_proventos - f.outros_descontos - vales
    )
    f.save()

    f.itens.all().delete()
    ordem = 0
    ItemFechamentoFolha.objects.create(
        fechamento=f,
        tipo=ItemFechamentoFolha.Tipo.SALARIO_BASE,
        descricao="Salário base (vigente no último dia do mês da competência)",
        valor=sal,
        ordem=ordem,
    )
    ordem += 1
    for v in (
        ValeFuncionario.objects.filter(
            funcionario=f.funcionario,
            data__year=y,
            data__month=m,
            cancelado=False,
        )
        .order_by("data", "id")
    ):
        ItemFechamentoFolha.objects.create(
            fechamento=f,
            tipo=ItemFechamentoFolha.Tipo.VALE,
            descricao=f"Vale {v.get_tipo_origem_display()} — {v.data:%d/%m/%Y}"
            + (f" ({v.observacao[:80]}…)" if len(v.observacao) > 80 else (f" — {v.observacao}" if v.observacao else "")),
            valor=v.valor,
            referencia_tipo="ValeFuncionario",
            referencia_id=str(v.pk),
            ordem=ordem,
        )
        ordem += 1
    if f.outros_proventos and f.outros_proventos != Decimal("0"):
        ItemFechamentoFolha.objects.create(
            fechamento=f,
            tipo=ItemFechamentoFolha.Tipo.ACRESCIMO,
            descricao="Outros proventos",
            valor=f.outros_proventos,
            ordem=ordem,
        )
        ordem += 1
    if f.outros_descontos and f.outros_descontos != Decimal("0"):
        ItemFechamentoFolha.objects.create(
            fechamento=f,
            tipo=ItemFechamentoFolha.Tipo.DESCONTO,
            descricao="Outros descontos",
            valor=f.outros_descontos,
            ordem=ordem,
        )
    return f


def garantir_fechamento_aberto(funcionario: Funcionario, competencia: date) -> FechamentoFolhaSimplificado:
    comp = primeiro_dia_mes(competencia)
    f, _ = FechamentoFolhaSimplificado.objects.get_or_create(
        funcionario=funcionario,
        competencia=comp,
        defaults={
            "empresa": funcionario.empresa,
            "salario_base_na_competencia": salario_vigente_para_competencia(funcionario, comp),
            "total_vales": Decimal("0"),
            "outros_descontos": Decimal("0"),
            "outros_proventos": Decimal("0"),
            "valor_liquido_previsto": Decimal("0"),
            "valor_pago": Decimal("0"),
            "status": FechamentoFolhaSimplificado.Status.ABERTO,
        },
    )
    if f.status == FechamentoFolhaSimplificado.Status.ABERTO:
        recalcular_fechamento(f)
    return f


def recalcular_todos_abertos_funcionario(funcionario: Funcionario):
    from rh.services.salario_financeiro_mongo import sincronizar_valores_titulo_salario_mongo

    for f in FechamentoFolhaSimplificado.objects.filter(
        funcionario=funcionario,
        status=FechamentoFolhaSimplificado.Status.ABERTO,
    ):
        recalcular_fechamento(f)
        if (f.mongo_lancamento_salario_id or "").strip():
            try:
                sincronizar_valores_titulo_salario_mongo(f)
            except Exception:
                logger.exception("RH: sync título salário após recalcular fechamento #%s", f.pk)


def reabrir_fechamento(f: FechamentoFolhaSimplificado) -> FechamentoFolhaSimplificado:
    """Volta status para Aberto (ex.: fechamento ou «pago» por engano). Recalcula a folha."""
    if f.status == FechamentoFolhaSimplificado.Status.ABERTO:
        recalcular_fechamento(f)
        return f
    needs_zero_pago = f.status in (
        FechamentoFolhaSimplificado.Status.PAGO,
        FechamentoFolhaSimplificado.Status.PAGO_PARCIAL,
    )
    f.status = FechamentoFolhaSimplificado.Status.ABERTO
    f.fechado_em = None
    update_fields = ["status", "fechado_em", "atualizado_em"]
    if needs_zero_pago:
        f.valor_pago = Decimal("0")
        update_fields.append("valor_pago")
    f.save(update_fields=update_fields)
    recalcular_fechamento(f)
    return f


def motivo_bloqueio_exclusao_fechamento(f: FechamentoFolhaSimplificado) -> str | None:
    """
    Retorna mensagem se não pode apagar o registro de competência, ou None se pode.
    Evita apagar com título Mongo vinculado (ficaria lançamento órfão no financeiro).
    """
    if (f.mongo_lancamento_salario_id or "").strip():
        return (
            "Não é possível excluir: existe título de salário no financeiro vinculado a esta competência. "
            "Use «Reabrir folha» para editar; no financeiro, trate o lançamento manualmente se precisar remover."
        )
    if f.status == FechamentoFolhaSimplificado.Status.PAGO:
        return "Reabra a folha primeiro (volta a Aberto e zera o valor pago de controle) para poder excluir."
    return None
