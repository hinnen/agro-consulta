"""Card de empréstimos no DRE visual — títulos PG."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from django.db.models import Q

from financeiro.services.resumo_operacional_mongo import (
    _fold,
    classificar_despesa_plano,
    classificar_receita_plano,
)
from financeiro.models import LancamentoFinanceiro as NF
from produtos.lancamentos_financeiro_pg_analytics_util import (
    _campo_data_titulo,
    _valor_titulo_dre,
)
from produtos.lancamentos_financeiro_pg_util import _dec2, dedup_titulos
from produtos.models import TituloFinanceiroAgro


def eh_entrada_emprestimo(plano: str, *, despesa: bool) -> bool:
    if despesa:
        return False
    return classificar_receita_plano(plano) == NF.NATUREZA_EMPRESTIMO_ENTRADA


def eh_juros_emprestimo(plano: str, *, despesa: bool) -> bool:
    if not despesa:
        return False
    f = _fold(plano)
    return "juros" in f and "emprestimo" in f


def eh_pagamento_principal_emprestimo(plano: str, *, despesa: bool) -> bool:
    if not despesa or eh_juros_emprestimo(plano, despesa=True):
        return False
    f = _fold(plano)
    if "emprestimo" in f and (
        "pagamento" in f or "amortizacao" in f or "principal" in f or "liquidacao" in f
    ):
        return True
    return classificar_despesa_plano(plano) == NF.NATUREZA_EMPRESTIMO_AMORTIZACAO


def _qs_empresa(empresa_nome: str):
    nome = (empresa_nome or "").strip()
    qs = TituloFinanceiroAgro.objects.all()
    if nome:
        qs = qs.filter(empresa__iexact=nome)
    return qs


def resumo_emprestimos_pg(
    *,
    empresa_nome: str,
    data_inicio: date,
    data_fim: date,
    por: str = "competencia",
    valor: str = "bruto",
) -> dict[str, Any]:
    nome = (empresa_nome or "").strip()
    if not nome:
        return {"ok": False, "erro": "empresa vazia"}
    por_n = (por or "competencia").strip().lower()
    valor_n = (valor or "bruto").strip().lower()

    devido = Decimal("0")
    qs_aberto = _qs_empresa(nome).filter(despesa=True, quitado=False).filter(
        Q(plano_conta__icontains="empréstimo") | Q(plano_conta__icontains="emprestimo")
    )
    for t in dedup_titulos(list(qs_aberto)):
        if eh_pagamento_principal_emprestimo(t.plano_conta or "", despesa=True):
            devido += _dec2(t.valor_restante)

    pago = Decimal("0")
    juros = Decimal("0")
    qs_periodo = _qs_empresa(nome)
    if por_n == "vencimento":
        qs_periodo = qs_periodo.filter(
            data_vencimento__gte=data_inicio, data_vencimento__lte=data_fim
        )
    elif por_n == "pagamento":
        qs_periodo = qs_periodo.filter(
            data_pagamento__gte=data_inicio, data_pagamento__lte=data_fim
        )
    else:
        qs_periodo = qs_periodo.filter(
            data_competencia__gte=data_inicio, data_competencia__lte=data_fim
        )
    for t in dedup_titulos(list(qs_periodo)):
        dt = _campo_data_titulo(t, por_n)
        if dt is None or dt < data_inicio or dt > data_fim:
            continue
        if por_n == "pagamento" and _dec2(t.valor_pago) <= 0:
            continue
        plano = t.plano_conta or ""
        val = _valor_titulo_dre(t, valor_n)
        if val <= 0:
            continue
        if eh_juros_emprestimo(plano, despesa=bool(t.despesa)):
            juros += val
        elif eh_pagamento_principal_emprestimo(plano, despesa=bool(t.despesa)):
            pago += val

    emprestado = Decimal("0")
    qs_ent = _qs_empresa(nome).filter(despesa=False).filter(
        data_competencia__gte=data_inicio, data_competencia__lte=data_fim
    )
    for t in dedup_titulos(list(qs_ent)):
        dt = t.data_competencia
        if dt is None or dt < data_inicio or dt > data_fim:
            continue
        if t.despesa or not eh_entrada_emprestimo(t.plano_conta or "", despesa=False):
            continue
        val = _valor_titulo_dre(t, valor_n)
        if val > 0:
            emprestado += val

    q = Decimal("0.01")
    return {
        "ok": True,
        "valor_devido": float(devido.quantize(q)),
        "valor_pago": float(pago.quantize(q)),
        "juros": float(juros.quantize(q)),
        "valor_emprestado": float(emprestado.quantize(q)),
        "entrada_por": "competencia",
        "periodo_por": por_n,
    }
