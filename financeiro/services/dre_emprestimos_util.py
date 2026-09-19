"""Card de empréstimos no DRE visual — títulos PG."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from financeiro.services.resumo_operacional_mongo import (
    _fold,
    classificar_despesa_plano,
    classificar_receita_plano,
)
from financeiro.models import LancamentoFinanceiro as NF
from produtos.lancamentos_financeiro_pg_analytics_util import (
    _campo_data_titulo,
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
    if "entrada" in f:
        return False
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

    emp_devido = Decimal("0")
    juros_devido = Decimal("0")
    emp_pago = Decimal("0")
    juros_pago = Decimal("0")
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
        plano = t.plano_conta or ""
        bruto = _dec2(getattr(t, "valor_bruto", 0) or 0)
        pago = _dec2(getattr(t, "valor_pago", 0) or 0)
        if eh_juros_emprestimo(plano, despesa=bool(t.despesa)):
            if bruto > 0:
                juros_devido += bruto
            if pago > 0:
                juros_pago += pago
        elif eh_pagamento_principal_emprestimo(plano, despesa=bool(t.despesa)):
            if bruto > 0:
                emp_devido += bruto
            if pago > 0:
                emp_pago += pago

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
        val = _dec2(getattr(t, "valor_bruto", 0) or 0)
        if val > 0:
            emprestado += val

    q = Decimal("0.01")
    emp_d = float(emp_devido.quantize(q))
    jur_d = float(juros_devido.quantize(q))
    emp_p = float(emp_pago.quantize(q))
    jur_p = float(juros_pago.quantize(q))
    total_d = float((emp_devido + juros_devido).quantize(q))
    pago_tot = float((emp_pago + juros_pago).quantize(q))
    return {
        "ok": True,
        "emprestimo_devido": emp_d,
        "juros_devido": jur_d,
        "total_devido": total_d,
        "valor_pago": pago_tot,
        "emprestimo_pago": emp_p,
        "juros_pago": jur_p,
        "valor_emprestado": float(emprestado.quantize(q)),
        "valor_devido": emp_d,
        "juros": jur_d,
        "entrada_por": "competencia",
        "periodo_por": por_n,
        "valor": valor_n,
    }
