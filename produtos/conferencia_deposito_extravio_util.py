"""Conferência mensal: depósitos (caixa) × baixas CP forma BANCO → Extravio.

Extravio no Mini DRE = (depósitos − baixas BANCO/DEPÓSITO) com sinal
(+ ou −) + títulos manuais do plano (legado). Não corta lucro operacional —
só Saldo final / geração de caixa.
"""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from django.utils import timezone

from produtos.extravio_deposito_util import forma_eh_banco, plano_eh_extravio_apos_deposito


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _eh_deposito_obs(obs: str) -> bool:
    o = (obs or "").strip().lower()
    return o.startswith("depósito") or o.startswith("deposito")


def soma_depositos_caixa(
    data_inicio: date,
    data_fim: date,
    *,
    deposito: str | None = None,
) -> Decimal:
    """Soma retiradas de caixa com obs «Depósito → …» no período (criado_em)."""
    from produtos.caixa_util import ponto_pai_de_deposito
    from produtos.models import MovimentoCaixa

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_inicio, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = (
        MovimentoCaixa.objects.filter(
            tipo=MovimentoCaixa.Tipo.RETIRADA,
            criado_em__gte=ini,
            criado_em__lte=fim,
        )
        .exclude(sessao_caixa__ponto_caixa="teste")
        .select_related("sessao_caixa")
    )
    dep = (deposito or "").strip().lower()
    if dep in ("centro", "vila"):
        ponto = ponto_pai_de_deposito(dep)
        qs = qs.filter(sessao_caixa__ponto_caixa=ponto)

    total = Decimal("0.00")
    for m in qs.iterator(chunk_size=500):
        if _eh_deposito_obs(m.observacao or ""):
            total += _dec(m.valor)
    return total.quantize(Decimal("0.01"))


def soma_baixas_cp_forma_banco(
    data_inicio: date,
    data_fim: date,
    *,
    empresa_nome: str | None = None,
) -> Decimal:
    """Soma valor_pago de CP com data_pagamento no período e forma BANCO."""
    from produtos.models import TituloFinanceiroAgro

    qs = TituloFinanceiroAgro.objects.filter(
        despesa=True,
        data_pagamento__gte=data_inicio,
        data_pagamento__lte=data_fim,
        valor_pago__gt=0,
    )
    nome = (empresa_nome or "").strip()
    if nome:
        qs = qs.filter(empresa__icontains=nome)

    total = Decimal("0.00")
    for t in qs.only("forma_pagamento", "valor_pago", "plano_conta").iterator(chunk_size=800):
        if plano_eh_extravio_apos_deposito(t.plano_conta or ""):
            continue
        if not forma_eh_banco(t.forma_pagamento or ""):
            continue
        total += _dec(t.valor_pago)
    return total.quantize(Decimal("0.01"))


def extravio_auto_periodo(
    data_inicio: date,
    data_fim: date,
    *,
    deposito: str | None = None,
    empresa_nome: str | None = None,
) -> dict[str, Decimal]:
    dep = soma_depositos_caixa(data_inicio, data_fim, deposito=deposito)
    ban = soma_baixas_cp_forma_banco(
        data_inicio, data_fim, empresa_nome=empresa_nome
    )
    auto = (dep - ban).quantize(Decimal("0.01"))
    return {
        "depositos_caixa": dep,
        "baixas_banco": ban,
        "extravio_auto": auto,
    }


def aplicar_extravio_auto_no_resumo(
    core: dict[str, Any],
    data_inicio: date,
    data_fim: date,
    *,
    deposito: str | None = None,
    empresa_nome: str | None = None,
) -> dict[str, Any]:
    """Substitui a linha Mini DRE: auto + manuais (legado); recalcula geração de caixa."""
    manual = _dec(core.get("extravio_apos_deposito"))
    pack = extravio_auto_periodo(
        data_inicio,
        data_fim,
        deposito=deposito,
        empresa_nome=empresa_nome,
    )
    auto = pack["extravio_auto"]
    novo = (auto + manual).quantize(Decimal("0.01"))
    delta = (novo - manual).quantize(Decimal("0.01"))
    core["extravio_manual"] = manual
    core["depositos_caixa"] = pack["depositos_caixa"]
    core["baixas_banco"] = pack["baixas_banco"]
    core["extravio_auto"] = auto
    core["extravio_apos_deposito"] = novo
    core["extravio_fonte"] = "deposito_menos_banco"
    try:
        gc = _dec(core.get("geracao_caixa"))
        core["geracao_caixa"] = (gc - delta).quantize(Decimal("0.01"))
    except Exception:
        pass
    return core
