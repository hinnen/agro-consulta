"""Histórico de retiradas / saídas do caixa (financeiro + movimento turno)."""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Any

from django.db.models import Q
from django.utils import timezone

from produtos.caixa_util import normalizar_rotulo_operador_exibicao, rotulo_usuario_django
from produtos.models import MovimentoCaixa, TituloFinanceiroAgro


def _op_exib(raw: str) -> str:
    n = normalizar_rotulo_operador_exibicao(raw)
    return n or "—"


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _extrair_quem_descricao(desc: str) -> str:
    m = re.search(r"Quem:\s*(.+?)(?:\s*·|$)", str(desc or ""), re.I)
    return (m.group(1).strip() if m else "")[:200]


def _row_sort_key(row: dict[str, Any]) -> tuple:
    d = row.get("data") or date.min
    ts = row.get("criado_em")
    return (d, ts or timezone.now())


def listar_retiradas_historico(
    *,
    data_de: date,
    data_ate: date,
    plano: str = "",
    quem: str = "",
    limite: int = 300,
    exportar: bool = False,
) -> dict[str, Any]:
    plano_f = (plano or "").strip()
    quem_f = (quem or "").strip().lower()
    cap = 10000 if exportar else 500
    default_lim = 5000 if exportar else 300
    limite = max(1, min(int(limite or default_lim), cap))

    linhas: list[dict[str, Any]] = []
    ids_mov_vistos: set[int] = set()

    qs = TituloFinanceiroAgro.objects.filter(
        despesa=True,
        descricao__icontains="Saída caixa",
        data_competencia__gte=data_de,
        data_competencia__lte=data_ate,
    )
    if plano_f:
        qs = qs.filter(plano_conta__icontains=plano_f)
    if quem_f:
        qs = qs.filter(
            Q(cliente__icontains=quem_f) | Q(descricao__icontains=quem_f)
        )

    for t in qs.order_by("-data_competencia", "-importado_em")[:limite]:
        nome_quem = (t.cliente or "").strip() or _extrair_quem_descricao(t.descricao)
        snap = t.dados_snapshot_json if isinstance(t.dados_snapshot_json, dict) else {}
        mov_id = snap.get("movimento_caixa_id")
        if mov_id:
            try:
                ids_mov_vistos.add(int(mov_id))
            except (TypeError, ValueError):
                pass
        linhas.append(
            {
                "id": f"t-{t.pk}",
                "fonte": "financeiro",
                "data": t.data_competencia,
                "criado_em": t.importado_em or t.atualizado_em,
                "valor": _dec(t.valor_bruto),
                "plano": (t.plano_conta or "").strip() or "—",
                "quem": nome_quem or "—",
                "forma": (t.forma_pagamento or "").strip() or "—",
                "banco": (t.banco or "").strip() or "—",
                "descricao": (t.descricao or "").strip(),
                "observacoes": (t.observacoes or "").strip(),
                "operador": _op_exib(t.usuario_lancou or t.criado_por or ""),
                "operador_pin": _op_exib(
                    t.usuario_lancou or t.criado_por or t.modificado_por or ""
                ),
                "sessao_id": snap.get("sessao_caixa_id"),
                "mongo_id": (t.mongo_id or "").strip(),
            }
        )

    mov_qs = MovimentoCaixa.objects.filter(
        tipo=MovimentoCaixa.Tipo.RETIRADA,
        criado_em__date__gte=data_de,
        criado_em__date__lte=data_ate,
    ).select_related("sessao_caixa", "usuario")
    if quem_f:
        mov_qs = mov_qs.filter(observacao__icontains=quem_f)
    if plano_f:
        mov_qs = mov_qs.filter(observacao__icontains=plano_f)

    for m in mov_qs.order_by("-criado_em")[:limite]:
        if m.pk in ids_mov_vistos:
            continue
        obs = (m.observacao or "").strip()
        if plano_f and plano_f.lower() not in obs.lower():
            continue
        data_mov = timezone.localdate(m.criado_em)
        val_mov = _dec(m.valor)
        if any(
            r["fonte"] == "financeiro"
            and r["data"] == data_mov
            and _dec(r["valor"]) == val_mov
            for r in linhas
        ):
            continue
        op_mov = rotulo_usuario_django(m.usuario) if m.usuario else ""
        linhas.append(
            {
                "id": f"m-{m.pk}",
                "fonte": "caixa",
                "data": timezone.localdate(m.criado_em),
                "criado_em": m.criado_em,
                "valor": _dec(m.valor),
                "plano": obs.split(" · ")[0][:120] if obs else "Depósito / caixa",
                "quem": "—",
                "forma": (m.forma_pagamento or "").strip() or "—",
                "banco": "—",
                "descricao": obs or "Retirada no turno",
                "observacoes": "",
                "operador": _op_exib(op_mov),
                "operador_pin": _op_exib(op_mov),
                "sessao_id": m.sessao_caixa_id,
                "mongo_id": "",
            }
        )

    linhas.sort(key=_row_sort_key, reverse=True)
    linhas = linhas[:limite]
    total = sum((_dec(r["valor"]) for r in linhas), Decimal("0.00"))

    return {
        "linhas": linhas,
        "qtd": len(linhas),
        "total": total,
    }


def listar_quem_retiradas_distintas(*, limite: int = 80) -> list[str]:
    """Nomes usados em retiradas recentes (para filtro)."""
    limite = max(1, min(int(limite or 80), 200))
    nomes: list[str] = []
    vistos: set[str] = set()
    qs = (
        TituloFinanceiroAgro.objects.filter(despesa=True, descricao__icontains="Saída caixa")
        .exclude(cliente="")
        .order_by("-data_competencia")[:500]
    )
    for t in qs:
        n = (t.cliente or "").strip()
        if not n:
            n = _extrair_quem_descricao(t.descricao)
        if not n:
            continue
        key = n.lower()
        if key in vistos:
            continue
        vistos.add(key)
        nomes.append(n)
        if len(nomes) >= limite:
            break
    return nomes
