"""Dados do cupom térmico 80mm para venda Agro (PDV e reimpressão)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.utils import timezone

from produtos.caixa_util import format_moeda_br, pagamentos_lista_de_venda


def _formatar_data_venda(dt) -> str:
    if not dt:
        return ""
    if timezone.is_aware(dt):
        dt = timezone.localtime(dt)
    return dt.strftime("%d/%m/%Y %H:%M")


def _forma_pagamento_cupom(venda) -> str:
    pag = pagamentos_lista_de_venda(venda)
    if len(pag) > 1:
        parts = []
        for row in pag:
            fn = str(row.get("forma") or "").strip()
            val = row.get("valor")
            if fn and val is not None:
                parts.append(f"{fn} R$ {format_moeda_br(val)}")
        if parts:
            return " · ".join(parts)
    if pag:
        return str(pag[0].get("forma") or "").strip() or "—"
    return str(getattr(venda, "forma_pagamento", "") or "").strip() or "—"


def serializar_venda_cupom_80mm(venda, *, segunda_via: bool = False) -> dict[str, Any]:
    """Payload JSON para impressão 80mm (lista de vendas, detalhe, PDV)."""
    itens: list[dict[str, Any]] = []
    for it in venda.itens.all().order_by("pk"):
        qtd = Decimal(str(it.quantidade or 0))
        vu = Decimal(str(it.valor_unitario or 0))
        vt = Decimal(str(it.valor_total or 0))
        if vt <= 0 and qtd > 0:
            vt = (qtd * vu).quantize(Decimal("0.01"))
        itens.append(
            {
                "nome": str(it.descricao or "").strip()[:500],
                "codigo": str(it.codigo or "").strip()[:120],
                "qtd": float(qtd),
                "preco": float(vu.quantize(Decimal("0.0001"))),
                "subtotal": float(vt.quantize(Decimal("0.01"))),
            }
        )
    total = Decimal(str(getattr(venda, "total", 0) or 0)).quantize(Decimal("0.01"))
    return {
        "venda_id": int(venda.pk),
        "criado_em": _formatar_data_venda(getattr(venda, "criado_em", None)),
        "segunda_via": bool(segunda_via),
        "cliente_nome": str(getattr(venda, "cliente_nome", "") or "").strip()[:300],
        "forma_pagamento": _forma_pagamento_cupom(venda),
        "total": float(total),
        "total_texto": "R$ " + format_moeda_br(total),
        "operador": str(getattr(venda, "usuario_registro", "") or "").strip()[:150],
        "caixa_id": getattr(venda, "sessao_caixa_id", None),
        "devolvida": bool(getattr(venda, "devolvida_em", None)),
        "itens": itens,
    }
