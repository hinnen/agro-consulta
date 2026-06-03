"""Dados do cupom térmico 80mm para venda Agro (PDV e reimpressão)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
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


def _formatar_data_curta(d) -> str:
    if isinstance(d, datetime):
        if timezone.is_aware(d):
            d = timezone.localtime(d)
        d = d.date()
    if isinstance(d, date):
        return d.strftime("%d/%m/%Y")
    s = str(d or "").strip()
    if len(s) >= 10 and s[4] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        except ValueError:
            pass
    return s[:10]


def _venda_eh_fiado_cupom(venda) -> bool:
    if hasattr(venda, "tem_fiado") and venda.tem_fiado():
        return True
    if "fiado" in str(getattr(venda, "forma_pagamento", "") or "").lower():
        return True
    for row in pagamentos_lista_de_venda(venda):
        if "fiado" in str(row.get("forma") or "").lower():
            return True
    return False


def _fiado_dias_vencimento_cupom(venda) -> int:
    for row in pagamentos_lista_de_venda(venda):
        if "fiado" not in str(row.get("forma") or "").lower():
            continue
        for key in ("fiado_dias_vencimento", "fiadoDiasVencimento", "fiado_dias_primeiro"):
            raw = row.get(key)
            if raw is not None and str(raw).strip() != "":
                try:
                    n = int(raw)
                    return max(1, n)
                except (TypeError, ValueError):
                    pass
    return 30


def _vencimento_fiado_cupom(venda) -> str:
    cron = getattr(venda, "fiado_cronograma_json", None) or []
    if isinstance(cron, list):
        for row in cron:
            if not isinstance(row, dict):
                continue
            for key in ("vencimento", "data_vencimento", "dataVencimento"):
                if row.get(key):
                    return _formatar_data_curta(row[key])
    dias = _fiado_dias_vencimento_cupom(venda)
    dt = getattr(venda, "criado_em", None)
    if not dt:
        return ""
    if timezone.is_aware(dt):
        dt = timezone.localtime(dt)
    ref = dt.date() if hasattr(dt, "date") else timezone.localdate()
    return (ref + timedelta(days=dias)).strftime("%d/%m/%Y")


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
    eh_fiado = _venda_eh_fiado_cupom(venda)
    fiado_dias = _fiado_dias_vencimento_cupom(venda) if eh_fiado else 0
    out: dict[str, Any] = {
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
        "eh_fiado": eh_fiado,
        "itens": itens,
    }
    if eh_fiado:
        out["fiado_dias"] = fiado_dias
        out["vencimento"] = _vencimento_fiado_cupom(venda)
    return out
