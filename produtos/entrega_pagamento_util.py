"""Rótulos de pagamento da venda no painel de entregas."""

from __future__ import annotations

from produtos.models import PedidoEntrega, VendaAgro


def serializar_pagamento_entrega(ent: PedidoEntrega) -> dict:
    """Status legível para a coluna Pagamento (pago / aguarda / cobrar)."""
    base = {
        "pago": None,
        "label": "—",
        "erp_label": "",
        "classe": "bg-slate-100 text-slate-700",
        "venda_agro_id": ent.venda_agro_id,
    }
    if ent.aguarda_pagamento_pdv:
        base.update(
            {
                "pago": False,
                "label": "Não pago",
                "erp_label": "Aguarda PDV",
                "classe": "bg-rose-100 text-rose-900",
            }
        )
        return base

    v: VendaAgro | None = getattr(ent, "venda_agro", None)
    if v is not None:
        if v.devolvida_em:
            base.update(
                {
                    "pago": False,
                    "label": "Devolvida",
                    "classe": "bg-rose-50 text-rose-800",
                }
            )
            return base
        base["pago"] = True
        base["label"] = "Pago · Fiado" if v.tem_fiado() else "Pago"
        base["classe"] = "bg-emerald-100 text-emerald-900"
        return base

    fp = (ent.forma_pagamento or "").strip()
    low = fp.lower()
    if "loja" in low or ("pago" in low and "entrega" not in low):
        base.update(
            {
                "pago": True,
                "label": "Pago na loja",
                "classe": "bg-emerald-100 text-emerald-900",
            }
        )
        return base
    if fp and fp not in ("Não informado", "Nao informado", ""):
        base.update(
            {
                "pago": False,
                "label": "Cobrar na entrega",
                "erp_label": fp[:40],
                "classe": "bg-orange-100 text-orange-950",
            }
        )
        return base
    base["label"] = "Não informado"
    return base
