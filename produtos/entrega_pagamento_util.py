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
                    "erp_label": "",
                    "classe": "bg-rose-50 text-rose-800",
                }
            )
            return base
        base["pago"] = True
        base["label"] = "Pago"
        if v.fiado_aguarda_envio_erp:
            base.update(
                {
                    "erp_label": "ERP pendente",
                    "classe": "bg-amber-100 text-amber-950",
                }
            )
        elif v.enviado_erp or v.erp_sync_efetivo == VendaAgro.ErpSyncStatus.ACEITO:
            base.update(
                {
                    "erp_label": "ERP enviado",
                    "classe": "bg-emerald-100 text-emerald-900",
                }
            )
        elif v.erp_sync_efetivo == VendaAgro.ErpSyncStatus.RECUSADO_ERP:
            base.update(
                {
                    "erp_label": "ERP recusou",
                    "classe": "bg-orange-100 text-orange-950",
                }
            )
        elif v.erp_sync_efetivo == VendaAgro.ErpSyncStatus.FALHA_COMUNICACAO:
            base.update(
                {
                    "erp_label": "Falha ERP",
                    "classe": "bg-amber-100 text-amber-950",
                }
            )
        else:
            base.update(
                {
                    "erp_label": "PDV",
                    "classe": "bg-emerald-100 text-emerald-900",
                }
            )
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
