"""Entregas com venda PDV pendente (pagamento na entrega)."""

from __future__ import annotations

from django.db.models import Q

from produtos.models import PedidoEntrega, SessaoCaixa


def queryset_entregas_aguardando_pagamento_pdv():
    return PedidoEntrega.objects.filter(aguarda_pagamento_pdv=True).exclude(
        status=PedidoEntrega.Status.CANCELADO
    )


def queryset_entregas_bloqueando_fechamento_caixa():
    """Entregas pendentes em caixas ainda abertos (ou sem caixa vinculado)."""
    return queryset_entregas_aguardando_pagamento_pdv().filter(
        Q(sessao_caixa__isnull=True) | Q(sessao_caixa__fechado_em__isnull=True)
    )


def contar_entregas_pendentes_pdv(*, sessao_caixa_id=None) -> int:
    qs = queryset_entregas_aguardando_pagamento_pdv()
    if sessao_caixa_id:
        try:
            sid = int(sessao_caixa_id)
            qs = qs.filter(Q(sessao_caixa_id=sid) | Q(sessao_caixa__isnull=True))
        except (TypeError, ValueError):
            pass
    return qs.count()


def serializar_entrega_pendente_pdv(ent: PedidoEntrega, *, incluir_estado: bool = False) -> dict:
    row = {
        "id": ent.pk,
        "cliente_nome": ent.cliente_nome or "",
        "telefone": ent.telefone or "",
        "total_texto": ent.total_texto or "",
        "forma_pagamento": ent.forma_pagamento or "",
        "status": ent.status,
        "criado_em": ent.criado_em.isoformat() if ent.criado_em else "",
        "retomar_codigo": (ent.retomar_codigo or "").strip()
        or (f"GMORC{ent.orc_local_id}" if ent.orc_local_id else f"ENT{ent.pk}"),
        "sessao_caixa_id": ent.sessao_caixa_id,
    }
    if incluir_estado:
        row["pdv_wizard_state"] = ent.pdv_wizard_state if isinstance(ent.pdv_wizard_state, dict) else {}
    return row


def listar_entregas_pendentes_pdv(*, limite: int = 80, sessao_caixa_id=None) -> list[dict]:
    qs = (
        queryset_entregas_aguardando_pagamento_pdv()
        .select_related("sessao_caixa")
        .order_by("criado_em")
    )
    if sessao_caixa_id:
        try:
            sid = int(sessao_caixa_id)
            qs = qs.filter(Q(sessao_caixa_id=sid) | Q(sessao_caixa__isnull=True))
        except (TypeError, ValueError):
            pass
    return [serializar_entrega_pendente_pdv(e) for e in qs[:limite]]


def listar_entregas_bloqueando_fechamento_caixa(*, limite: int = 50) -> list[dict]:
    qs = (
        queryset_entregas_bloqueando_fechamento_caixa()
        .select_related("sessao_caixa", "sessao_caixa__usuario")
        .order_by("criado_em")
    )
    out = []
    for ent in qs[:limite]:
        row = serializar_entrega_pendente_pdv(ent)
        if ent.sessao_caixa_id:
            u = ent.sessao_caixa.usuario if ent.sessao_caixa else None
            row["sessao_caixa_label"] = f"Caixa #{ent.sessao_caixa_id}"
            if u:
                row["sessao_caixa_label"] += (
                    " — "
                    + ((u.get_full_name() or "").strip() or u.get_username() or "")
                )
        else:
            row["sessao_caixa_label"] = "Sem caixa vinculado"
        out.append(row)
    return out


def resolver_sessao_caixa_entrega_pdv(request, body: dict | None = None) -> SessaoCaixa | None:
    from produtos.caixa_util import obter_sessao_caixa_aberta_request

    raw = None
    if body and body.get("sessao_caixa_id") is not None:
        raw = body.get("sessao_caixa_id")
    if raw is None and request is not None:
        try:
            raw = request.session.get("pdv_sessao_caixa_id")
        except Exception:
            raw = None
    if raw is not None and str(raw).strip() != "":
        try:
            return SessaoCaixa.objects.filter(pk=int(raw), fechado_em__isnull=True).first()
        except (TypeError, ValueError):
            pass
    if request is not None:
        return obter_sessao_caixa_aberta_request(request)
    return None


def marcar_entrega_pendente_fechada(
    entrega_id: int,
    *,
    venda_agro_id: int | None = None,
) -> PedidoEntrega | None:
    ent = PedidoEntrega.objects.filter(pk=entrega_id, aguarda_pagamento_pdv=True).first()
    if not ent:
        return None
    ent.aguarda_pagamento_pdv = False
    ent.pdv_wizard_state = {}
    update_fields = ["aguarda_pagamento_pdv", "pdv_wizard_state", "atualizado_em"]
    if venda_agro_id:
        ent.venda_agro_id = int(venda_agro_id)
        update_fields.append("venda_agro_id")
    ent.save(update_fields=update_fields)
    return ent


def cancelar_entrega_pendente_pdv(entrega_id: int, *, motivo: str = "") -> PedidoEntrega | None:
    ent = PedidoEntrega.objects.filter(pk=entrega_id, aguarda_pagamento_pdv=True).first()
    if not ent:
        return None
    ent.aguarda_pagamento_pdv = False
    ent.pdv_wizard_state = {}
    ent.status = PedidoEntrega.Status.CANCELADO
    if motivo:
        obs = (ent.observacoes or "").strip()
        ent.observacoes = (obs + " | " if obs else "") + f"Cancelado no PDV: {motivo[:200]}"
    ent.save(
        update_fields=[
            "aguarda_pagamento_pdv",
            "pdv_wizard_state",
            "status",
            "observacoes",
            "atualizado_em",
        ]
    )
    return ent
