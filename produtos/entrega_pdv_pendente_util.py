"""Entregas com venda PDV pendente (pagamento na entrega)."""

from __future__ import annotations

from django.db.models import Q
from django.utils import timezone

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


def _sessao_caixa_label_entrega(ent: PedidoEntrega) -> str:
    if ent.sessao_caixa_id:
        u = ent.sessao_caixa.usuario if ent.sessao_caixa else None
        label = f"Caixa #{ent.sessao_caixa_id}"
        if u:
            label += (
                " — "
                + ((u.get_full_name() or "").strip() or u.get_username() or "")
            )
        return label
    return "Sem caixa vinculado"


def contar_entregas_pendentes_pdv(*, apenas_caixas_abertos: bool = True) -> int:
    """Conta pendências visíveis no PDV (mesmo critério do bloqueio ao fechar caixa)."""
    if apenas_caixas_abertos:
        return queryset_entregas_bloqueando_fechamento_caixa().count()
    return queryset_entregas_aguardando_pagamento_pdv().count()


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


def listar_entregas_pendentes_pdv(*, limite: int = 80, apenas_caixas_abertos: bool = True) -> list[dict]:
    """Lista pendências do PDV — alinhado ao bloqueio em /caixa/fechar/ (todos caixas abertos)."""
    if apenas_caixas_abertos:
        qs = queryset_entregas_bloqueando_fechamento_caixa()
    else:
        qs = queryset_entregas_aguardando_pagamento_pdv()
    qs = qs.select_related("sessao_caixa", "sessao_caixa__usuario").order_by("criado_em")
    out = []
    for ent in qs[:limite]:
        row = serializar_entrega_pendente_pdv(ent)
        row["sessao_caixa_label"] = _sessao_caixa_label_entrega(ent)
        out.append(row)
    return out


def listar_entregas_bloqueando_fechamento_caixa(*, limite: int = 50) -> list[dict]:
    return listar_entregas_pendentes_pdv(limite=limite, apenas_caixas_abertos=True)


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
    ent.status = PedidoEntrega.Status.ENTREGUE
    if not ent.hora_entrega:
        ent.hora_entrega = timezone.now()
    update_fields = [
        "aguarda_pagamento_pdv",
        "pdv_wizard_state",
        "status",
        "hora_entrega",
        "atualizado_em",
    ]
    if venda_agro_id:
        ent.venda_agro_id = int(venda_agro_id)
        update_fields.append("venda_agro_id")
    ent.save(update_fields=update_fields)
    return ent


def tentar_vincular_entrega_pendente_apos_venda(data: dict | None, venda_id: int | None) -> None:
    """Encerra entrega «aguarda PDV» quando a venda foi confirmada (fallback server-side)."""
    if not venda_id or not isinstance(data, dict):
        return
    raw = data.get("pedido_entrega_pendente_id") or data.get("pedido_entrega_id")
    if raw is None or str(raw).strip() == "":
        return
    try:
        eid = int(raw)
    except (TypeError, ValueError):
        return
    marcar_entrega_pendente_fechada(eid, venda_agro_id=int(venda_id))


def finalizar_entregas_pagas_pendentes_ao_fechar_caixa(sessao_ids: list[int]) -> int:
    """Entregas pagas na loja (venda fechada) permanecem pendentes até o fechamento do caixa."""
    ids = [int(x) for x in sessao_ids if x is not None]
    if not ids:
        return 0
    agora = timezone.now()
    qs = (
        PedidoEntrega.objects.filter(
            status=PedidoEntrega.Status.PENDENTE,
            aguarda_pagamento_pdv=False,
            venda_agro_id__isnull=False,
        )
        .filter(Q(sessao_caixa_id__in=ids) | Q(venda_agro__sessao_caixa_id__in=ids))
        .only("pk", "status", "hora_entrega")
    )
    n = 0
    for ent in qs.iterator():
        ent.status = PedidoEntrega.Status.ENTREGUE
        if not ent.hora_entrega:
            ent.hora_entrega = agora
        ent.save(update_fields=["status", "hora_entrega", "atualizado_em"])
        n += 1
    return n


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
