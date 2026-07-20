"""Entregas com venda PDV pendente (pagamento na entrega)."""

from __future__ import annotations

from django.db.models import Q
from django.utils import timezone

from produtos.models import PedidoEntrega, SessaoCaixa

LOJAS_ENTREGA = frozenset({"centro", "vila"})


def normalizar_loja_entrega(raw) -> str:
    v = str(raw or "").strip().lower()
    if v in LOJAS_ENTREGA:
        return v
    return ""


def queryset_entregas_aguardando_pagamento_pdv():
    return PedidoEntrega.objects.filter(aguarda_pagamento_pdv=True).exclude(
        status=PedidoEntrega.Status.CANCELADO
    )


def queryset_entregas_bloqueando_fechamento_caixa():
    """Entregas pendentes em caixas ainda abertos (ou sem caixa vinculado)."""
    return queryset_entregas_aguardando_pagamento_pdv().filter(
        Q(sessao_caixa__isnull=True) | Q(sessao_caixa__fechado_em__isnull=True)
    )


def filtrar_qs_por_loja(qs, loja: str | None):
    """Sem dono (vazio) OU dono = loja do PDV."""
    loja_n = normalizar_loja_entrega(loja)
    if not loja_n:
        return qs
    return qs.filter(Q(loja_entrega="") | Q(loja_entrega=loja_n))


def _sessao_caixa_label_entrega(ent: PedidoEntrega) -> str:
    """Caixa #N + operador do PIN/venda — não o login Django que abriu o turno."""
    op = (getattr(ent, "operador", None) or "").strip()
    if not op:
        op = (getattr(ent, "loja_assumida_por", None) or "").strip()
    if ent.sessao_caixa_id:
        label = f"Caixa #{ent.sessao_caixa_id}"
        if op:
            label += f" — {op[:40]}"
        return label
    if op:
        return op[:40]
    return "Sem caixa vinculado"


def _itens_resumo(ent: PedidoEntrega) -> list[dict]:
    raw = ent.itens_json if isinstance(ent.itens_json, list) else []
    out = []
    for linha in raw[:40]:
        if not isinstance(linha, dict):
            continue
        out.append(
            {
                "produto_id": str(linha.get("produto_id") or linha.get("id") or ""),
                "codigo_gm": str(
                    linha.get("codigo_gm") or linha.get("codigoGm") or linha.get("codigo") or ""
                )[:40],
                "codigo": str(linha.get("codigo") or "")[:40],
                "nome": str(linha.get("nome") or "")[:200],
                "qtd": linha.get("qtd") if linha.get("qtd") is not None else linha.get("quantidade"),
                "preco": linha.get("preco"),
                "total": linha.get("total"),
                "unidade": str(linha.get("unidade") or "UN")[:20],
                "prateleira": str(linha.get("prateleira") or "")[:40],
            }
        )
    return out


def contar_entregas_pendentes_pdv(
    *,
    apenas_caixas_abertos: bool = True,
    loja: str | None = None,
) -> int:
    """Conta pendências visíveis no PDV (mesmo critério do bloqueio ao fechar caixa)."""
    if apenas_caixas_abertos:
        qs = queryset_entregas_bloqueando_fechamento_caixa()
    else:
        qs = queryset_entregas_aguardando_pagamento_pdv()
    qs = filtrar_qs_por_loja(qs, loja)
    return qs.count()


def serializar_entrega_pendente_pdv(ent: PedidoEntrega, *, incluir_estado: bool = False) -> dict:
    loja = (ent.loja_entrega or "").strip()
    origem = (ent.origem or "").strip()
    tem_estado = isinstance(ent.pdv_wizard_state, dict) and bool(ent.pdv_wizard_state)
    itens = _itens_resumo(ent)
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
        "origem": origem,
        "loja_entrega": loja,
        "loja_assumida_em": ent.loja_assumida_em.isoformat() if ent.loja_assumida_em else "",
        "loja_assumida_por": (ent.loja_assumida_por or "").strip(),
        "endereco_linha": (ent.endereco_linha or "").strip(),
        "plus_code": (ent.plus_code or "").strip(),
        "referencia_rural": (ent.referencia_rural or "").strip(),
        "maps_url_manual": (ent.maps_url_manual or "").strip(),
        "troco_precisa": bool(getattr(ent, "troco_precisa", False)),
        "itens": itens,
        "pode_assumir": not loja,
        "pode_imprimir": bool(itens),
        "pode_retomar": tem_estado,
        "eh_catalogo": origem == "catalogo",
    }
    if incluir_estado:
        row["pdv_wizard_state"] = ent.pdv_wizard_state if isinstance(ent.pdv_wizard_state, dict) else {}
    return row


def listar_entregas_pendentes_pdv(
    *,
    limite: int = 80,
    apenas_caixas_abertos: bool = True,
    loja: str | None = None,
) -> list[dict]:
    """Lista pendências do PDV — filtradas pela loja (sem dono OU dono = loja)."""
    if apenas_caixas_abertos:
        qs = queryset_entregas_bloqueando_fechamento_caixa()
    else:
        qs = queryset_entregas_aguardando_pagamento_pdv()
    qs = filtrar_qs_por_loja(qs, loja)
    qs = qs.select_related("sessao_caixa", "sessao_caixa__usuario").order_by("criado_em")
    out = []
    for ent in qs[:limite]:
        row = serializar_entrega_pendente_pdv(ent)
        row["sessao_caixa_label"] = _sessao_caixa_label_entrega(ent)
        out.append(row)
    return out


def listar_entregas_bloqueando_fechamento_caixa(
    *,
    limite: int = 50,
    sessao_ids: list[int] | None = None,
) -> list[dict]:
    """
    Pendências que impedem fechar caixa.
    Se ``sessao_ids`` for passado, só as daqueles turnos (ex.: lote Vila)
    — entrega do Centro com caixa #104 não bloqueia fechar a Vila.
    """
    if sessao_ids is not None:
        ids: list[int] = []
        for x in sessao_ids:
            try:
                ids.append(int(x))
            except (TypeError, ValueError):
                continue
        if not ids:
            return []
        qs = queryset_entregas_bloqueando_fechamento_caixa().filter(
            sessao_caixa_id__in=ids
        )
        qs = qs.select_related("sessao_caixa", "sessao_caixa__usuario").order_by(
            "criado_em"
        )
        out = []
        for ent in qs[:limite]:
            row = serializar_entrega_pendente_pdv(ent)
            row["sessao_caixa_label"] = _sessao_caixa_label_entrega(ent)
            out.append(row)
        return out
    return listar_entregas_pendentes_pdv(limite=limite, apenas_caixas_abertos=True)


def assumir_entrega_loja(
    entrega_id: int,
    *,
    loja: str,
    username: str = "",
) -> tuple[PedidoEntrega | None, str | None]:
    """
    Define dono Centro/Vila. Retorna (pedido, erro).
    Se já tiver outro dono → erro conflito.
    """
    loja_n = normalizar_loja_entrega(loja)
    if not loja_n:
        return None, "Informe a loja (centro ou vila)."
    ent = PedidoEntrega.objects.filter(pk=entrega_id).exclude(
        status=PedidoEntrega.Status.CANCELADO
    ).first()
    if not ent:
        return None, "Entrega não encontrada."
    atual = (ent.loja_entrega or "").strip()
    if atual and atual != loja_n:
        return None, f"Já assumida pela loja {atual}."
    if atual == loja_n:
        return ent, None
    ent.loja_entrega = loja_n
    ent.loja_assumida_em = timezone.now()
    ent.loja_assumida_por = (username or "")[:120]
    ent.save(
        update_fields=[
            "loja_entrega",
            "loja_assumida_em",
            "loja_assumida_por",
            "atualizado_em",
        ]
    )
    return ent, None


def resolver_sessao_caixa_entrega_pdv(request, body: dict | None = None) -> SessaoCaixa | None:
    from produtos.caixa_util import (
        adotar_sessao_caixa_unica_aberta,
        obter_sessao_caixa_aberta_request,
        sessao_caixa_compativel_loja_browser,
    )

    # Só o turno deste aparelho/loja — ignora sessao_caixa_id de outra loja no body.
    if request is not None:
        s = obter_sessao_caixa_aberta_request(request) or adotar_sessao_caixa_unica_aberta(
            request
        )
        if s and sessao_caixa_compativel_loja_browser(request, s):
            return s
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
    """Legado — encerramento fica no PDV (api_pdv_entrega_pendente_finalizar após a venda)."""


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
