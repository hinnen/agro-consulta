"""Entregas com venda PDV pendente (pagamento na entrega)."""

from __future__ import annotations

from datetime import timedelta
from urllib.parse import quote

from django.db.models import Q
from django.utils import timezone

from produtos.caixa_util import PONTO_CAIXA_VILA, rotulo_operador_pin, validar_pin_operador
from produtos.models import PedidoEntrega, SessaoCaixa

LOJAS_ENTREGA = frozenset({"centro", "vila"})
HORAS_PAGAS_LOJA_PDV = 24


def normalizar_loja_entrega(raw) -> str:
    v = str(raw or "").strip().lower()
    if v in LOJAS_ENTREGA:
        return v
    return ""


def queryset_entregas_aguardando_pagamento_pdv():
    return PedidoEntrega.objects.filter(aguarda_pagamento_pdv=True).exclude(
        status=PedidoEntrega.Status.CANCELADO
    )


def corte_pagas_loja_pdv():
    return timezone.now() - timedelta(hours=HORAS_PAGAS_LOJA_PDV)


def queryset_entregas_pagas_loja_pdv():
    """Pagas no caixa ao lançar — overlay 24h, não trava fechar caixa."""
    return PedidoEntrega.objects.filter(
        paga_na_loja=True,
        pdv_lista_concluida=False,
        criado_em__gte=corte_pagas_loja_pdv(),
    ).exclude(status=PedidoEntrega.Status.CANCELADO)


def maps_query_entrega(ent: PedidoEntrega) -> str:
    manual = (getattr(ent, "maps_url_manual", None) or "").strip()
    if manual:
        return manual
    plus = (getattr(ent, "plus_code", None) or "").strip()
    if plus:
        return plus
    return (getattr(ent, "endereco_linha", None) or "").strip()


def maps_url_entrega(ent: PedidoEntrega) -> str:
    q = maps_query_entrega(ent)
    if not q:
        return ""
    if q.lower().startswith("http://") or q.lower().startswith("https://"):
        return q
    return "https://www.google.com/maps/search/?api=1&query=" + quote(q)


def queryset_entregas_bloqueando_fechamento_caixa():
    """Entregas pendentes em caixas ainda abertos (ou sem caixa vinculado)."""
    return queryset_entregas_aguardando_pagamento_pdv().filter(
        Q(sessao_caixa__isnull=True) | Q(sessao_caixa__fechado_em__isnull=True)
    )


def filtrar_qs_por_loja(qs, loja: str | None):
    """Sem dono, dono = loja do PDV, pagamento nesta loja, ou caixa daquela loja."""
    loja_n = normalizar_loja_entrega(loja)
    if not loja_n:
        return qs
    q = Q(loja_entrega="") | Q(loja_entrega=loja_n) | Q(loja_pagamento=loja_n)
    if loja_n == "vila":
        q |= Q(sessao_caixa__ponto_caixa=PONTO_CAIXA_VILA)
    else:
        q |= Q(sessao_caixa_id__isnull=False) & ~Q(sessao_caixa__ponto_caixa=PONTO_CAIXA_VILA)
    return qs.filter(q)


def loja_pagamento_efetiva(ent: PedidoEntrega) -> str:
    """centro|vila do caixa desta entrega (campo, sessão ou dono)."""
    lp = normalizar_loja_entrega(getattr(ent, "loja_pagamento", None) or "")
    if lp:
        return lp
    sess = getattr(ent, "sessao_caixa", None)
    if sess is not None:
        from produtos.caixa_util import deposito_de_ponto_caixa

        return deposito_de_ponto_caixa(getattr(sess, "ponto_caixa", None))
    return normalizar_loja_entrega(ent.loja_entrega or "")


def rotulo_loja_curto(loja: str) -> str:
    v = normalizar_loja_entrega(loja)
    if v == "vila":
        return "Vila"
    if v == "centro":
        return "Centro"
    return ""


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
    loja_pag = loja_pagamento_efetiva(ent)
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
        "loja_pagamento": loja_pag,
        "loja_entrega_label": rotulo_loja_curto(loja),
        "loja_pagamento_label": rotulo_loja_curto(loja_pag),
        "loja_divergente": bool(loja and loja_pag and loja != loja_pag),
        "pode_mudar_loja": bool(ent.aguarda_pagamento_pdv)
        or bool(getattr(ent, "paga_na_loja", False)),
        "loja_assumida_em": ent.loja_assumida_em.isoformat() if ent.loja_assumida_em else "",
        "loja_assumida_por": (ent.loja_assumida_por or "").strip(),
        "caixa_adiada_para": ent.caixa_adiada_para.isoformat()
        if getattr(ent, "caixa_adiada_para", None)
        else "",
        "caixa_adiada_por": (getattr(ent, "caixa_adiada_por", None) or "").strip(),
        "pode_adiar": bool(ent.aguarda_pagamento_pdv),
        "endereco_linha": (ent.endereco_linha or "").strip(),
        "plus_code": (ent.plus_code or "").strip(),
        "referencia_rural": (ent.referencia_rural or "").strip(),
        "maps_url_manual": (ent.maps_url_manual or "").strip(),
        "maps_query": maps_query_entrega(ent),
        "maps_url": maps_url_entrega(ent),
        "observacoes": (ent.observacoes or "").strip(),
        "troco_precisa": bool(getattr(ent, "troco_precisa", False)),
        "itens": itens,
        "pode_assumir": not loja,
        "pode_imprimir": bool(itens),
        "pode_retomar": tem_estado,
        "pode_cancelar": bool(ent.aguarda_pagamento_pdv),
        "eh_catalogo": origem == "catalogo",
        "paga_na_loja": bool(getattr(ent, "paga_na_loja", False)),
        "pdv_lista_concluida": bool(getattr(ent, "pdv_lista_concluida", False)),
        "pode_concluir_overlay": bool(getattr(ent, "paga_na_loja", False))
        and not bool(getattr(ent, "pdv_lista_concluida", False)),
        "venda_agro_id": ent.venda_agro_id,
        "aguarda_pagamento_pdv": bool(ent.aguarda_pagamento_pdv),
        "hora_prevista": ent.hora_prevista.strftime("%H:%M")
        if getattr(ent, "hora_prevista", None)
        else "",
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


def listar_entregas_pagas_loja_pdv(
    *,
    limite: int = 80,
    loja: str | None = None,
) -> list[dict]:
    qs = queryset_entregas_pagas_loja_pdv()
    qs = filtrar_qs_por_loja(qs, loja)
    qs = qs.select_related("sessao_caixa", "sessao_caixa__usuario").order_by("-criado_em")
    out = []
    for ent in qs[:limite]:
        row = serializar_entrega_pendente_pdv(ent)
        row["sessao_caixa_label"] = _sessao_caixa_label_entrega(ent)
        row["pode_adiar"] = False
        row["pode_retomar"] = False
        row["pode_assumir"] = False
        row["pode_cancelar"] = False
        row["paga_na_loja"] = True
        row["pode_concluir_overlay"] = True
        row["pode_mudar_loja"] = True
        out.append(row)
    return out


def concluir_entrega_paga_overlay(entrega_id: int, *, loja: str | None = None):
    """Tira da lista Pagas na loja. Sem isso, some sozinha em 24 h."""
    qs = PedidoEntrega.objects.filter(pk=entrega_id, paga_na_loja=True).exclude(
        status=PedidoEntrega.Status.CANCELADO
    )
    qs = filtrar_qs_por_loja(qs, loja)
    ent = qs.first()
    if not ent:
        return None, "Entrega não encontrada."
    if ent.pdv_lista_concluida:
        return ent, ""
    ent.pdv_lista_concluida = True
    ent.save(update_fields=["pdv_lista_concluida", "atualizado_em"])
    return ent, ""


def listar_entregas_bloqueando_fechamento_caixa(
    *,
    limite: int = 50,
    sessao_ids: list[int] | None = None,
    loja: str | None = None,
) -> list[dict]:
    """
    Pendências que impedem fechar caixa.
    Com ``sessao_ids``: só o lote que está fechando —
    · entrega com caixa #104 (Centro) não trava a Vila;
    · catálogo SEM DONO (sem loja) não trava ninguém;
    · catálogo já Assumido pela loja (sem sessão) trava essa loja.
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
        loja_n = normalizar_loja_entrega(loja)
        q = Q(sessao_caixa_id__in=ids)
        if loja_n:
            q |= Q(sessao_caixa_id__isnull=True, loja_entrega=loja_n)
        qs = queryset_entregas_bloqueando_fechamento_caixa().filter(q)
        qs = qs_excluindo_adiadas_futuras(qs)
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


def data_hoje_loja():
    return timezone.localdate()


def qs_excluindo_adiadas_futuras(qs, hoje=None):
    """Adiada para data futura não trava o caixa de hoje."""
    dia = hoje or data_hoje_loja()
    return qs.filter(Q(caixa_adiada_para__isnull=True) | Q(caixa_adiada_para__lte=dia))


def adiar_entrega_caixa_um_dia(
    entrega_id: int,
    *,
    loja: str,
    pin: str = "",
    quem: str = "",
) -> tuple[PedidoEntrega | None, str | None]:
    """
    Solta o caixa de hoje. Amanhã a mesma entrega trava de novo.
    Pagamento, quando fechar a venda, entra no caixa aberto naquele dia.
    """
    loja_n = normalizar_loja_entrega(loja)
    if not loja_n:
        return None, "Informe a loja (centro ou vila)."
    rotulo = (quem or "").strip()
    pin_n = (pin or "").strip()
    if pin_n:
        ok_pin, err_pin = validar_pin_operador(pin_n)
        if not ok_pin:
            return None, err_pin
        rotulo = rotulo_operador_pin(pin_n) or rotulo
    if not rotulo:
        return None, "Informe o PIN."
    ent = (
        PedidoEntrega.objects.filter(pk=entrega_id, aguarda_pagamento_pdv=True)
        .exclude(status=PedidoEntrega.Status.CANCELADO)
        .first()
    )
    if not ent:
        return None, "Entrega pendente não encontrada."
    hoje = data_hoje_loja()
    ent.sessao_caixa = None
    ent.loja_entrega = loja_n
    ent.caixa_adiada_para = hoje + timedelta(days=1)
    ent.caixa_adiada_em = timezone.now()
    ent.caixa_adiada_por = rotulo[:120]
    ent.save(
        update_fields=[
            "sessao_caixa",
            "loja_entrega",
            "caixa_adiada_para",
            "caixa_adiada_em",
            "caixa_adiada_por",
            "atualizado_em",
        ]
    )
    return ent, None


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
        deposito_caixa_browser,
        obter_caixa_pai_aberto,
        obter_sessao_caixa_aberta_request,
        sessao_caixa_compativel_loja_browser,
    )

    body = body if isinstance(body, dict) else {}
    loja_pag = normalizar_loja_entrega(body.get("loja_pagamento"))
    loja_dest = loja_pag or normalizar_loja_entrega(
        body.get("loja_entrega") or body.get("loja")
    )
    loja_nav = ""
    if request is not None:
        loja_nav = normalizar_loja_entrega(deposito_caixa_browser(request))
    if loja_dest and loja_dest != loja_nav:
        s_dest = obter_caixa_pai_aberto(loja_dest)
        if s_dest and getattr(s_dest, "fechado_em", None) is None:
            return s_dest
        return None

    if request is not None:
        s = obter_sessao_caixa_aberta_request(request) or adotar_sessao_caixa_unica_aberta(
            request
        )
        if s and sessao_caixa_compativel_loja_browser(request, s):
            return s
    return None


ESCOPOS_MUDAR_LOJA = frozenset({"entrega", "pagamento", "ambos"})


def mudar_loja_entrega_pdv(
    entrega_id: int,
    *,
    loja: str,
    escopo: str,
    pin: str = "",
    quem: str = "",
) -> tuple[PedidoEntrega | None, str | None]:
    """
    Reaponta loja de saída e/ou caixa de pagamento.
    escopo: entrega | pagamento | ambos
    """
    from produtos.caixa_util import obter_caixa_pai_aberto

    loja_n = normalizar_loja_entrega(loja)
    esc = str(escopo or "").strip().lower()
    if not loja_n:
        return None, "Informe a loja (centro ou vila)."
    if esc not in ESCOPOS_MUDAR_LOJA:
        return None, "Escolha: só entrega, só pagamento ou as duas."

    rotulo = (quem or "").strip()
    pin_n = (pin or "").strip()
    if pin_n:
        ok_pin, err_pin = validar_pin_operador(pin_n)
        if not ok_pin:
            return None, err_pin
        rotulo = rotulo_operador_pin(pin_n) or rotulo
    if not rotulo:
        return None, "Informe o PIN."

    ent = (
        PedidoEntrega.objects.filter(pk=entrega_id)
        .exclude(status=PedidoEntrega.Status.CANCELADO)
        .select_related("sessao_caixa")
        .first()
    )
    if not ent:
        return None, "Entrega não encontrada."

    paga = bool(getattr(ent, "paga_na_loja", False)) and not bool(ent.aguarda_pagamento_pdv)
    if paga and esc in ("pagamento", "ambos"):
        return None, "Já paga na loja — só dá para mudar a loja de entrega (quem sai)."
    if not paga and not ent.aguarda_pagamento_pdv:
        return None, "Só dá para mudar loja em entrega a pagar ou paga na loja."

    loja_ent_atual = normalizar_loja_entrega(ent.loja_entrega or "")
    loja_pag_atual = loja_pagamento_efetiva(ent)
    nova_ent = loja_ent_atual
    nova_pag = loja_pag_atual
    if esc in ("entrega", "ambos"):
        nova_ent = loja_n
    if esc in ("pagamento", "ambos"):
        nova_pag = loja_n

    update_fields = ["atualizado_em", "loja_assumida_em", "loja_assumida_por"]
    ent.loja_assumida_em = timezone.now()
    ent.loja_assumida_por = rotulo[:120]

    if esc in ("entrega", "ambos"):
        ent.loja_entrega = nova_ent
        update_fields.append("loja_entrega")

    if esc in ("pagamento", "ambos"):
        ent.loja_pagamento = nova_pag
        update_fields.append("loja_pagamento")
        if ent.aguarda_pagamento_pdv:
            s_dest = obter_caixa_pai_aberto(nova_pag)
            if not s_dest or getattr(s_dest, "fechado_em", None) is not None:
                nome = "Vila" if nova_pag == "vila" else "Centro"
                return None, f"Abra o caixa da {nome} antes de mudar o pagamento."
            ent.sessao_caixa = s_dest
            update_fields.append("sessao_caixa")
    elif esc == "entrega" and not (getattr(ent, "loja_pagamento", None) or "").strip():
        # Mantém pagamento explícito no valor atual (legado sem campo).
        ent.loja_pagamento = loja_pag_atual or loja_ent_atual or loja_n
        update_fields.append("loja_pagamento")

    ent.save(update_fields=list(dict.fromkeys(update_fields)))
    return ent, None


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
    ent.caixa_adiada_para = None
    ent.caixa_adiada_em = None
    ent.caixa_adiada_por = ""
    if not ent.hora_entrega:
        ent.hora_entrega = timezone.now()
    update_fields = [
        "aguarda_pagamento_pdv",
        "pdv_wizard_state",
        "status",
        "hora_entrega",
        "caixa_adiada_para",
        "caixa_adiada_em",
        "caixa_adiada_por",
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
