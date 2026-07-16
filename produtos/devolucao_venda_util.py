"""Devolução parcial/total de VendaAgro (itens + frete opcional)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from produtos.caixa_util import (
    FORMAS_PAGAMENTO_CAIXA,
    _dec,
    normalizar_forma_pagamento_caixa,
    parse_valor_moeda_br,
)
from produtos.fiado_credito_util import venda_local_tem_fiado
from produtos.models import (
    DevolucaoItemVendaAgro,
    DevolucaoVendaAgro,
    FiadoEventoAgro,
    FiadoTituloAgro,
    ItemVendaAgro,
    VendaAgro,
)


def formas_pagamento_devolucao(venda: VendaAgro) -> list[str]:
    """Formas do modal: Fiado só se a venda original teve fiado."""
    formas = list(FORMAS_PAGAMENTO_CAIXA)
    if venda_local_tem_fiado(venda):
        return formas
    return [f for f in formas if f != "Fiado"]


def frete_restante(venda: VendaAgro) -> Decimal:
    frete = _dec(getattr(venda, "frete", 0) or 0)
    ja = _dec(getattr(venda, "frete_devolvido", 0) or 0)
    r = frete - ja
    return r if r > 0 else Decimal("0.00")


def valor_linha_proporcional(item: ItemVendaAgro, qtd: Decimal) -> Decimal:
    """Valor a devolver para `qtd` unidades do item."""
    q_total = Decimal(str(item.quantidade or 0))
    if q_total <= 0:
        return Decimal("0.00")
    q = Decimal(str(qtd or 0))
    if q <= 0:
        return Decimal("0.00")
    if q >= q_total:
        return _dec(item.valor_total)
    unit = (Decimal(str(item.valor_total or 0)) / q_total).quantize(Decimal("0.0001"))
    return (unit * q).quantize(Decimal("0.01"))


def venda_restante_zerada(venda: VendaAgro) -> bool:
    if frete_restante(venda) > Decimal("0.009"):
        return False
    for it in venda.itens.all():
        if it.quantidade_restante > Decimal("0.0001"):
            return False
    return True


def montar_selecao_devolucao(
    venda: VendaAgro,
    *,
    itens_raw,
    devolver_frete: bool,
    devolver_tudo: bool,
) -> tuple[list[tuple[ItemVendaAgro, Decimal, Decimal]] | None, Decimal, str | None]:
    """
    Retorna (linhas [(item, qtd, valor)], frete_valor, erro).
    `devolver_tudo` ou ausência de `itens` no total clássico → restante inteiro.
    """
    itens_venda = {it.pk: it for it in venda.itens.all()}
    linhas: list[tuple[ItemVendaAgro, Decimal, Decimal]] = []

    if devolver_tudo or itens_raw is None:
        for it in itens_venda.values():
            q = it.quantidade_restante
            if q <= 0:
                continue
            linhas.append((it, q, valor_linha_proporcional(it, q)))
        frete_v = frete_restante(venda)
        return linhas, frete_v, None

    if not isinstance(itens_raw, list):
        return None, Decimal("0"), "Lista de itens inválida."

    seen: set[int] = set()
    for row in itens_raw[:200]:
        if not isinstance(row, dict):
            continue
        try:
            iid = int(row.get("item_id") or row.get("id") or 0)
        except (TypeError, ValueError):
            continue
        if iid in seen or iid not in itens_venda:
            continue
        seen.add(iid)
        it = itens_venda[iid]
        try:
            q = Decimal(str(row.get("quantidade") or "0").replace(",", "."))
        except Exception:
            return None, Decimal("0"), f"Quantidade inválida no item #{iid}."
        rest = it.quantidade_restante
        if q <= 0:
            continue
        if q > rest + Decimal("0.0001"):
            return (
                None,
                Decimal("0"),
                f"Quantidade maior que o restante do item «{(it.descricao or '')[:40]}» "
                f"(restante {rest}).",
            )
        q = min(q, rest)
        linhas.append((it, q, valor_linha_proporcional(it, q)))

    frete_v = frete_restante(venda) if devolver_frete else Decimal("0.00")
    if not linhas and frete_v <= 0:
        return None, Decimal("0"), "Selecione ao menos um item ou a taxa de entrega."
    return linhas, frete_v, None


def total_evento(
    linhas: list[tuple[ItemVendaAgro, Decimal, Decimal]],
    frete_v: Decimal,
) -> Decimal:
    s = sum((v for _i, _q, v in linhas), Decimal("0")) + _dec(frete_v)
    return s.quantize(Decimal("0.01"))


def separar_pagamentos_caixa_fiado(
    pagamentos: list[dict[str, Any]],
    *,
    venda_tem_fiado: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Separa formas caixa vs fiado; rejeita fiado se a venda não era fiado."""
    caixa: list[dict[str, Any]] = []
    fiado: list[dict[str, Any]] = []
    for row in pagamentos or []:
        fn = normalizar_forma_pagamento_caixa(str(row.get("forma") or ""))
        val = parse_valor_moeda_br(row.get("valor"))
        if val is None or val <= 0:
            continue
        if fn == "Fiado":
            if not venda_tem_fiado:
                return [], [], "Forma Fiado só é permitida se a venda original foi no fiado."
            fiado.append({"forma": fn, "valor": float(val)})
        else:
            caixa.append({"forma": fn, "valor": float(val)})
    return caixa, fiado, None


def abater_valor_fiado_venda(
    venda: VendaAgro,
    valor: Decimal | float,
    *,
    usuario: str = "",
    motivo: str = "",
) -> Decimal:
    """
    Reduz saldo em aberto dos títulos da venda (parcelas mais novas primeiro).
    Não registra movimento de caixa.
    """
    from produtos.fiado_gestao_util import registrar_evento_fiado, titulo_snapshot

    restante = _dec(valor)
    if restante <= 0:
        return Decimal("0.00")

    abatido = Decimal("0.00")
    qs = (
        FiadoTituloAgro.objects.filter(venda_agro=venda)
        .exclude(
            situacao__in=[
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            ]
        )
        .order_by("-vencimento", "-pk")
    )
    for t in qs:
        if restante <= Decimal("0.009"):
            break
        saldo = t.saldo_aberto
        if saldo <= 0:
            continue
        abate = min(saldo, restante)
        snap = titulo_snapshot(t)
        novo_bruto = (t.valor_bruto - abate).quantize(Decimal("0.01"))
        pago = _dec(t.valor_pago)
        if novo_bruto <= Decimal("0.009") and pago <= Decimal("0.009"):
            t.valor_bruto = Decimal("0.00")
            t.situacao = FiadoTituloAgro.Situacao.CANCELADO
        elif novo_bruto <= pago + Decimal("0.009"):
            t.valor_bruto = pago
            t.situacao = FiadoTituloAgro.Situacao.QUITADO
        else:
            t.valor_bruto = novo_bruto
        t.save(update_fields=["valor_bruto", "situacao", "atualizado_em"])
        registrar_evento_fiado(
            FiadoEventoAgro.Tipo.CANCELAMENTO,
            cliente_agro=t.cliente_agro,
            titulo=t,
            payload={
                "titulo": snap,
                "motivo": motivo or "Devolução parcial — abate fiado",
                "venda_id": venda.pk,
                "abate": float(abate),
                "tipo": "abate_devolucao",
            },
            usuario=usuario,
        )
        restante -= abate
        abatido += abate

    return abatido.quantize(Decimal("0.01"))


def serializar_itens_devolucao_ui(venda: VendaAgro) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for it in venda.itens.all():
        rest = it.quantidade_restante
        out.append(
            {
                "id": it.pk,
                "descricao": (it.descricao or "")[:200],
                "quantidade": float(it.quantidade or 0),
                "quantidade_devolvida": float(it.quantidade_devolvida or 0),
                "quantidade_restante": float(rest),
                "valor_unitario": float(it.valor_unitario or 0),
                "valor_total": float(it.valor_total or 0),
                "valor_restante": float(valor_linha_proporcional(it, rest)),
            }
        )
    return out


def serializar_historico_devolucoes(venda: VendaAgro) -> list[dict[str, Any]]:
    hist: list[dict[str, Any]] = []
    for d in venda.devolucoes.prefetch_related("itens", "itens__item").all()[:50]:
        itens = [
            {
                "descricao": (di.item.descricao if di.item_id else "")[:80],
                "quantidade": float(di.quantidade or 0),
                "valor_total": float(di.valor_total or 0),
            }
            for di in d.itens.all()
        ]
        hist.append(
            {
                "id": d.pk,
                "criado_em": d.criado_em.isoformat() if d.criado_em else "",
                "usuario": d.usuario or "",
                "motivo": d.motivo or "",
                "total": float(d.total or 0),
                "pagamentos": d.pagamentos_json or [],
                "incluiu_frete": bool(d.incluiu_frete),
                "frete_valor": float(d.frete_valor or 0),
                "totalizou_venda": bool(d.totalizou_venda),
                "itens": itens,
            }
        )
    return hist


def registrar_evento_devolucao(
    *,
    venda: VendaAgro,
    linhas: list[tuple[ItemVendaAgro, Decimal, Decimal]],
    frete_v: Decimal,
    pagamentos: list[dict[str, Any]],
    movimento_ids: list[int],
    motivo: str,
    usuario: str,
    totalizou: bool,
) -> DevolucaoVendaAgro:
    total = total_evento(linhas, frete_v)
    ev = DevolucaoVendaAgro.objects.create(
        venda=venda,
        usuario=(usuario or "")[:150],
        motivo=(motivo or "")[:2000],
        total=total,
        pagamentos_json=pagamentos,
        movimento_caixa_ids=movimento_ids or [],
        incluiu_frete=frete_v > 0,
        frete_valor=_dec(frete_v),
        totalizou_venda=totalizou,
    )
    for it, qtd, val in linhas:
        DevolucaoItemVendaAgro.objects.create(
            devolucao=ev,
            item=it,
            quantidade=qtd,
            valor_total=val,
        )
        it.quantidade_devolvida = (Decimal(str(it.quantidade_devolvida or 0)) + qtd).quantize(
            Decimal("0.0001")
        )
        it.save(update_fields=["quantidade_devolvida"])
    if frete_v > 0:
        venda.frete_devolvido = (_dec(venda.frete_devolvido) + _dec(frete_v)).quantize(
            Decimal("0.01")
        )
    return ev
