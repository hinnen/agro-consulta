"""Pedidos de transferência entre lojas feitos no PDV (Centro ↔ Vila Elias)."""
from __future__ import annotations

import json
import uuid
from decimal import Decimal, InvalidOperation

from django.utils.timezone import localtime

from estoque.models import HistoricoTransferencia, SolicitacaoTransferenciaPdv
from produtos.pdv_deposito_util import (
    DEPOSITO_CENTRO,
    DEPOSITO_VILA,
    normalizar_deposito,
    rotulo_deposito,
)

STATUS_ABERTOS = (
    SolicitacaoTransferenciaPdv.STATUS_PENDENTE,
    SolicitacaoTransferenciaPdv.STATUS_ACEITO,
)
STATUS_FINAIS = (
    SolicitacaoTransferenciaPdv.STATUS_RECUSADO,
    SolicitacaoTransferenciaPdv.STATUS_TRANSFERIDO,
    SolicitacaoTransferenciaPdv.STATUS_CANCELADO,
)


def outra_loja(deposito: str) -> str:
    dep = normalizar_deposito(deposito)
    return DEPOSITO_VILA if dep == DEPOSITO_CENTRO else DEPOSITO_CENTRO


def _dec(valor) -> Decimal:
    if isinstance(valor, Decimal):
        qtd = valor
    else:
        qtd = Decimal(str(valor).strip().replace(",", "."))
    if qtd <= 0:
        raise InvalidOperation("quantidade")
    return qtd.quantize(Decimal("0.001"))


def _usuario_request(request) -> str:
    if request is None:
        return ""
    u = getattr(request, "user", None)
    if u is None or not getattr(u, "is_authenticated", False):
        return ""
    nome = (u.get_full_name() or u.first_name or u.username or "").strip()
    return nome[:200]


def _hist(tipo, *, usuario_label="", produto_externo_id="", quantidade=None, observacao=""):
    try:
        HistoricoTransferencia.objects.create(
            tipo=tipo,
            produto_externo_id=(produto_externo_id or "")[:100],
            quantidade=quantidade,
            usuario_label=(usuario_label or "")[:200],
            observacao=observacao or "",
        )
    except Exception:
        pass


def serializar(row: SolicitacaoTransferenciaPdv) -> dict:
    criado = row.criado_em
    atualizado = row.atualizado_em
    return {
        "id": row.pk,
        "produto_id": row.produto_externo_id,
        "nome": row.nome_produto,
        "codigo": row.codigo_interno,
        "quantidade": float(row.quantidade),
        "loja_origem": row.loja_origem,
        "loja_origem_label": rotulo_deposito(row.loja_origem),
        "loja_destino": row.loja_destino,
        "loja_destino_label": rotulo_deposito(row.loja_destino),
        "status": row.status,
        "grupo_uuid": str(row.grupo_uuid) if row.grupo_uuid else None,
        "usuario_solicitante": row.usuario_solicitante,
        "usuario_resposta": row.usuario_resposta,
        "observacao": row.observacao,
        "criado_em": localtime(criado).strftime("%d/%m/%Y %H:%M") if criado else "",
        "atualizado_em": localtime(atualizado).strftime("%d/%m/%Y %H:%M") if atualizado else "",
    }


def criar_solicitacoes(itens, deposito_destino, *, usuario_label="", observacao=""):
    destino = normalizar_deposito(deposito_destino)
    origem = outra_loja(destino)
    if not isinstance(itens, list) or not itens:
        return None, "Informe ao menos um produto."
    if len(itens) > 80:
        return None, "Máximo 80 itens por pedido."

    agrupado: dict[str, dict] = {}
    for row in itens:
        if not isinstance(row, dict):
            continue
        pid = str(row.get("produto_id") or row.get("id") or "").strip()[:100]
        if not pid:
            continue
        try:
            qtd = _dec(row.get("quantidade", row.get("qtd", 1)))
        except (InvalidOperation, TypeError, ValueError, AttributeError):
            return None, "Quantidade inválida."
        nome = str(row.get("nome") or row.get("nome_produto") or "").strip()[:255] or f"Produto {pid}"
        codigo = str(row.get("codigo") or row.get("codigo_interno") or "").strip()[:100]
        if pid in agrupado:
            agrupado[pid]["quantidade"] += qtd
            if codigo and not agrupado[pid]["codigo"]:
                agrupado[pid]["codigo"] = codigo
        else:
            agrupado[pid] = {
                "produto_id": pid,
                "nome": nome,
                "codigo": codigo,
                "quantidade": qtd,
            }
    if not agrupado:
        return None, "Nenhum produto válido."

    grupo = uuid.uuid4()
    obs = (observacao or "").strip()[:500]
    criados = []
    for item in agrupado.values():
        row = SolicitacaoTransferenciaPdv.objects.create(
            produto_externo_id=item["produto_id"],
            nome_produto=item["nome"],
            codigo_interno=item["codigo"],
            quantidade=item["quantidade"],
            loja_origem=origem,
            loja_destino=destino,
            status=SolicitacaoTransferenciaPdv.STATUS_PENDENTE,
            grupo_uuid=grupo,
            usuario_solicitante=(usuario_label or "")[:200],
            observacao=obs,
        )
        criados.append(row)
        _hist(
            HistoricoTransferencia.TIPO_PEDIDO_PDV,
            usuario_label=usuario_label,
            produto_externo_id=item["produto_id"],
            quantidade=item["quantidade"],
            observacao=json.dumps(
                {
                    "id": row.pk,
                    "origem": origem,
                    "destino": destino,
                    "nome": item["nome"],
                },
                ensure_ascii=False,
            )[:8000],
        )
    return [serializar(r) for r in criados], ""


def resumo(deposito: str) -> dict:
    dep = normalizar_deposito(deposito)
    qs = SolicitacaoTransferenciaPdv.objects
    pend_rec = qs.filter(
        loja_origem=dep, status=SolicitacaoTransferenciaPdv.STATUS_PENDENTE
    ).count()
    ace_rec = qs.filter(
        loja_origem=dep, status=SolicitacaoTransferenciaPdv.STATUS_ACEITO
    ).count()
    pend_env = qs.filter(
        loja_destino=dep, status=SolicitacaoTransferenciaPdv.STATUS_PENDENTE
    ).count()
    return {
        "deposito": dep,
        "deposito_label": rotulo_deposito(dep),
        "outra_loja": outra_loja(dep),
        "outra_loja_label": rotulo_deposito(outra_loja(dep)),
        "pendentes_recebidos": pend_rec,
        "aceitos_recebidos": ace_rec,
        "pendentes_enviados": pend_env,
        "badge": pend_rec + ace_rec,
    }


def listar(deposito: str, papel: str, *, incluir_historico: bool = False, limite: int = 80):
    dep = normalizar_deposito(deposito)
    papel = (papel or "recebidos").strip().lower()
    qs = SolicitacaoTransferenciaPdv.objects.all()
    if papel == "enviados":
        qs = qs.filter(loja_destino=dep)
        if not incluir_historico:
            qs = qs.filter(status__in=STATUS_ABERTOS)
    elif papel == "todos":
        if not incluir_historico:
            qs = qs.filter(status__in=STATUS_ABERTOS)
    else:
        qs = qs.filter(loja_origem=dep)
        if not incluir_historico:
            qs = qs.filter(status__in=STATUS_ABERTOS)
    limite = max(1, min(int(limite or 80), 200))
    return [serializar(r) for r in qs.order_by("-criado_em", "-id")[:limite]]


def obter(pk: int) -> SolicitacaoTransferenciaPdv | None:
    try:
        return SolicitacaoTransferenciaPdv.objects.filter(pk=int(pk)).first()
    except (TypeError, ValueError):
        return None


def mudar_status(row: SolicitacaoTransferenciaPdv, novo_status: str, *, usuario_label=""):
    antigo = row.status
    row.status = novo_status
    if usuario_label:
        row.usuario_resposta = usuario_label[:200]
    row.save(update_fields=["status", "usuario_resposta", "atualizado_em"])
    _hist(
        HistoricoTransferencia.TIPO_PEDIDO_PDV_STATUS,
        usuario_label=usuario_label,
        produto_externo_id=row.produto_externo_id,
        quantidade=row.quantidade,
        observacao=json.dumps(
            {"id": row.pk, "de": antigo, "para": novo_status},
            ensure_ascii=False,
        )[:8000],
    )
    return serializar(row)


def aceitar(row: SolicitacaoTransferenciaPdv, deposito_ator: str, *, usuario_label=""):
    dep = normalizar_deposito(deposito_ator)
    if row.loja_origem != dep:
        return None, "Só a loja que tem o produto pode aceitar."
    if row.status != SolicitacaoTransferenciaPdv.STATUS_PENDENTE:
        return None, "Este pedido não está pendente."
    return mudar_status(
        row, SolicitacaoTransferenciaPdv.STATUS_ACEITO, usuario_label=usuario_label
    ), ""


def recusar(row: SolicitacaoTransferenciaPdv, deposito_ator: str, *, usuario_label=""):
    dep = normalizar_deposito(deposito_ator)
    if row.loja_origem != dep:
        return None, "Só a loja que recebeu o pedido pode recusar."
    if row.status not in (
        SolicitacaoTransferenciaPdv.STATUS_PENDENTE,
        SolicitacaoTransferenciaPdv.STATUS_ACEITO,
    ):
        return None, "Este pedido já foi encerrado."
    return mudar_status(
        row, SolicitacaoTransferenciaPdv.STATUS_RECUSADO, usuario_label=usuario_label
    ), ""


def cancelar(row: SolicitacaoTransferenciaPdv, deposito_ator: str, *, usuario_label=""):
    dep = normalizar_deposito(deposito_ator)
    if row.loja_destino != dep:
        return None, "Só quem pediu pode cancelar."
    if row.status != SolicitacaoTransferenciaPdv.STATUS_PENDENTE:
        return None, "Só dá para cancelar enquanto está pendente."
    return mudar_status(
        row, SolicitacaoTransferenciaPdv.STATUS_CANCELADO, usuario_label=usuario_label
    ), ""


def marcar_transferido(row: SolicitacaoTransferenciaPdv, *, usuario_label=""):
    return mudar_status(
        row,
        SolicitacaoTransferenciaPdv.STATUS_TRANSFERIDO,
        usuario_label=usuario_label,
    )
