"""Pedido de transferência entre lojas a partir do PDV (Centro ↔ Vila)."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.utils.timezone import localtime

from estoque.models import SolicitacaoTransferencia
from produtos.pdv_deposito_util import (
    DEPOSITO_CENTRO,
    DEPOSITO_VILA,
    ROTULO_DEPOSITO,
    bootstrap_deposito,
    normalizar_deposito,
)

STATUS_ABERTOS = frozenset(
    {
        SolicitacaoTransferencia.STATUS_PENDENTE,
        SolicitacaoTransferencia.STATUS_ACEITO,
    }
)
STATUS_TERMINAIS = frozenset(
    {
        SolicitacaoTransferencia.STATUS_RECUSADO,
        SolicitacaoTransferencia.STATUS_TRANSFERIDO,
        SolicitacaoTransferencia.STATUS_CANCELADO,
    }
)
STATUS_LABEL = {
    SolicitacaoTransferencia.STATUS_PENDENTE: "Pendente",
    SolicitacaoTransferencia.STATUS_ACEITO: "Aceito",
    SolicitacaoTransferencia.STATUS_RECUSADO: "Recusado",
    SolicitacaoTransferencia.STATUS_TRANSFERIDO: "Transferido",
    SolicitacaoTransferencia.STATUS_CANCELADO: "Cancelado",
}


def rotulo_loja(deposito: str) -> str:
    return ROTULO_DEPOSITO.get(normalizar_deposito(deposito), "Centro")


def outra_loja(deposito: str) -> str:
    dep = normalizar_deposito(deposito)
    return DEPOSITO_VILA if dep == DEPOSITO_CENTRO else DEPOSITO_CENTRO


def deposito_pdv(request) -> str:
    boot = bootstrap_deposito(request)
    return normalizar_deposito(boot.get("deposito") or DEPOSITO_CENTRO)


def rotulo_usuario(request) -> str:
    u = getattr(request, "user", None)
    if not u or not u.is_authenticated:
        return ""
    nome = (u.get_full_name() or u.first_name or u.username or "").strip()
    return nome[:200]


def _dec_qtd(valor) -> Decimal | None:
    try:
        q = Decimal(str(valor).strip().replace(",", "."))
    except (InvalidOperation, TypeError, ValueError, AttributeError):
        return None
    if q <= 0:
        return None
    return q.quantize(Decimal("0.001"))


def serializar(row: SolicitacaoTransferencia, deposito: str) -> dict:
    dep = normalizar_deposito(deposito)
    origem = normalizar_deposito(row.loja_origem)
    destino = normalizar_deposito(row.loja_destino)
    criado = localtime(row.criado_em) if row.criado_em else None
    return {
        "id": row.pk,
        "produto_id": row.produto_externo_id,
        "nome": row.nome_produto or row.produto_externo_id,
        "codigo": row.codigo_interno or "",
        "quantidade": float(row.quantidade),
        "origem": origem,
        "destino": destino,
        "origem_label": rotulo_loja(origem),
        "destino_label": rotulo_loja(destino),
        "status": row.status,
        "status_label": STATUS_LABEL.get(row.status, row.status),
        "usuario_solicitante": row.usuario_solicitante or "",
        "usuario_resposta": row.usuario_resposta or "",
        "observacao": row.observacao or "",
        "criado_em": criado.strftime("%d/%m %H:%M") if criado else "",
        "recebido": origem == dep,
        "enviado": destino == dep,
    }


def criar_solicitacoes(request, itens) -> tuple[list[SolicitacaoTransferencia] | None, str]:
    destino = deposito_pdv(request)
    origem = outra_loja(destino)
    if not isinstance(itens, list) or not itens:
        return None, "Informe ao menos um produto."
    if len(itens) > 80:
        return None, "Máximo 80 itens por pedido."
    usuario = rotulo_usuario(request)
    criados: list[SolicitacaoTransferencia] = []
    for raw in itens:
        if not isinstance(raw, dict):
            return None, "Item inválido."
        pid = str(raw.get("produto_id") or raw.get("id") or "").strip()[:100]
        if not pid:
            return None, "Produto sem id."
        qtd = _dec_qtd(raw.get("quantidade") or raw.get("qtd") or "0")
        if qtd is None:
            return None, "Quantidade inválida."
        nome = str(raw.get("nome") or raw.get("nome_produto") or pid).strip()[:255]
        codigo = str(raw.get("codigo") or raw.get("codigo_interno") or "").strip()[:100]
        criados.append(
            SolicitacaoTransferencia(
                produto_externo_id=pid,
                nome_produto=nome or pid,
                codigo_interno=codigo,
                quantidade=qtd,
                loja_origem=origem,
                loja_destino=destino,
                status=SolicitacaoTransferencia.STATUS_PENDENTE,
                usuario_solicitante=usuario,
            )
        )
    SolicitacaoTransferencia.objects.bulk_create(criados)
    return list(criados), ""


def qs_recebidos(deposito: str):
    dep = normalizar_deposito(deposito)
    return SolicitacaoTransferencia.objects.filter(
        loja_origem=dep,
        status__in=list(STATUS_ABERTOS),
    ).order_by("criado_em", "id")


def qs_enviados(deposito: str):
    dep = normalizar_deposito(deposito)
    return SolicitacaoTransferencia.objects.filter(
        loja_destino=dep,
        status__in=list(STATUS_ABERTOS),
    ).order_by("-criado_em", "-id")


def qs_historico(deposito: str, limite: int = 40):
    dep = normalizar_deposito(deposito)
    from django.db.models import Q

    return SolicitacaoTransferencia.objects.filter(
        Q(loja_origem=dep) | Q(loja_destino=dep),
        status__in=list(STATUS_TERMINAIS),
    ).order_by("-atualizado_em", "-id")[:limite]


def badge_count(deposito: str) -> int:
    return qs_recebidos(deposito).count()


def listar_painel(request) -> dict:
    dep = deposito_pdv(request)
    outra = outra_loja(dep)
    recebidos = [serializar(r, dep) for r in qs_recebidos(dep)]
    enviados = [serializar(r, dep) for r in qs_enviados(dep)]
    historico = [serializar(r, dep) for r in qs_historico(dep)]
    return {
        "ok": True,
        "deposito": dep,
        "deposito_label": rotulo_loja(dep),
        "outra": outra,
        "outra_label": rotulo_loja(outra),
        "badge": len(recebidos),
        "recebidos": recebidos,
        "enviados": enviados,
        "historico": historico,
    }


def _pegar(pk: int) -> SolicitacaoTransferencia | None:
    try:
        return SolicitacaoTransferencia.objects.get(pk=int(pk))
    except (SolicitacaoTransferencia.DoesNotExist, TypeError, ValueError):
        return None


def alterar_status(request, pk: int, acao: str) -> tuple[SolicitacaoTransferencia | None, str]:
    acao = (acao or "").strip().lower()
    row = _pegar(pk)
    if not row:
        return None, "Pedido não encontrado."
    dep = deposito_pdv(request)
    usuario = rotulo_usuario(request)
    origem = normalizar_deposito(row.loja_origem)
    destino = normalizar_deposito(row.loja_destino)

    if acao == "aceitar":
        if origem != dep:
            return None, "Só a loja que envia pode aceitar."
        if row.status != SolicitacaoTransferencia.STATUS_PENDENTE:
            return None, "Este pedido não está pendente."
        row.status = SolicitacaoTransferencia.STATUS_ACEITO
        row.usuario_resposta = usuario
        row.save(update_fields=["status", "usuario_resposta", "atualizado_em"])
        return row, ""

    if acao == "recusar":
        if origem != dep:
            return None, "Só a loja que envia pode recusar."
        if row.status not in STATUS_ABERTOS:
            return None, "Este pedido já foi encerrado."
        row.status = SolicitacaoTransferencia.STATUS_RECUSADO
        row.usuario_resposta = usuario
        row.save(update_fields=["status", "usuario_resposta", "atualizado_em"])
        return row, ""

    if acao == "cancelar":
        if destino != dep:
            return None, "Só quem pediu pode cancelar."
        if row.status not in STATUS_ABERTOS:
            return None, "Este pedido já foi encerrado."
        row.status = SolicitacaoTransferencia.STATUS_CANCELADO
        row.usuario_resposta = usuario
        row.save(update_fields=["status", "usuario_resposta", "atualizado_em"])
        return row, ""

    return None, "Ação inválida."


def marcar_transferido(row: SolicitacaoTransferencia, usuario: str) -> SolicitacaoTransferencia:
    row.status = SolicitacaoTransferencia.STATUS_TRANSFERIDO
    row.usuario_resposta = (usuario or row.usuario_resposta or "")[:200]
    row.save(update_fields=["status", "usuario_resposta", "atualizado_em"])
    return row


def pedidos_para_transferir(request, pks: list[int] | None) -> tuple[list[SolicitacaoTransferencia] | None, str]:
    dep = deposito_pdv(request)
    qs = qs_recebidos(dep)
    if pks:
        qs = qs.filter(pk__in=pks)
    rows = list(qs)
    if not rows:
        return None, "Nenhum pedido aberto para transferir."
    for row in rows:
        if normalizar_deposito(row.loja_origem) != dep:
            return None, "Só a loja que envia pode transferir."
        if row.status not in STATUS_ABERTOS:
            return None, "Há pedido já encerrado na lista."
    return rows, ""
