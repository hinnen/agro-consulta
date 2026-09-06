"""Criar / alterar tarefas com linha do tempo."""

from __future__ import annotations

from django.db import transaction
from django.utils import timezone

from .models import TarefaAgro, TarefaComentarioAgro, TarefaEventoAgro

STATUS_LABEL = dict(TarefaAgro.Status.choices)


def _evento(tarefa: TarefaAgro, *, tipo: str, quem: str, detalhe: str = "") -> TarefaEventoAgro:
    return TarefaEventoAgro.objects.create(
        tarefa=tarefa,
        tipo=tipo,
        autor_nome=(quem or "")[:150],
        detalhe=(detalhe or "")[:2000],
    )


@transaction.atomic
def criar_tarefa(
    *,
    titulo: str,
    descricao: str = "",
    status: str = TarefaAgro.Status.DECIDIR,
    loja: str = TarefaAgro.Loja.GERAL,
    responsavel: str = "",
    quem: str,
) -> TarefaAgro:
    titulo = (titulo or "").strip()
    if not titulo:
        raise ValueError("Informe o título.")
    if status not in STATUS_LABEL:
        status = TarefaAgro.Status.DECIDIR
    if loja not in dict(TarefaAgro.Loja.choices):
        loja = TarefaAgro.Loja.GERAL
    quem = (quem or "").strip()[:150]
    t = TarefaAgro.objects.create(
        titulo=titulo[:200],
        descricao=(descricao or "").strip(),
        status=status,
        loja=loja,
        responsavel=(responsavel or "").strip()[:120],
        criado_por_nome=quem,
        atualizado_por_nome=quem,
        concluido_em=timezone.now() if status == TarefaAgro.Status.CONCLUIDO else None,
    )
    _evento(t, tipo=TarefaEventoAgro.Tipo.CRIADA, quem=quem, detalhe=f"Status: {STATUS_LABEL.get(status, status)}")
    return t


@transaction.atomic
def atualizar_tarefa(
    tarefa: TarefaAgro,
    *,
    titulo: str | None = None,
    descricao: str | None = None,
    loja: str | None = None,
    responsavel: str | None = None,
    quem: str,
) -> TarefaAgro:
    quem = (quem or "").strip()[:150]
    mudancas: list[str] = []
    if titulo is not None:
        nt = titulo.strip()[:200]
        if not nt:
            raise ValueError("Informe o título.")
        if nt != tarefa.titulo:
            mudancas.append("título")
            tarefa.titulo = nt
    if descricao is not None and descricao.strip() != (tarefa.descricao or ""):
        mudancas.append("descrição")
        tarefa.descricao = descricao.strip()
    if loja is not None and loja in dict(TarefaAgro.Loja.choices) and loja != tarefa.loja:
        mudancas.append("loja")
        tarefa.loja = loja
    if responsavel is not None and responsavel.strip()[:120] != (tarefa.responsavel or ""):
        mudancas.append("responsável")
        tarefa.responsavel = responsavel.strip()[:120]
    if not mudancas:
        return tarefa
    tarefa.atualizado_por_nome = quem
    tarefa.save()
    _evento(
        tarefa,
        tipo=TarefaEventoAgro.Tipo.EDITADA,
        quem=quem,
        detalhe="Alterou: " + ", ".join(mudancas),
    )
    return tarefa


@transaction.atomic
def mudar_status(tarefa: TarefaAgro, *, status: str, quem: str) -> TarefaAgro:
    if status not in STATUS_LABEL:
        raise ValueError("Status inválido.")
    quem = (quem or "").strip()[:150]
    antes = tarefa.status
    if antes == status:
        return tarefa
    tarefa.status = status
    tarefa.atualizado_por_nome = quem
    if status == TarefaAgro.Status.CONCLUIDO:
        tarefa.concluido_em = timezone.now()
        tipo = TarefaEventoAgro.Tipo.CONCLUIDA
    else:
        if antes == TarefaAgro.Status.CONCLUIDO:
            tarefa.concluido_em = None
        tipo = TarefaEventoAgro.Tipo.STATUS
    tarefa.save()
    _evento(
        tarefa,
        tipo=tipo,
        quem=quem,
        detalhe=f"{STATUS_LABEL.get(antes, antes)} → {STATUS_LABEL.get(status, status)}",
    )
    return tarefa


@transaction.atomic
def adicionar_comentario(tarefa: TarefaAgro, *, texto: str, quem: str) -> TarefaComentarioAgro:
    texto = (texto or "").strip()
    if not texto:
        raise ValueError("Escreva o comentário.")
    quem = (quem or "").strip()[:150]
    c = TarefaComentarioAgro.objects.create(
        tarefa=tarefa,
        texto=texto[:5000],
        autor_nome=quem,
    )
    tarefa.atualizado_por_nome = quem
    tarefa.save(update_fields=["atualizado_por_nome", "atualizado_em"])
    _evento(
        tarefa,
        tipo=TarefaEventoAgro.Tipo.COMENTARIO,
        quem=quem,
        detalhe=texto[:280],
    )
    return c


def tarefa_para_dict(t: TarefaAgro) -> dict:
    return {
        "id": t.pk,
        "titulo": t.titulo,
        "descricao": t.descricao or "",
        "status": t.status,
        "status_label": STATUS_LABEL.get(t.status, t.status),
        "loja": t.loja,
        "loja_label": dict(TarefaAgro.Loja.choices).get(t.loja, t.loja),
        "responsavel": t.responsavel or "",
        "criado_por_nome": t.criado_por_nome or "",
        "atualizado_por_nome": t.atualizado_por_nome or "",
        "criado_em": t.criado_em.isoformat() if t.criado_em else "",
        "atualizado_em": t.atualizado_em.isoformat() if t.atualizado_em else "",
        "concluido_em": t.concluido_em.isoformat() if t.concluido_em else "",
    }
