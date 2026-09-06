"""GM Pendências — telas e APIs (dentro do PWA /vendas/lojas/)."""

from __future__ import annotations

import json

from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from .models import TarefaAgro
from .pin_util import (
    exigir_operador_api,
    exigir_operador_html,
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
)
from .services import (
    STATUS_LABEL,
    adicionar_comentario,
    atualizar_tarefa,
    criar_tarefa,
    mudar_status,
    tarefa_para_dict,
)

STATUS_ORDEM = [
    TarefaAgro.Status.DECIDIR,
    TarefaAgro.Status.EM_ANDAMENTO,
    TarefaAgro.Status.AGUARDANDO,
    TarefaAgro.Status.CONCLUIDO,
    TarefaAgro.Status.ADIADO,
]

STATUS_COR = {
    TarefaAgro.Status.DECIDIR: "vermelho",
    TarefaAgro.Status.EM_ANDAMENTO: "amarelo",
    TarefaAgro.Status.AGUARDANDO: "laranja",
    TarefaAgro.Status.CONCLUIDO: "verde",
    TarefaAgro.Status.ADIADO: "azul",
}


def _payload_json(request) -> dict:
    if request.content_type and "application/json" in request.content_type:
        try:
            return json.loads(request.body.decode("utf-8") or "{}")
        except Exception:
            return {}
    return {k: request.POST.get(k) for k in request.POST.keys()}


@require_http_methods(["GET", "POST"])
def tarefas_pin(request):
    if request.method == "GET":
        if operador_da_sessao(request) and request.GET.get("trocar") != "1":
            return redirect("tarefas_lista")
        return render(
            request,
            "tarefas/pin.html",
            {"operador": operador_da_sessao(request)},
        )
    pin = str(request.POST.get("pin") or "").strip()
    ok, nome, err = gravar_operador_sessao(request, pin)
    if not ok:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest" or (
            request.content_type and "json" in (request.content_type or "")
        ):
            return JsonResponse({"ok": False, "erro": err}, status=403)
        return render(
            request,
            "tarefas/pin.html",
            {"erro": err, "operador": ""},
            status=403,
        )
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return JsonResponse({"ok": True, "operador": nome, "next": reverse("tarefas_lista")})
    return redirect("tarefas_lista")


@require_POST
def tarefas_logout(request):
    limpar_operador_sessao(request)
    return redirect("tarefas_pin")


@require_GET
@exigir_operador_html
def tarefas_lista(request):
    qs = TarefaAgro.objects.all().order_by("ordem", "-atualizado_em", "pk")
    grupos = []
    for st in STATUS_ORDEM:
        itens = [t for t in qs if t.status == st]
        grupos.append(
            {
                "status": st,
                "label": STATUS_LABEL.get(st, st),
                "cor": STATUS_COR.get(st, ""),
                "itens": itens,
                "qtd": len(itens),
            }
        )
    return render(
        request,
        "tarefas/lista.html",
        {
            "operador": operador_da_sessao(request),
            "grupos": grupos,
            "hub_url": reverse("vendas_lojas_hub"),
        },
    )


@require_GET
@exigir_operador_html
def tarefas_nova(request):
    return render(
        request,
        "tarefas/nova.html",
        {
            "operador": operador_da_sessao(request),
            "status_choices": TarefaAgro.Status.choices,
            "loja_choices": TarefaAgro.Loja.choices,
            "hub_url": reverse("vendas_lojas_hub"),
        },
    )


@require_GET
@exigir_operador_html
def tarefas_detalhe(request, pk: int):
    t = get_object_or_404(TarefaAgro, pk=pk)
    comentarios = list(t.comentarios.all().order_by("criado_em"))
    eventos = list(t.eventos.all().order_by("-criado_em", "-pk")[:80])
    return render(
        request,
        "tarefas/detalhe.html",
        {
            "operador": operador_da_sessao(request),
            "tarefa": t,
            "comentarios": comentarios,
            "eventos": eventos,
            "status_choices": TarefaAgro.Status.choices,
            "loja_choices": TarefaAgro.Loja.choices,
            "status_label": STATUS_LABEL.get(t.status, t.status),
            "status_cor": STATUS_COR.get(t.status, ""),
            "hub_url": reverse("vendas_lojas_hub"),
        },
    )


@require_POST
@exigir_operador_api
def api_tarefa_criar(request):
    data = _payload_json(request)
    quem = operador_da_sessao(request)
    try:
        t = criar_tarefa(
            titulo=str(data.get("titulo") or ""),
            descricao=str(data.get("descricao") or ""),
            status=str(data.get("status") or TarefaAgro.Status.DECIDIR),
            loja=str(data.get("loja") or TarefaAgro.Loja.GERAL),
            responsavel=str(data.get("responsavel") or ""),
            quem=quem,
        )
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    return JsonResponse({"ok": True, "tarefa": tarefa_para_dict(t), "url": reverse("tarefas_detalhe", args=[t.pk])})


@require_POST
@exigir_operador_api
def api_tarefa_atualizar(request, pk: int):
    t = get_object_or_404(TarefaAgro, pk=pk)
    data = _payload_json(request)
    quem = operador_da_sessao(request)
    try:
        if "status" in data and data.get("status") is not None:
            t = mudar_status(t, status=str(data.get("status")), quem=quem)
        t = atualizar_tarefa(
            t,
            titulo=None if data.get("titulo") is None else str(data.get("titulo")),
            descricao=None if data.get("descricao") is None else str(data.get("descricao")),
            loja=None if data.get("loja") is None else str(data.get("loja")),
            responsavel=None if data.get("responsavel") is None else str(data.get("responsavel")),
            quem=quem,
        )
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    return JsonResponse({"ok": True, "tarefa": tarefa_para_dict(t)})


@require_POST
@exigir_operador_api
def api_tarefa_status(request, pk: int):
    t = get_object_or_404(TarefaAgro, pk=pk)
    data = _payload_json(request)
    try:
        t = mudar_status(t, status=str(data.get("status") or ""), quem=operador_da_sessao(request))
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    return JsonResponse({"ok": True, "tarefa": tarefa_para_dict(t)})


@require_POST
@exigir_operador_api
def api_tarefa_comentar(request, pk: int):
    t = get_object_or_404(TarefaAgro, pk=pk)
    data = _payload_json(request)
    try:
        c = adicionar_comentario(
            t,
            texto=str(data.get("texto") or data.get("comentario") or ""),
            quem=operador_da_sessao(request),
        )
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    return JsonResponse(
        {
            "ok": True,
            "comentario": {
                "id": c.pk,
                "texto": c.texto,
                "autor_nome": c.autor_nome,
                "criado_em": c.criado_em.isoformat() if c.criado_em else "",
            },
        }
    )


@require_GET
def tarefas_manifest(request):
    """Manifest sob o escopo do PWA Vendas (ícone/nome do hub)."""
    return redirect("vendas_lojas_manifest")
