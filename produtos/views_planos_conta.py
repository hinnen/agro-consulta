"""Tela e APIs — cadastro de plano de contas (Configuração)."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.db import IntegrityError
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.models import PlanoContaAgro
from produtos.planos_conta_util import listar_planos_agro, serializar_plano

logger = logging.getLogger(__name__)


@login_required(login_url="/admin/login/")
@require_GET
def planos_conta_config_view(request):
    return render(
        request,
        "produtos/planos_conta_config.html",
        {
            "naturezas": PlanoContaAgro.Natureza.choices,
        },
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_planos_conta_lista(request):
    q = (request.GET.get("q") or "").strip()
    inativos = str(request.GET.get("inativos") or "").strip().lower() in (
        "1",
        "true",
        "sim",
        "yes",
    )
    itens = listar_planos_agro(q=q, incluir_inativos=inativos)
    return JsonResponse({"ok": True, "itens": itens, "total": len(itens)})


@login_required(login_url="/admin/login/")
@require_POST
def api_planos_conta_salvar(request):
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)

    nome = str(body.get("nome") or "").strip()[:200]
    if len(nome) < 2:
        return JsonResponse(
            {"ok": False, "erro": "Informe o nome do plano (mín. 2 letras)."},
            status=400,
        )

    codigo = str(body.get("codigo") or "").strip()[:40]
    grupo = str(body.get("grupo") or "").strip()[:120]
    observacao = str(body.get("observacao") or "").strip()[:300]
    natureza = str(body.get("natureza") or PlanoContaAgro.Natureza.DESPESA).strip().lower()
    if natureza not in {c.value for c in PlanoContaAgro.Natureza}:
        natureza = PlanoContaAgro.Natureza.DESPESA
    ativo = body.get("ativo")
    if isinstance(ativo, bool):
        ativo_b = ativo
    else:
        ativo_b = str(ativo or "1").strip().lower() not in ("0", "false", "nao", "não", "off")

    pk_raw = body.get("pk") or body.get("id")
    pk = None
    if pk_raw is not None and str(pk_raw).strip():
        s = str(pk_raw).strip()
        if s.lower().startswith("agro:"):
            s = s.split(":", 1)[1]
        try:
            pk = int(s)
        except (TypeError, ValueError):
            return JsonResponse({"ok": False, "erro": "ID inválido."}, status=400)

    try:
        if pk:
            obj = PlanoContaAgro.objects.filter(pk=pk).first()
            if not obj:
                return JsonResponse({"ok": False, "erro": "Plano não encontrado."}, status=404)
            if (
                PlanoContaAgro.objects.filter(nome__iexact=nome)
                .exclude(pk=obj.pk)
                .exists()
            ):
                return JsonResponse(
                    {"ok": False, "erro": "Já existe outro plano com esse nome."},
                    status=400,
                )
            obj.nome = nome
            obj.codigo = codigo
            obj.grupo = grupo
            obj.observacao = observacao
            obj.natureza = natureza
            obj.ativo = ativo_b
            obj.save()
        else:
            if PlanoContaAgro.objects.filter(nome__iexact=nome).exists():
                return JsonResponse(
                    {"ok": False, "erro": "Já existe um plano com esse nome."},
                    status=400,
                )
            obj = PlanoContaAgro.objects.create(
                nome=nome,
                codigo=codigo,
                grupo=grupo,
                observacao=observacao,
                natureza=natureza,
                ativo=ativo_b,
                criado_por=request.user if request.user.is_authenticated else None,
            )
    except IntegrityError:
        return JsonResponse(
            {"ok": False, "erro": "Nome duplicado — escolha outro."},
            status=400,
        )
    except Exception:
        logger.exception("api_planos_conta_salvar")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)

    return JsonResponse({"ok": True, "item": serializar_plano(obj)})


@login_required(login_url="/admin/login/")
@require_http_methods(["POST"])
def api_planos_conta_toggle(request, pk: int):
    obj = PlanoContaAgro.objects.filter(pk=pk).first()
    if not obj:
        return JsonResponse({"ok": False, "erro": "Plano não encontrado."}, status=404)
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        body = {}
    if "ativo" in body:
        v = body.get("ativo")
        obj.ativo = bool(v) if isinstance(v, bool) else str(v).strip().lower() in (
            "1",
            "true",
            "sim",
            "yes",
            "on",
        )
    else:
        obj.ativo = not obj.ativo
    obj.save(update_fields=["ativo", "atualizado_em"])
    return JsonResponse({"ok": True, "item": serializar_plano(obj)})
