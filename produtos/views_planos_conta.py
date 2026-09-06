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
from produtos.planos_conta_util import listar_planos_agro, seed_planos_padrao, serializar_plano

logger = logging.getLogger(__name__)


def _invalidar_cache_dre_planos() -> None:
    try:
        from financeiro.services.plano_conta_dre_util import invalidar_cache_cadastro_dre

        invalidar_cache_cadastro_dre()
    except Exception:
        logger.debug("invalidar cache DRE planos", exc_info=True)


@login_required(login_url="/entrar/")
@require_GET
def planos_conta_config_view(request):
    return render(
        request,
        "produtos/planos_conta_config.html",
        {
            "tipos": PlanoContaAgro.Tipo.choices,
        },
    )


@login_required(login_url="/entrar/")
@require_POST
def api_planos_conta_seed(request):
    """Carrega a lista padrão (mesma da loja) — não sobrescreve o que já existe."""
    try:
        stats = seed_planos_padrao()
    except Exception:
        logger.exception("api_planos_conta_seed")
        return JsonResponse({"ok": False, "erro": "Falha ao carregar a lista padrão."}, status=500)
    _invalidar_cache_dre_planos()
    return JsonResponse({"ok": True, **stats, "total": PlanoContaAgro.objects.count()})


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
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

    grupo = str(body.get("grupo") or "").strip()[:120]
    observacao = str(body.get("observacao") or "").strip()[:400]
    tipo = str(body.get("tipo") or PlanoContaAgro.Tipo.OUTRA).strip().lower()
    if tipo not in {c.value for c in PlanoContaAgro.Tipo}:
        tipo = PlanoContaAgro.Tipo.OUTRA
    ativo = body.get("ativo")
    if isinstance(ativo, bool):
        ativo_b = ativo
    else:
        ativo_b = str(ativo or "1").strip().lower() not in ("0", "false", "nao", "não", "off")

    if "exibir_pdv" in body:
        ep = body.get("exibir_pdv")
        exibir_pdv_b = bool(ep) if isinstance(ep, bool) else str(ep).strip().lower() in (
            "1",
            "true",
            "sim",
            "yes",
            "on",
        )
    else:
        exibir_pdv_b = None

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
            obj.grupo = grupo
            obj.observacao = observacao
            obj.tipo = tipo
            obj.ativo = ativo_b
            if exibir_pdv_b is not None:
                obj.exibir_pdv = exibir_pdv_b
            obj.save()
        else:
            if PlanoContaAgro.objects.filter(nome__iexact=nome).exists():
                return JsonResponse(
                    {"ok": False, "erro": "Já existe um plano com esse nome."},
                    status=400,
                )
            obj = PlanoContaAgro.objects.create(
                nome=nome,
                grupo=grupo,
                observacao=observacao,
                tipo=tipo,
                ativo=ativo_b,
                exibir_pdv=bool(exibir_pdv_b) if exibir_pdv_b is not None else False,
            )
    except IntegrityError:
        return JsonResponse(
            {"ok": False, "erro": "Nome duplicado — escolha outro."},
            status=400,
        )
    except Exception:
        logger.exception("api_planos_conta_salvar")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)

    _invalidar_cache_dre_planos()
    return JsonResponse({"ok": True, "item": serializar_plano(obj)})


@login_required(login_url="/entrar/")
@require_http_methods(["POST"])
def api_planos_conta_toggle(request, pk: int):
    obj = PlanoContaAgro.objects.filter(pk=pk).first()
    if not obj:
        return JsonResponse({"ok": False, "erro": "Plano não encontrado."}, status=404)
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        body = {}
    if "exibir_pdv" in body and "ativo" not in body:
        v = body.get("exibir_pdv")
        obj.exibir_pdv = bool(v) if isinstance(v, bool) else str(v).strip().lower() in (
            "1",
            "true",
            "sim",
            "yes",
            "on",
        )
        obj.save(update_fields=["exibir_pdv", "atualizado_em"])
    elif "ativo" in body:
        v = body.get("ativo")
        obj.ativo = bool(v) if isinstance(v, bool) else str(v).strip().lower() in (
            "1",
            "true",
            "sim",
            "yes",
            "on",
        )
        obj.save(update_fields=["ativo", "atualizado_em"])
    else:
        obj.ativo = not obj.ativo
        obj.save(update_fields=["ativo", "atualizado_em"])
    _invalidar_cache_dre_planos()
    return JsonResponse({"ok": True, "item": serializar_plano(obj)})
