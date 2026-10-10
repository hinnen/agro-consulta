"""Tela META — mostruário de metas de venda + média esperada."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods

from produtos.meta_vendas_util import (
    meta_excluir_faixa,
    meta_listar_faixas,
    meta_montar_mostruario,
    meta_parse_competencia,
    meta_salvar_faixa,
    meta_texto_zap,
)

logger = logging.getLogger(__name__)


def _payload_json(request) -> dict:
    if request.content_type and "application/json" in request.content_type:
        try:
            raw = json.loads(request.body.decode("utf-8") or "{}")
            return raw if isinstance(raw, dict) else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}
    return {k: request.POST.get(k) for k in request.POST.keys()}


@ensure_csrf_cookie
@never_cache
@login_required(login_url="/entrar/")
def meta_vendas_painel(request):
    hoje = timezone.localdate()
    agora = timezone.localtime()
    competencia = meta_parse_competencia(request.GET.get("competencia"), hoje)
    try:
        mostruario = meta_montar_mostruario(
            competencia=competencia, hoje=hoje, agora=agora
        )
    except Exception:
        logger.exception("meta_vendas_painel mostruario")
        mostruario = {
            "competencia": competencia,
            "competencia_fmt": competencia,
            "mes_atual": True,
            "mes_fechado": False,
            "vendido_mes_fmt": "—",
            "faixas": [],
            "hoje": {"ativo": False},
            "vs_media_mes": {"sentido": "sem", "diff_fmt": "—", "pct_signed_fmt": "—"},
        }
    texto_zap = meta_texto_zap(mostruario) if mostruario.get("faixas") is not None else ""
    return render(
        request,
        "produtos/meta_vendas.html",
        {
            "competencia": competencia,
            "mostruario": mostruario,
            "texto_zap": texto_zap,
            "faixas": meta_listar_faixas(competencia),
            "api_resumo_url": reverse("api_meta_vendas_resumo"),
            "api_faixas_url": reverse("api_meta_vendas_faixas"),
            "home_url": reverse("home"),
        },
    )


@never_cache
@login_required(login_url="/entrar/")
@require_GET
def api_meta_vendas_resumo(request):
    hoje = timezone.localdate()
    agora = timezone.localtime()
    competencia = meta_parse_competencia(request.GET.get("competencia"), hoje)
    deposito = (request.GET.get("deposito") or "").strip().lower() or None
    if deposito not in ("centro", "vila", None):
        deposito = None
    modo = (request.GET.get("modo") or "mes").strip().lower()
    try:
        mostruario = meta_montar_mostruario(
            competencia=competencia, hoje=hoje, agora=agora, deposito=deposito
        )
        return JsonResponse(
            {
                "ok": True,
                "mostruario": mostruario,
                "modo": modo,
                "texto_zap": meta_texto_zap(mostruario, modo=modo),
            }
        )
    except Exception as exc:
        logger.exception("api_meta_vendas_resumo")
        return JsonResponse({"ok": False, "erro": str(exc)[:200]}, status=500)


@never_cache
@login_required(login_url="/entrar/")
@require_http_methods(["GET", "POST", "DELETE"])
def api_meta_vendas_faixas(request):
    hoje = timezone.localdate()
    if request.method == "GET":
        competencia = meta_parse_competencia(request.GET.get("competencia"), hoje)
        return JsonResponse(
            {"ok": True, "competencia": competencia, "faixas": meta_listar_faixas(competencia)}
        )

    body = _payload_json(request)
    competencia = meta_parse_competencia(
        body.get("competencia") or request.GET.get("competencia"), hoje
    )

    if request.method == "DELETE" or (
        request.method == "POST" and str(body.get("acao") or "").lower() == "excluir"
    ):
        faixa_id = body.get("id") or body.get("faixa_id") or request.GET.get("id")
        try:
            meta_excluir_faixa(competencia=competencia, faixa_id=int(faixa_id))
        except (TypeError, ValueError):
            return JsonResponse({"ok": False, "erro": "Faixa inválida."}, status=400)
        except Exception as exc:
            logger.exception("api_meta_vendas_faixas excluir")
            return JsonResponse({"ok": False, "erro": str(exc)[:200]}, status=500)
        return JsonResponse(
            {"ok": True, "faixas": meta_listar_faixas(competencia), "competencia": competencia}
        )

    # POST salvar (criar/editar)
    try:
        faixa_id = body.get("id") or body.get("faixa_id")
        faixa_id = int(faixa_id) if faixa_id not in (None, "", 0, "0") else None
        saved = meta_salvar_faixa(
            competencia=competencia,
            valor_meta=body.get("valor_meta"),
            bonus_valor=body.get("bonus_valor", 0),
            bonus_extra=body.get("bonus_extra") or "",
            faixa_id=faixa_id,
            ordem=body.get("ordem"),
        )
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    except Exception as exc:
        logger.exception("api_meta_vendas_faixas salvar")
        return JsonResponse({"ok": False, "erro": str(exc)[:200]}, status=500)
    return JsonResponse(
        {
            "ok": True,
            "faixa": saved,
            "faixas": meta_listar_faixas(competencia),
            "competencia": competencia,
        }
    )
