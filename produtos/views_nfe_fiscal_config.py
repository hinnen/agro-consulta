"""Página e APIs — Configuração fiscal NF-e 55 (Etapa 2). Sem efeito no motor."""
from __future__ import annotations

import logging

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from produtos.nfe_fiscal_config_util import (
    MSG_SALVA,
    listar_historico,
    normalizar_estabelecimento,
    painel_estabelecimento,
    parse_json_body,
    salvar_credito,
    salvar_empresa,
    salvar_ibpt,
    salvar_pis,
    salvar_questionario_item,
    salvar_regra,
    salvar_st,
    texto_questionario,
    validar_registro,
)
from produtos.views_nfe_saida import nfe_saida_staff_required

logger = logging.getLogger(__name__)


def _estab(request, body: dict | None = None) -> str:
    raw = ""
    if body:
        raw = body.get("estabelecimento") or ""
    if not raw:
        raw = request.GET.get("estabelecimento") or request.POST.get("estabelecimento") or ""
    return normalizar_estabelecimento(raw)


@nfe_saida_staff_required
@require_GET
def fiscal_nfe_config_view(request):
    return render(
        request,
        "produtos/fiscal_nfe_config.html",
        {"estabelecimento": _estab(request)},
    )


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_painel(request):
    return JsonResponse(painel_estabelecimento(_estab(request)))


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_empresa_salvar(request):
    body = parse_json_body(request)
    loja = _estab(request, body)
    try:
        empresa = salvar_empresa(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe empresa")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    return JsonResponse({"ok": True, "mensagem": MSG_SALVA, "empresa": empresa})


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_credito_salvar(request):
    body = parse_json_body(request)
    loja = _estab(request, body)
    try:
        out = salvar_credito(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe credito")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    out["mensagem"] = MSG_SALVA
    return JsonResponse(out)


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_pis_salvar(request):
    body = parse_json_body(request)
    loja = _estab(request, body)
    try:
        pis = salvar_pis(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe pis")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    return JsonResponse({"ok": True, "mensagem": MSG_SALVA, "pis": pis})


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_ibpt_salvar(request):
    body = parse_json_body(request)
    loja = _estab(request, body)
    try:
        ibpt = salvar_ibpt(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe ibpt")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    return JsonResponse({"ok": True, "mensagem": MSG_SALVA, "ibpt": ibpt})


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_regra_salvar(request):
    body = parse_json_body(request)
    loja = _estab(request, body)
    try:
        out = salvar_regra(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe regra")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    out["mensagem"] = MSG_SALVA
    return JsonResponse(out)


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_st_salvar(request):
    body = parse_json_body(request)
    loja = _estab(request, body)
    try:
        out = salvar_st(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe st")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    out["mensagem"] = MSG_SALVA
    return JsonResponse(out)


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_questionario_salvar(request):
    body = parse_json_body(request)
    try:
        out = salvar_questionario_item(body, request.user)
    except Exception:
        logger.exception("fiscal_nfe questionario")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    out["mensagem"] = MSG_SALVA
    return JsonResponse(out)


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_validar(request):
    body = parse_json_body(request)
    try:
        pk = int(body.get("id") or 0)
    except (TypeError, ValueError):
        pk = 0
    if not pk:
        return JsonResponse({"ok": False, "erro": "Informe o registro."}, status=400)
    out = validar_registro(str(body.get("tipo") or ""), pk, request.user, _estab(request, body))
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    out["mensagem"] = "Regra validada."
    out["painel"] = painel_estabelecimento(_estab(request, body))
    return JsonResponse(out)


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_historico(request):
    return JsonResponse({"ok": True, "itens": listar_historico(_estab(request))})


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_questionario_texto(request):
    txt = texto_questionario()
    resp = HttpResponse(txt, content_type="text/plain; charset=utf-8")
    resp["Content-Disposition"] = 'inline; filename="questionario-fiscal-nfe.txt"'
    return resp
