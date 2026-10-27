"""Página e APIs — Configuração fiscal NF-e 55 (Etapa 2). Sem efeito no motor."""
from __future__ import annotations

import logging

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from produtos.nfe_fiscal_padrao_util import aplicar_padrao_contador, previa_padrao_contador
from produtos.nfe_fiscal_config_util import (
    ESTAB_CENTRO,
    MSG_SALVA,
    listar_historico,
    painel_estabelecimento,
    parse_estabelecimento,
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

_ERRO_ESTAB = "Informe o estabelecimento (centro ou vila)."


def _estab_leitura(request) -> str | None:
    raw = request.GET.get("estabelecimento")
    if raw in (None, ""):
        return ESTAB_CENTRO
    return parse_estabelecimento(raw)


def _estab_gravacao(request, body: dict | None = None) -> str | None:
    raw = ""
    if isinstance(body, dict) and "estabelecimento" in body:
        raw = body.get("estabelecimento")
    elif request.POST.get("estabelecimento"):
        raw = request.POST.get("estabelecimento")
    return parse_estabelecimento(raw)


def _json_estab_obrigatorio(request, body: dict | None = None):
    loja = _estab_gravacao(request, body)
    if not loja:
        return None, JsonResponse({"ok": False, "erro": _ERRO_ESTAB}, status=400)
    return loja, None


@nfe_saida_staff_required
@require_GET
def fiscal_nfe_config_view(request):
    loja = _estab_leitura(request) or ESTAB_CENTRO
    return render(
        request,
        "produtos/fiscal_nfe_config.html",
        {"estabelecimento": loja},
    )


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_painel(request):
    loja = _estab_leitura(request)
    if not loja:
        return JsonResponse({"ok": False, "erro": _ERRO_ESTAB}, status=400)
    return JsonResponse(painel_estabelecimento(loja))


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_empresa_salvar(request):
    body = parse_json_body(request)
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
    try:
        out = salvar_empresa(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe empresa")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    return JsonResponse({"ok": True, "mensagem": MSG_SALVA, "empresa": out.get("empresa")})


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_credito_salvar(request):
    body = parse_json_body(request)
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
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
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
    try:
        out = salvar_pis(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe pis")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    return JsonResponse({"ok": True, "mensagem": MSG_SALVA, "pis": out.get("pis")})


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_ibpt_salvar(request):
    body = parse_json_body(request)
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
    try:
        out = salvar_ibpt(loja, body, request.user)
    except Exception:
        logger.exception("fiscal_nfe ibpt")
        return JsonResponse({"ok": False, "erro": "Falha ao salvar."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    return JsonResponse({"ok": True, "mensagem": MSG_SALVA, "ibpt": out.get("ibpt")})


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_regra_salvar(request):
    body = parse_json_body(request)
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
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
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
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
    tipo = str(body.get("tipo") or "")
    loja = None
    if tipo != "questionario":
        loja, err = _json_estab_obrigatorio(request, body)
        if err:
            return err
    elif body.get("estabelecimento") not in (None, ""):
        loja = parse_estabelecimento(body.get("estabelecimento"))
        if not loja:
            return JsonResponse({"ok": False, "erro": _ERRO_ESTAB}, status=400)
    out = validar_registro(tipo, pk, request.user, loja or "", body)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    out["mensagem"] = "Regra validada."
    painel_loja = loja or _estab_leitura(request) or ESTAB_CENTRO
    out["painel"] = painel_estabelecimento(painel_loja)
    return JsonResponse(out)


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_historico(request):
    loja = _estab_leitura(request)
    if not loja:
        return JsonResponse({"ok": False, "erro": _ERRO_ESTAB}, status=400)
    return JsonResponse({"ok": True, "itens": listar_historico(loja)})


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_padrao_previa(request):
    loja = _estab_leitura(request)
    if not loja:
        return JsonResponse({"ok": False, "erro": _ERRO_ESTAB}, status=400)
    out = previa_padrao_contador(loja)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    return JsonResponse(out)


@nfe_saida_staff_required
@require_POST
def api_fiscal_nfe_padrao_aplicar(request):
    body = parse_json_body(request)
    loja, err = _json_estab_obrigatorio(request, body)
    if err:
        return err
    if not body.get("confirmar"):
        return JsonResponse({"ok": False, "erro": "Confirme a prévia antes de gravar."}, status=400)
    try:
        out = aplicar_padrao_contador(loja, request.user)
    except Exception:
        logger.exception("fiscal_nfe padrao")
        return JsonResponse({"ok": False, "erro": "Falha ao aplicar o padrão."}, status=500)
    if not out.get("ok"):
        return JsonResponse(out, status=400)
    from produtos.nfe_fiscal_config_util import painel_estabelecimento

    out["painel"] = painel_estabelecimento(loja)
    return JsonResponse(out)


@nfe_saida_staff_required
@require_GET
def api_fiscal_nfe_questionario_texto(request):
    txt = texto_questionario()
    resp = HttpResponse(txt, content_type="text/plain; charset=utf-8")
    resp["Content-Disposition"] = 'inline; filename="questionario-fiscal-nfe.txt"'
    return resp
