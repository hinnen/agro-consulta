"""APIs de cadastro de cliente: duplicata, exclusão, vale crédito, histórico."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.http import require_GET, require_POST

from produtos.cliente_operacoes_util import (
    creditar_vale_manual,
    excluir_cliente,
    limpar_whatsapp_duplicado,
    listar_eventos_cliente,
    preview_exclusao,
    transferir_saldos,
)
from produtos.cliente_whatsapp_util import extrair_whatsapp_digits, info_whatsapp_duplicado
from produtos.models import ClienteAgro

logger = logging.getLogger(__name__)


def _json_body(request) -> tuple[dict | None, JsonResponse | None]:
    try:
        data = json.loads(request.body or "{}")
    except Exception:
        return None, JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    if not isinstance(data, dict):
        return None, JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    return data, None


def _origem(request, data: dict | None = None) -> str:
    raw = ""
    if isinstance(data, dict):
        raw = str(data.get("origem_tela") or "").strip()
    if not raw:
        raw = str(request.GET.get("origem_tela") or "").strip()
    return (raw or "pdv")[:32]


@login_required(login_url="/admin/login/")
@require_GET
def api_cliente_whatsapp_duplicado(request):
    digits = extrair_whatsapp_digits(request.GET.get("whatsapp") or request.GET.get("telefone") or "")
    excluir_pk = request.GET.get("excluir_pk")
    pk = None
    if excluir_pk not in (None, ""):
        try:
            pk = int(excluir_pk)
        except (TypeError, ValueError):
            pk = None
    info = info_whatsapp_duplicado(digits, excluir_pk=pk) if digits else None
    if not info:
        return JsonResponse({"ok": True, "duplicado": None})
    return JsonResponse({"ok": True, "duplicado": info})


@login_required(login_url="/admin/login/")
@require_POST
def api_cliente_limpar_whatsapp(request, pk: int):
    data, err = _json_body(request)
    if err:
        return err
    out = limpar_whatsapp_duplicado(
        alvo_pk=pk,
        pin=str(data.get("pin") or ""),
        origem_tela=_origem(request, data),
    )
    status = 200 if out.get("ok") else 400
    return JsonResponse(out, status=status)


@login_required(login_url="/admin/login/")
@require_GET
def api_cliente_exclusao_preview(request, pk: int):
    get_object_or_404(ClienteAgro, pk=pk)
    out = preview_exclusao(pk)
    status = 200 if out.get("ok") else 400
    return JsonResponse(out, status=status)


@login_required(login_url="/admin/login/")
@require_POST
def api_cliente_transferir_saldos(request, pk: int):
    data, err = _json_body(request)
    if err:
        return err
    dest = data.get("destino_pk") or data.get("destino")
    try:
        dest_pk = int(dest)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "Informe o cadastro destino."}, status=400)
    out = transferir_saldos(
        origem_pk=pk,
        destino_pk=dest_pk,
        pin=str(data.get("pin") or ""),
        origem_tela=_origem(request, data),
        cashback=data.get("cashback", True) is not False,
        vale=data.get("vale", True) is not False,
    )
    status = 200 if out.get("ok") else 400
    return JsonResponse(out, status=status)


@login_required(login_url="/admin/login/")
@require_POST
def api_cliente_excluir(request, pk: int):
    data, err = _json_body(request)
    if err:
        return err
    dest_raw = data.get("destino_pk") or data.get("destino")
    dest_pk = None
    if dest_raw not in (None, ""):
        try:
            dest_pk = int(dest_raw)
        except (TypeError, ValueError):
            return JsonResponse({"ok": False, "erro": "Cadastro destino inválido."}, status=400)
    out = excluir_cliente(
        pk=pk,
        pin=str(data.get("pin") or ""),
        destino_pk=dest_pk,
        origem_tela=_origem(request, data),
    )
    status = 200 if out.get("ok") else 400
    return JsonResponse(out, status=status)


@login_required(login_url="/admin/login/")
@require_POST
def api_cliente_vale_credito_manual(request, pk: int):
    data, err = _json_body(request)
    if err:
        return err
    out = creditar_vale_manual(
        pk=pk,
        valor=data.get("valor"),
        motivo=str(data.get("motivo") or ""),
        pin=str(data.get("pin") or ""),
        origem_tela=_origem(request, data),
    )
    status = 200 if out.get("ok") else 400
    return JsonResponse(out, status=status)


@login_required(login_url="/admin/login/")
@require_GET
def api_cliente_eventos(request, pk: int):
    get_object_or_404(ClienteAgro, pk=pk)
    try:
        limite = int(request.GET.get("limite") or 40)
    except (TypeError, ValueError):
        limite = 40
    return JsonResponse({"ok": True, "eventos": listar_eventos_cliente(pk, limite=limite)})
