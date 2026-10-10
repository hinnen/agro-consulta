"""APIs — cadastro de contas/bancos da loja (CP / baixa)."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from produtos.conta_bancaria_loja_util import (
    criar_conta_loja,
    listar_contas_loja,
    renomear_conta_loja,
    serializar_conta,
    set_ativo_conta_loja,
)

logger = logging.getLogger(__name__)


@login_required(login_url="/entrar/")
@require_GET
def api_contas_bancarias_loja_lista(request):
    q = (request.GET.get("q") or "").strip()
    inativos = str(request.GET.get("inativos") or "").strip().lower() in (
        "1",
        "true",
        "sim",
        "yes",
        "on",
    )
    try:
        itens = listar_contas_loja(incluir_inativos=inativos, q=q)
    except Exception:
        logger.exception("api_contas_bancarias_loja_lista")
        return JsonResponse({"ok": False, "erro": "Falha ao listar contas."}, status=500)
    return JsonResponse({"ok": True, "itens": itens, "total": len(itens)})


@login_required(login_url="/entrar/")
@require_POST
def api_contas_bancarias_loja_criar(request):
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        obj = criar_conta_loja(str(body.get("nome") or ""))
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception:
        logger.exception("api_contas_bancarias_loja_criar")
        return JsonResponse({"ok": False, "erro": "Falha ao criar conta."}, status=500)
    return JsonResponse({"ok": True, "item": serializar_conta(obj)})


@login_required(login_url="/entrar/")
@require_POST
def api_contas_bancarias_loja_renomear(request, pk: int):
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        obj = renomear_conta_loja(int(pk), str(body.get("nome") or ""))
    except LookupError:
        return JsonResponse({"ok": False, "erro": "Conta não encontrada."}, status=404)
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception:
        logger.exception("api_contas_bancarias_loja_renomear")
        return JsonResponse({"ok": False, "erro": "Falha ao renomear."}, status=500)
    return JsonResponse({"ok": True, "item": serializar_conta(obj)})


@login_required(login_url="/entrar/")
@require_POST
def api_contas_bancarias_loja_toggle(request, pk: int):
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        body = {}
    if "ativo" in body:
        ativo = body.get("ativo")
        if isinstance(ativo, bool):
            ativo_b = ativo
        else:
            ativo_b = str(ativo).strip().lower() not in ("0", "false", "nao", "não", "off")
    else:
        from produtos.models import ContaBancariaLojaAgro

        cur = ContaBancariaLojaAgro.objects.filter(pk=pk).first()
        if not cur:
            return JsonResponse({"ok": False, "erro": "Conta não encontrada."}, status=404)
        ativo_b = not cur.ativo
    try:
        obj = set_ativo_conta_loja(int(pk), ativo_b)
    except LookupError:
        return JsonResponse({"ok": False, "erro": "Conta não encontrada."}, status=404)
    except Exception:
        logger.exception("api_contas_bancarias_loja_toggle")
        return JsonResponse({"ok": False, "erro": "Falha ao alterar status."}, status=500)
    return JsonResponse({"ok": True, "item": serializar_conta(obj)})
