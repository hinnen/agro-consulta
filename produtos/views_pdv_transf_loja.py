"""APIs PDV — pedido de transferência entre lojas."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.db.models import Q
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from estoque.models import SolicitacaoTransferenciaPdv
from produtos.pdv_deposito_util import bootstrap_deposito, normalizar_deposito, rotulo_deposito
from produtos.pdv_transf_loja_util import (
    STATUS_CANCELADO,
    STATUS_CONCLUIDO,
    aplicar_status,
    concluir_transferencia,
    criar_solicitacao,
    resolver_operador_pdv,
    resumo_loja,
    serializar_solicitacao,
)

logger = logging.getLogger(__name__)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _loja_atual(request, payload=None) -> str:
    if payload and payload.get("loja"):
        return normalizar_deposito(payload.get("loja"))
    boot = bootstrap_deposito(request)
    return normalizar_deposito(boot.get("deposito") or "centro")


def _operador(request, payload=None):
    pin = ""
    if payload:
        pin = str(payload.get("pin") or "").strip()
    return resolver_operador_pdv(request, pin)


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_transf_loja_resumo(request):
    loja = _loja_atual(request)
    ok, label, _user, _err = resolver_operador_pdv(request, "")
    return JsonResponse(
        {
            "ok": True,
            "operador": label if ok else "",
            "precisa_pin": not ok,
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_transf_loja_lista(request):
    loja = _loja_atual(request)
    aba = str(request.GET.get("aba") or "recebidos").strip().lower()
    qs = SolicitacaoTransferenciaPdv.objects.prefetch_related("itens")
    if aba == "enviados":
        qs = qs.filter(
            loja_destino=loja,
            status__in=SolicitacaoTransferenciaPdv.STATUS_ABERTOS,
        )
    elif aba == "historico":
        qs = qs.filter(
            Q(loja_origem=loja) | Q(loja_destino=loja),
            status__in=(STATUS_CONCLUIDO, STATUS_CANCELADO),
        ).order_by("-atualizado_em", "-id")
    else:
        qs = qs.filter(
            loja_origem=loja,
            status__in=SolicitacaoTransferenciaPdv.STATUS_ABERTOS,
        )
    qs = qs[:80]
    return JsonResponse(
        {
            "ok": True,
            "loja": loja,
            "loja_label": rotulo_deposito(loja),
            "aba": aba,
            "itens": [serializar_solicitacao(s) for s in qs],
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_transf_loja_criar(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    ok, label, user, err = _operador(request, payload)
    if not ok:
        return JsonResponse({"ok": False, "erro": err, "precisa_pin": True}, status=403)
    loja = _loja_atual(request, payload)
    sol, err_c = criar_solicitacao(
        loja_destino=loja,
        itens_raw=payload.get("itens") or [],
        observacao=str(payload.get("observacao") or ""),
        operador_label=label,
        usuario=user,
    )
    if err_c or sol is None:
        return JsonResponse({"ok": False, "erro": err_c or "Não foi possível criar"}, status=400)
    sol = (
        SolicitacaoTransferenciaPdv.objects.prefetch_related("itens", "eventos")
        .filter(pk=sol.pk)
        .first()
    )
    return JsonResponse(
        {
            "ok": True,
            "mensagem": "Pedido enviado para " + rotulo_deposito(sol.loja_origem) + ".",
            "solicitacao": serializar_solicitacao(sol, com_eventos=True),
            **resumo_loja(loja),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_transf_loja_acao(request, pk: int):
    payload = _payload(request) or {}
    ok, label, user, err = _operador(request, payload)
    if not ok:
        return JsonResponse({"ok": False, "erro": err, "precisa_pin": True}, status=403)
    loja = _loja_atual(request, payload)
    sol = (
        SolicitacaoTransferenciaPdv.objects.prefetch_related("itens")
        .filter(pk=pk)
        .first()
    )
    if sol is None:
        return JsonResponse({"ok": False, "erro": "Pedido não encontrado."}, status=404)
    acao = str(payload.get("acao") or "").strip().lower()
    if acao == "transferir":
        ok_t, err_t, _res = concluir_transferencia(
            request,
            sol,
            loja_atual=loja,
            operador_label=label,
            usuario=user,
        )
        if not ok_t:
            return JsonResponse({"ok": False, "erro": err_t}, status=400)
        sol.refresh_from_db()
        sol = (
            SolicitacaoTransferenciaPdv.objects.prefetch_related("itens", "eventos")
            .filter(pk=sol.pk)
            .first()
        )
        return JsonResponse(
            {
                "ok": True,
                "mensagem": "Estoque transferido.",
                "solicitacao": serializar_solicitacao(sol, com_eventos=True),
                **resumo_loja(loja),
            }
        )
    ok_s, err_s = aplicar_status(
        sol,
        acao,
        loja_atual=loja,
        operador_label=label,
        usuario=user,
        motivo=str(payload.get("motivo") or payload.get("observacao") or ""),
    )
    if not ok_s:
        return JsonResponse({"ok": False, "erro": err_s}, status=400)
    sol = (
        SolicitacaoTransferenciaPdv.objects.prefetch_related("itens", "eventos")
        .filter(pk=sol.pk)
        .first()
    )
    return JsonResponse(
        {
            "ok": True,
            "mensagem": sol.get_status_display(),
            "solicitacao": serializar_solicitacao(sol, com_eventos=True),
            **resumo_loja(loja),
        }
    )
