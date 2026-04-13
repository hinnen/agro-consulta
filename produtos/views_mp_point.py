"""
APIs PDV — Mercado Pago Point: criar cobrança no terminal e finalizar venda após pagamento.
"""

from __future__ import annotations

import json
import logging
import uuid
from decimal import Decimal

from django.conf import settings
from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from .mercado_pago_point import (
    mp_point_create_order,
    mp_point_get_order,
    mp_point_mensagem_erro,
    mp_point_order_indica_pago,
)
from .models import PdvMercadoPagoPointOrder
from .views import (
    _fluxo_enviar_pedido_erp_interno,
    _json_legivel,
    _pdv_pedido_linhas_e_valor_final,
    _persistir_venda_agro,
    obter_conexao_mongo,
)

logger = logging.getLogger(__name__)

_ERP_PAYLOAD_KEYS = frozenset(
    {
        "cliente",
        "itens",
        "forma_pagamento",
        "formaPagamento",
        "pagamentos",
        "cliente_id",
        "ClienteID",
        "cliente_documento",
        "CpfCnpj",
        "forma_pagamento_id",
        "formaPagamentoID",
        "formaPagamentoId",
    }
)


def _mp_point_configurado() -> bool:
    return bool(
        getattr(settings, "MP_POINT_ENABLED", False)
        and (getattr(settings, "MP_POINT_ACCESS_TOKEN", "") or "").strip()
        and (getattr(settings, "MP_POINT_TERMINAL_ID", "") or "").strip()
    )


def _sanear_erp_payload(data: dict) -> dict:
    return {k: data[k] for k in _ERP_PAYLOAD_KEYS if k in data}


def _sessao_key(request) -> str:
    return (getattr(request.session, "session_key", None) or "")[:50]


@require_POST
def api_pdv_mp_point_criar(request):
    if not _mp_point_configurado():
        return JsonResponse(
            {"ok": False, "erro": "Integração Mercado Pago Point desativada ou incompleta (.env)."},
            status=503,
        )
    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    if not isinstance(raw, dict):
        return JsonResponse({"ok": False, "erro": "Payload inválido"}, status=400)

    erp_payload = _sanear_erp_payload(raw)
    if not erp_payload.get("itens"):
        return JsonResponse({"ok": False, "erro": "Informe os itens da venda."}, status=400)

    client_m, db = obter_conexao_mongo()
    err_resp, _linhas, valor_final = _pdv_pedido_linhas_e_valor_final(erp_payload, client_m=client_m, db=db)
    if err_resp is not None:
        try:
            payload = json.loads(err_resp.content.decode("utf-8"))
        except Exception:
            payload = {"ok": False, "erro": "Itens inválidos para o ERP."}
        return JsonResponse(payload, status=err_resp.status_code)

    external_reference = f"agro-{uuid.uuid4()}"
    token = settings.MP_POINT_ACCESS_TOKEN.strip()
    terminal_id = settings.MP_POINT_TERMINAL_ID.strip()
    exp = (getattr(settings, "MP_POINT_EXPIRATION", None) or "PT16M").strip()

    ok_mp, st, body = mp_point_create_order(
        access_token=token,
        terminal_id=terminal_id,
        amount=float(valor_final),
        external_reference=external_reference,
        expiration_time=exp,
        description=(str(erp_payload.get("cliente") or "") or None),
    )
    if not ok_mp:
        msg = mp_point_mensagem_erro(body)
        logger.warning("MP Point criar: HTTP %s — %s", st, msg)
        return JsonResponse(
            {"ok": False, "erro": f"Mercado Pago: {msg}", "http_status": st},
            status=502,
        )

    if not isinstance(body, dict):
        return JsonResponse({"ok": False, "erro": "Resposta inesperada do Mercado Pago."}, status=502)

    order_id = str(body.get("id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "Mercado Pago não retornou o id do pedido."}, status=502)

    dec_valor = Decimal(str(valor_final)).quantize(Decimal("0.01"))
    PdvMercadoPagoPointOrder.objects.create(
        external_reference=external_reference,
        mp_order_id=order_id,
        valor_cobrado=dec_valor,
        erp_payload=erp_payload,
        django_session_key=_sessao_key(request),
        status=PdvMercadoPagoPointOrder.Status.PENDING,
        mp_last_status=str(body.get("status") or "")[:48],
    )

    return JsonResponse(
        {
            "ok": True,
            "order_id": order_id,
            "external_reference": external_reference,
            "amount": float(dec_valor),
        }
    )


@require_GET
def api_pdv_mp_point_status(request):
    if not _mp_point_configurado():
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)

    order_id = (request.GET.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    try:
        row = PdvMercadoPagoPointOrder.objects.get(mp_order_id=order_id)
    except PdvMercadoPagoPointOrder.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Pedido não encontrado."}, status=404)

    sk = _sessao_key(request)
    if row.django_session_key and sk and row.django_session_key != sk:
        return JsonResponse({"ok": False, "erro": "Sessão não confere com o pedido."}, status=403)

    if row.status == PdvMercadoPagoPointOrder.Status.ABANDONED:
        return JsonResponse(
            {
                "ok": True,
                "abandoned": True,
                "paid": False,
                "finalized": False,
                "mp_status": "abandoned",
                "venda_id": None,
            }
        )

    token = settings.MP_POINT_ACCESS_TOKEN.strip()
    ok_mp, st, body = mp_point_get_order(access_token=token, order_id=order_id)
    if not ok_mp or not isinstance(body, dict):
        msg = mp_point_mensagem_erro(body)
        return JsonResponse({"ok": False, "erro": msg, "http_status": st}, status=502)

    mp_status = str(body.get("status") or "")
    row.mp_last_status = mp_status[:48]
    row.save(update_fields=["mp_last_status", "atualizado_em"])

    return JsonResponse(
        {
            "ok": True,
            "mp_status": mp_status,
            "paid": mp_point_order_indica_pago(body),
            "finalized": row.status == PdvMercadoPagoPointOrder.Status.FINALIZED,
            "venda_id": row.venda_id,
        }
    )


@require_POST
def api_pdv_mp_point_finalizar(request):
    if not _mp_point_configurado():
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)

    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    order_id = str(raw.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    client_m, db = obter_conexao_mongo()
    token = settings.MP_POINT_ACCESS_TOKEN.strip()
    ok_mp, st, body = mp_point_get_order(access_token=token, order_id=order_id)
    if not ok_mp or not isinstance(body, dict):
        return JsonResponse(
            {"ok": False, "erro": mp_point_mensagem_erro(body), "http_status": st},
            status=502,
        )

    if not mp_point_order_indica_pago(body):
        return JsonResponse(
            {
                "ok": False,
                "erro": "Pagamento ainda não confirmado no terminal.",
                "mp_status": body.get("status"),
            },
            status=409,
        )

    with transaction.atomic():
        try:
            row = PdvMercadoPagoPointOrder.objects.select_for_update().get(mp_order_id=order_id)
        except PdvMercadoPagoPointOrder.DoesNotExist:
            return JsonResponse({"ok": False, "erro": "Pedido local não encontrado."}, status=404)

        sk = _sessao_key(request)
        if row.django_session_key and sk and row.django_session_key != sk:
            return JsonResponse({"ok": False, "erro": "Sessão não confere."}, status=403)

        if row.status == PdvMercadoPagoPointOrder.Status.FINALIZED and row.venda_id:
            return JsonResponse({"ok": True, "venda_id": row.venda_id, "ja_finalizado": True})

        if row.status == PdvMercadoPagoPointOrder.Status.ABANDONED:
            return JsonResponse(
                {"ok": False, "erro": "Pedido Point cancelado na tela do PDV."},
                status=409,
            )

        erp_data = row.erp_payload
        if not isinstance(erp_data, dict):
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            return JsonResponse({"ok": False, "erro": "Payload local inválido."}, status=500)

        err_early, _ln, vf = _pdv_pedido_linhas_e_valor_final(erp_data, client_m=client_m, db=db)
        if err_early is not None:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            try:
                pe = json.loads(err_early.content.decode("utf-8"))
            except Exception:
                pe = {"erro": "Itens inválidos"}
            return JsonResponse({"ok": False, **pe}, status=err_early.status_code)

        if Decimal(str(vf)).quantize(Decimal("0.01")) != row.valor_cobrado:
            logger.error(
                "MP Point finalizar: valor ERP %s difere do cobrado %s (order %s)",
                vf,
                row.valor_cobrado,
                order_id,
            )
            return JsonResponse(
                {"ok": False, "erro": "Valor do pedido mudou em relação à cobrança; cancele no MP e gere de novo."},
                status=409,
            )

        err, out = _fluxo_enviar_pedido_erp_interno(request, erp_data, client_m=client_m, db=db)
        if err is not None:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            try:
                pe = json.loads(err.content.decode("utf-8"))
            except Exception:
                pe = {"erro": str(err)}
            return JsonResponse({"ok": False, **pe}, status=err.status_code)

        venda_local = _persistir_venda_agro(
            request,
            erp_data,
            out["raw_itens"],
            out["status"],
            out["res"],
            out["sucesso_erp"],
            erp_sync_status=out["erp_sync"],
        )
        vid = venda_local.pk if venda_local else None
        msg_erro_ui = out["msg_erro_ui"]

        if out["ok"] and out["recusa_erp"]:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            return JsonResponse(
                {"ok": False, "erro": msg_erro_ui, "http_status": out["status"], "venda_id": vid},
                status=502,
            )
        if out["ok"]:
            row.status = PdvMercadoPagoPointOrder.Status.FINALIZED
            row.venda_id = vid
            row.save(update_fields=["status", "venda", "atualizado_em"])
            return JsonResponse(
                {
                    "ok": True,
                    "mensagem": _json_legivel(out["res"]),
                    "venda_id": vid,
                }
            )

        row.status = PdvMercadoPagoPointOrder.Status.FAILED
        row.save(update_fields=["status", "atualizado_em"])
        return JsonResponse(
            {
                "ok": False,
                "erro": msg_erro_ui or _json_legivel(out["res"]),
                "http_status": out["status"],
                "venda_id": vid,
            },
            status=502 if out["status"] and out["status"] != 0 else 500,
        )


@require_POST
def api_pdv_mp_point_abandon(request):
    """Operador desistiu da espera no Point; não finaliza venda e libera outra forma no PDV."""
    if not _mp_point_configurado():
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)

    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    order_id = str(raw.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    try:
        row = PdvMercadoPagoPointOrder.objects.get(mp_order_id=order_id)
    except PdvMercadoPagoPointOrder.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Pedido não encontrado."}, status=404)

    sk = _sessao_key(request)
    if row.django_session_key and sk and row.django_session_key != sk:
        return JsonResponse({"ok": False, "erro": "Sessão não confere."}, status=403)

    if row.status == PdvMercadoPagoPointOrder.Status.FINALIZED:
        return JsonResponse({"ok": False, "erro": "Esta cobrança já virou venda finalizada."}, status=409)
    if row.status == PdvMercadoPagoPointOrder.Status.ABANDONED:
        return JsonResponse({"ok": True, "ja_abandonado": True})
    if row.status != PdvMercadoPagoPointOrder.Status.PENDING:
        return JsonResponse({"ok": False, "erro": "Não é possível cancelar este pedido."}, status=409)

    row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
    row.save(update_fields=["status", "atualizado_em"])
    return JsonResponse({"ok": True})
