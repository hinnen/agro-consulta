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
    mp_point_cancel_order,
    mp_point_classe_forma_caixa,
    mp_point_create_order,
    mp_point_extrair_tipo_pagamento,
    mp_point_forma_pdv_de_tipo_mp,
    mp_point_get_order,
    mp_point_mensagem_erro,
    mp_point_order_indica_cancelado,
    mp_point_order_indica_falha,
    mp_point_order_indica_pago,
)
from .caixa_util import (
    SessaoCaixaObrigatoriaError,
    exigir_sessao_caixa_para_venda,
    navegador_pode_mp_point_automatico,
    normalizar_forma_pagamento_caixa,
    pagamento_linha_eh_mercado_pago,
)
from .models import PdvMercadoPagoPointOrder, VendaAgro
from .views import (
    _disparar_envio_erp_venda_background,
    _fluxo_enviar_pedido_erp_interno,
    _json_legivel,
    _pdv_pedido_linhas_e_valor_final,
    _persistir_venda_agro,
    obter_conexao_mongo_pdv,
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
        "desconto_geral",
        "frete",
        "fiado_cobranca",
        "valor_total",
        "modo",
        "titulo_id",
        "titulo_ids",
        "cliente_agro_pk",
        "client_request_id",
        "observacao",
    }
)


def _pdv_decimal_campo(val) -> Decimal:
    if val is None:
        return Decimal("0")
    s = str(val).strip()
    if not s:
        return Decimal("0")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def _pdv_valor_cobranca_tranche_override(raw: dict) -> Decimal | None:
    """Valor parcial enviado ao Point na tranche (Enter no PDV), não o total da venda."""
    if not isinstance(raw, dict):
        return None
    v = raw.get("valor_cobranca_tranche")
    if v is None:
        return None
    dec = _pdv_decimal_campo(v).quantize(Decimal("0.01"))
    if dec <= 0:
        return None
    return dec


def _pdv_valor_cobranca_pdv(data: dict, valor_linhas: float) -> Decimal:
    """Total PDV (itens − desconto geral + frete), alinhado ao computed do wizard."""
    total = Decimal(str(valor_linhas))
    total -= max(Decimal("0"), _pdv_decimal_campo(data.get("desconto_geral")))
    total += max(Decimal("0"), _pdv_decimal_campo(data.get("frete")))
    total = max(Decimal("0"), total).quantize(Decimal("0.01"))
    return total


def _mp_point_forma_pagamento_texto(data: dict) -> str:
    pag = data.get("pagamentos")
    if isinstance(pag, list) and pag and isinstance(pag[0], dict):
        for key in ("forma", "formaPagamento", "forma_pagamento"):
            v = str(pag[0].get(key) or "").strip()
            if v:
                return v
    return str(data.get("forma_pagamento") or data.get("formaPagamento") or "").strip()


def _mp_point_parcelas_pdv(data: dict) -> int | None:
    pag = data.get("pagamentos")
    if isinstance(pag, list):
        for row in pag:
            if not isinstance(row, dict):
                continue
            for key in ("creditoParcelas", "credito_parcelas", "parcelas"):
                v = row.get(key)
                if v is not None:
                    try:
                        n = int(v)
                        if n >= 2:
                            return n
                    except (TypeError, ValueError):
                        pass
            label = str(
                row.get("formaPagamento") or row.get("forma_pagamento") or row.get("forma") or ""
            ).lower()
            if "parcelado" in label or "crédito" in label or "credito" in label:
                import re

                m = re.search(r"(\d+)\s*x", label)
                if m:
                    try:
                        n = int(m.group(1))
                        if n >= 2:
                            return n
                    except (TypeError, ValueError):
                        pass
    fp = _mp_point_forma_pagamento_texto(data).lower()
    if "parcelado" not in fp and "crédito" not in fp and "credito" not in fp:
        return None
    import re

    m = re.search(r"(\d+)\s*x", fp)
    if m:
        try:
            n = int(m.group(1))
            return n if n >= 2 else None
        except (TypeError, ValueError):
            pass
    if "parcelado" in fp:
        return 2
    return None


def _mp_point_payment_method_config(data: dict) -> dict | None:
    """Mapeia forma do PDV para default_type da API Point (quando possível)."""
    fp = _mp_point_forma_pagamento_texto(data).lower()
    if not fp:
        return None
    if "pix" in fp:
        return {"default_type": "qr"}
    if "débito" in fp or "debito" in fp:
        return {"default_type": "debit_card"}
    if "parcelado" in fp:
        cfg: dict = {"default_type": "credit_card"}
        n = _mp_point_parcelas_pdv(data) or 2
        cfg["default_installments"] = int(n)
        cfg["installments_cost"] = "seller"
        return cfg
    if "crédito" in fp or "credito" in fp:
        # Crédito à vista: 1 parcela evita tela «à vista / parcelado» na maquininha.
        return {"default_type": "credit_card", "default_installments": 1}
    return None


def _mp_point_marcar_metadados_pagamento(erp_data: dict) -> None:
    """Grava maquinaId/cobrarNoPointMp para split MP no fechar caixa."""
    pag = erp_data.get("pagamentos")
    if not isinstance(pag, list):
        return
    for i, row in enumerate(pag):
        if not isinstance(row, dict):
            continue
        if pagamento_linha_eh_mercado_pago(row):
            continue
        fn = normalizar_forma_pagamento_caixa(
            str(row.get("formaPagamento") or row.get("forma_pagamento") or row.get("forma") or "")
        )
        if fn not in ("PIX", "Cartão de débito", "Cartão de crédito", "Cartão de crédito parcelado"):
            continue
        r = dict(row)
        mid = str(r.get("maquinaId") or r.get("maquina_id") or "").strip()
        if not mid:
            r["maquinaId"] = "pix_mp_qr" if fn == "PIX" else "mp_balcao"
        r["cobrarNoPointMp"] = True
        r["mpBalcaoModo"] = "point"
        pag[i] = r
    erp_data["pagamentos"] = pag


def _mp_point_reconciliar_forma_venda(erp_data: dict, mp_body: dict) -> dict:
    """
    Ajusta forma gravada na venda conforme o MP confirmou.
    Retorna metadados {forma_pdv, forma_mp, divergiu, aviso}.
    """
    forma_pdv = normalizar_forma_pagamento_caixa(_mp_point_forma_pagamento_texto(erp_data))
    tipo_mp, inst_mp = mp_point_extrair_tipo_pagamento(mp_body)
    forma_mp = mp_point_forma_pdv_de_tipo_mp(tipo_mp, inst_mp)
    if not forma_mp:
        forma_mp = forma_pdv
    else:
        forma_mp = normalizar_forma_pagamento_caixa(forma_mp)
        n_pdv = _mp_point_parcelas_pdv(erp_data)
        if forma_mp == "Cartão de crédito" and n_pdv and n_pdv >= 2:
            forma_mp = "Cartão de crédito parcelado"

    divergiu = bool(
        forma_pdv
        and forma_mp
        and mp_point_classe_forma_caixa(forma_pdv) != mp_point_classe_forma_caixa(forma_mp)
    )
    fp_low = forma_pdv.lower()
    credito_pdv = "crédito" in fp_low or "credito" in fp_low
    n_pdv_ct = _mp_point_parcelas_pdv(erp_data) or 1
    try:
        n_mp_ct = int(inst_mp) if inst_mp is not None else 1
    except (TypeError, ValueError):
        n_mp_ct = 1
    parcelas_divergiu = bool(credito_pdv and n_pdv_ct >= 2 and n_mp_ct >= 1 and n_pdv_ct != n_mp_ct)
    aviso = ""
    if divergiu:
        aviso = (
            f"Atenção: no PDV estava «{forma_pdv}», mas a maquininha confirmou «{forma_mp}». "
            "A venda foi gravada conforme a maquininha (fechamento de caixa)."
        )
    elif parcelas_divergiu:
        aviso = (
            f"Atenção: no PDV estava {n_pdv_ct}x, mas a maquininha confirmou {n_mp_ct}x. "
            "A venda foi gravada conforme a maquininha."
        )

    if divergiu or parcelas_divergiu:
        label = forma_mp
        if n_mp_ct > 1 or (parcelas_divergiu and n_mp_ct >= 1):
            if n_mp_ct >= 2:
                label = f"Cartão de crédito parcelado {n_mp_ct}x"
                forma_mp = "Cartão de crédito parcelado"
            elif forma_mp == "Cartão de crédito parcelado" and n_mp_ct == 1:
                label = "Cartão de crédito"
                forma_mp = "Cartão de crédito"
        pag = erp_data.get("pagamentos")
        if isinstance(pag, list) and pag and isinstance(pag[0], dict):
            row = dict(pag[0])
            if n_mp_ct >= 2:
                row["creditoParcelas"] = n_mp_ct
            elif "creditoParcelas" in row:
                row["creditoParcelas"] = None
            row["formaPagamento"] = label[:200]
            row["forma_pagamento"] = label[:200]
            pag[0] = row
            erp_data["pagamentos"] = pag
        erp_data["forma_pagamento"] = label[:80]
        erp_data["formaPagamento"] = erp_data["forma_pagamento"]

    _mp_point_marcar_metadados_pagamento(erp_data)

    return {
        "forma_pdv": forma_pdv,
        "forma_mp": forma_mp,
        "divergiu": divergiu or parcelas_divergiu,
        "parcelas_divergiu": parcelas_divergiu,
        "aviso": aviso,
    }


def _mp_point_anexar_recon_payload(payload: dict, recon: dict) -> dict:
    if recon.get("forma_mp"):
        payload["mp_point_forma_confirmada"] = recon["forma_mp"]
    if recon.get("divergiu"):
        payload["mp_point_forma_divergencia"] = True
        payload["mp_point_aviso"] = recon.get("aviso") or ""
    return payload


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


def _resposta_mp_point_so_gaveta():
    return JsonResponse(
        {
            "ok": False,
            "erro": (
                "Mercado Pago automático só no Caixa Gaveta (computador principal). "
                "Use Cielo, Sicredi ou Sicoob neste PDV. "
                "Se a gaveta já estava aberta antes da atualização, feche e abra de novo neste PC."
            ),
        },
        status=403,
    )


@require_POST
def api_pdv_mp_point_criar(request):
    try:
        return _api_pdv_mp_point_criar_impl(request)
    except Exception:
        logger.exception("MP Point criar: erro interno")
        return JsonResponse(
            {"ok": False, "erro": "Erro interno ao enviar cobrança ao Mercado Pago. Tente de novo."},
            status=500,
        )


def _api_pdv_mp_point_criar_impl(request):
    if not _mp_point_configurado():
        return JsonResponse(
            {"ok": False, "erro": "Integração Mercado Pago Point desativada ou incompleta (.env)."},
            status=503,
        )
    if not navegador_pode_mp_point_automatico(request):
        return _resposta_mp_point_so_gaveta()
    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    if not isinstance(raw, dict):
        return JsonResponse({"ok": False, "erro": "Payload inválido"}, status=400)

    erp_payload = _sanear_erp_payload(raw)
    fiado_cobranca = bool(erp_payload.get("fiado_cobranca") or raw.get("fiado_cobranca"))

    if fiado_cobranca:
        valor_tranche = _pdv_valor_cobranca_tranche_override(raw)
        if valor_tranche is not None:
            valor_cobrar = valor_tranche
        else:
            valor_cobrar = _pdv_decimal_campo(erp_payload.get("valor_total") or raw.get("valor_total"))
        if valor_cobrar <= 0:
            return JsonResponse({"ok": False, "erro": "Valor da quitação fiado inválido."}, status=400)
    else:
        if not erp_payload.get("itens"):
            return JsonResponse({"ok": False, "erro": "Informe os itens da venda."}, status=400)

        client_m, db = obter_conexao_mongo_pdv()
        err_resp, _linhas, valor_final = _pdv_pedido_linhas_e_valor_final(erp_payload, client_m=client_m, db=db)
        if err_resp is not None:
            try:
                payload = json.loads(err_resp.content.decode("utf-8"))
            except Exception:
                payload = {"ok": False, "erro": "Itens inválidos para o ERP."}
            return JsonResponse(payload, status=err_resp.status_code)

        valor_tranche = _pdv_valor_cobranca_tranche_override(raw)
        if valor_tranche is not None:
            valor_cobrar = valor_tranche
        else:
            valor_cobrar = _pdv_valor_cobranca_pdv(erp_payload, float(valor_final))

    external_reference = f"agro-{uuid.uuid4()}"
    token = settings.MP_POINT_ACCESS_TOKEN.strip()
    terminal_id = settings.MP_POINT_TERMINAL_ID.strip()
    exp = (getattr(settings, "MP_POINT_EXPIRATION", None) or "PT16M").strip()
    pm_cfg = _mp_point_payment_method_config(erp_payload)

    ok_mp, st, body = mp_point_create_order(
        access_token=token,
        terminal_id=terminal_id,
        amount=float(valor_cobrar),
        external_reference=external_reference,
        expiration_time=exp,
        description=(str(erp_payload.get("cliente") or "") or None),
        payment_method_config=pm_cfg,
    )
    if not ok_mp:
        msg = mp_point_mensagem_erro(body)
        if st == 409:
            low = msg.lower()
            if "queued" in low or "terminal" in low:
                msg = (
                    "A maquininha já tem uma cobrança em andamento. "
                    "Cancele na maquininha ou aguarde o cliente terminar."
                )
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

    dec_valor = valor_cobrar
    PdvMercadoPagoPointOrder.objects.create(
        external_reference=external_reference,
        mp_order_id=order_id,
        valor_cobrado=dec_valor,
        erp_payload=erp_payload,
        django_session_key=_sessao_key(request),
        status=PdvMercadoPagoPointOrder.Status.PENDING,
        mp_last_status=str(body.get("status") or "")[:48],
    )

    parcelas_env = _mp_point_parcelas_pdv(erp_payload)
    return JsonResponse(
        {
            "ok": True,
            "order_id": order_id,
            "external_reference": external_reference,
            "amount": float(dec_valor),
            "parcelas_enviadas": parcelas_env,
            "modo_tranche": valor_tranche is not None,
        }
    )


def _mp_point_pagamento_valor_mp(erp_data: dict, valor_esperado: Decimal) -> bool:
    """Confere se algum pagamento MP no payload bate com o valor cobrado na tranche."""
    pag = erp_data.get("pagamentos")
    if not isinstance(pag, list):
        return False
    alvo = valor_esperado.quantize(Decimal("0.01"))
    for row in pag:
        if not isinstance(row, dict):
            continue
        if not pagamento_linha_eh_mercado_pago(row):
            continue
        v = _pdv_decimal_campo(row.get("valorPagamento") or row.get("valor") or row.get("valor_pagamento"))
        if abs(v - alvo) <= Decimal("0.02"):
            return True
    return False


@require_POST
def api_pdv_mp_point_confirmar_tranche(request):
    """Pagamento confirmado no terminal; marca pedido PAID sem gravar venda ainda."""
    if not _mp_point_configurado():
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request):
        return _resposta_mp_point_so_gaveta()

    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    order_id = str(raw.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

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

        if row.status == PdvMercadoPagoPointOrder.Status.FINALIZED:
            return JsonResponse({"ok": True, "ja_finalizado": True, "venda_id": row.venda_id})
        if row.status == PdvMercadoPagoPointOrder.Status.PAID:
            return JsonResponse({"ok": True, "ja_pago": True, "order_id": order_id})
        if row.status != PdvMercadoPagoPointOrder.Status.PENDING:
            return JsonResponse({"ok": False, "erro": "Pedido não está aguardando pagamento."}, status=409)

        erp_data = dict(row.erp_payload) if isinstance(row.erp_payload, dict) else {}
        if not erp_data:
            return JsonResponse({"ok": False, "erro": "Payload local inválido."}, status=500)

        recon = _mp_point_reconciliar_forma_venda(erp_data, body)
        row.erp_payload = erp_data
        row.status = PdvMercadoPagoPointOrder.Status.PAID
        row.mp_last_status = str(body.get("status") or "")[:48]
        row.save(update_fields=["erp_payload", "status", "mp_last_status", "atualizado_em"])

    payload = {"ok": True, "order_id": order_id, "amount": float(row.valor_cobrado)}
    _mp_point_anexar_recon_payload(payload, recon)
    return JsonResponse(payload)


@require_GET
def api_pdv_mp_point_status(request):
    if not _mp_point_configurado():
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request):
        return _resposta_mp_point_so_gaveta()

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
                "paid_tranche": False,
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

    canceled = mp_point_order_indica_cancelado(body)
    failed, failed_msg = mp_point_order_indica_falha(body)
    if (canceled or failed) and row.status == PdvMercadoPagoPointOrder.Status.PENDING:
        row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
        row.save(update_fields=["status", "atualizado_em"])

    paid_tranche = row.status == PdvMercadoPagoPointOrder.Status.PAID
    mp_paid = mp_point_order_indica_pago(body) or paid_tranche

    return JsonResponse(
        {
            "ok": True,
            "mp_status": mp_status,
            "paid": mp_paid,
            "paid_tranche": paid_tranche,
            "canceled": canceled,
            "failed": failed,
            "failed_msg": failed_msg[:300] if failed_msg else "",
            "finalized": row.status == PdvMercadoPagoPointOrder.Status.FINALIZED,
            "venda_id": row.venda_id,
        }
    )


@require_POST
def api_pdv_mp_point_finalizar(request):
    if not _mp_point_configurado():
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request):
        return _resposta_mp_point_so_gaveta()

    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    order_id = str(raw.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    erp_override = raw.get("erp_payload") or raw.get("erp")
    if not isinstance(erp_override, dict):
        erp_override = None

    client_m, db = obter_conexao_mongo_pdv()
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

        modo_tranche = row.status == PdvMercadoPagoPointOrder.Status.PAID

        if erp_override and erp_override.get("itens"):
            erp_data = _sanear_erp_payload(erp_override)
        else:
            erp_data = dict(row.erp_payload) if isinstance(row.erp_payload, dict) else {}
        if not erp_data:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            return JsonResponse({"ok": False, "erro": "Payload local inválido."}, status=500)

        fiado_cobranca = bool(erp_data.get("fiado_cobranca"))

        if fiado_cobranca:
            try:
                exigir_sessao_caixa_para_venda(request, erp_data)
            except SessaoCaixaObrigatoriaError as e:
                row.status = PdvMercadoPagoPointOrder.Status.FAILED
                row.save(update_fields=["status", "atualizado_em"])
                return JsonResponse({"ok": False, "erro": str(e)}, status=400)
            from produtos.fiado_gestao_util import baixar_fiado_via_pdv

            titulo_ids = erp_data.get("titulo_ids")
            if not isinstance(titulo_ids, list):
                titulo_ids = []
            try:
                titulo_id_raw = erp_data.get("titulo_id")
                titulo_id = int(titulo_id_raw) if titulo_id_raw is not None else None
            except (TypeError, ValueError):
                titulo_id = None
            try:
                cliente_pk_raw = erp_data.get("cliente_agro_pk")
                cliente_pk = int(cliente_pk_raw) if cliente_pk_raw is not None else None
            except (TypeError, ValueError):
                cliente_pk = None
            from produtos.caixa_util import parse_valor_moeda_br

            # Garante metadados MP no payload (mesmo caminho da venda) antes da baixa.
            if not modo_tranche:
                try:
                    _mp_point_reconciliar_forma_venda(erp_data, body)
                    row.erp_payload = erp_data
                    row.save(update_fields=["erp_payload", "atualizado_em"])
                except Exception:
                    logger.warning("mp point fiado: recon forma", exc_info=True)

            valor_baixa = None
            if erp_data.get("valor") is not None:
                valor_baixa = parse_valor_moeda_br(erp_data.get("valor"))
            elif erp_data.get("valor_total") is not None:
                valor_baixa = parse_valor_moeda_br(erp_data.get("valor_total"))
            try:
                resultado = baixar_fiado_via_pdv(
                    modo=str(erp_data.get("modo") or "titulo"),
                    titulo_id=titulo_id,
                    titulo_ids=titulo_ids or None,
                    cliente_agro_pk=cliente_pk,
                    cliente_nome=str(erp_data.get("cliente") or "").strip(),
                    cliente_codigo="",
                    valor=valor_baixa,
                    pagamentos=erp_data.get("pagamentos") or [],
                    request=request,
                    observacao=str(erp_data.get("observacao") or "").strip(),
                    client_request_id=str(erp_data.get("client_request_id") or "").strip(),
                    usuario=(request.user.get_username() if getattr(request, "user", None) and request.user.is_authenticated else ""),
                )
            except ValueError as exc:
                row.status = PdvMercadoPagoPointOrder.Status.FAILED
                row.save(update_fields=["status", "atualizado_em"])
                return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
            row.status = PdvMercadoPagoPointOrder.Status.FINALIZED
            row.save(update_fields=["status", "atualizado_em"])
            return JsonResponse({"ok": True, "fiado_baixa": True, **resultado})

        if modo_tranche:
            if not _mp_point_pagamento_valor_mp(erp_data, row.valor_cobrado):
                return JsonResponse(
                    {
                        "ok": False,
                        "erro": (
                            "Pagamento MP na venda não bate com o valor cobrado na maquininha "
                            f"({row.valor_cobrado}). Ajuste os lançamentos ou fale com o gerente."
                        ),
                    },
                    status=409,
                )
            recon = {
                "forma_pdv": "",
                "forma_mp": "",
                "divergiu": False,
                "parcelas_divergiu": False,
                "aviso": "",
            }
        else:
            recon = _mp_point_reconciliar_forma_venda(erp_data, body)

        err_early, _ln, vf = _pdv_pedido_linhas_e_valor_final(erp_data, client_m=client_m, db=db)
        if err_early is not None:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            try:
                pe = json.loads(err_early.content.decode("utf-8"))
            except Exception:
                pe = {"erro": "Itens inválidos"}
            return JsonResponse({"ok": False, **pe}, status=err_early.status_code)

        if not modo_tranche:
            vf_cobranca = _pdv_valor_cobranca_pdv(erp_data, float(vf))
            if vf_cobranca != row.valor_cobrado:
                logger.error(
                    "MP Point finalizar: valor ERP %s difere do cobrado %s (order %s)",
                    vf,
                    row.valor_cobrado,
                    order_id,
                )
                return JsonResponse(
                    {
                        "ok": False,
                        "erro": "Valor do pedido mudou em relação à cobrança; cancele no MP e gere de novo.",
                    },
                    status=409,
                )

        try:
            exigir_sessao_caixa_para_venda(request, erp_data)
        except SessaoCaixaObrigatoriaError as e:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            return JsonResponse({"ok": False, "erro": str(e)}, status=400)

        raw_itens = erp_data.get("itens", [])
        if not isinstance(raw_itens, list):
            raw_itens = []

        from produtos.views_nfce import anexar_nfce_resposta_venda

        if getattr(settings, "PDV_ERP_ENVIO_ASSINCRONO", True):
            try:
                venda_local = _persistir_venda_agro(
                    request,
                    erp_data,
                    raw_itens,
                    None,
                    None,
                    False,
                    erp_sync_status=VendaAgro.ErpSyncStatus.PENDENTE,
                )
            except SessaoCaixaObrigatoriaError as e:
                row.status = PdvMercadoPagoPointOrder.Status.FAILED
                row.save(update_fields=["status", "atualizado_em"])
                return JsonResponse({"ok": False, "erro": str(e)}, status=400)
            vid = venda_local.pk if venda_local else None
            if vid:
                _disparar_envio_erp_venda_background(vid, erp_data)
            row.status = PdvMercadoPagoPointOrder.Status.FINALIZED
            row.venda_id = vid
            row.erp_payload = erp_data
            row.save(update_fields=["status", "venda", "erp_payload", "atualizado_em"])
            payload = {
                "ok": True,
                "mensagem": "Venda registrada. Envio ao ERP em segundo plano.",
                "venda_id": vid,
                "erp_pendente": True,
            }
            _mp_point_anexar_recon_payload(payload, recon)
            anexar_nfce_resposta_venda(venda_local, erp_data, payload)
            return JsonResponse(payload)

        err, out = _fluxo_enviar_pedido_erp_interno(request, erp_data, client_m=client_m, db=db)
        if err is not None:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            try:
                pe = json.loads(err.content.decode("utf-8"))
            except Exception:
                pe = {"erro": str(err)}
            return JsonResponse({"ok": False, **pe}, status=err.status_code)

        try:
            venda_local = _persistir_venda_agro(
                request,
                erp_data,
                out["raw_itens"],
                out["status"],
                out["res"],
                out["sucesso_erp"],
                erp_sync_status=out["erp_sync"],
            )
        except SessaoCaixaObrigatoriaError as e:
            row.status = PdvMercadoPagoPointOrder.Status.FAILED
            row.save(update_fields=["status", "atualizado_em"])
            return JsonResponse({"ok": False, "erro": str(e)}, status=400)
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
            row.erp_payload = erp_data
            row.save(update_fields=["status", "venda", "erp_payload", "atualizado_em"])
            payload = {
                "ok": True,
                "mensagem": _json_legivel(out["res"]),
                "venda_id": vid,
            }
            from produtos.views_nfce import anexar_nfce_resposta_venda

            _mp_point_anexar_recon_payload(payload, recon)
            anexar_nfce_resposta_venda(venda_local, erp_data, payload)
            return JsonResponse(payload)

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
    if not navegador_pode_mp_point_automatico(request):
        return _resposta_mp_point_so_gaveta()

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
    if row.status == PdvMercadoPagoPointOrder.Status.PAID:
        return JsonResponse(
            {
                "ok": False,
                "erro": "Pagamento já confirmado na maquininha. Finalize a venda ou fale com o gerente.",
            },
            status=409,
        )
    if row.status == PdvMercadoPagoPointOrder.Status.ABANDONED:
        return JsonResponse({"ok": True, "ja_abandonado": True})
    if row.status != PdvMercadoPagoPointOrder.Status.PENDING:
        return JsonResponse({"ok": False, "erro": "Não é possível cancelar este pedido."}, status=409)

    aviso_terminal = ""
    token = settings.MP_POINT_ACCESS_TOKEN.strip()
    ok_mp, st_cancel, body_cancel = mp_point_cancel_order(access_token=token, order_id=order_id)
    if not ok_mp:
        low = mp_point_mensagem_erro(body_cancel).lower()
        if st_cancel == 409 or "terminal" in low or "at_terminal" in low:
            aviso_terminal = (
                "Não foi possível cancelar na maquininha — cancele também no terminal."
            )
        elif st_cancel not in (0, 404):
            logger.warning("MP Point abandonar: cancel API HTTP %s — %s", st_cancel, body_cancel)

    row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
    row.save(update_fields=["status", "atualizado_em"])
    payload = {"ok": True, "cancelou_maquininha": bool(ok_mp)}
    if aviso_terminal:
        payload["aviso"] = aviso_terminal
    return JsonResponse(payload)
