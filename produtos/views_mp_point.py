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
    MP_POINT_CONTA_CENTRO,
    MP_POINT_CONTA_VILA,
    mp_point_cancel_order,
    mp_point_classe_forma_caixa,
    mp_point_conta_configurada,
    mp_point_conta_de_maquina,
    mp_point_create_order,
    mp_point_credenciais,
    mp_point_extrair_tipo_pagamento,
    mp_point_forma_pdv_de_tipo_mp,
    mp_point_get_order,
    mp_point_mensagem_erro,
    mp_point_order_indica_cancelado,
    mp_point_order_indica_falha,
    mp_point_order_indica_pago,
    normalizar_mp_point_conta,
)
from .caixa_util import (
    SessaoCaixaObrigatoriaError,
    exigir_sessao_caixa_para_venda,
    mp_point_host_conta,
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
        "mp_point_conta",
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


def _mp_point_marcar_metadados_pagamento(erp_data: dict, conta: str | None = None) -> None:
    """Grava maquinaId/cobrarNoPointMp para split MP no fechar caixa."""
    pag = erp_data.get("pagamentos")
    if not isinstance(pag, list):
        return
    c = normalizar_mp_point_conta(conta or erp_data.get("mp_point_conta"))
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
            if c == MP_POINT_CONTA_VILA:
                r["maquinaId"] = "pix_mp_vila" if fn == "PIX" else "mp_vila"
            else:
                r["maquinaId"] = "pix_mp_qr" if fn == "PIX" else "mp_balcao"
        r["cobrarNoPointMp"] = True
        r["mpBalcaoModo"] = "point"
        pag[i] = r
    erp_data["pagamentos"] = pag


def _mp_point_promover_pago_local(row: PdvMercadoPagoPointOrder, body: dict) -> bool:
    """
    Se o MP indica pagamento OK, grava status PAID no Postgres.
    Também recupera ABANDONED precoce (cancel local sem cobrança cancelada no terminal).
    Retorna True se o pedido está (ou passou a estar) pago / já finalizado.
    """
    if row.status == PdvMercadoPagoPointOrder.Status.FINALIZED:
        return True
    if row.status == PdvMercadoPagoPointOrder.Status.PAID:
        return True
    if not mp_point_order_indica_pago(body):
        return False
    if row.status not in (
        PdvMercadoPagoPointOrder.Status.PENDING,
        PdvMercadoPagoPointOrder.Status.ABANDONED,
    ):
        return False
    erp_data = dict(row.erp_payload) if isinstance(row.erp_payload, dict) else {}
    if erp_data:
        _mp_point_reconciliar_forma_venda(erp_data, body)
        row.erp_payload = erp_data
    row.status = PdvMercadoPagoPointOrder.Status.PAID
    row.mp_last_status = str(body.get("status") or "")[:48]
    fields = ["status", "mp_last_status", "atualizado_em"]
    if erp_data:
        fields.insert(0, "erp_payload")
    row.save(update_fields=fields)
    return True


def mp_point_bloqueio_venda_sessao(request) -> str | None:
    """
    Bloqueia fechar venda por outra forma se a sessão ainda tem Point PENDING/PAID
    (buraco que permitiu mp_renan com cobrança Point viva — incidente 19/08 R$460).
    Centro e Vila usam o mesmo critério (conta só muda o token).
    Bypass: PIN gerencial (Geraldo / Geraldinho / Renan Hinnen) via forçar liberar.
    """
    from datetime import timedelta

    from django.utils import timezone

    from produtos.pin_gerencial_util import mp_point_forcar_bypass_ativo

    if mp_point_forcar_bypass_ativo(request):
        return None

    sk = _sessao_key(request)
    if not sk:
        return None
    cutoff = timezone.now() - timedelta(hours=2)
    row = (
        PdvMercadoPagoPointOrder.objects.filter(
            django_session_key=sk,
            status__in=(
                PdvMercadoPagoPointOrder.Status.PENDING,
                PdvMercadoPagoPointOrder.Status.PAID,
            ),
            criado_em__gte=cutoff,
        )
        .order_by("-criado_em")
        .first()
    )
    if not row:
        return None
    valor = row.valor_cobrado
    if row.status == PdvMercadoPagoPointOrder.Status.PAID:
        return (
            f"Há pagamento na maquininha Mercado Pago já confirmado nesta sessão (R$ {valor}). "
            "Finalize essa venda pelo Point automático — não use outra máquina. "
            "Em emergência, peça PIN gerencial (Geraldo, Geraldinho ou Renan Hinnen) para forçar."
        )
    return (
        f"Há cobrança aberta na maquininha Mercado Pago nesta sessão (R$ {valor}). "
        "Cancele no PDV e na maquininha (ou aguarde o fim) antes de fechar com outra forma. "
        "Em emergência, peça PIN gerencial (Geraldo, Geraldinho ou Renan Hinnen) para forçar."
    )


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

    _mp_point_marcar_metadados_pagamento(erp_data, conta=erp_data.get("mp_point_conta"))

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


def _mp_point_configurado(conta: str | None = None) -> bool:
    if conta:
        return mp_point_conta_configurada(conta)
    return mp_point_conta_configurada(MP_POINT_CONTA_CENTRO) or mp_point_conta_configurada(
        MP_POINT_CONTA_VILA
    )


def _mp_point_ids_do_payload(data: dict | None) -> list[str]:
    if not isinstance(data, dict):
        return []
    out: list[str] = []
    pag = data.get("pagamentos")
    if isinstance(pag, list):
        for row in pag:
            if not isinstance(row, dict):
                continue
            mid = str(row.get("maquinaId") or row.get("maquina_id") or "").strip()
            if mid:
                out.append(mid)
    mid_top = str(data.get("maquinaId") or data.get("maquina_id") or "").strip()
    if mid_top:
        out.append(mid_top)
    return out


def _resolver_mp_point_conta(request, *payloads: dict | None) -> str:
    for data in payloads:
        if not isinstance(data, dict):
            continue
        raw_c = str(data.get("mp_point_conta") or "").strip()
        if raw_c:
            return normalizar_mp_point_conta(raw_c)
        for mid in _mp_point_ids_do_payload(data):
            c = mp_point_conta_de_maquina(mid)
            if c:
                return c
    host = mp_point_host_conta(request)
    if host == MP_POINT_CONTA_VILA:
        return MP_POINT_CONTA_VILA
    return MP_POINT_CONTA_CENTRO


def _token_da_conta(conta: str) -> str:
    token, _terminal = mp_point_credenciais(conta)
    return token


def _conta_do_pedido_local(request, row, extra: dict | None = None) -> str:
    """Conta Point = a gravada no pedido local (não confiar no JSON do browser)."""
    erp = row.erp_payload if isinstance(getattr(row, "erp_payload", None), dict) else {}
    return _resolver_mp_point_conta(request, erp)


def _resposta_mp_point_so_gaveta(conta: str | None = None):
    c = normalizar_mp_point_conta(conta) if conta else ""
    if c == MP_POINT_CONTA_VILA:
        msg = (
            "Mercado Pago automático da Vila só no computador do Caixa Vila Elias. "
            "Neste PDV use Sicredi. "
            "Se o caixa Vila já estava aberto, feche e abra de novo neste PC."
        )
    else:
        msg = (
            "Mercado Pago automático só no Caixa Gaveta (computador principal). "
            "Use Cielo, Sicredi ou Sicoob neste PDV. "
            "Se a gaveta já estava aberta antes da atualização, feche e abra de novo neste PC."
        )
    return JsonResponse({"ok": False, "erro": msg}, status=403)


def _sanear_erp_payload(data: dict) -> dict:
    return {k: data[k] for k in _ERP_PAYLOAD_KEYS if k in data}


def _sessao_key(request) -> str:
    return (getattr(request.session, "session_key", None) or "")[:50]


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
    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    if not isinstance(raw, dict):
        return JsonResponse({"ok": False, "erro": "Payload inválido"}, status=400)

    erp_payload = _sanear_erp_payload(raw)
    conta = _resolver_mp_point_conta(request, raw, erp_payload)
    if not _mp_point_configurado(conta):
        loja = "Vila" if conta == MP_POINT_CONTA_VILA else "Centro"
        return JsonResponse(
            {
                "ok": False,
                "erro": f"Integração Mercado Pago Point ({loja}) desativada ou incompleta (.env).",
            },
            status=503,
        )
    if not navegador_pode_mp_point_automatico(request, conta=conta):
        return _resposta_mp_point_so_gaveta(conta)
    erp_payload["mp_point_conta"] = conta
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
    token, terminal_id = mp_point_credenciais(conta)
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
    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    order_id = str(raw.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    try:
        row0 = PdvMercadoPagoPointOrder.objects.get(mp_order_id=order_id)
    except PdvMercadoPagoPointOrder.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Pedido local não encontrado."}, status=404)

    conta = _conta_do_pedido_local(request, row0, raw)
    if not _mp_point_configurado(conta):
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request, conta=conta):
        return _resposta_mp_point_so_gaveta(conta)

    token = _token_da_conta(conta)
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
    order_id = (request.GET.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    try:
        row = PdvMercadoPagoPointOrder.objects.get(mp_order_id=order_id)
    except PdvMercadoPagoPointOrder.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Pedido não encontrado."}, status=404)

    conta = _conta_do_pedido_local(request, row)
    if not _mp_point_configurado(conta):
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request, conta=conta):
        return _resposta_mp_point_so_gaveta(conta)

    sk = _sessao_key(request)
    if row.django_session_key and sk and row.django_session_key != sk:
        return JsonResponse({"ok": False, "erro": "Sessão não confere com o pedido."}, status=403)

    if row.status == PdvMercadoPagoPointOrder.Status.ABANDONED:
        # Reconsultar MP: abandon local pode ter sido precoce (cancel falhou / timeout).
        token = _token_da_conta(conta)
        ok_mp_ab, _st_ab, body_ab = mp_point_get_order(access_token=token, order_id=order_id)
        if ok_mp_ab and isinstance(body_ab, dict) and _mp_point_promover_pago_local(row, body_ab):
            return JsonResponse(
                {
                    "ok": True,
                    "abandoned": False,
                    "paid": True,
                    "paid_tranche": True,
                    "finalized": False,
                    "mp_status": str(body_ab.get("status") or "processed"),
                    "venda_id": row.venda_id,
                    "recuperado_de_abandon": True,
                }
            )
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

    token = _token_da_conta(conta)
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

    _mp_point_promover_pago_local(row, body)
    row.refresh_from_db(fields=["status", "venda_id"])

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
    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    order_id = str(raw.get("order_id") or "").strip()
    if not order_id:
        return JsonResponse({"ok": False, "erro": "order_id obrigatório."}, status=400)

    try:
        row0 = PdvMercadoPagoPointOrder.objects.get(mp_order_id=order_id)
    except PdvMercadoPagoPointOrder.DoesNotExist:
        return JsonResponse({"ok": False, "erro": "Pedido local não encontrado."}, status=404)

    conta = _conta_do_pedido_local(request, row0, raw)
    if not _mp_point_configurado(conta):
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request, conta=conta):
        return _resposta_mp_point_so_gaveta(conta)

    erp_override = raw.get("erp_payload") or raw.get("erp")
    if not isinstance(erp_override, dict):
        erp_override = None

    client_m, db = obter_conexao_mongo_pdv()
    token = _token_da_conta(conta)
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

        if not getattr(settings, "PDV_VENDA_ERP_ENVIO", False):
            try:
                venda_local = _persistir_venda_agro(
                    request,
                    erp_data,
                    raw_itens,
                    None,
                    None,
                    False,
                    erp_sync_status=VendaAgro.ErpSyncStatus.ACEITO,
                )
            except SessaoCaixaObrigatoriaError as e:
                row.status = PdvMercadoPagoPointOrder.Status.FAILED
                row.save(update_fields=["status", "atualizado_em"])
                return JsonResponse({"ok": False, "erro": str(e)}, status=400)
            vid = venda_local.pk if venda_local else None
            row.status = PdvMercadoPagoPointOrder.Status.FINALIZED
            row.venda_id = vid
            row.erp_payload = erp_data
            row.save(update_fields=["status", "venda", "erp_payload", "atualizado_em"])
            payload = {
                "ok": True,
                "mensagem": "Venda registrada no Agro.",
                "venda_id": vid,
                "erp_pendente": False,
            }
            _mp_point_anexar_recon_payload(payload, recon)
            anexar_nfce_resposta_venda(venda_local, erp_data, payload)
            return JsonResponse(payload)

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
    """
    Operador desistiu da espera no Point.
    Bidirecional (Centro e Vila — mesmo código, contas distintas):
      - PDV → tenta cancelar na maquininha via API MP
      - Só marca ABANDONED local se o MP confirmou cancel/falha OU cancel OK
      - Se o MP já cobrou → 409 pagamento_efetivado (não abandona)
      - Se não conseguiu cancelar e cobrança ainda viva → 409, pedido fica PENDING
    """
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

    conta = _conta_do_pedido_local(request, row, raw)
    if not _mp_point_configurado(conta):
        return JsonResponse({"ok": False, "erro": "Point desativado."}, status=503)
    if not navegador_pode_mp_point_automatico(request, conta=conta):
        return _resposta_mp_point_so_gaveta(conta)

    sk = _sessao_key(request)
    if row.django_session_key and sk and row.django_session_key != sk:
        return JsonResponse({"ok": False, "erro": "Sessão não confere."}, status=403)

    if row.status == PdvMercadoPagoPointOrder.Status.FINALIZED:
        return JsonResponse({"ok": False, "erro": "Esta cobrança já virou venda finalizada."}, status=409)
    if row.status == PdvMercadoPagoPointOrder.Status.PAID:
        return JsonResponse(
            {
                "ok": False,
                "pagamento_efetivado": True,
                "erro": "Pagamento já confirmado na maquininha. Finalize a venda ou fale com o gerente.",
            },
            status=409,
        )
    if row.status == PdvMercadoPagoPointOrder.Status.ABANDONED:
        return JsonResponse({"ok": True, "ja_abandonado": True, "cancelou_maquininha": True})
    if row.status != PdvMercadoPagoPointOrder.Status.PENDING:
        return JsonResponse({"ok": False, "erro": "Não é possível cancelar este pedido."}, status=409)

    token = _token_da_conta(conta)

    # 1) Conferir MP antes de cancelar — evita abandonar se já cobrou.
    ok_get, _st_get, body_get = mp_point_get_order(access_token=token, order_id=order_id)
    if ok_get and isinstance(body_get, dict):
        if _mp_point_promover_pago_local(row, body_get):
            return JsonResponse(
                {
                    "ok": False,
                    "pagamento_efetivado": True,
                    "erro": "Pagamento já confirmado na maquininha. Finalize a venda ou fale com o gerente.",
                },
                status=409,
            )
        if mp_point_order_indica_cancelado(body_get) or mp_point_order_indica_falha(body_get)[0]:
            row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
            row.mp_last_status = str(body_get.get("status") or "")[:48]
            row.save(update_fields=["status", "mp_last_status", "atualizado_em"])
            return JsonResponse({"ok": True, "cancelou_maquininha": True, "ja_cancelado_mp": True})

    # 2) Pedir cancelamento na maquininha (mesmo fluxo Centro/Vila).
    ok_mp, st_cancel, body_cancel = mp_point_cancel_order(access_token=token, order_id=order_id)

    # 3) Reconsultar estado real após o cancel.
    ok_after, _st_after, body_after = mp_point_get_order(access_token=token, order_id=order_id)
    if ok_after and isinstance(body_after, dict):
        if _mp_point_promover_pago_local(row, body_after):
            return JsonResponse(
                {
                    "ok": False,
                    "pagamento_efetivado": True,
                    "erro": "A maquininha confirmou o pagamento enquanto cancelávamos. Finalize a venda.",
                },
                status=409,
            )
        canceled = mp_point_order_indica_cancelado(body_after)
        failed, _failed_msg = mp_point_order_indica_falha(body_after)
        if canceled or failed or ok_mp:
            row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
            row.mp_last_status = str(body_after.get("status") or "")[:48]
            row.save(update_fields=["status", "mp_last_status", "atualizado_em"])
            return JsonResponse(
                {
                    "ok": True,
                    "cancelou_maquininha": bool(ok_mp or canceled or failed),
                }
            )
        # Ainda vivo no terminal — NÃO marcar abandoned local.
        aviso = (
            "Não foi possível cancelar na maquininha — cancele também no terminal. "
            "A cobrança continua aberta no sistema até cancelar ou pagar."
        )
        low = mp_point_mensagem_erro(body_cancel).lower() if not ok_mp else ""
        if st_cancel == 409 or "terminal" in low or "at_terminal" in low:
            aviso = (
                "A maquininha ainda está com o valor na tela. Cancele no terminal "
                "e só então use outra forma de pagamento."
            )
        return JsonResponse(
            {
                "ok": False,
                "cancelou_maquininha": False,
                "pedido_ainda_ativo": True,
                "aviso": aviso,
                "erro": aviso,
            },
            status=409,
        )

    # Sem GET após cancel: só abandona local se a API de cancel confirmou.
    if ok_mp:
        row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
        row.save(update_fields=["status", "atualizado_em"])
        return JsonResponse({"ok": True, "cancelou_maquininha": True})

    aviso = (
        "Falha ao cancelar na maquininha. Cancele no terminal e tente de novo — "
        "a cobrança continua aberta no sistema."
    )
    if st_cancel not in (0, 404):
        logger.warning("MP Point abandonar: cancel API HTTP %s — %s", st_cancel, body_cancel)
    return JsonResponse(
        {
            "ok": False,
            "cancelou_maquininha": False,
            "pedido_ainda_ativo": True,
            "aviso": aviso,
            "erro": aviso,
        },
        status=409,
    )


@require_POST
def api_pdv_mp_point_forcar_liberar(request):
    """
    Emergência: PIN gerencial (Geraldo / Geraldinho / Renan Hinnen) libera a sessão
    para fechar a venda com outra forma, mesmo com Point PENDING/PAID.
    """
    from datetime import timedelta

    from django.utils import timezone

    from produtos.pin_gerencial_util import (
        PIN_GERENCIAL_HINT,
        PIN_GERENCIAL_NOMES_UI,
        gravar_mp_point_forcar_bypass,
        validar_pin_gerencial,
    )

    try:
        raw = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    ok_pin, rotulo, err_pin = validar_pin_gerencial(str(raw.get("pin") or ""))
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin or "PIN gerencial inválido."}, status=403)

    sk = _sessao_key(request)
    if not sk:
        return JsonResponse({"ok": False, "erro": "Sessão inválida. Recarregue o PDV (F5)."}, status=400)

    cutoff = timezone.now() - timedelta(hours=2)
    qs = PdvMercadoPagoPointOrder.objects.filter(
        django_session_key=sk,
        status__in=(
            PdvMercadoPagoPointOrder.Status.PENDING,
            PdvMercadoPagoPointOrder.Status.PAID,
        ),
        criado_em__gte=cutoff,
    ).order_by("-criado_em")
    rows = list(qs[:20])
    order_ids = [r.mp_order_id for r in rows]
    tinha_pago = any(r.status == PdvMercadoPagoPointOrder.Status.PAID for r in rows)

    # Tenta cancelar PENDING no terminal; se falhar, ainda libera via bypass (emergência).
    for row in rows:
        if row.status != PdvMercadoPagoPointOrder.Status.PENDING:
            continue
        conta = _conta_do_pedido_local(request, row, raw if isinstance(raw, dict) else None)
        if not _mp_point_configurado(conta):
            continue
        token = _token_da_conta(conta)
        ok_get, _st, body = mp_point_get_order(access_token=token, order_id=row.mp_order_id)
        if ok_get and isinstance(body, dict) and _mp_point_promover_pago_local(row, body):
            tinha_pago = True
            continue
        ok_cancel, _st_c, _body_c = mp_point_cancel_order(
            access_token=token, order_id=row.mp_order_id
        )
        row.refresh_from_db()
        if row.status == PdvMercadoPagoPointOrder.Status.PAID:
            tinha_pago = True
            continue
        # Força abandon local do PENDING (auditoria: gerente liberou).
        row.status = PdvMercadoPagoPointOrder.Status.ABANDONED
        row.mp_last_status = ("forced_by_" + rotulo.replace(" ", "_"))[:48]
        row.save(update_fields=["status", "mp_last_status", "atualizado_em"])
        if not ok_cancel:
            logger.warning(
                "MP Point forçar liberar: cancel MP falhou order=%s por=%s",
                row.mp_order_id,
                rotulo,
            )

    gravar_mp_point_forcar_bypass(request, por=rotulo, order_ids=order_ids)
    logger.warning(
        "MP Point forçar liberar: por=%s orders=%s tinha_pago=%s",
        rotulo,
        order_ids,
        tinha_pago,
    )

    aviso_pago = ""
    if tinha_pago:
        aviso_pago = (
            " Atenção: havia pagamento já confirmado no Point — confira no extrato MP "
            "se não vai duplicar cobrança."
        )

    return JsonResponse(
        {
            "ok": True,
            "por": rotulo,
            "mensagem": (
                f"Liberado por {rotulo}. Pode fechar a venda com outra forma agora."
                + aviso_pago
            ),
            "tinha_pago_point": tinha_pago,
            "pin_gerencial_nomes": PIN_GERENCIAL_NOMES_UI,
            "pin_gerencial_hint": PIN_GERENCIAL_HINT,
        }
    )
