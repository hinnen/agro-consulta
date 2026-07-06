"""
Cliente HTTP mínimo para Mercado Pago Point (Orders API).

Documentação: POST/GET https://api.mercadopago.com/v1/orders — type point + terminal_id.
"""

from __future__ import annotations

import logging
import uuid

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

MP_ORDERS_URL = "https://api.mercadopago.com/v1/orders"


def mp_point_order_indica_cancelado(doc: dict) -> bool:
    if not isinstance(doc, dict):
        return False
    st = str(doc.get("status") or "").strip().lower()
    if st in ("canceled", "cancelled"):
        return True
    tx = doc.get("transactions") or {}
    pays = tx.get("payments") or []
    if isinstance(pays, dict):
        pays = [pays]
    if isinstance(pays, list):
        for p in pays:
            if not isinstance(p, dict):
                continue
            ps = str(p.get("status") or "").strip().lower()
            if ps in ("canceled", "cancelled"):
                return True
    return False


def mp_point_order_indica_falha(doc: dict) -> tuple[bool, str]:
    """Pagamento recusado, expirado ou erro no terminal."""
    if not isinstance(doc, dict):
        return False, ""
    _MAP = {
        "insufficient_amount": "Saldo ou limite insuficiente no cartão.",
        "card_disabled": "Cartão bloqueado ou desativado.",
        "amount_limit_exceeded": "Valor acima do limite do cartão.",
        "invalid_installments": "Número de parcelas não aceito neste cartão.",
        "processing_error": "Erro ao processar na maquininha.",
        "rejected_by_issuer": "Pagamento recusado pelo banco.",
        "high_risk": "Pagamento recusado (risco).",
        "failed": "Pagamento não concluído na maquininha.",
        "expired": "Cobrança expirou na maquininha.",
    }
    st = str(doc.get("status") or "").strip().lower()
    detail = str(doc.get("status_detail") or "").strip().lower()
    if st in ("failed", "expired"):
        msg = _MAP.get(detail) or _MAP.get(st) or "Pagamento não concluído na maquininha."
        return True, msg
    for p in _mp_point_payment_dicts(doc):
        ps = str(p.get("status") or "").strip().lower()
        if ps != "failed":
            continue
        sd = str(p.get("status_detail") or "").strip().lower()
        msg = _MAP.get(sd) or _MAP.get(ps) or "Pagamento recusado na maquininha."
        return True, msg
    return False, ""


def mp_point_order_indica_pago(doc: dict) -> bool:
    if not isinstance(doc, dict):
        return False
    st = str(doc.get("status") or "").strip().lower()
    if st == "processed":
        return True
    tx = doc.get("transactions") or {}
    pays = tx.get("payments") or []
    for p in pays:
        if not isinstance(p, dict):
            continue
        ps = str(p.get("status") or "").strip().lower()
        if ps in ("processed", "approved", "accredited"):
            return True
    return False


def _mp_point_payment_dicts(doc: dict) -> list[dict]:
    if not isinstance(doc, dict):
        return []
    tx = doc.get("transactions") or {}
    pays = tx.get("payments") or []
    if isinstance(pays, dict):
        pays = [pays]
    if not isinstance(pays, list):
        return []
    return [p for p in pays if isinstance(p, dict)]


def mp_point_extrair_tipo_pagamento(doc: dict) -> tuple[str | None, int | None]:
    """
    Lê o tipo efetivo no pedido MP processado.
    Retorna (debit_card | credit_card | qr | ..., parcelas ou None).
    """
    for p in _mp_point_payment_dicts(doc):
        ps = str(p.get("status") or "").strip().lower()
        if ps and ps not in ("processed", "approved", "accredited"):
            continue
        pm = p.get("payment_method")
        if isinstance(pm, dict):
            tipo = str(pm.get("type") or pm.get("id") or "").strip().lower()
            if tipo:
                inst = pm.get("installments")
                try:
                    n_inst = int(inst) if inst is not None else None
                except (TypeError, ValueError):
                    n_inst = None
                return tipo, n_inst
        tipo = str(p.get("payment_method_id") or p.get("type") or "").strip().lower()
        if tipo:
            return tipo, None
    cfg = doc.get("config") or {}
    pm_cfg = cfg.get("payment_method") if isinstance(cfg, dict) else None
    if isinstance(pm_cfg, dict):
        tipo = str(pm_cfg.get("default_type") or "").strip().lower()
        if tipo:
            try:
                n_inst = int(pm_cfg.get("default_installments") or 0) or None
            except (TypeError, ValueError):
                n_inst = None
            return tipo, n_inst
    return None, None


def mp_point_forma_pdv_de_tipo_mp(tipo_mp: str | None, parcelas: int | None = None) -> str:
    t = str(tipo_mp or "").strip().lower()
    if not t:
        return ""
    if t in ("qr", "qr_code") or "pix" in t:
        return "PIX"
    if t == "debit_card" or ("debit" in t and "credit" not in t):
        return "Cartão de débito"
    if t == "credit_card" or "credit" in t:
        try:
            n = int(parcelas) if parcelas is not None else 1
        except (TypeError, ValueError):
            n = 1
        if n > 1:
            return "Cartão de crédito parcelado"
        return "Cartão de crédito"
    return ""


def mp_point_classe_forma_caixa(forma: str) -> str:
    """Agrupa formas para comparar débito/crédito/pix (ignora parcelado vs à vista)."""
    f = str(forma or "").strip().lower()
    if "pix" in f:
        return "pix"
    if "débito" in f or "debito" in f:
        return "debito"
    if "crédito" in f or "credito" in f:
        return "credito"
    return f


def mp_point_create_order(
    *,
    access_token: str,
    terminal_id: str,
    amount: float,
    external_reference: str,
    expiration_time: str,
    description: str | None = None,
    payment_method_config: dict | None = None,
) -> tuple[bool, int, dict | list | str]:
    """
    Cria pedido no terminal Point. Retorna (ok, http_status, corpo_json_ou_texto).
    """
    idem = str(uuid.uuid4())
    print_mode = (getattr(settings, "MP_POINT_PRINT_ON_TERMINAL", None) or "seller_ticket").strip()
    body: dict = {
        "type": "point",
        "external_reference": external_reference,
        "expiration_time": (expiration_time or "PT16M").strip(),
        "transactions": {"payments": [{"amount": f"{float(amount):.2f}"}]},
        "config": {
            "point": {
                "terminal_id": terminal_id.strip(),
                "print_on_terminal": print_mode,
            }
        },
    }
    if description:
        body["description"] = description[:200]
    pm = payment_method_config if isinstance(payment_method_config, dict) and payment_method_config else None
    if not pm:
        env_pm = getattr(settings, "MP_POINT_PAYMENT_METHOD_CONFIG", None)
        if isinstance(env_pm, dict) and env_pm:
            pm = env_pm
    if pm:
        body["config"]["payment_method"] = pm

    headers = {
        "Authorization": f"Bearer {access_token.strip()}",
        "Content-Type": "application/json",
        "X-Idempotency-Key": idem,
    }
    try:
        r = requests.post(MP_ORDERS_URL, headers=headers, json=body, timeout=30)
    except requests.RequestException:
        logger.exception("MP Point: falha de rede ao criar pedido")
        return False, 0, "Erro de rede ao falar com o Mercado Pago."

    try:
        data = r.json()
    except Exception:
        data = {"raw": (r.text or "")[:2000]}

    if r.status_code == 201:
        return True, r.status_code, data
    if r.status_code == 409:
        return False, r.status_code, data
    return False, r.status_code, data


def mp_point_get_order(*, access_token: str, order_id: str) -> tuple[bool, int, dict | list | str]:
    try:
        r = requests.get(
            f"{MP_ORDERS_URL}/{order_id.strip()}",
            headers={
                "Authorization": f"Bearer {access_token.strip()}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
    except requests.RequestException:
        logger.exception("MP Point: falha de rede ao consultar pedido")
        return False, 0, "Erro de rede ao falar com o Mercado Pago."

    try:
        data = r.json()
    except Exception:
        data = {"raw": (r.text or "")[:2000]}

    if r.status_code == 200:
        return True, r.status_code, data
    return False, r.status_code, data


def mp_point_cancel_order(*, access_token: str, order_id: str) -> tuple[bool, int, dict | list | str]:
    """Cancela pedido Point (quando ainda permitido pela API)."""
    idem = str(uuid.uuid4())
    headers = {
        "Authorization": f"Bearer {access_token.strip()}",
        "Content-Type": "application/json",
        "X-Idempotency-Key": idem,
    }
    try:
        r = requests.post(
            f"{MP_ORDERS_URL}/{order_id.strip()}/cancel",
            headers=headers,
            timeout=30,
        )
    except requests.RequestException:
        logger.exception("MP Point: falha de rede ao cancelar pedido")
        return False, 0, "Erro de rede ao cancelar no Mercado Pago."

    try:
        data = r.json()
    except Exception:
        data = {"raw": (r.text or "")[:2000]}

    if r.status_code in (200, 201):
        return True, r.status_code, data
    return False, r.status_code, data


def mp_point_mensagem_erro(body) -> str:
    if isinstance(body, str):
        return body[:500]
    if not isinstance(body, dict):
        return str(body)[:500]
    errs = body.get("errors")
    if isinstance(errs, list) and errs:
        partes: list[str] = []
        for e in errs:
            if not isinstance(e, dict):
                continue
            msg = str(e.get("message") or e.get("code") or "").strip()
            det = e.get("details")
            if isinstance(det, list) and det:
                msg = (msg + ": " + str(det[0])) if msg else str(det[0])
            elif det:
                msg = (msg + ": " + str(det)) if msg else str(det)
            if msg:
                partes.append(msg)
        if partes:
            return "; ".join(partes)[:500]
    for key in ("message", "error", "cause"):
        v = body.get(key)
        if v is None:
            continue
        if isinstance(v, str):
            return v[:500]
        if isinstance(v, list) and v:
            return str(v[0])[:500]
    return str(body)[:500]
