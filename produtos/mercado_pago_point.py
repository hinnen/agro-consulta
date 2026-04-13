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


def mp_point_create_order(
    *,
    access_token: str,
    terminal_id: str,
    amount: float,
    external_reference: str,
    expiration_time: str,
    description: str | None = None,
) -> tuple[bool, int, dict | list | str]:
    """
    Cria pedido no terminal Point. Retorna (ok, http_status, corpo_json_ou_texto).
    """
    idem = str(uuid.uuid4())
    print_mode = (getattr(settings, "MP_POINT_PRINT_ON_TERMINAL", None) or "no_ticket").strip()
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
    pm = getattr(settings, "MP_POINT_PAYMENT_METHOD_CONFIG", None)
    if isinstance(pm, dict) and pm:
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


def mp_point_mensagem_erro(body) -> str:
    if isinstance(body, str):
        return body[:500]
    if not isinstance(body, dict):
        return str(body)[:500]
    for key in ("message", "error", "cause"):
        v = body.get(key)
        if v is None:
            continue
        if isinstance(v, str):
            return v[:500]
        if isinstance(v, list) and v:
            return str(v[0])[:500]
    return str(body)[:500]
