"""
Envio de texto para WhatsApp via CallMeBot (API gratuita com apikey por número).

Cadastro: https://www.callmebot.com/blog/free-api-whatsapp-messages/

Variáveis (.env):
  WHATSAPP_CALLMEBOT_PHONE   — número com DDI, ex: 5511999998888
  WHATSAPP_CALLMEBOT_APIKEY  — apikey exibido pelo CallMeBot após ativar o bot

  Opcional — segundo destinatário (cada número tem apikey própria):
  WHATSAPP_CALLMEBOT_PHONE_2
  WHATSAPP_CALLMEBOT_APIKEY_2
"""
from __future__ import annotations

import logging
import requests
from decouple import config

logger = logging.getLogger(__name__)


def _destinos_callmebot() -> list[tuple[str, str]]:
    pares: list[tuple[str, str]] = []
    for phone_key, key_key in (
        ("WHATSAPP_CALLMEBOT_PHONE", "WHATSAPP_CALLMEBOT_APIKEY"),
        ("WHATSAPP_CALLMEBOT_PHONE_2", "WHATSAPP_CALLMEBOT_APIKEY_2"),
    ):
        phone = (config(phone_key, default="") or "").strip()
        apikey = (config(key_key, default="") or "").strip()
        if phone and apikey:
            if phone_key.endswith("_2") and any(p == phone for p, _ in pares):
                continue
            pares.append((phone, apikey))
    return pares


def enviar_whatsapp_callmebot(mensagem: str) -> tuple[bool, str]:
    destinos = _destinos_callmebot()
    if not destinos:
        return False, "Nenhum destino CallMeBot (WHATSAPP_CALLMEBOT_PHONE/APIKEY)"
    url = "https://api.callmebot.com/whatsapp.php"
    text = mensagem[:3500]
    partes: list[str] = []
    algum_ok = False
    for phone, apikey in destinos:
        try:
            r = requests.get(
                url,
                params={"phone": phone, "apikey": apikey, "text": text},
                timeout=20,
            )
            ok = 200 <= r.status_code < 300
            if not ok:
                logger.warning(
                    "CallMeBot HTTP %s (%s): %s",
                    r.status_code,
                    phone,
                    (r.text or "")[:300],
                )
            else:
                algum_ok = True
            partes.append(f"{phone}:{'OK' if ok else 'FALHOU'}")
        except Exception as exc:
            logger.exception("CallMeBot (%s): %s", phone, exc)
            partes.append(f"{phone}:ERRO")
    return algum_ok, "; ".join(partes)


def enviar_alerta_custom_url(mensagem: str) -> tuple[bool, str]:
    """Opcional: webhook próprio (n8n, Make, etc.) — POST JSON."""
    url = (config("ALERTA_VENDAS_WEBHOOK_URL", default="") or "").strip()
    if not url:
        return False, "ALERTA_VENDAS_WEBHOOK_URL vazio"
    try:
        r = requests.post(
            url,
            json={"texto": mensagem[:8000], "origem": "agro-consulta-vendas-dia"},
            timeout=15,
        )
        return 200 <= r.status_code < 300, (r.text or "")[:300]
    except Exception as exc:
        logger.exception("Webhook alerta: %s", exc)
        return False, str(exc)
