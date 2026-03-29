"""
Envio de texto para WhatsApp via CallMeBot (API gratuita com apikey por número).

Cadastro: https://www.callmebot.com/blog/free-api-whatsapp-messages/

Variáveis (.env):
  WHATSAPP_CALLMEBOT_PHONE   — número com DDI, ex: 5511999998888
  WHATSAPP_CALLMEBOT_APIKEY  — apikey exibido pelo CallMeBot após ativar o bot

  Opcional — 2º e 3º destinatários (cada número tem apikey própria):
  WHATSAPP_CALLMEBOT_PHONE_2, WHATSAPP_CALLMEBOT_APIKEY_2
  WHATSAPP_CALLMEBOT_PHONE_3, WHATSAPP_CALLMEBOT_APIKEY_3
"""
from __future__ import annotations

import logging
import re
import time
import requests
from decouple import config

logger = logging.getLogger(__name__)


def _snippet_resposta_callmebot(html: str, max_len: int = 140) -> str:
    plain = re.sub(r"<[^>]+>", " ", html or "")
    plain = " ".join(plain.split())
    return plain[:max_len]


def _callmebot_corpo_indica_erro(html: str) -> bool:
    """CallMeBot às vezes responde 200 com HTML explicando falha (número não ativado, apikey, etc.)."""
    low = (html or "").lower()
    needles = (
        "not activated",
        "not registered",
        "invalid apikey",
        "invalid api key",
        "error:",
        "you need to get",
        "phone is not",
        "the user has blocked",
        "spam",
    )
    return any(n in low for n in needles)


def _destinos_callmebot() -> list[tuple[str, str]]:
    pares: list[tuple[str, str]] = []
    vistos: set[str] = set()
    for phone_key, key_key in (
        ("WHATSAPP_CALLMEBOT_PHONE", "WHATSAPP_CALLMEBOT_APIKEY"),
        ("WHATSAPP_CALLMEBOT_PHONE_2", "WHATSAPP_CALLMEBOT_APIKEY_2"),
        ("WHATSAPP_CALLMEBOT_PHONE_3", "WHATSAPP_CALLMEBOT_APIKEY_3"),
    ):
        phone = (config(phone_key, default="") or "").strip()
        apikey = (config(key_key, default="") or "").strip()
        if phone and apikey and phone not in vistos:
            vistos.add(phone)
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
    for i, (phone, apikey) in enumerate(destinos):
        if i > 0:
            time.sleep(2.5)
        try:
            r = requests.get(
                url,
                params={"phone": phone, "apikey": apikey, "text": text},
                timeout=20,
                headers={"User-Agent": "agro-consulta-alerta/1.0"},
            )
            body = r.text or ""
            http_ok = 200 <= r.status_code < 300
            corpo_erro = http_ok and _callmebot_corpo_indica_erro(body)
            ok = http_ok and not corpo_erro
            if corpo_erro:
                logger.warning(
                    "CallMeBot HTTP 200 mas resposta indica erro (%s): %s",
                    phone,
                    body[:500],
                )
            elif not http_ok:
                logger.warning(
                    "CallMeBot HTTP %s (%s): %s",
                    r.status_code,
                    phone,
                    body[:300],
                )
            else:
                algum_ok = True
                logger.info("CallMeBot OK %s: %s", phone, _snippet_resposta_callmebot(body))
            sn = _snippet_resposta_callmebot(body)
            partes.append(f"{phone}:{'OK' if ok else 'FALHOU'} [{sn}]")
        except Exception as exc:
            logger.exception("CallMeBot (%s): %s", phone, exc)
            partes.append(f"{phone}:ERRO [{exc}]")
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
