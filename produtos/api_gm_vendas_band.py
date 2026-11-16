"""API de leitura, protegida por token, para GM Vendas na Smart Band 10."""
from __future__ import annotations

import hmac
import os
from decimal import Decimal

from django.http import HttpResponseNotAllowed, JsonResponse
from django.utils import timezone


def api_gm_vendas_band(request):
    if request.method != "GET":
        return HttpResponseNotAllowed(["GET"])

    esperado = os.environ.get("GM_VENDAS_BAND_TOKEN", "").strip()
    if len(esperado) < 32:
        return JsonResponse({"ok": False, "erro": "servico_indisponivel"}, status=503)

    esquema, separador, recebido = request.headers.get("Authorization", "").partition(" ")
    if not (separador and esquema.lower() == "bearer" and recebido and
            hmac.compare_digest(recebido.encode(), esperado.encode())):
        return JsonResponse({"ok": False, "erro": "nao_autorizado"}, status=401)

    from produtos.vendas_lojas_util import vendas_lojas_totais

    agora = timezone.localtime()
    hoje = agora.date()
    centro, vila, total = vendas_lojas_totais(hoje, hoje)

    def valor(numero):
        decimal = Decimal(str(numero))
        if not decimal.is_finite():
            raise ValueError("Valor monetário inválido")
        return float(decimal)

    resposta = JsonResponse({
        "tipo": "gm_vendas_totais", "data": hoje.isoformat(),
        "hora": agora.strftime("%H:%M"), "centro": valor(centro),
        "vila": valor(vila), "total": valor(total),
    })
    resposta["Cache-Control"] = "private, no-store, max-age=0"
    resposta["Pragma"] = "no-cache"
    resposta["Vary"] = "Authorization"
    resposta["X-Content-Type-Options"] = "nosniff"
    resposta["Referrer-Policy"] = "no-referrer"
    return resposta
