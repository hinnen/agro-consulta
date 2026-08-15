"""Painel simples: valor de vendas Centro + Vila + soma."""
from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET

from produtos.vendas_lojas_util import parse_data_iso, payload_vendas_lojas


def _payload_from_request(request) -> dict:
    hoje = timezone.localdate()
    modo = (request.GET.get("periodo") or "dia").strip().lower()
    ref = parse_data_iso(request.GET.get("ref") or request.GET.get("data"))
    return payload_vendas_lojas(modo=modo, ref=ref, hoje=hoje)


@never_cache
@login_required(login_url="/admin/login/")
@require_GET
def vendas_lojas_view(request):
    payload = _payload_from_request(request)
    if (request.GET.get("fmt") or "").strip().lower() == "json":
        return JsonResponse(payload)
    return render(
        request,
        "produtos/vendas_lojas.html",
        {
            "vl": payload,
            "periodo": payload["periodo"],
        },
    )
