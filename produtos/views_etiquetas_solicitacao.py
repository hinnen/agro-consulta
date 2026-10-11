"""App celular + API desktop — solicitações de etiquetas."""

from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.etiquetas_solicitacao_util import (
    criar_pacote_solicitacao,
    hidratar_itens_para_fila,
    marcar_pacote_importado,
    serializar_pacote,
)
from produtos.etiquetas_solicitacao_pin_util import (
    exigir_operador_api,
    exigir_operador_html,
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
)
from produtos.models import EtiquetaSolicitacaoPacoteAgro

logger = logging.getLogger(__name__)


def _payload_json(request) -> dict:
    if request.content_type and "application/json" in (request.content_type or ""):
        try:
            return json.loads(request.body.decode("utf-8") or "{}")
        except Exception:
            return {}
    return {k: request.POST.get(k) for k in request.POST.keys()}


def _usuario_etiquetas(request) -> str:
    u = getattr(request, "user", None)
    if u and getattr(u, "is_authenticated", False):
        return (u.get_username() or str(u))[:150]
    return ""


@require_http_methods(["GET", "POST"])
@ensure_csrf_cookie
def etiquetas_solicitacao_pin(request):
    if request.method == "GET":
        if operador_da_sessao(request) and request.GET.get("trocar") != "1":
            return redirect("etiquetas_solicitacao_app")
        return render(
            request,
            "produtos/etiquetas_solicitacao/pin.html",
            {"operador": operador_da_sessao(request)},
        )
    pin = str(request.POST.get("pin") or "").strip()
    ok, nome, err = gravar_operador_sessao(request, pin)
    if not ok:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return JsonResponse({"ok": False, "erro": err}, status=403)
        return render(
            request,
            "produtos/etiquetas_solicitacao/pin.html",
            {"erro": err, "operador": ""},
            status=403,
        )
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return JsonResponse(
            {"ok": True, "operador": nome, "next": reverse("etiquetas_solicitacao_app")}
        )
    return redirect("etiquetas_solicitacao_app")


@require_GET
def etiquetas_solicitacao_logout(request):
    limpar_operador_sessao(request)
    return redirect("etiquetas_solicitacao_pin")


@never_cache
@require_GET
@ensure_csrf_cookie
@exigir_operador_html
def etiquetas_solicitacao_app(request):
    return render(
        request,
        "produtos/etiquetas_solicitacao/app.html",
        {
            "operador": operador_da_sessao(request),
            "url_catalogo_slim": reverse("api_pdv_catalogo_slim"),
            "url_api_solicitacao": reverse("api_etiquetas_solicitacao_mobile_criar"),
            "url_hub": reverse("vendas_lojas_hub"),
            "url_sair": reverse("etiquetas_solicitacao_logout"),
            "url_pin": reverse("etiquetas_solicitacao_pin"),
        },
    )


@require_POST
@exigir_operador_api
def api_etiquetas_solicitacao_mobile_criar(request):
    body = _payload_json(request)
    operador = getattr(request, "fotos_operador", "") or operador_da_sessao(request)
    try:
        pac = criar_pacote_solicitacao(
            operador_nome=operador,
            itens_raw=body.get("itens"),
            observacao=str(body.get("observacao") or ""),
        )
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    except Exception:
        logger.exception("api_etiquetas_solicitacao_mobile_criar")
        return JsonResponse({"ok": False, "erro": "Falha ao gravar solicitação."}, status=500)
    return JsonResponse({"ok": True, "pacote": serializar_pacote(pac)})


@login_required(login_url="/entrar/")
@require_GET
def api_etiquetas_solicitacoes_lista(request):
    status = str(request.GET.get("status") or EtiquetaSolicitacaoPacoteAgro.Status.PENDENTE).strip()
    if status not in dict(EtiquetaSolicitacaoPacoteAgro.Status.choices):
        status = EtiquetaSolicitacaoPacoteAgro.Status.PENDENTE
    try:
        lim = int(request.GET.get("limit") or 80)
    except (TypeError, ValueError):
        lim = 80
    lim = max(1, min(lim, 200))
    qs = EtiquetaSolicitacaoPacoteAgro.objects.filter(status=status).order_by("-criado_em")[:lim]
    pendentes = EtiquetaSolicitacaoPacoteAgro.objects.filter(
        status=EtiquetaSolicitacaoPacoteAgro.Status.PENDENTE
    ).count()
    return JsonResponse(
        {
            "ok": True,
            "pendentes": pendentes,
            "pacotes": [serializar_pacote(p) for p in qs],
        }
    )


@login_required(login_url="/entrar/")
@require_GET
def api_etiquetas_solicitacao_detalhe(request, pk: int):
    pac = get_object_or_404(EtiquetaSolicitacaoPacoteAgro, pk=pk)
    return JsonResponse({"ok": True, "pacote": serializar_pacote(pac, incluir_itens=True)})


@login_required(login_url="/entrar/")
@require_POST
def api_etiquetas_solicitacao_importar(request, pk: int):
    pac = get_object_or_404(EtiquetaSolicitacaoPacoteAgro, pk=pk)
    try:
        marcar_pacote_importado(pac, usuario=_usuario_etiquetas(request))
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    itens = hidratar_itens_para_fila(pac.itens_json or [])
    return JsonResponse(
        {
            "ok": True,
            "pacote": serializar_pacote(pac, incluir_itens=True),
            "itens": itens,
        }
    )
