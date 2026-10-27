"""Painel web + API JSON — contagem cíclica (somente PIN administrador / gerência)."""

from __future__ import annotations

from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_GET
from django.views.decorators.cache import never_cache

from estoque.models import ContagemCiclicaSessao
from produtos.contagem_ciclica_admin_util import (
    atividade_recente_payload,
    painel_dashboard_payload,
    request_pode_ver_painel_ciclica_admin,
    sessao_payload_admin,
)
from produtos.vendas_lojas_hub_sessao_util import (
    exigir_hub_operador_html,
    operador_da_sessao,
    redirect_para_pin,
)


def _negado(request):
    accept = (request.META.get("HTTP_ACCEPT") or "").lower()
    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or "application/json" in accept:
        return JsonResponse(
            {"ok": False, "erro": "Acesso restrito à gerência.", "precisa_admin": True},
            status=403,
        )
    return HttpResponseForbidden(
        "<!DOCTYPE html><html lang='pt-br'><meta charset='utf-8'>"
        "<title>Acesso negado</title>"
        "<body style='font-family:system-ui;padding:2rem'>"
        "<h1>Acesso negado</h1>"
        "<p>Painel de contagem cíclica — somente PIN de administrador / gerência.</p>"
        "<p><a href='/vendas/lojas/'>Voltar ao SistApp</a></p></body></html>"
    )


def _gate_admin(request):
    if not request_pode_ver_painel_ciclica_admin(request):
        return _negado(request)
    return None


@never_cache
@require_GET
def api_ciclica_admin_painel(request):
    err = _gate_admin(request)
    if err:
        return err
    dep = str(request.GET.get("deposito") or "").strip()
    busca = str(request.GET.get("busca") or request.GET.get("q") or "").strip()
    data = painel_dashboard_payload(deposito=dep, busca=busca)
    data["atividade"] = atividade_recente_payload(35)
    return JsonResponse(data)


@never_cache
@require_GET
def api_ciclica_admin_sessao(request, pk: int):
    err = _gate_admin(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    q = str(request.GET.get("q") or "").strip()
    return JsonResponse(
        {
            "ok": True,
            "sessao": sessao_payload_admin(sessao, q=q),
        }
    )


@never_cache
@require_GET
@exigir_hub_operador_html
def contagem_ciclica_painel_admin(request):
    err = _gate_admin(request)
    if err:
        return err
    return render(
        request,
        "produtos/contagem_ciclica_painel_admin.html",
        {
            "operador": operador_da_sessao(request),
            "url_api_painel": "/api/ajuste-mobile/ciclica/admin/painel/",
            "url_api_sessao": "/api/ajuste-mobile/ciclica/admin/sessao/",
            "url_hub": "/vendas/lojas/",
            "url_ajuste": "/vendas/lojas/ajuste/",
        },
    )


@never_cache
@require_GET
def contagem_ciclica_painel_admin_desktop(request):
    """Atalho BI / Chrome logado (staff) sem passar pelo hub PIN."""
    err = _gate_admin(request)
    if err:
        return err
    if not operador_da_sessao(request):
        user = getattr(request, "user", None)
        if not (user and user.is_authenticated and (user.is_staff or user.is_superuser)):
            return redirect_para_pin(request)
    return render(
        request,
        "produtos/contagem_ciclica_painel_admin.html",
        {
            "operador": operador_da_sessao(request)
            or (
                getattr(request.user, "get_full_name", lambda: "")()
                or getattr(request.user, "username", "")
            ),
            "url_api_painel": "/api/ajuste-mobile/ciclica/admin/painel/",
            "url_api_sessao": "/api/ajuste-mobile/ciclica/admin/sessao/",
            "url_hub": "/vendas/lojas/",
            "url_ajuste": "/vendas/lojas/ajuste/",
        },
    )
