"""Painel web + API JSON — contagem cíclica (somente PIN administrador / gerência)."""

from __future__ import annotations

from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.cache import never_cache

import json

from estoque.models import ContagemCiclicaBloco, ContagemCiclicaSessao
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


def _operadores_pin_opcoes() -> list[dict]:
    from base.operador_util import listar_operadores

    rows = []
    for op in listar_operadores(incluir_inativos=False):
        nome = str(op.get("nome") or "").strip()
        if not nome:
            continue
        rows.append(
            {
                "id": op.get("id"),
                "nome": nome,
                "codigo": str(op.get("codigo_vendedor") or "").strip(),
            }
        )
    rows.sort(key=lambda r: r["nome"].lower())
    return rows


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
            "url_api_inventario": "/api/ajuste-mobile/ciclica/admin/inventario/",
            "operadores_pin": _operadores_pin_opcoes(),
            "contagem_tela_app": True,
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
            "url_api_inventario": "/api/ajuste-mobile/ciclica/admin/inventario/",
            "operadores_pin": _operadores_pin_opcoes(),
        },
    )


@never_cache
@require_POST
def api_ciclica_admin_inventario(request):
    err = _gate_admin(request)
    if err:
        return err
    from produtos.contagem_ciclica_util import abrir_sessao, _operador_da_request, sessao_payload

    rot, user = _operador_da_request(request)
    pins = [
        p.strip()
        for p in str(request.POST.get("pins") or "").replace(";", ",").split(",")
        if p.strip()
    ]
    from base.models import PerfilUsuario

    ids = []
    for raw in request.POST.getlist("perfil_id"):
        try:
            ids.append(int(raw))
        except (TypeError, ValueError):
            continue
    if ids:
        for perfil in PerfilUsuario.objects.filter(pk__in=ids, ativo=True):
            pin = str(perfil.senha_rapida or "").strip()
            if pin and pin not in pins:
                pins.append(pin)
    if not pins:
        return JsonResponse({"ok": False, "erro": "Escolha ao menos um operador."}, status=400)
    try:
        sessao = abrir_sessao(
            deposito=str(request.POST.get("deposito") or "centro"),
            escopo_tipo=str(request.POST.get("escopo_tipo") or "corredor"),
            escopo_valor=str(request.POST.get("escopo_valor") or ""),
            dias_movimentacao=0,
            operador_rotulo=rot or "Gerência",
            user=user,
            modo="inventario",
            pins=pins,
        )
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
    return JsonResponse({"ok": True, "sessao": sessao_payload(sessao)})


@never_cache
@require_POST
def api_ciclica_admin_zerar(request):
    err = _gate_admin(request)
    if err:
        return err
    if str(request.POST.get("confirmar") or "").strip().upper() != "ZERAR":
        return JsonResponse({"ok": False, "erro": "Digite ZERAR para confirmar."}, status=400)
    from produtos.contagem_ciclica_util import zerar_saldo_deposito, _operador_da_request

    rot, user = _operador_da_request(request)
    try:
        resumo = zerar_saldo_deposito(
            str(request.POST.get("deposito") or ""),
            user=user,
            operador_rotulo=rot or "Gerência",
        )
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
    return JsonResponse({"ok": True, "resumo": resumo})


@never_cache
@require_GET
def contagem_ciclica_bloco_print(request, pk: int):
    from produtos.contagem_ciclica_util import sessao_gate_ok

    if not (request_pode_ver_painel_ciclica_admin(request) or sessao_gate_ok(request)):
        return _negado(request)
    bloco = ContagemCiclicaBloco.objects.select_related("sessao").filter(pk=pk).first()
    if bloco is None:
        return HttpResponseForbidden("Bloco não encontrado.")
    try:
        amostra = json.loads(bloco.amostra_json or "[]")
    except Exception:
        amostra = []
    return render(
        request,
        "produtos/contagem_ciclica_bloco_print.html",
        {"bloco": bloco, "amostra": amostra, "sessao": bloco.sessao},
    )
