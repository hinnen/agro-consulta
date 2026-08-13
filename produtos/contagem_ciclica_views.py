"""APIs — contagem cíclica no Ajuste Mobile."""

from __future__ import annotations

import json

from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from estoque.models import ContagemCiclicaSessao, ContagemCiclicaStatus
from produtos.contagem_ciclica_util import (
    abrir_sessao,
    cancelar_sessao,
    categorias_disponiveis,
    entrar_sessao,
    fechar_passagem_1,
    gravar_fechamento,
    listar_sessoes_abertas,
    registrar_contagem,
    sessao_gate_ok,
    sessao_payload,
    _operador_da_request,
)


def _json_body(request) -> dict:
    if request.content_type and "application/json" in request.content_type:
        try:
            return json.loads(request.body.decode("utf-8") or "{}")
        except Exception:
            return {}
    return {}


def _get(request, key: str, default: str = "") -> str:
    body = _json_body(request)
    if key in body and body.get(key) is not None:
        return str(body.get(key))
    return str(request.POST.get(key) or request.GET.get(key) or default)


def _gate(request):
    if not sessao_gate_ok(request):
        return JsonResponse(
            {"ok": False, "erro": "Sessão expirada. Entre com o PIN de novo."},
            status=403,
        )
    return None


@require_GET
def api_ciclica_sessoes(request):
    err = _gate(request)
    if err:
        return err
    dep = _get(request, "deposito", "")
    rows = [sessao_payload(s) for s in listar_sessoes_abertas(dep or None)]
    return JsonResponse({"ok": True, "sessoes": rows})


@require_GET
def api_ciclica_categorias(request):
    err = _gate(request)
    if err:
        return err
    return JsonResponse({"ok": True, "categorias": categorias_disponiveis()})


@require_POST
def api_ciclica_abrir(request):
    err = _gate(request)
    if err:
        return err
    rot, user = _operador_da_request(request)
    if not rot:
        return JsonResponse({"ok": False, "erro": "Operador não identificado."}, status=400)
    try:
        sessao = abrir_sessao(
            deposito=_get(request, "deposito", "centro"),
            escopo_tipo=_get(request, "escopo_tipo", "loja"),
            escopo_valor=_get(request, "escopo_valor", ""),
            operador_rotulo=rot,
            user=user,
        )
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
    return JsonResponse({"ok": True, "sessao": sessao_payload(sessao, detalhe=True)})


@require_POST
def api_ciclica_entrar(request, pk: int):
    err = _gate(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    rot, user = _operador_da_request(request)
    try:
        entrar_sessao(sessao, rot or "Operador", user)
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    return JsonResponse({"ok": True, "sessao": sessao_payload(sessao, detalhe=True)})


@require_GET
def api_ciclica_detalhe(request, pk: int):
    err = _gate(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    return JsonResponse({"ok": True, "sessao": sessao_payload(sessao, detalhe=True)})


@require_POST
def api_ciclica_contar(request, pk: int):
    err = _gate(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    rot, _user = _operador_da_request(request)
    try:
        ln = registrar_contagem(
            sessao,
            produto_externo_id=_get(request, "produto_id"),
            qtd=_get(request, "qtd", "0"),
            operador_rotulo=rot,
            nome_produto=_get(request, "nome_produto"),
            codigo_interno=_get(request, "codigo_interno"),
            categoria=_get(request, "categoria"),
        )
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
    sessao.refresh_from_db()
    ln.refresh_from_db()
    if sessao.status == ContagemCiclicaStatus.PASS2:
        q_acum = ln.qtd_pass2
    else:
        q_acum = ln.qtd_pass1
    try:
        q_f = float(q_acum if q_acum is not None else 0)
    except (TypeError, ValueError):
        q_f = 0.0
    return JsonResponse(
        {
            "ok": True,
            "linha_id": ln.pk,
            "produto_id": ln.produto_externo_id,
            "qtd_acumulada": q_f,
            "somou": True,
            "sessao": sessao_payload(sessao),
        }
    )


@require_POST
def api_ciclica_fechar_pass1(request, pk: int):
    err = _gate(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    try:
        resumo = fechar_passagem_1(sessao)
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    sessao.refresh_from_db()
    return JsonResponse(
        {"ok": True, "resumo": resumo, "sessao": sessao_payload(sessao, detalhe=True)}
    )


@require_POST
def api_ciclica_gravar(request, pk: int):
    err = _gate(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    rot, user = _operador_da_request(request)
    try:
        resumo = gravar_fechamento(sessao, user=user, operador_rotulo=rot)
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
    sessao.refresh_from_db()
    return JsonResponse({"ok": True, "resumo": resumo, "sessao": sessao_payload(sessao)})


@require_POST
def api_ciclica_cancelar(request, pk: int):
    err = _gate(request)
    if err:
        return err
    sessao = ContagemCiclicaSessao.objects.filter(pk=pk).first()
    if sessao is None:
        return JsonResponse({"ok": False, "erro": "Sessão não encontrada."}, status=404)
    try:
        cancelar_sessao(sessao)
    except ValueError as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=400)
    return JsonResponse({"ok": True})
