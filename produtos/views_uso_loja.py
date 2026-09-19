"""APIs PDV — Uso loja (saída estoque consumo interno)."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.caixa_util import (
    obter_sessao_caixa_aberta_request,
    operador_label_de_pin,
    usuario_django_de_pin,
)
from produtos.models import UsoLojaRetiradaAgro
from produtos.uso_loja_util import (
    MOTIVO_LABEL,
    confirmar_retirada_uso_loja,
    estornar_retirada_uso_loja,
    resolver_deposito_uso_loja,
    serializar_retirada,
    totais_uso_loja_por_deposito,
)

logger = logging.getLogger(__name__)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


@login_required(login_url="/entrar/")
@require_GET
def api_pdv_uso_loja_meta(request):
    from produtos.pdv_deposito_util import bootstrap_deposito, trava_loja_por_caixa
    from rh.models import Funcionario

    boot = bootstrap_deposito(request)
    trava = trava_loja_por_caixa(request)
    caixa = obter_sessao_caixa_aberta_request(request)
    motivos = [{"value": k, "label": v} for k, v in MOTIVO_LABEL.items()]
    funcionarios = []
    qs = (
        Funcionario.objects.filter(ativo=True)
        .select_related("cliente_agro", "empresa")
        .order_by("nome_cache", "id")[:200]
    )
    for f in qs:
        nome = (f.nome_exibicao or "").strip()
        if not nome:
            continue
        ap = (f.apelido_interno or "").strip()
        label = f"{nome} ({ap})" if ap else nome
        funcionarios.append({"id": f.pk, "nome": label})
    return JsonResponse(
        {
            "ok": True,
            "caixa_aberto": bool(caixa),
            "deposito_travado": bool(trava),
            "deposito": (trava or {}).get("deposito") or boot.get("deposito") or "centro",
            "deposito_label": (trava or {}).get("depositoLabel")
            or boot.get("depositoLabel")
            or "Centro",
            "motivos": motivos,
            "funcionarios": funcionarios,
        }
    )


@login_required(login_url="/entrar/")
@require_POST
def api_pdv_uso_loja_confirmar(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)

    pin = str(payload.get("pin") or "").strip()
    ok_pin, label, err_pin = operador_label_de_pin(pin)
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin or label or "PIN inválido"}, status=400)
    user_dj = usuario_django_de_pin(pin)

    dep, err_dep = resolver_deposito_uso_loja(request, payload.get("deposito"))
    if err_dep:
        return JsonResponse({"ok": False, "erro": err_dep}, status=400)

    itens = payload.get("itens") or []
    if not isinstance(itens, list):
        return JsonResponse({"ok": False, "erro": "itens inválidos"}, status=400)

    sessao = obter_sessao_caixa_aberta_request(request)
    quem = str(payload.get("quem_levou") or "").strip()
    motivo = str(payload.get("motivo") or "").strip()
    obs = str(payload.get("observacao") or "").strip()
    cliente_brinde_id = payload.get("cliente_brinde_id") or payload.get("cliente_agro_pk")

    try:
        retirada, err = confirmar_retirada_uso_loja(
            deposito=dep,
            itens=itens,
            quem_levou=quem,
            motivo=motivo,
            operador_label=label,
            usuario_django=user_dj,
            sessao_caixa=sessao,
            observacao=obs,
            cliente_brinde_id=cliente_brinde_id,
        )
    except Exception as exc:
        logger.exception("api_pdv_uso_loja_confirmar")
        return JsonResponse({"ok": False, "erro": str(exc)[:300]}, status=500)

    if err or retirada is None:
        return JsonResponse({"ok": False, "erro": err or "Falha ao gravar"}, status=400)

    return JsonResponse(
        {
            "ok": True,
            "retirada": serializar_retirada(retirada),
            "mensagem": f"Saída #{retirada.pk} registrada.",
        }
    )


@login_required(login_url="/entrar/")
@require_GET
def api_pdv_uso_loja_historico(request):
    try:
        limit = min(max(int(request.GET.get("limit") or 40), 1), 100)
    except (TypeError, ValueError):
        limit = 40
    qs = (
        UsoLojaRetiradaAgro.objects.select_related("cliente_brinde")
        .prefetch_related("itens")
        .all()
        .order_by("-criado_em", "-pk")[:limit]
    )
    return JsonResponse(
        {
            "ok": True,
            "itens": [serializar_retirada(r) for r in qs],
            "totais": totais_uso_loja_por_deposito(),
        }
    )


@login_required(login_url="/entrar/")
@require_http_methods(["POST"])
def api_pdv_uso_loja_estornar(request, pk: int):
    payload = _payload(request)
    pin = str(payload.get("pin") or "").strip()
    ok_pin, label, err_pin = operador_label_de_pin(pin)
    if not ok_pin:
        return JsonResponse({"ok": False, "erro": err_pin or label or "PIN inválido"}, status=400)
    user_dj = usuario_django_de_pin(pin)

    retirada = (
        UsoLojaRetiradaAgro.objects.prefetch_related("itens")
        .filter(pk=pk)
        .first()
    )
    if retirada is None:
        return JsonResponse({"ok": False, "erro": "Saída não encontrada"}, status=404)

    try:
        ok, err = estornar_retirada_uso_loja(
            retirada=retirada,
            operador_label=label,
            usuario_django=user_dj,
        )
    except Exception as exc:
        logger.exception("api_pdv_uso_loja_estornar pk=%s", pk)
        return JsonResponse({"ok": False, "erro": str(exc)[:300]}, status=500)

    if not ok:
        return JsonResponse({"ok": False, "erro": err}, status=400)

    retirada.refresh_from_db()
    return JsonResponse(
        {
            "ok": True,
            "retirada": serializar_retirada(retirada),
            "mensagem": f"Saída #{pk} estornada.",
        }
    )
