"""APIs PDV — pedido de transferência entre lojas."""
from __future__ import annotations

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from produtos.pdv_pedido_transferencia_util import (
    alterar_status,
    badge_count,
    criar_solicitacoes,
    deposito_pdv,
    listar_painel,
    marcar_transferido,
    pedidos_para_transferir,
    rotulo_loja,
    rotulo_usuario,
    serializar,
)

logger = logging.getLogger(__name__)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _ids(raw) -> list[int]:
    if raw is None:
        return []
    if isinstance(raw, (int, float, str)):
        raw = [raw]
    out = []
    if not isinstance(raw, list):
        return out
    for v in raw:
        try:
            n = int(v)
        except (TypeError, ValueError):
            continue
        if n > 0:
            out.append(n)
    return out


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_pedido_transferencia_resumo(request):
    dep = deposito_pdv(request)
    n = badge_count(dep)
    return JsonResponse(
        {
            "ok": True,
            "deposito": dep,
            "deposito_label": rotulo_loja(dep),
            "badge": n,
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_pedido_transferencia_lista(request):
    return JsonResponse(listar_painel(request))


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_pedido_transferencia_criar(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    criados, err = criar_solicitacoes(request, payload.get("itens"))
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    dep = deposito_pdv(request)
    return JsonResponse(
        {
            "ok": True,
            "mensagem": f"{len(criados)} pedido(s) enviado(s) para {rotulo_loja(criados[0].loja_origem)}."
            if criados
            else "Pedido enviado.",
            "itens": [serializar(r, dep) for r in criados],
            **listar_painel(request),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_pedido_transferencia_status(request, pk: int):
    payload = _payload(request) or {}
    acao = str(payload.get("acao") or "").strip().lower()
    row, err = alterar_status(request, pk, acao)
    if err:
        status = 404 if "não encontrado" in err.lower() else 400
        return JsonResponse({"ok": False, "erro": err}, status=status)
    dep = deposito_pdv(request)
    return JsonResponse(
        {
            "ok": True,
            "item": serializar(row, dep),
            **listar_painel(request),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_pedido_transferencia_transferir(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    pin = str(payload.get("pin") or "").strip()
    if not pin:
        return JsonResponse({"ok": False, "erro": "Informe o PIN."}, status=400)
    pks = _ids(payload.get("ids") or payload.get("id"))
    rows, err = pedidos_para_transferir(request, pks or None)
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)

    from estoque.views import _rotulo_usuario_pin, _transferir_entre_depositos_exec
    from produtos.views import _invalidar_caches_apos_ajuste_pin

    usuario = _rotulo_usuario_pin(pin) or rotulo_usuario(request)
    ok_rows = []
    falhas = []
    for row in rows:
        out = _transferir_entre_depositos_exec(
            request,
            pin,
            row.produto_externo_id,
            row.quantidade,
            row.nome_produto,
            row.codigo_interno,
            f"pedido PDV #{row.pk}",
            origem=row.loja_origem,
            destino=row.loja_destino,
            registrar_historico=True,
            invalidar_cache=False,
        )
        if out.get("ok"):
            marcar_transferido(row, usuario)
            ok_rows.append(row.pk)
        else:
            falhas.append(
                {
                    "id": row.pk,
                    "nome": row.nome_produto,
                    "erro": out.get("erro") or "Falha ao transferir.",
                }
            )
            if out.get("status") == 403:
                return JsonResponse(
                    {
                        "ok": False,
                        "erro": out.get("erro") or "PIN incorreto.",
                        "transferidos": ok_rows,
                        "falhas": falhas,
                    },
                    status=403,
                )
    if ok_rows:
        try:
            _invalidar_caches_apos_ajuste_pin()
        except Exception:
            logger.exception("invalidar cache após pedido transferência PDV")
    if not ok_rows:
        return JsonResponse(
            {
                "ok": False,
                "erro": (falhas[0].get("erro") if falhas else "Não foi possível transferir."),
                "falhas": falhas,
            },
            status=400,
        )
    painel = listar_painel(request)
    msg = f"{len(ok_rows)} item(ns) transferido(s)."
    if falhas:
        msg += f" {len(falhas)} falhou."
    return JsonResponse(
        {
            "ok": True,
            "mensagem": msg,
            "transferidos": ok_rows,
            "falhas": falhas,
            **painel,
        }
    )
