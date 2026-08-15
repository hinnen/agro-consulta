"""APIs PDV — pedir transferência de produto da outra loja."""
from __future__ import annotations

import json
import logging

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

from estoque.models import SolicitacaoTransferenciaPdv
from estoque.solicitacao_pdv_util import (
    _usuario_request,
    aceitar,
    cancelar,
    criar_solicitacoes,
    listar,
    marcar_transferido,
    obter,
    recusar,
    resumo,
    serializar,
)
from produtos.pdv_deposito_util import resolver_deposito_request, rotulo_deposito

logger = logging.getLogger(__name__)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _deposito_req(request, payload=None) -> str:
    if payload and payload.get("deposito"):
        from produtos.pdv_deposito_util import normalizar_deposito

        return normalizar_deposito(payload.get("deposito"))
    return resolver_deposito_request(request)


@require_GET
def api_pdv_pedir_loja_resumo(request):
    dep = _deposito_req(request)
    data = resumo(dep)
    data["ok"] = True
    return JsonResponse(data)


@require_GET
def api_pdv_pedir_loja_lista(request):
    dep = _deposito_req(request)
    papel = str(request.GET.get("papel") or "recebidos").strip().lower()
    hist = str(request.GET.get("historico") or "").strip() in ("1", "true", "sim")
    try:
        limite = int(request.GET.get("limite") or 80)
    except (TypeError, ValueError):
        limite = 80
    itens = listar(dep, papel, incluir_historico=hist, limite=limite)
    meta = resumo(dep)
    meta["ok"] = True
    meta["papel"] = papel
    meta["itens"] = itens
    return JsonResponse(meta)


@require_POST
@csrf_protect
def api_pdv_pedir_loja_criar(request):
    payload = _payload(request)
    if payload is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    dep = _deposito_req(request, payload)
    usuario = _usuario_request(request)
    criados, err = criar_solicitacoes(
        payload.get("itens") or [],
        dep,
        usuario_label=usuario,
        observacao=str(payload.get("observacao") or ""),
    )
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    meta = resumo(dep)
    return JsonResponse(
        {
            "ok": True,
            "criados": criados,
            "n": len(criados or []),
            "mensagem": f"Pedido enviado para {rotulo_deposito(meta['outra_loja'])}.",
            **{k: meta[k] for k in meta},
        }
    )


@require_POST
@csrf_protect
def api_pdv_pedir_loja_acao(request, pk: int):
    payload = _payload(request) or {}
    row = obter(pk)
    if not row:
        return JsonResponse({"ok": False, "erro": "Pedido não encontrado."}, status=404)

    acao = str(payload.get("acao") or "").strip().lower()
    dep = _deposito_req(request, payload)
    usuario = _usuario_request(request)
    pin = str(payload.get("pin") or "").strip()

    if acao == "aceitar":
        out, err = aceitar(row, dep, usuario_label=usuario)
    elif acao == "recusar":
        out, err = recusar(row, dep, usuario_label=usuario)
    elif acao == "cancelar":
        out, err = cancelar(row, dep, usuario_label=usuario)
    elif acao == "transferir":
        if row.status not in (
            SolicitacaoTransferenciaPdv.STATUS_PENDENTE,
            SolicitacaoTransferenciaPdv.STATUS_ACEITO,
        ):
            return JsonResponse({"ok": False, "erro": "Este pedido já foi encerrado."}, status=400)
        if row.loja_origem != dep:
            return JsonResponse(
                {"ok": False, "erro": "Só a loja de origem transfere o estoque."},
                status=403,
            )
        from estoque.views import _rotulo_usuario_pin, _transferir_entre_depositos_exec

        rotulo_pin = _rotulo_usuario_pin(pin) if pin else ""
        out_tr = _transferir_entre_depositos_exec(
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
            invalidar_cache=True,
        )
        if not out_tr.get("ok"):
            status = int(out_tr.get("status") or 400)
            return JsonResponse(
                {"ok": False, "erro": out_tr.get("erro") or "Falha ao transferir."},
                status=status if status >= 400 else 400,
            )
        usuario_final = rotulo_pin or usuario
        if row.status == SolicitacaoTransferenciaPdv.STATUS_PENDENTE:
            aceitar(row, dep, usuario_label=usuario_final)
            row = obter(pk) or row
        out = marcar_transferido(row, usuario_label=usuario_final)
        err = ""
        meta = resumo(dep)
        return JsonResponse(
            {
                "ok": True,
                "item": out,
                "transferencia": {
                    "saldo_vila": out_tr.get("saldo_vila"),
                    "saldo_centro": out_tr.get("saldo_centro"),
                    "quantidade": out_tr.get("quantidade"),
                },
                **{k: meta[k] for k in meta},
            }
        )
    else:
        return JsonResponse({"ok": False, "erro": "Ação inválida."}, status=400)

    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    meta = resumo(dep)
    return JsonResponse({"ok": True, "item": out or serializar(row), **{k: meta[k] for k in meta}})
