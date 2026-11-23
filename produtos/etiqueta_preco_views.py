"""Lista do PDV: etiquetas de preço para trocar."""
from __future__ import annotations

import json

from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from produtos.etiqueta_preco_pendente_util import confirmar, itens_etiqueta, listar_pendentes, listar_trocadas
from produtos.pdv_deposito_util import resolver_deposito_request, rotulo_deposito


def _corpo(request) -> dict:
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {}


@require_http_methods(["GET", "POST"])
def api_pdv_etiqueta_preco(request):
    dep = resolver_deposito_request(request)
    if request.method == "GET":
        if request.GET.get("historico") == "1":
            hist = listar_trocadas(dep)
            return JsonResponse(
                {
                    "ok": True,
                    "deposito": dep,
                    "loja": rotulo_deposito(dep),
                    "total": len(hist),
                    "historico": hist,
                }
            )
        itens = listar_pendentes(dep)
        return JsonResponse(
            {
                "ok": True,
                "deposito": dep,
                "loja": rotulo_deposito(dep),
                "total": len(itens),
                "itens": itens,
            }
        )

    data = _corpo(request)
    acao = str(data.get("acao") or "").strip().lower()
    if acao == "ok":
        try:
            pk = int(data.get("id"))
        except (TypeError, ValueError):
            return JsonResponse({"ok": False, "erro": "Produto inválido."}, status=400)
        if not confirmar(pk, dep):
            return JsonResponse({"ok": False, "erro": "Esse item já saiu da lista."}, status=404)
        itens = listar_pendentes(dep)
        return JsonResponse({"ok": True, "total": len(itens), "itens": itens, "loja": rotulo_deposito(dep)})

    if acao == "etiquetas":
        from produtos.models import EtiquetaLoteAgro
        from produtos.views import (
            _etiquetas_lote_normalizar_itens_entrada,
            _etiquetas_lote_row,
            _etiquetas_lote_usuario,
        )

        brutos = itens_etiqueta(dep)
        itens = _etiquetas_lote_normalizar_itens_entrada(brutos)
        if not itens:
            return JsonResponse({"ok": False, "erro": "Nenhuma etiqueta pendente."}, status=400)
        lote = EtiquetaLoteAgro.objects.create(
            nome=f"Preço trocado · {rotulo_deposito(dep)} · {timezone.now():%d/%m %H:%M}",
            loja="fila",
            filtros_json={"origem": "fila", "motivo": "preco_gondola"},
            preset_id="gondola",
            status=EtiquetaLoteAgro.Status.ABERTO,
            itens_json=itens,
            cursor=0,
            ultima_folha_qtd=0,
            usuario=_etiquetas_lote_usuario(request),
        )
        return JsonResponse(
            {
                "ok": True,
                "lote": _etiquetas_lote_row(lote),
                "url": "/produtos/etiquetas/",
            }
        )

    return JsonResponse({"ok": False, "erro": "Ação inválida."}, status=400)
