"""Tabelas globais de % por forma de pagamento — tela e APIs."""
from __future__ import annotations

import json

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.precos_forma_pagamento_util import formas_pagamento_lista
from produtos.tabela_preco_forma_util import (
    aplicar_payload_tabela,
    listar_conflitos,
    mesclar_resolucoes,
    obter_ou_criar_duas,
    payload_pdv_tabelas,
    tabela_para_dict,
    validar_overlap_formas,
)


def _parse_body(request) -> dict:
    try:
        raw = request.body.decode("utf-8") if request.body else "{}"
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def tabelas_preco_forma_view(request):
    import json

    from django.utils.safestring import mark_safe

    obter_ou_criar_duas()
    return render(
        request,
        "produtos/tabelas_preco_forma.html",
        {
            "formas_pagamento_json": mark_safe(
                json.dumps(formas_pagamento_lista(), ensure_ascii=False)
            ),
        },
    )


@require_GET
def api_tabelas_preco_forma_estado(request):
    tabelas = [tabela_para_dict(t) for t in obter_ou_criar_duas()]
    conflitos = {}
    for t in obter_ou_criar_duas():
        conflitos[str(t.slot)] = listar_conflitos(t, limit=150)
    cats = sorted(
        {
            str(c).strip()
            for c in __import__(
                "produtos.models", fromlist=["ProdutoGestaoOverlayAgro"]
            ).ProdutoGestaoOverlayAgro.objects.exclude(categoria="")
            .values_list("categoria", flat=True)
            .distinct()[:800]
            if str(c or "").strip()
        }
    )
    return JsonResponse(
        {
            "ok": True,
            "tabelas": tabelas,
            "conflitos": conflitos,
            "formas_disponiveis": formas_pagamento_lista(),
            "categorias": cats,
        }
    )


@require_POST
def api_tabelas_preco_forma_salvar(request):
    data = _parse_body(request)
    itens = data.get("tabelas")
    if not isinstance(itens, list) or len(itens) < 1:
        return JsonResponse({"ok": False, "erro": "Envie as duas tabelas."}, status=400)
    err = validar_overlap_formas(itens)
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    by_slot = {int(t.slot): t for t in obter_ou_criar_duas()}
    for raw in itens:
        if not isinstance(raw, dict):
            continue
        try:
            slot = int(raw.get("slot") or 0)
        except (TypeError, ValueError):
            continue
        obj = by_slot.get(slot)
        if not obj:
            continue
        aplicar_payload_tabela(obj, raw)
        obj.save()
    return JsonResponse(
        {
            "ok": True,
            "tabelas": [tabela_para_dict(t) for t in obter_ou_criar_duas()],
        }
    )


@require_POST
def api_tabelas_preco_forma_resolucoes(request):
    data = _parse_body(request)
    try:
        slot = int(data.get("slot") or 0)
    except (TypeError, ValueError):
        slot = 0
    by_slot = {int(t.slot): t for t in obter_ou_criar_duas()}
    obj = by_slot.get(slot)
    if not obj:
        return JsonResponse({"ok": False, "erro": "Tabela inválida."}, status=400)
    itens = data.get("itens") if isinstance(data.get("itens"), list) else []
    # Atalho massa
    massa = str(data.get("massa") or "").strip().lower()
    if massa in ("tabela", "individual"):
        conflitos = listar_conflitos(obj, limit=500)
        itens = [{"produto_id": c["produto_id"], "preferencia": massa} for c in conflitos]
    n = mesclar_resolucoes(obj, itens)
    return JsonResponse(
        {
            "ok": True,
            "gravados": n,
            "conflitos": listar_conflitos(obj, limit=150),
        }
    )


@require_GET
def api_tabelas_preco_forma_pdv(request):
    return JsonResponse({"ok": True, **payload_pdv_tabelas()})


@require_http_methods(["GET"])
def api_tabelas_preco_forma_buscar_produto(request):
    """Busca leve para vetar itens (overlay PG)."""
    from produtos.models import ProdutoGestaoOverlayAgro

    q = str(request.GET.get("q") or "").strip()
    if len(q) < 2:
        return JsonResponse({"ok": True, "itens": []})
    qs = ProdutoGestaoOverlayAgro.objects.all()
    if q.isdigit():
        qs = qs.filter(produto_externo_id__icontains=q) | qs.filter(
            codigo_nfe__icontains=q
        )
    else:
        qs = qs.filter(nome__icontains=q) | qs.filter(codigo_nfe__icontains=q)
    rows = []
    for ov in qs.order_by("nome")[:30]:
        rows.append(
            {
                "id": str(ov.produto_externo_id),
                "nome": str(ov.nome or ov.produto_externo_id)[:120],
                "codigo": str(ov.codigo_nfe or "")[:40],
                "categoria": str(ov.categoria or "")[:80],
            }
        )
    return JsonResponse({"ok": True, "itens": rows})
