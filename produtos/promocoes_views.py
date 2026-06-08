"""Views e APIs da tela de promoções."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal, InvalidOperation

from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from .models import PromocaoAgro, PromocaoProdutoAgro
from .promocoes_util import (
    EMPRESAS_PROMOCAO,
    buscar_produtos_para_promocao,
    buscar_promocoes_pdv_ativas,
    empresas_promocao_labels,
    promocao_eh_permanente,
    promocao_tipo_label,
)


def _parse_decimal(val, *, allow_none=True):
    if val is None or (isinstance(val, str) and not str(val).strip()):
        if allow_none:
            return None
        return Decimal("0")
    s = str(val).strip().replace("R$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        if allow_none:
            return None
        return Decimal("0")


def _parse_date(val) -> date | None:
    if not val:
        return None
    s = str(val).strip()
    if len(s) == 10 and s[4] == "-":
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None
    parts = s.split("/")
    if len(parts) == 3:
        try:
            d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
            return date(y, m, d)
        except (ValueError, TypeError):
            return None
    return None


def _promocao_form_context(promo: PromocaoAgro | None = None):
    produtos = []
    if promo:
        for p in promo.produtos.all():
            produtos.append(
                {
                    "produto_externo_id": p.produto_externo_id,
                    "codigo": p.codigo,
                    "nome_produto": p.nome_produto,
                    "preco_padrao": float(p.preco_padrao or 0),
                    "preco_promocional": float(p.preco_promocional or 0)
                    if p.preco_promocional is not None
                    else None,
                }
            )
    initial = {
        "id": promo.pk if promo else None,
        "nome": promo.nome if promo else "",
        "tipo": promo.tipo if promo else PromocaoAgro.Tipo.LEVE_PAGUE,
        "qtd_x": float(promo.qtd_x) if promo and promo.qtd_x is not None else None,
        "preco_y": float(promo.preco_y) if promo and promo.preco_y is not None else None,
        "data_inicio": promo.data_inicio.isoformat() if promo else "",
        "data_fim": promo.data_fim.isoformat() if promo and promo.data_fim else "",
        "permanente": promocao_eh_permanente(promo) if promo else False,
        "telas": promo.telas if promo else [],
        "empresas": promo.empresas if promo else ["centro"],
        "ativo": promo.ativo if promo else True,
        "produtos": produtos,
    }
    return {
        "promocao": promo,
        "promocao_initial": initial,
        "empresas_opcoes": EMPRESAS_PROMOCAO,
        "tipos_opcoes": [
            {"id": t.value, "label": t.label}
            for t in PromocaoAgro.Tipo
        ],
        "api_salvar_url": reverse("api_promocoes_salvar"),
        "api_buscar_produto_url": reverse("api_promocoes_buscar_produto"),
        "api_buscar_url": reverse("api_buscar_mobile"),
        "lista_url": reverse("promocoes_lista"),
    }


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def promocoes_lista_view(request):
    busca = (request.GET.get("q") or "").strip()
    qs = PromocaoAgro.objects.all().order_by("-data_inicio", "-pk")
    if busca:
        qs = qs.filter(nome__icontains=busca)
    rows = []
    for p in qs[:200]:
        rows.append(
            {
                "obj": p,
                "tipo_label": promocao_tipo_label(p.tipo),
                "empresas_label": empresas_promocao_labels(p.empresas),
                "qtd_produtos": p.produtos.count(),
                "fim_label": "Permanente" if promocao_eh_permanente(p) else p.data_fim.strftime("%d/%m/%Y") if p.data_fim else "—",
            }
        )
    return render(
        request,
        "produtos/promocoes_lista.html",
        {"promocoes": rows, "busca": busca},
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def promocoes_nova_view(request):
    return render(
        request,
        "produtos/promocoes_form.html",
        {**_promocao_form_context(None), "titulo": "Nova promoção"},
    )


@ensure_csrf_cookie
@login_required(login_url="/admin/login/")
def promocoes_editar_view(request, pk):
    promo = get_object_or_404(PromocaoAgro, pk=pk)
    return render(
        request,
        "produtos/promocoes_form.html",
        {**_promocao_form_context(promo), "titulo": f"Editar: {promo.nome}"},
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_promocoes_salvar(request):
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)

    pk = payload.get("id")
    nome = str(payload.get("nome") or "").strip()
    if not nome:
        return JsonResponse({"ok": False, "erro": "Informe o nome da promoção."}, status=400)

    tipo = str(payload.get("tipo") or "").strip()
    if tipo not in {t.value for t in PromocaoAgro.Tipo}:
        return JsonResponse({"ok": False, "erro": "Tipo de promoção inválido."}, status=400)

    data_inicio = _parse_date(payload.get("data_inicio"))
    permanente = bool(payload.get("permanente"))
    data_fim = None if permanente else _parse_date(payload.get("data_fim"))
    if not data_inicio:
        return JsonResponse({"ok": False, "erro": "Informe o início da promoção."}, status=400)
    if not permanente and not data_fim:
        return JsonResponse({"ok": False, "erro": "Informe o fim da promoção ou marque como permanente."}, status=400)
    if data_fim and data_fim < data_inicio:
        return JsonResponse({"ok": False, "erro": "A data de fim deve ser igual ou posterior ao início."}, status=400)

    empresas = payload.get("empresas") or []
    if not isinstance(empresas, list):
        empresas = []

    qtd_x = _parse_decimal(payload.get("qtd_x"))
    preco_y = _parse_decimal(payload.get("preco_y"))

    if tipo in (PromocaoAgro.Tipo.LEVE_PAGUE, PromocaoAgro.Tipo.ACIMA_UNIDADES):
        if not qtd_x or qtd_x <= 0:
            return JsonResponse({"ok": False, "erro": "Informe a quantidade X."}, status=400)
        if not preco_y or preco_y <= 0:
            return JsonResponse({"ok": False, "erro": "Informe o preço Y por unidade."}, status=400)

    raw_produtos = payload.get("produtos") or []
    if not isinstance(raw_produtos, list) or not raw_produtos:
        return JsonResponse({"ok": False, "erro": "Adicione ao menos um produto."}, status=400)

    with transaction.atomic():
        if pk:
            promo = get_object_or_404(PromocaoAgro, pk=pk)
        else:
            promo = PromocaoAgro()
        promo.nome = nome[:200]
        promo.tipo = tipo
        promo.qtd_x = qtd_x
        promo.preco_y = preco_y if tipo != PromocaoAgro.Tipo.VALOR_DIRETO else None
        promo.data_inicio = data_inicio
        promo.data_fim = data_fim
        promo.permanente = permanente
        promo.telas = []
        promo.empresas = [str(e).strip().lower() for e in empresas if str(e).strip()]
        promo.ativo = bool(payload.get("ativo", True))
        promo.save()

        promo.produtos.all().delete()
        novos = []
        for item in raw_produtos:
            if not isinstance(item, dict):
                continue
            pid = str(item.get("produto_externo_id") or item.get("id") or "").strip()
            if not pid:
                continue
            pp = _parse_decimal(item.get("preco_promocional"), allow_none=True)
            if tipo == PromocaoAgro.Tipo.VALOR_DIRETO and (pp is None or pp <= 0):
                return JsonResponse(
                    {
                        "ok": False,
                        "erro": f"Informe o preço promocional do produto {item.get('codigo') or pid}.",
                    },
                    status=400,
                )
            novos.append(
                PromocaoProdutoAgro(
                    promocao=promo,
                    produto_externo_id=pid[:64],
                    codigo=str(item.get("codigo") or "")[:80],
                    nome_produto=str(item.get("nome_produto") or item.get("nome") or "")[:300],
                    preco_padrao=_parse_decimal(item.get("preco_padrao"), allow_none=True),
                    preco_promocional=pp if tipo == PromocaoAgro.Tipo.VALOR_DIRETO else None,
                )
            )
        if not novos:
            return JsonResponse({"ok": False, "erro": "Nenhum produto válido."}, status=400)
        PromocaoProdutoAgro.objects.bulk_create(novos)

    return JsonResponse(
        {
            "ok": True,
            "id": promo.pk,
            "redirect": reverse("promocoes_editar", args=[promo.pk]),
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_promocoes_buscar_produto(request):
    q = (request.GET.get("q") or "").strip()
    if len(q) < 2:
        return JsonResponse({"produtos": []})
    try:
        out = buscar_produtos_para_promocao(q, limit=24)
    except Exception:
        return JsonResponse({"produtos": [], "erro": "Falha na busca."}, status=500)
    return JsonResponse({"produtos": out})


@require_GET
def api_promocoes_ativas_pdv(request):
    """Regras vigentes para o PDV (público autenticado ou leitura leve — login opcional)."""
    empresa = (request.GET.get("empresa") or "centro").strip().lower()
    tela = (request.GET.get("tela") or "pdv").strip().lower()
    promo_map = buscar_promocoes_pdv_ativas(empresa=empresa, tela=tela)
    return JsonResponse({"ok": True, "empresa": empresa, "tela": tela, "promocoes": promo_map})
