"""Fila de códigos bipados sem cadastro — Ajuste Mobile (Postgres)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from produtos.models import AjusteCodigoPendenteAgro


def _sessao_ajuste_ok(request) -> bool:
    return bool(
        str(request.session.get("ajuste_mobile_operador") or "").strip()
        or request.session.get("ajuste_mobile_user_id")
    )


def _pode_gravar(request) -> bool:
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        return True
    return _sessao_ajuste_ok(request)


def _operador_rotulo(request) -> str:
    rotulo = str(request.session.get("ajuste_mobile_operador") or "").strip()
    if rotulo:
        return rotulo[:120]
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        full = (u.get_full_name() or "").strip()
        if full:
            return full[:120]
        return (getattr(u, "username", "") or "").strip()[:120]
    return ""


def _usuario_fk(request):
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        return u
    uid = request.session.get("ajuste_mobile_user_id")
    if uid:
        from django.contrib.auth import get_user_model

        return get_user_model().objects.filter(pk=uid).first()
    return None


@require_POST
def api_ajuste_codigo_pendente_criar(request):
    if not _pode_gravar(request):
        return JsonResponse(
            {"ok": False, "erro": "Sessão de ajuste expirada. Entre com o PIN de novo."},
            status=403,
        )
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = {}
        if request.POST:
            data = {
                "codigo_bipado": request.POST.get("codigo_bipado"),
                "produto_externo_id": request.POST.get("produto_externo_id"),
                "nome_produto": request.POST.get("nome_produto"),
            }

    codigo = str(data.get("codigo_bipado") or "").strip()[:64]
    pid = str(data.get("produto_externo_id") or "").strip()[:100]
    nome = str(data.get("nome_produto") or "").strip()[:255]
    if len(codigo) < 3:
        return JsonResponse({"ok": False, "erro": "Código inválido."}, status=400)
    if not pid:
        return JsonResponse({"ok": False, "erro": "Produto obrigatório."}, status=400)

    obj = AjusteCodigoPendenteAgro.objects.create(
        codigo_bipado=codigo,
        produto_externo_id=pid,
        nome_produto=nome,
        operador=_operador_rotulo(request),
        usuario=_usuario_fk(request),
        status=AjusteCodigoPendenteAgro.STATUS_PENDENTE,
    )
    return JsonResponse({"ok": True, "id": obj.pk})


@login_required(login_url="/admin/login/")
@require_POST
def api_ajuste_codigo_pendente_status(request, pk: int):
    obj = get_object_or_404(AjusteCodigoPendenteAgro, pk=pk)
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = {"status": request.POST.get("status")}
    st = str(data.get("status") or "").strip().lower()
    allowed = {
        AjusteCodigoPendenteAgro.STATUS_PENDENTE,
        AjusteCodigoPendenteAgro.STATUS_FEITO,
        AjusteCodigoPendenteAgro.STATUS_DESCARTADO,
    }
    if st not in allowed:
        return JsonResponse({"ok": False, "erro": "Status inválido."}, status=400)
    obj.status = st
    obj.save(update_fields=["status"])
    return JsonResponse({"ok": True, "id": obj.pk, "status": obj.status})


@login_required(login_url="/admin/login/")
@ensure_csrf_cookie
@never_cache
@require_GET
def ajuste_codigos_pendentes_lista_view(request):
    status_f = str(request.GET.get("status") or "pendente").strip().lower()
    qs = AjusteCodigoPendenteAgro.objects.all()
    if status_f in (
        AjusteCodigoPendenteAgro.STATUS_PENDENTE,
        AjusteCodigoPendenteAgro.STATUS_FEITO,
        AjusteCodigoPendenteAgro.STATUS_DESCARTADO,
    ):
        qs = qs.filter(status=status_f)
    total_pendente = AjusteCodigoPendenteAgro.objects.filter(
        status=AjusteCodigoPendenteAgro.STATUS_PENDENTE
    ).count()
    return render(
        request,
        "produtos/ajuste_codigos_pendentes_lista.html",
        {
            "rows": list(qs[:300]),
            "status_filtro": status_f,
            "total_pendente": total_pendente,
            "csrf_token_value": request.META.get("CSRF_COOKIE") or "",
        },
    )
