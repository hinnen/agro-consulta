"""Fila de códigos bipados sem cadastro — Ajuste Mobile (Postgres)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from produtos.models import AjusteCodigoPendenteAgro, ProdutoGestaoOverlayAgro
from produtos.mongo_index_codigos import (
    mesclar_codigos_barras_opcionais_adicionar,
    normalizar_codigos_barras_opcionais,
)


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


def _so_digitos(s) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())


def _principal_codigo_produto(pid: str, ov: ProdutoGestaoOverlayAgro) -> str:
    """Principal do overlay; se vazio, tenta catálogo/Mongo (melhor esforço)."""
    principal = _so_digitos(getattr(ov, "codigo_barras", None))
    if principal:
        return principal
    try:
        from produtos.views import obter_conexao_mongo, _produto_doc_por_id_externo

        db, client_m = obter_conexao_mongo()
        doc = _produto_doc_por_id_externo(db, client_m, pid)
        if isinstance(doc, dict):
            for k in (
                "CodigoBarras",
                "EAN_NFe",
                "EAN",
                "CodigoDeBarras",
                "CodigoBarrasProduto",
                "GTIN",
            ):
                dig = _so_digitos(doc.get(k))
                if dig:
                    return dig
    except Exception:
        pass
    return ""


def _refresh_index_codigos_mongo(pid: str) -> None:
    try:
        from produtos.mongo_index_codigos import aplicar_index_codigos_no_mongo
        from produtos.views import obter_conexao_mongo, _produto_mongo_por_id_externo

        db, client_m = obter_conexao_mongo()
        if db is None or client_m is None:
            return
        doc = _produto_mongo_por_id_externo(db, client_m, pid)
        if isinstance(doc, dict) and doc.get("_id") is not None:
            aplicar_index_codigos_no_mongo(
                db,
                client_m.col_p,
                doc,
                produto_externo_id=pid,
            )
    except Exception:
        pass


def aplicar_codigo_pendente_no_cadastro(obj: AjusteCodigoPendenteAgro) -> dict:
    """
    Grava o bipado em ``cadastro_extras.codigos_barras_opcionais`` do overlay.
    Idempotente: se já estiver na lista, só confirma.
    """
    pid = str(obj.produto_externo_id or "").strip()
    dig = _so_digitos(obj.codigo_bipado)
    if not pid:
        return {"ok": False, "erro": "Produto obrigatório."}
    if len(dig) < 8:
        return {
            "ok": False,
            "erro": "Código precisa ter pelo menos 8 dígitos para ir no cadastro.",
        }

    ov, _created = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=pid,
        defaults={"cadastro_extras": {}},
    )
    principal = _principal_codigo_produto(pid, ov)
    if principal and dig == principal:
        return {
            "ok": True,
            "ja_era": True,
            "aviso": "Esse código já é o principal do produto.",
            "codigos_barras_opcionais": normalizar_codigos_barras_opcionais(
                (ov.cadastro_extras or {}).get("codigos_barras_opcionais"),
                excluir=principal,
            ),
        }

    ex = dict(ov.cadastro_extras) if isinstance(ov.cadastro_extras, dict) else {}
    antes = set(
        normalizar_codigos_barras_opcionais(
            ex.get("codigos_barras_opcionais"),
            excluir=principal,
        )
    )
    lista = mesclar_codigos_barras_opcionais_adicionar(
        ex,
        [dig],
        principal=principal or None,
    )
    if dig not in lista and dig not in antes:
        return {
            "ok": False,
            "erro": "Não foi possível gravar este código (formato inválido).",
        }

    if lista:
        ex["codigos_barras_opcionais"] = lista
        ex.pop("codigos_barras_alternativos", None)
    else:
        ex.pop("codigos_barras_opcionais", None)
        ex.pop("codigos_barras_alternativos", None)

    ov.cadastro_extras = ex
    ov.save(update_fields=["cadastro_extras", "atualizado_em"])
    _refresh_index_codigos_mongo(pid)
    try:
        from produtos.views import CATALOGO_PDV_CACHE_ENTRY_KEY

        cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    except Exception:
        pass

    return {
        "ok": True,
        "ja_era": dig in antes,
        "codigos_barras_opcionais": lista,
    }


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

    cadastro_info = None
    if st == AjusteCodigoPendenteAgro.STATUS_FEITO:
        cadastro_info = aplicar_codigo_pendente_no_cadastro(obj)
        if not cadastro_info.get("ok"):
            return JsonResponse(
                {
                    "ok": False,
                    "erro": cadastro_info.get("erro") or "Falha ao gravar no cadastro.",
                },
                status=400,
            )

    obj.status = st
    obj.save(update_fields=["status"])
    payload = {"ok": True, "id": obj.pk, "status": obj.status}
    if cadastro_info:
        payload["cadastro"] = {
            "ja_era": bool(cadastro_info.get("ja_era")),
            "aviso": cadastro_info.get("aviso") or "",
            "codigos_barras_opcionais": cadastro_info.get("codigos_barras_opcionais") or [],
        }
    return JsonResponse(payload)


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
