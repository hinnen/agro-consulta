"""App celular — fotos de produto (hub /vendas/lojas/fotos/)."""

from __future__ import annotations

import json
import logging

from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.catalogo_agro import obter_produto_model
from produtos.catalogo_delivery_util import delivery_de_extras
from produtos.fotos_produto_pin_util import (
    exigir_operador_api,
    exigir_operador_html,
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
)
from produtos.fotos_produto_util import (
    MAX_SLOTS,
    apagar_slot_foto,
    bytes_e_mime_do_slot,
    contagem_fotos,
    delivery_com_extras,
    gravar_slot_foto,
    overlay_get_or_create,
    overlay_por_pid,
    urls_slots_presentes,
    versao_galeria,
)

logger = logging.getLogger(__name__)


def _payload_json(request) -> dict:
    if request.content_type and "application/json" in (request.content_type or ""):
        try:
            return json.loads(request.body.decode("utf-8") or "{}")
        except Exception:
            return {}
    return {k: request.POST.get(k) for k in request.POST.keys()}


def _nome_produto(pid: str, ov) -> str:
    if ov is not None and str(getattr(ov, "nome", "") or "").strip():
        return str(ov.nome).strip()[:200]
    p = obter_produto_model(pid)
    if p is not None and str(getattr(p, "nome", "") or "").strip():
        return str(p.nome).strip()[:200]
    return pid


@require_http_methods(["GET", "POST"])
@ensure_csrf_cookie
def fotos_produto_pin(request):
    if request.method == "GET":
        if operador_da_sessao(request) and request.GET.get("trocar") != "1":
            return redirect("fotos_produto_app")
        return render(
            request,
            "produtos/fotos_produto/pin.html",
            {"operador": operador_da_sessao(request)},
        )
    pin = str(request.POST.get("pin") or "").strip()
    ok, nome, err = gravar_operador_sessao(request, pin)
    if not ok:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest" or (
            request.content_type and "json" in (request.content_type or "")
        ):
            return JsonResponse({"ok": False, "erro": err}, status=403)
        return render(
            request,
            "produtos/fotos_produto/pin.html",
            {"erro": err, "operador": ""},
            status=403,
        )
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return JsonResponse({"ok": True, "operador": nome, "next": reverse("fotos_produto_app")})
    return redirect("fotos_produto_app")


@require_GET
def fotos_produto_logout(request):
    limpar_operador_sessao(request)
    return redirect("fotos_produto_pin")


@never_cache
@require_GET
@ensure_csrf_cookie
@exigir_operador_html
def fotos_produto_app(request):
    return render(
        request,
        "produtos/fotos_produto/app.html",
        {
            "operador": operador_da_sessao(request),
            "url_api_produto": reverse("api_fotos_produto_detalhe", kwargs={"produto_id": "__ID__"}),
            "url_api_foto": reverse("api_fotos_produto_slot", kwargs={"produto_id": "__ID__"}),
            "url_catalogo_slim": reverse("api_pdv_catalogo_slim"),
            "url_hub": reverse("vendas_lojas_hub"),
            "url_sair": reverse("fotos_produto_logout"),
            "url_pin": reverse("fotos_produto_pin"),
        },
    )


@require_GET
@never_cache
def produto_foto_bytes(request, produto_id: str):
    """Bytes da foto (slot i=0..3). Público leve — só a imagem, sem dados sensíveis."""
    pid = str(produto_id or "").strip()[:64]
    try:
        indice = int(request.GET.get("i") or 0)
    except (TypeError, ValueError):
        indice = 0
    if indice < 0 or indice >= MAX_SLOTS:
        return HttpResponse(status=404)
    ov = overlay_por_pid(pid)
    if ov is None:
        return HttpResponse(status=404)
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras), processar_extras=False)
    raw, mime = bytes_e_mime_do_slot(d, indice)
    if not raw:
        return HttpResponse(status=404)
    resp = HttpResponse(raw, content_type=mime or "image/jpeg")
    resp["Cache-Control"] = "public, max-age=3600"
    return resp


@require_GET
@exigir_operador_api
def api_fotos_produto_detalhe(request, produto_id: str):
    pid = str(produto_id or "").strip()[:64]
    if not pid or pid == "__ID__":
        return JsonResponse({"ok": False, "erro": "Produto inválido."}, status=400)
    ov = overlay_por_pid(pid)
    d = delivery_com_extras(
        delivery_de_extras(getattr(ov, "cadastro_extras", None) if ov else {}),
        processar_extras=False,
    )
    v = versao_galeria(d, getattr(ov, "atualizado_em", None) if ov else None)
    return JsonResponse(
        {
            "ok": True,
            "produto_id": pid,
            "nome": _nome_produto(pid, ov),
            "n_fotos": contagem_fotos(d),
            "slots": urls_slots_presentes(pid, d, v=v),
            "versao": v,
            "operador": operador_da_sessao(request),
        }
    )


@require_http_methods(["POST", "DELETE"])
@exigir_operador_api
def api_fotos_produto_slot(request, produto_id: str):
    pid = str(produto_id or "").strip()[:64]
    if not pid or pid == "__ID__":
        return JsonResponse({"ok": False, "erro": "Produto inválido."}, status=400)
    payload = _payload_json(request)
    try:
        indice = int(payload.get("indice") if payload.get("indice") is not None else request.GET.get("i") or 0)
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "Índice inválido."}, status=400)
    if indice < 0 or indice >= MAX_SLOTS:
        return JsonResponse({"ok": False, "erro": "Slot 0 a 3."}, status=400)

    ov = overlay_get_or_create(pid)
    if ov is None:
        return JsonResponse({"ok": False, "erro": "Não foi possível abrir o cadastro."}, status=500)

    if request.method == "DELETE" or str(payload.get("acao") or "").lower() in ("apagar", "delete", "remover"):
        ok, err = apagar_slot_foto(ov, indice=indice)
        if not ok:
            return JsonResponse({"ok": False, "erro": err}, status=400)
    else:
        b64 = str(payload.get("imagem_base64") or payload.get("foto") or "").strip()
        mime = str(payload.get("imagem_mime") or "image/jpeg").strip()[:40]
        ok, err = gravar_slot_foto(ov, indice=indice, imagem_base64=b64, imagem_mime=mime)
        if not ok:
            return JsonResponse({"ok": False, "erro": err}, status=400)

    ov.refresh_from_db()
    d = delivery_com_extras(delivery_de_extras(ov.cadastro_extras), processar_extras=False)
    v = versao_galeria(d, ov.atualizado_em)
    return JsonResponse(
        {
            "ok": True,
            "produto_id": pid,
            "nome": _nome_produto(pid, ov),
            "n_fotos": contagem_fotos(d),
            "slots": urls_slots_presentes(pid, d, v=v),
            "versao": v,
            "operador": getattr(request, "fotos_operador", "") or operador_da_sessao(request),
        }
    )
