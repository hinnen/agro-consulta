"""Views públicas e gestão do catálogo delivery GM Agro."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.catalogo_delivery_util import (
    ErroPedidoCatalogo,
    cliente_catalogo_json,
    criar_pedido_catalogo_delivery,
    listar_itens_catalogo,
    obter_config_catalogo,
)
from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp, extrair_whatsapp_digits
from produtos.models import PedidoEntrega


def _staff(u):
    return bool(u and u.is_authenticated and u.is_staff)


def _hex_cor(valor: str, padrao: str) -> str:
    v = (valor or "").strip()
    if len(v) == 7 and v.startswith("#"):
        return v
    return padrao


@ensure_csrf_cookie
def catalogo_delivery_view(request):
    cfg = obter_config_catalogo()
    if not cfg.publicado and not _staff(request.user):
        return render(
            request,
            "produtos/catalogo/catalogo_indisponivel.html",
            {"config": cfg},
        )
    itens = listar_itens_catalogo(incluir_ocultos_estoque=False)
    catalogo_json = json.dumps(
        [
            {
                "id": i["id"],
                "nome": i["nome"],
                "descricao": i["descricao"],
                "preco": i["preco"],
                "marca": i["marca"],
                "peso_texto": i["peso_texto"],
                "destaque": i["destaque"],
                "imagem": i["imagem"],
            }
            for i in itens
        ],
        ensure_ascii=False,
    )
    return render(
        request,
        "produtos/catalogo/catalogo_delivery.html",
        {
            "config": cfg,
            "itens": itens,
            "catalogo_json": catalogo_json,
            "catalogo_vazio": not itens,
            "eh_staff": _staff(request.user),
        },
    )


@ensure_csrf_cookie
def catalogo_pedido_ok_view(request):
    cfg = obter_config_catalogo()
    pk = request.GET.get("id")
    pedido = None
    try:
        pedido = PedidoEntrega.objects.filter(pk=int(pk), origem="catalogo").first()
    except (TypeError, ValueError):
        pedido = None
    return render(
        request,
        "produtos/catalogo/catalogo_pedido_ok.html",
        {"config": cfg, "pedido": pedido},
    )


@require_POST
def api_catalogo_pedido(request):
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    cfg = obter_config_catalogo()
    if not cfg.publicado and not _staff(request.user):
        return JsonResponse({"ok": False, "erro": "Catálogo indisponível no momento."}, status=403)
    try:
        pedido = criar_pedido_catalogo_delivery(payload if isinstance(payload, dict) else {})
    except ErroPedidoCatalogo as e:
        return JsonResponse({"ok": False, "erro": e.mensagem}, status=e.status)
    return JsonResponse(
        {
            "ok": True,
            "id": pedido.pk,
            "redirect": f"/catalogo/pedido-ok/?id={pedido.pk}",
        }
    )


@require_GET
def api_catalogo_saldo_produto(request):
    """Saldo operacional de 1 produto — aba Delivery no cadastro."""
    pid = str(request.GET.get("id") or "").strip()
    if not pid:
        return JsonResponse({"ok": False, "erro": "id obrigatório"}, status=400)
    try:
        from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro

        m = mapa_saldos_operacionais_agro([pid])
        info = m.get(pid) or {}
        sc = float(info.get("centro") or 0)
        sv = float(info.get("vila") or 0)
        return JsonResponse(
            {
                "ok": True,
                "saldo_centro": round(sc, 3),
                "saldo_vila": round(sv, 3),
                "saldo_total": round(sc + sv, 3),
            }
        )
    except Exception:
        return JsonResponse({"ok": False, "erro": "Falha ao ler saldo"}, status=500)


@require_GET
def api_catalogo_cliente(request):
    digits = extrair_whatsapp_digits(request.GET.get("telefone") or "")
    if len(digits) < 10:
        return JsonResponse({"ok": False, "erro": "Telefone inválido"}, status=400)
    cli = cliente_agro_por_whatsapp(digits)
    if not cli:
        return JsonResponse({"ok": True, "encontrado": False})
    return JsonResponse({"ok": True, "encontrado": True, "cliente": cliente_catalogo_json(cli)})


@login_required(login_url="/admin/login/")
@user_passes_test(_staff, login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def catalogo_gestao_view(request):
    cfg = obter_config_catalogo()
    msg = ""
    if request.method == "POST":
        cfg.nome_loja = (request.POST.get("nome_loja") or "GM Agro").strip()[:100] or "GM Agro"
        cfg.whatsapp_contato = "".join(
            c for c in (request.POST.get("whatsapp_contato") or "") if c.isdigit()
        )[:20]
        cfg.mensagem_boas_vindas = (request.POST.get("mensagem_boas_vindas") or "").strip()[:2000]
        cfg.area_entrega = (request.POST.get("area_entrega") or "").strip()[:300]
        cfg.endereco_loja = (request.POST.get("endereco_loja") or "").strip()[:320]
        cfg.cor_primaria = _hex_cor(request.POST.get("cor_primaria"), "#059669")
        cfg.cor_secundaria = _hex_cor(request.POST.get("cor_secundaria"), "#fff7ed")
        cfg.publicado = request.POST.get("publicado") in ("1", "on", "true", "True")
        cfg.save()
        msg = "Salvo."
        return redirect("catalogo_gestao")
    qtd = len(listar_itens_catalogo(incluir_ocultos_estoque=True))
    return render(
        request,
        "produtos/catalogo/catalogo_gestao.html",
        {"config": cfg, "msg": msg, "qtd_produtos_marcados": qtd},
    )
