"""Impressão do cartão no caixa, bip no PDV e página do celular."""
from __future__ import annotations

import json

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles.finders import find
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods, require_POST

from produtos.cliente_cartao_util import (
    SESSAO_OK,
    cartao_por_codigo,
    cartao_por_token,
    compras_do_cliente,
    confirmar_codigo,
    eh_codigo_cartao,
    emitir_cartoes,
    pedir_codigo_whatsapp,
    vincular,
)
from produtos.models import ClienteAgro


def _qr_svg(url: str) -> str:
    import qrcode
    import qrcode.image.svg

    imagem = qrcode.make(
        url,
        image_factory=qrcode.image.svg.SvgPathImage,
        box_size=6,
        border=1,
    )
    return imagem.to_string(encoding="unicode")


@login_required(login_url="/entrar/")
@require_http_methods(["GET", "POST"])
def caixa_cartao_imprimir(request):
    cartoes = []
    if request.method == "POST":
        try:
            qtd = int(request.POST.get("qtd") or 8)
        except (TypeError, ValueError):
            qtd = 8
        cartoes = emitir_cartoes(qtd)
    blocos = []
    for cartao in cartoes:
        url = request.build_absolute_uri(reverse("cliente_cartao_portal", args=[cartao.token]))
        blocos.append({"codigo": cartao.codigo, "qr": _qr_svg(url)})
    return render(
        request,
        "produtos/caixa_cartao_cliente_imprimir.html",
        {
            "blocos": blocos,
            "tem_arte": bool(find("produtos/img/cartao_cliente_arte.png")),
        },
    )


@login_required(login_url="/entrar/")
@require_POST
def api_pdv_cartao_cliente(request):
    from produtos.views import _linha_clienteagro_pdv

    try:
        data = json.loads(request.body or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "Não entendi o bip."}, status=400)
    bruto = str(data.get("codigo") or "")
    if not eh_codigo_cartao(bruto):
        return JsonResponse({"ok": False, "erro": "Não é cartão de cliente."}, status=400)
    cartao = cartao_por_codigo(bruto)
    if cartao is None:
        return JsonResponse({"ok": False, "erro": "Cartão não cadastrado. Imprima no caixa."}, status=404)
    if not cartao.ativo:
        return JsonResponse({"ok": False, "erro": "Este cartão foi cancelado. Use o novo."}, status=409)

    pk = data.get("cliente_agro_pk")
    cliente = None
    if pk not in (None, "", 0, "0"):
        try:
            cliente = ClienteAgro.objects.filter(pk=int(pk), ativo=True).first()
        except (TypeError, ValueError):
            cliente = None
        if cliente is None:
            return JsonResponse({"ok": False, "erro": "Cliente da tela não encontrado."}, status=404)

    if cliente is not None and cartao.cliente_id is None:
        vincular(cartao, cliente)
        return JsonResponse(
            {
                "ok": True,
                "acao": "vinculou",
                "mensagem": f"Cartão agora é de {cliente.nome}.",
                "cliente": _linha_clienteagro_pdv(cliente),
            }
        )
    if cliente is not None and cartao.cliente_id == cliente.pk:
        return JsonResponse(
            {
                "ok": True,
                "acao": "ja_dele",
                "mensagem": f"Este cartão já é de {cliente.nome}.",
                "cliente": _linha_clienteagro_pdv(cliente),
            }
        )
    if cliente is not None and cartao.cliente_id != cliente.pk:
        return JsonResponse(
            {"ok": False, "erro": "Este cartão já é de outra pessoa."},
            status=409,
        )
    if cartao.cliente_id is None:
        return JsonResponse(
            {
                "ok": True,
                "acao": "livre",
                "mensagem": "Cartão em branco. Busque o cliente e bipe de novo.",
            }
        )
    dono = cartao.cliente
    if dono is None or not dono.ativo:
        return JsonResponse({"ok": False, "erro": "Cliente deste cartão não está ativo."}, status=409)
    return JsonResponse(
        {
            "ok": True,
            "acao": "abriu",
            "mensagem": dono.nome,
            "cliente": _linha_clienteagro_pdv(dono),
        }
    )


def _portal_liberado(request, token: str) -> bool:
    return (request.session.get(SESSAO_OK) or "") == token


@ensure_csrf_cookie
@require_http_methods(["GET", "POST"])
def cliente_cartao_portal(request, token: str):
    cartao = cartao_por_token(token)
    if cartao is None or not cartao.ativo or cartao.cliente_id is None:
        return HttpResponse(
            "Este cartão ainda não abre cadastro. Peça para vincular na loja.",
            status=404,
            content_type="text/plain; charset=utf-8",
        )
    cliente = cartao.cliente
    aviso = ""
    if request.method == "POST":
        acao = (request.POST.get("acao") or "").strip()
        if acao == "pedir":
            _codigo, situacao = pedir_codigo_whatsapp(cartao, request.POST.get("whatsapp") or "")
            avisos = {
                "enviado": "Mandei o código no WhatsApp da loja.",
                "sem_conversa": "O número confere, mas não achei conversa no Zap da loja. Manda um oi para a loja e peça de novo.",
                "sem_whatsapp": "Este cadastro está sem WhatsApp. Peça para completar na loja.",
                "numero_diferente": "Este WhatsApp não é o do cadastro.",
                "invalido": "Cartão inválido.",
                "falha": "Não consegui mandar o código. Tente de novo.",
            }
            aviso = avisos.get(situacao, "Não deu para enviar.")
            if settings.DEBUG and getattr(settings, "CLIENTE_CARTAO_REVELAR_CODIGO", False):
                aviso = f"{aviso} CODE:{_codigo}"
        elif acao == "confirmar":
            if confirmar_codigo(cartao, request.POST.get("codigo") or ""):
                request.session[SESSAO_OK] = token
            else:
                aviso = "Código não confere ou já venceu."
    liberado = _portal_liberado(request, token)
    compras = compras_do_cliente(cliente) if liberado else []
    return render(
        request,
        "produtos/cliente_cartao_portal.html",
        {
            "cliente": cliente,
            "liberado": liberado,
            "aviso": aviso,
            "compras": compras,
            "token": token,
        },
    )
