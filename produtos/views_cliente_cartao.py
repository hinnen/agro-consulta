"""Impressão do cartão no caixa, bip no PDV e página do celular."""
from __future__ import annotations

import json

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
    aviso_racao,
    compras_do_cliente,
    contar_compras,
    fiado_em_aberto,
    gravar_aniversario,
    mais_levados,
    texto_aniversario,
    codigo_cartao_lido,
    eh_codigo_cartao,
    emitir_cartoes,
    mesmo_whatsapp,
    numero_curto,
    vincular,
)
from produtos.models import ClienteAgro


def _qr_svg(url: str) -> str:
    import qrcode
    import qrcode.image.svg

    imagem = qrcode.make(
        url,
        image_factory=qrcode.image.svg.SvgPathImage,
        box_size=8,
        border=1,
    )
    return imagem.to_string(encoding="unicode")


@login_required(login_url="/entrar/")
@require_http_methods(["GET", "POST"])
def caixa_cartao_imprimir(request):
    cartoes = []
    if request.method == "POST":
        try:
            qtd = int(request.POST.get("qtd") or 10)
        except (TypeError, ValueError):
            qtd = 10
        cartoes = emitir_cartoes(qtd)
    blocos = []
    for cartao in cartoes:
        url = request.build_absolute_uri(reverse("cliente_cartao_portal", args=[cartao.token]))
        blocos.append({
            "codigo": cartao.codigo,
            "numero": numero_curto(cartao.codigo),
            "qr": _qr_svg(url),
        })
    cortes = _cortes_guilhotina(len(blocos))
    return render(
        request,
        "produtos/caixa_cartao_cliente_imprimir.html",
        {
            "blocos": blocos,
            "tem_arte": bool(find("produtos/img/cartao_cliente_arte.png")),
            **cortes,
        },
    )


def _cortes_guilhotina(qtd: int) -> dict:
    """Linha só no encontro dos cartões, esticada até a borda da A4."""
    if qtd < 2:
        return {"folha_left": "19.4", "folha_top": "8.5", "corte_v": "", "cortes_h": []}
    colunas = 2
    linhas = (qtd + colunas - 1) // colunas
    largura = 85.6 * min(qtd, colunas)
    altura = 56.0 * linhas
    left = (210.0 - largura) / 2
    top = (297.0 - altura) / 2
    return {
        "folha_left": f"{left:.2f}",
        "folha_top": f"{top:.2f}",
        "corte_v": f"{left + 85.6:.2f}",
        "cortes_h": [f"{top + 56.0 * i:.2f}" for i in range(1, linhas)],
    }


@login_required(login_url="/entrar/")
@require_POST
def api_pdv_cartao_cliente(request):
    from produtos.views import _linha_clienteagro_pdv

    try:
        data = json.loads(request.body or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "Não entendi o bip."}, status=400)
    bruto = codigo_cartao_lido(str(data.get("codigo") or ""))
    if not bruto:
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
    liberado = (request.session.get(SESSAO_OK) or "") == token
    aviso_niver = ""
    tela = (request.GET.get("tela") or "").strip()
    if request.method == "POST" and not liberado and "whatsapp" in request.POST:
        if mesmo_whatsapp(cliente.whatsapp, request.POST.get("whatsapp") or ""):
            request.session[SESSAO_OK] = token
            liberado = True
        else:
            aviso = "Esse WhatsApp não é o do cadastro. Fale com a loja para corrigir."
    if request.method == "POST" and liberado and "aniversario_dia" in request.POST:
        aviso_niver = gravar_aniversario(
            cliente,
            request.POST.get("aniversario_dia") or "",
            request.POST.get("aniversario_mes") or "",
        )
        cliente.refresh_from_db()
        tela = "aniversario" if aviso_niver else ""
    if not liberado:
        tela = ""
    compras = compras_do_cliente(cliente) if liberado and tela == "compras" else []
    qtd_compras = contar_compras(cliente) if liberado else 0
    levados = mais_levados(cliente) if liberado else []
    racoes = aviso_racao(cliente) if liberado else []
    fiado = fiado_em_aberto(cliente) if liberado else None
    niver = texto_aniversario(cliente) if liberado else {"preenchido": False, "frase": "", "aviso": ""}
    from urllib.parse import quote

    from produtos.models import CatalogoDeliveryConfig

    cfg = CatalogoDeliveryConfig.objects.order_by("pk").first()
    wa = "".join(c for c in ((cfg.whatsapp_contato if cfg else "") or "") if c.isdigit())
    if wa and not wa.startswith("55"):
        wa = "55" + wa
    curto = numero_curto(cartao.codigo)
    texto = (
        f"Olá, tentei ver as compras do meu cartão {curto} "
        "e o WhatsApp não bateu com o cadastro. Pode conferir?"
    )
    zap_url = "https://wa.me/" + (wa + "?" if wa else "?") + "text=" + quote(texto)
    return render(
        request,
        "produtos/cliente_cartao_portal.html",
        {
            "cliente": cliente,
            "liberado": liberado,
            "aviso": aviso,
            "compras": compras,
            "qtd_compras": qtd_compras,
            "tela": tela,
            "levados": levados,
            "racoes": racoes,
            "fiado": fiado,
            "niver": niver,
            "aviso_niver": aviso_niver,
            "token": token,
            "zap_url": zap_url,
            "numero_curto": curto,
        },
    )
