"""Views públicas e gestão do catálogo delivery GM Agro."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.catalogo_delivery_util import (
    ErroPedidoCatalogo,
    agrupar_itens_por_categoria,
    cards_home_catalogo,
    cliente_catalogo_json,
    criar_pedido_catalogo_delivery,
    listar_categorias_arvore,
    listar_itens_catalogo,
    obter_config_catalogo,
    salvar_foto_categoria,
    slugify_categoria,
)
from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp, extrair_whatsapp_digits
from produtos.models import CatalogoDeliveryCategoria, PedidoEntrega


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
    secoes = agrupar_itens_por_categoria(itens)
    home_cats = cards_home_catalogo(itens)
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
                "categoria_slug": i.get("categoria_slug") or "",
                "subcategoria_slug": i.get("subcategoria_slug") or "",
            }
            for i in itens
        ],
        ensure_ascii=False,
    )
    wa = "".join(c for c in (cfg.whatsapp_contato or "") if c.isdigit())
    return render(
        request,
        "produtos/catalogo/catalogo_delivery.html",
        {
            "config": cfg,
            "itens": itens,
            "secoes": secoes,
            "home_cats": home_cats,
            "enderecos": cfg.enderecos_exibir(),
            "whatsapp_digits": wa,
            "catalogo_json": catalogo_json,
            "catalogo_vazio": not itens and not home_cats,
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
    wa = "".join(c for c in (cfg.whatsapp_contato or "") if c.isdigit())
    return render(
        request,
        "produtos/catalogo/catalogo_pedido_ok.html",
        {"config": cfg, "pedido": pedido, "whatsapp_digits": wa},
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


@require_GET
def api_catalogo_categorias(request):
    """Lista categorias/sub para selects da aba Delivery (cadastro)."""
    return JsonResponse({"ok": True, "categorias": listar_categorias_arvore(so_ativas=True)})


@login_required(login_url="/admin/login/")
@user_passes_test(_staff, login_url="/admin/login/")
@require_POST
def api_catalogo_categoria_criar(request):
    """Cria categoria ou subcategoria sem sair do modal de produto (estilo iFood)."""
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    nome = (payload.get("nome") or request.POST.get("nome") or "").strip()[:80]
    if not nome:
        return JsonResponse({"ok": False, "erro": "Informe o nome."}, status=400)
    parent = None
    parent_raw = payload.get("parent_id") if "parent_id" in payload else request.POST.get("parent_id")
    if parent_raw not in (None, "", 0, "0"):
        try:
            parent = CatalogoDeliveryCategoria.objects.filter(
                pk=int(parent_raw), parent__isnull=True, ativo=True
            ).first()
        except (TypeError, ValueError):
            parent = None
        if parent is None:
            return JsonResponse(
                {"ok": False, "erro": "Categoria pai inválida. Escolha a categoria principal primeiro."},
                status=400,
            )
    try:
        ordem = int(payload.get("ordem") or request.POST.get("ordem") or 0)
    except (TypeError, ValueError):
        ordem = 0
    cat = CatalogoDeliveryCategoria.objects.create(
        nome=nome,
        slug=slugify_categoria(nome),
        ordem=max(0, min(ordem, 9999)),
        ativo=True,
        parent=parent,
    )
    return JsonResponse(
        {
            "ok": True,
            "categoria": {
                "id": cat.pk,
                "nome": cat.nome,
                "slug": cat.slug,
                "parent_id": cat.parent_id,
            },
            "categorias": listar_categorias_arvore(so_ativas=True),
        }
    )


@login_required(login_url="/admin/login/")
@user_passes_test(_staff, login_url="/admin/login/")
@require_POST
def api_catalogo_categoria_foto(request):
    """Foto do card da categoria (home do catálogo)."""
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
    try:
        pk = int(payload.get("id") or 0)
    except (TypeError, ValueError):
        pk = 0
    cat = CatalogoDeliveryCategoria.objects.filter(pk=pk, parent__isnull=True).first()
    if not cat:
        return JsonResponse({"ok": False, "erro": "Categoria não encontrada."}, status=404)
    if payload.get("remover"):
        salvar_foto_categoria(cat, "", "")
    else:
        salvar_foto_categoria(
            cat,
            str(payload.get("imagem_base64") or ""),
            str(payload.get("imagem_mime") or ""),
        )
    return JsonResponse(
        {
            "ok": True,
            "categorias": listar_categorias_arvore(so_ativas=True),
        }
    )


@login_required(login_url="/admin/login/")
@user_passes_test(_staff, login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def catalogo_gestao_view(request):
    cfg = obter_config_catalogo()
    msg = request.GET.get("msg") or ""
    erro = ""

    if request.method == "POST":
        acao = (request.POST.get("acao") or "salvar_loja").strip()

        if acao == "salvar_loja":
            cfg.nome_loja = (request.POST.get("nome_loja") or "GM Agro").strip()[:100] or "GM Agro"
            cfg.whatsapp_contato = "".join(
                c for c in (request.POST.get("whatsapp_contato") or "") if c.isdigit()
            )[:20]
            cfg.mensagem_boas_vindas = (request.POST.get("mensagem_boas_vindas") or "").strip()[:2000]
            cfg.area_entrega = (request.POST.get("area_entrega") or "").strip()[:300]
            cfg.rotulo_loja_1 = (request.POST.get("rotulo_loja_1") or "Centro").strip()[:80]
            cfg.endereco_loja_1 = (request.POST.get("endereco_loja_1") or "").strip()[:320]
            cfg.rotulo_loja_2 = (request.POST.get("rotulo_loja_2") or "Vila Elias").strip()[:80]
            cfg.endereco_loja_2 = (request.POST.get("endereco_loja_2") or "").strip()[:320]
            if cfg.endereco_loja_1:
                cfg.endereco_loja = cfg.endereco_loja_1
            cfg.cor_primaria = _hex_cor(request.POST.get("cor_primaria"), "#059669")
            cfg.cor_secundaria = _hex_cor(request.POST.get("cor_secundaria"), "#fff7ed")
            cfg.publicado = request.POST.get("publicado") in ("1", "on", "true", "True")
            cfg.save()
            return redirect("/catalogo/gestao/?msg=loja")

        if acao == "nova_categoria":
            nome = (request.POST.get("cat_nome") or "").strip()[:80]
            parent_raw = (request.POST.get("cat_parent") or "").strip()
            parent = None
            if parent_raw:
                try:
                    parent = CatalogoDeliveryCategoria.objects.filter(
                        pk=int(parent_raw), parent__isnull=True
                    ).first()
                except (TypeError, ValueError):
                    parent = None
            if not nome:
                erro = "Informe o nome da categoria."
            else:
                CatalogoDeliveryCategoria.objects.create(
                    nome=nome,
                    slug=slugify_categoria(nome),
                    ordem=int(request.POST.get("cat_ordem") or 0) or 0,
                    ativo=True,
                    parent=parent,
                )
                return redirect("/catalogo/gestao/?msg=cat")

        if acao == "toggle_categoria":
            try:
                pk = int(request.POST.get("cat_id") or 0)
            except (TypeError, ValueError):
                pk = 0
            cat = get_object_or_404(CatalogoDeliveryCategoria, pk=pk)
            cat.ativo = not cat.ativo
            cat.save(update_fields=["ativo"])
            return redirect("/catalogo/gestao/?msg=cat")

        if acao == "excluir_categoria":
            try:
                pk = int(request.POST.get("cat_id") or 0)
            except (TypeError, ValueError):
                pk = 0
            CatalogoDeliveryCategoria.objects.filter(pk=pk).delete()
            return redirect("/catalogo/gestao/?msg=cat")

        if acao == "foto_categoria":
            try:
                pk = int(request.POST.get("cat_id") or 0)
            except (TypeError, ValueError):
                pk = 0
            cat = CatalogoDeliveryCategoria.objects.filter(pk=pk, parent__isnull=True).first()
            if not cat:
                erro = "Categoria inválida para foto."
            elif request.POST.get("remover_foto"):
                salvar_foto_categoria(cat, "", "")
                return redirect("/catalogo/gestao/?msg=foto")
            else:
                f = request.FILES.get("cat_foto")
                if not f:
                    erro = "Escolha uma imagem."
                elif f.size > 700 * 1024:
                    erro = "Foto muito grande (máx. ~700 KB)."
                else:
                    import base64

                    raw = f.read()
                    mime = (getattr(f, "content_type", None) or "image/jpeg")[:40]
                    salvar_foto_categoria(cat, base64.b64encode(raw).decode("ascii"), mime)
                    return redirect("/catalogo/gestao/?msg=foto")

    if msg == "loja":
        msg = "Dados da loja salvos."
    elif msg == "cat":
        msg = "Categorias atualizadas."
    elif msg == "foto":
        msg = "Foto da categoria salva."
    else:
        msg = ""

    qtd = len(listar_itens_catalogo(incluir_ocultos_estoque=True))
    return render(
        request,
        "produtos/catalogo/catalogo_gestao.html",
        {
            "config": cfg,
            "msg": msg,
            "erro": erro,
            "qtd_produtos_marcados": qtd,
            "categorias": listar_categorias_arvore(so_ativas=False),
            "categorias_raiz": listar_categorias_arvore(so_ativas=True),
        },
    )
