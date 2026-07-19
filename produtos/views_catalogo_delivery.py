"""Views públicas e gestão do catálogo delivery GM Agro."""
from __future__ import annotations

import base64
import json

from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.catalogo_delivery_util import (
    ErroPedidoCatalogo,
    _strip_data_url,
    agrupar_itens_por_categoria,
    arvore_navegacao_catalogo,
    cards_home_catalogo,
    cliente_catalogo_json,
    comprimir_imagem_upload,
    criar_pedido_catalogo_delivery,
    listar_categorias_arvore,
    listar_itens_catalogo,
    montar_imagem_og_preview,
    obter_config_catalogo,
    opcoes_pai_categoria,
    salvar_foto_categoria,
    salvar_logo_loja,
    slugify_categoria,
)
from produtos.catalogo_geo_util import localizacao_de_latlng
from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp, extrair_whatsapp_digits
from produtos.models import CatalogoDeliveryCategoria, PedidoEntrega


def _staff(u):
    return bool(u and u.is_authenticated and u.is_staff)


def _hex_cor(valor: str, padrao: str) -> str:
    v = (valor or "").strip()
    if len(v) == 7 and v.startswith("#"):
        return v
    return padrao


def _catalogo_og_context(request, cfg) -> dict:
    """Meta Open Graph para preview no WhatsApp (og:image = URL https da logo)."""
    nome = (cfg.nome_loja or "Delivery").strip() or "Delivery"
    desc = (
        (cfg.mensagem_boas_vindas or "").strip()
        or (cfg.area_entrega or "").strip()
        or f"Peça pelo catálogo delivery da {nome}."
    )
    og_url = request.build_absolute_uri("/catalogo/")
    if og_url.startswith("http://") and request.META.get("HTTP_X_FORWARDED_PROTO") == "https":
        og_url = "https://" + og_url[len("http://") :]
    og_image_url = ""
    og_image_type = "image/jpeg"
    if (cfg.logo_base64 or "").strip():
        path = reverse("catalogo_og_image")
        # ?v= muda a URL → força Facebook/WhatsApp a baixar de novo (cache)
        bust = f"card2-{len(cfg.logo_base64)}"
        og_image_url = request.build_absolute_uri(f"{path}?v={bust}")
        if og_image_url.startswith("http://") and request.META.get("HTTP_X_FORWARDED_PROTO") == "https":
            og_image_url = "https://" + og_image_url[len("http://") :]
    return {
        "og_title": f"{nome} · Delivery",
        "og_description": desc[:220],
        "og_url": og_url,
        "og_image_url": og_image_url,
        "og_image_type": og_image_type,
        "og_image_width": 1200,
        "og_image_height": 630,
    }


@ensure_csrf_cookie
def catalogo_delivery_view(request):
    cfg = obter_config_catalogo()
    og = _catalogo_og_context(request, cfg)
    if not cfg.publicado and not _staff(request.user):
        return render(
            request,
            "produtos/catalogo/catalogo_indisponivel.html",
            {"config": cfg, **og},
        )
    itens = listar_itens_catalogo(incluir_ocultos_estoque=False)
    secoes = agrupar_itens_por_categoria(itens)
    home_cats = cards_home_catalogo(itens)
    arvore = arvore_navegacao_catalogo(itens)
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
    arvore_json = json.dumps(arvore, ensure_ascii=False)
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
            "arvore_json": arvore_json,
            "catalogo_vazio": not itens and not home_cats,
            "eh_staff": _staff(request.user),
            **og,
        },
    )


@require_GET
def catalogo_og_image_view(request):
    """Cartão 1200×630 com logo centralizada (sem cortar) — preview WhatsApp/Facebook."""
    cfg = obter_config_catalogo()
    b64 = (cfg.logo_base64 or "").strip()
    if not b64:
        return HttpResponse(status=404)
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        return HttpResponse(status=404)
    if not raw:
        return HttpResponse(status=404)
    cor = (cfg.cor_secundaria or "").strip() or "#ecfdf5"
    card = montar_imagem_og_preview(raw, cor_fundo=cor)
    if not card:
        # Fallback: logo crua (pior no crop, mas melhor que 404)
        mime = (cfg.logo_mime or "image/png").strip() or "image/png"
        if mime == "image/jpg":
            mime = "image/jpeg"
        resp = HttpResponse(raw, content_type=mime)
        resp["Cache-Control"] = "public, max-age=3600"
        return resp
    resp = HttpResponse(card, content_type="image/jpeg")
    resp["Cache-Control"] = "public, max-age=3600"
    return resp


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


@require_POST
def api_catalogo_localizacao(request):
    """GPS do cliente → Plus Code + endereço para entregas (igual cardápio FOOD)."""
    cfg = obter_config_catalogo()
    if not cfg.publicado and not _staff(request.user):
        return JsonResponse({"ok": False, "erro": "Catálogo indisponível."}, status=503)
    try:
        body = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"ok": False, "erro": "Dados inválidos."}, status=400)
    try:
        lat = float(body.get("lat"))
        lng = float(body.get("lng"))
    except (TypeError, ValueError):
        return JsonResponse({"ok": False, "erro": "Coordenadas inválidas."}, status=400)
    try:
        loc = localizacao_de_latlng(lat, lng)
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    except Exception:
        return JsonResponse({"ok": False, "erro": "Não foi possível obter o endereço."}, status=502)
    return JsonResponse({"ok": True, **loc})


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
            parent = (
                CatalogoDeliveryCategoria.objects.filter(pk=int(parent_raw), ativo=True)
                .select_related("parent")
                .first()
            )
        except (TypeError, ValueError):
            parent = None
        if parent is None:
            return JsonResponse(
                {"ok": False, "erro": "Categoria pai inválida."},
                status=400,
            )
        # Máx. 3 níveis: pai pode ser raiz (nível 1) ou sub (nível 2)
        if parent.parent_id and parent.parent and parent.parent.parent_id:
            return JsonResponse(
                {"ok": False, "erro": "Máximo 3 níveis (categoria → sub → sub-sub)."},
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
    """Foto do card da categoria (home do catálogo). Aceita JSON base64 ou multipart."""
    import base64

    pk = 0
    remover = False
    raw = b""
    mime_in = "image/jpeg"

    ct = (request.content_type or "").split(";")[0].strip().lower()
    if ct == "multipart/form-data" or request.FILES:
        try:
            pk = int(request.POST.get("id") or request.POST.get("cat_id") or 0)
        except (TypeError, ValueError):
            pk = 0
        remover = request.POST.get("remover") in ("1", "true", "True", "on")
        f = request.FILES.get("cat_foto") or request.FILES.get("foto") or request.FILES.get("file")
        if f and not remover:
            raw = f.read()
            mime_in = (getattr(f, "content_type", None) or "image/jpeg")[:40]
    else:
        try:
            payload = json.loads(request.body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JsonResponse({"ok": False, "erro": "JSON inválido"}, status=400)
        try:
            pk = int(payload.get("id") or 0)
        except (TypeError, ValueError):
            pk = 0
        remover = bool(payload.get("remover"))
        if not remover:
            b64_in = str(payload.get("imagem_base64") or "")
            b64_clean, mime_guess = _strip_data_url(b64_in)
            mime_in = str(payload.get("imagem_mime") or mime_guess or "image/jpeg")[:40]
            try:
                raw = base64.b64decode(b64_clean, validate=False)
            except Exception:
                return JsonResponse({"ok": False, "erro": "Base64 inválido."}, status=400)

    cat = CatalogoDeliveryCategoria.objects.filter(pk=pk, parent__isnull=True).first()
    if not cat:
        return JsonResponse(
            {"ok": False, "erro": "Categoria não encontrada (foto só na principal)."},
            status=404,
        )
    try:
        if remover:
            salvar_foto_categoria(cat, "", "")
            return JsonResponse({"ok": True, "imagem": "", "cat_id": cat.pk})

        if not raw:
            return JsonResponse({"ok": False, "erro": "Nenhuma imagem recebida."}, status=400)
        if len(raw) > 6 * 1024 * 1024:
            return JsonResponse({"ok": False, "erro": "Arquivo acima de 6 MB."}, status=400)

        raw_ok, mime = comprimir_imagem_upload(raw, max_lado=1000, qualidade=80)
        if not raw_ok or len(raw_ok) > 900 * 1024:
            return JsonResponse(
                {"ok": False, "erro": "Não deu para comprimir. Use JPG 800×600."},
                status=400,
            )
        b64 = base64.b64encode(raw_ok).decode("ascii")
        salvar_foto_categoria(cat, b64, mime)
        cat.refresh_from_db(fields=["imagem_base64", "imagem_mime"])
        if not (cat.imagem_base64 or "").strip():
            return JsonResponse({"ok": False, "erro": "Gravou vazio no banco."}, status=500)
        mime_out = (cat.imagem_mime or "image/jpeg").strip() or "image/jpeg"
        return JsonResponse(
            {
                "ok": True,
                "cat_id": cat.pk,
                "imagem": f"data:{mime_out};base64,{cat.imagem_base64}",
                "bytes": len(raw_ok),
            }
        )
    except Exception as exc:
        return JsonResponse(
            {"ok": False, "erro": f"{type(exc).__name__}: {exc}"[:200]},
            status=500,
        )


@login_required(login_url="/admin/login/")
@user_passes_test(_staff, login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def catalogo_gestao_view(request):
    cfg = obter_config_catalogo()
    msg = request.GET.get("msg") or ""
    erro = (request.GET.get("erro") or "").strip()[:200]

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
            if request.POST.get("remover_logo"):
                salvar_logo_loja(cfg, "", "")
            else:
                f = request.FILES.get("logo_loja")
                if f:
                    if f.size > 1200 * 1024:
                        erro = "Logo muito grande (máx. ~1,2 MB)."
                    else:
                        import base64

                        raw = f.read()
                        mime = (getattr(f, "content_type", None) or "image/png")[:40]
                        salvar_logo_loja(cfg, base64.b64encode(raw).decode("ascii"), mime)
            if erro:
                pass
            else:
                return redirect("/catalogo/gestao/?msg=loja")

        if acao == "nova_categoria":
            nome = (request.POST.get("cat_nome") or "").strip()[:80]
            parent_raw = (request.POST.get("cat_parent") or "").strip()
            parent = None
            if parent_raw:
                try:
                    parent = (
                        CatalogoDeliveryCategoria.objects.filter(pk=int(parent_raw))
                        .select_related("parent")
                        .first()
                    )
                except (TypeError, ValueError):
                    parent = None
                if parent and parent.parent_id and parent.parent and parent.parent.parent_id:
                    parent = None
                    erro = "Máximo 3 níveis (categoria → sub → sub-sub)."
            if not nome:
                erro = "Informe o nome da categoria."
            elif not erro:
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
            from urllib.parse import quote

            try:
                pk = int(request.POST.get("cat_id") or 0)
            except (TypeError, ValueError):
                pk = 0
            cat = CatalogoDeliveryCategoria.objects.filter(pk=pk, parent__isnull=True).first()
            if not cat:
                return redirect("/catalogo/gestao/?erro=" + quote("Categoria inválida para foto (só principal)."))
            if request.POST.get("remover_foto"):
                salvar_foto_categoria(cat, "", "")
                return redirect("/catalogo/gestao/?msg=foto")
            f = request.FILES.get("cat_foto")
            if not f:
                return redirect(
                    "/catalogo/gestao/?erro="
                    + quote("Nenhuma imagem chegou ao servidor. Escolha o arquivo e toque em «Foto card».")
                )
            try:
                import base64

                raw = f.read()
                if not raw:
                    return redirect("/catalogo/gestao/?erro=" + quote("Arquivo de imagem vazio."))
                # Aceita até ~4 MB bruto; comprime para JPEG leve antes de gravar
                if len(raw) > 4 * 1024 * 1024:
                    return redirect(
                        "/catalogo/gestao/?erro="
                        + quote("Foto muito grande (máx. ~4 MB). Reduza e tente de novo.")
                    )
                raw_ok, mime = comprimir_imagem_upload(raw, max_lado=1000, qualidade=80)
                if len(raw_ok) > 1200 * 1024:
                    return redirect(
                        "/catalogo/gestao/?erro="
                        + quote("Mesmo comprimida a foto ficou grande. Use JPG 800×600.")
                    )
                b64 = base64.b64encode(raw_ok).decode("ascii")
                salvar_foto_categoria(cat, b64, mime)
                cat.refresh_from_db(fields=["imagem_base64", "imagem_mime"])
                if not (cat.imagem_base64 or "").strip():
                    return redirect(
                        "/catalogo/gestao/?erro="
                        + quote("Gravação falhou (campo vazio). Tente JPG menor.")
                    )
            except Exception as exc:
                return redirect(
                    "/catalogo/gestao/?erro="
                    + quote(f"Erro ao salvar foto: {type(exc).__name__}: {exc}"[:180])
                )
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
            "categorias_pai_opts": opcoes_pai_categoria(so_ativas=True),
        },
    )
