"""Views — painel backup/restore Postgres Agro (FL-048)."""
from __future__ import annotations

import json

from django.contrib.auth import authenticate
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import HttpResponse, HttpResponseForbidden, JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.http import require_GET, require_http_methods

from django.conf import settings

from produtos.pg_backup_registry import PG_BACKUP_ALL_SLUGS, RESTORE_CONFIRM_PHRASE
from produtos.pg_backup_render_checklist import (
    CHECKLIST_REV,
    DISASTER_RECOVERY_STEPS,
    NOTAS_CURTAS,
    RENDER_ENV_ROWS,
    ROLLBACK_NOITE_STEPS,
)
from produtos.pg_backup_disaster_kit import build_disaster_kit_zip
from produtos.pg_backup_upload import upload_status_resumo
from produtos.pg_backup_util import (
    build_backup_zip,
    listar_categorias_leve,
    listar_categorias_stats,
    restore_backup_zip,
)


def _superuser_ok(user) -> bool:
    return bool(user and user.is_authenticated and user.is_superuser)


def _verificar_senha_admin(request, senha: str) -> bool:
    senha = (senha or "").strip()
    if not senha:
        return False
    user = authenticate(request, username=request.user.get_username(), password=senha)
    return bool(user and user.is_superuser)


@login_required(login_url="/entrar/")
@user_passes_test(_superuser_ok, login_url="/entrar/")
@require_http_methods(["GET", "POST"])
def pg_backup_painel(request):
    # Na loja, COUNT em todas as tabelas ao abrir o painel estoura timeout (500).
    # Abertura = lista leve; contagens só após export/restore (quando o POST já rodou).
    ctx = {
        "categories": listar_categorias_leve(),
        "all_slugs": PG_BACKUP_ALL_SLUGS,
        "restore_confirm_phrase": RESTORE_CONFIRM_PHRASE,
        "checklist_rev": CHECKLIST_REV,
        "render_env_rows": RENDER_ENV_ROWS,
        "disaster_steps": DISASTER_RECOVERY_STEPS,
        "rollback_steps": ROLLBACK_NOITE_STEPS,
        "checklist_notas": NOTAS_CURTAS,
        "nightly_enabled": getattr(settings, "AGRO_PG_BACKUP_NIGHTLY_ENABLED", False),
        "upload_status": upload_status_resumo(),
        "flash_ok": "",
        "flash_erro": "",
        "restore_result": None,
    }

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        slugs = request.POST.getlist("categorias")

        if action == "export":
            try:
                blob, filename, _manifest = build_backup_zip(
                    slugs,
                    username=request.user.get_username(),
                )
            except ValueError as exc:
                ctx["flash_erro"] = str(exc)
                return render(request, "produtos/pg_backup_painel.html", ctx)
            except Exception as exc:
                ctx["flash_erro"] = f"Falha ao gerar ZIP: {exc}"
                return render(request, "produtos/pg_backup_painel.html", ctx)

            resp = HttpResponse(blob, content_type="application/zip")
            resp["Content-Disposition"] = f'attachment; filename="{filename}"'
            return resp

        if action == "kit":
            blob, filename = build_disaster_kit_zip()
            resp = HttpResponse(blob, content_type="application/zip")
            resp["Content-Disposition"] = f'attachment; filename="{filename}"'
            return resp

        if action == "restore":
            senha = request.POST.get("senha_admin") or ""
            confirm = (request.POST.get("confirmar_texto") or "").strip()
            upload = request.FILES.get("arquivo_zip")

            if confirm != RESTORE_CONFIRM_PHRASE:
                ctx["flash_erro"] = f'Digite exatamente: {RESTORE_CONFIRM_PHRASE}'
                return render(request, "produtos/pg_backup_painel.html", ctx)

            if not _verificar_senha_admin(request, senha):
                ctx["flash_erro"] = "Senha de administrador incorreta."
                return render(request, "produtos/pg_backup_painel.html", ctx)

            if not upload:
                ctx["flash_erro"] = "Selecione o arquivo ZIP do backup."
                return render(request, "produtos/pg_backup_painel.html", ctx)

            if upload.size and upload.size > 512 * 1024 * 1024:
                ctx["flash_erro"] = "Arquivo maior que 512 MB — recuse ou divida o backup."
                return render(request, "produtos/pg_backup_painel.html", ctx)

            result = restore_backup_zip(
                upload,
                slugs if slugs else None,
                username=request.user.get_username(),
            )
            ctx["restore_result"] = result
            ctx["restore_result_text"] = json.dumps(result, ensure_ascii=False, indent=2)
            if result.get("ok"):
                ctx["flash_ok"] = "Restore concluído. Confira contagens abaixo e teste o sistema."
            else:
                ctx["flash_erro"] = "Restore com erros — veja detalhes."
            ctx["categories"] = listar_categorias_stats()
            return render(request, "produtos/pg_backup_painel.html", ctx)

        ctx["flash_erro"] = "Ação inválida."
        return render(request, "produtos/pg_backup_painel.html", ctx)

    return render(request, "produtos/pg_backup_painel.html", ctx)

@login_required(login_url="/entrar/")
@user_passes_test(_superuser_ok, login_url="/entrar/")
@require_GET
def importar_catalogo_faltantes(request):
    """
    Emergência loja: Mongo → PG só IDs ausentes (fatias, evita timeout 500).
    Só superuser. Abre no browser — se ``continuar``, a página recarrega sozinha.
    ``?dry_run=1`` · ``?skip=N`` · ``?limit=300`` · ``?json=1``
    """
    import html
    import traceback

    try:
        skip = int(str(request.GET.get("skip") or "0").strip() or "0")
    except ValueError:
        skip = 0
    try:
        # Fatia pequena: proxy Render costuma matar request longo → 500 HTML.
        lim = int(str(request.GET.get("limit") or "300").strip() or "300")
    except ValueError:
        lim = 300
    lim = max(50, min(lim, 800))
    dry = str(request.GET.get("dry_run") or "").strip().lower() in ("1", "true", "yes")
    want_json = str(request.GET.get("json") or "").strip().lower() in ("1", "true", "yes")

    try:
        from produtos.management.commands.importar_catalogo_mongo_produto import (
            executar_importar_catalogo_mongo_produto,
        )

        out = executar_importar_catalogo_mongo_produto(
            somente_faltantes=True,
            dry_run=dry,
            skip=skip,
            limit=lim,
        )
    except Exception as exc:
        out = {
            "ok": False,
            "erro": str(exc)[:500],
            "traceback": traceback.format_exc()[-1500:],
            "skip": skip,
            "limit": lim,
        }

    out["usuario"] = request.user.get_username()
    # Acumula criados via querystring para o usuário ver o total da sessão
    try:
        acum = int(str(request.GET.get("acum_criados") or "0").strip() or "0")
    except ValueError:
        acum = 0
    acum += int(out.get("criados") or 0)
    out["acum_criados"] = acum

    if want_json or not out.get("ok"):
        st = 200 if out.get("ok") else 500
        return JsonResponse(out, status=st)

    continuar = bool(out.get("continuar"))
    prox = int(out.get("proximo_skip") or (skip + lim))
    meta = ""
    if continuar:
        meta = (
            f'<meta http-equiv="refresh" content="1;url='
            f'?skip={prox}&limit={lim}&acum_criados={acum}'
            f'{"&dry_run=1" if dry else ""}">'
        )
        status_txt = "CONTINUANDO… não feche esta aba"
    else:
        status_txt = "CONCLUÍDO — pode fechar e dar Ctrl+F5 no PDV"

    body = (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'>{meta}"
        f"<title>Import faltantes</title></head><body style='font-family:sans-serif;padding:1.5rem'>"
        f"<h1>{html.escape(status_txt)}</h1>"
        f"<p>skip={skip} · lidos={out.get('lidos')} · criados nesta fatia="
        f"<b>{out.get('criados')}</b> · já existiam={out.get('ja_existem')} · "
        f"erros={out.get('erros')} · <b>criados total sessão={acum}</b></p>"
        f"<p>total_pg={out.get('total_pg')} · total_mongo={out.get('total_mongo')}</p>"
        f"<pre style='background:#f4f4f4;padding:1rem;overflow:auto'>"
        f"{html.escape(json.dumps(out, ensure_ascii=False, indent=2, default=str))}</pre>"
        f"</body></html>"
    )
    return HttpResponse(body, content_type="text/html; charset=utf-8")


@login_required(login_url="/entrar/")
@user_passes_test(_superuser_ok, login_url="/entrar/")
@require_GET
def recuperar_produtos_vendas(request):
    """
    Emergência sem Mongo: recria ``Produto`` a partir de itens de venda (Postgres).
    ``?dias=90`` (padrão) · ``?dry_run=1`` · ``?json=1``
    """
    import html
    import traceback

    try:
        dias = int(str(request.GET.get("dias") or "90").strip() or "90")
    except ValueError:
        dias = 90
    dias = max(1, min(dias, 730))
    dry = str(request.GET.get("dry_run") or "").strip().lower() in ("1", "true", "yes")
    want_json = str(request.GET.get("json") or "").strip().lower() in ("1", "true", "yes")
    nome = str(request.GET.get("nome_contem") or "").strip()

    try:
        from produtos.management.commands.recuperar_produtos_itens_venda import (
            recuperar_produtos_de_itens,
        )

        out = recuperar_produtos_de_itens(
            nome_contem=nome,
            dias=dias,
            dry_run=dry,
            reparar=True,
        )
    except Exception as exc:
        out = {
            "ok": False,
            "erro": str(exc)[:500],
            "traceback": traceback.format_exc()[-1500:],
        }

    out["usuario"] = request.user.get_username()
    out["fonte"] = "itens_venda_postgres"
    out["sem_mongo"] = True

    if want_json or not out.get("ok"):
        st = 200 if out.get("ok") else 500
        return JsonResponse(out, status=st, json_dumps_params={"default": str})

    status_txt = "CONCLUÍDO (recuperação por vendas — sem Mongo)"
    body = (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<title>Recuperar de vendas</title></head>"
        f"<body style='font-family:sans-serif;padding:1.5rem'>"
        f"<h1>{html.escape(status_txt)}</h1>"
        f"<p>dias={dias} · candidatos={out.get('candidatos')} · "
        f"<b>criados={out.get('criados')}</b> · reparados={out.get('reparados')} · "
        f"reativados={out.get('reativados')} · "
        f"já existiam={out.get('ja_existem')} · erros={out.get('erros')}</p>"
        f"<p>Depois: Ctrl+F5 no PDV. Busque «sache kitekat».</p>"
        f"<pre style='background:#f4f4f4;padding:1rem;overflow:auto'>"
        f"{html.escape(json.dumps(out, ensure_ascii=False, indent=2, default=str))}</pre>"
        f"</body></html>"
    )
    return HttpResponse(body, content_type="text/html; charset=utf-8")

@login_required(login_url="/entrar/")
@user_passes_test(_superuser_ok, login_url="/entrar/")
@require_GET
def inspecionar_produto_pg(request):
    """Debug superuser: ?pid=...&pid=... ou ?nome=kitekat"""
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    pids = request.GET.getlist("pid") or []
    if not pids:
        one = str(request.GET.get("pid") or "").strip()
        if one:
            pids = [one]
    nome = str(request.GET.get("nome") or "").strip()
    rows = []
    qs = Produto.objects.all()
    if pids:
        qs = qs.filter(produto_externo_id__in=pids)
    elif nome:
        qs = qs.filter(nome__icontains=nome)[:30]
    else:
        return JsonResponse({"ok": False, "erro": "pid ou nome"}, status=400)
    for p in qs[:40]:
        pid = str(p.produto_externo_id or "")
        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first() if pid else None
        rows.append(
            {
                "pk": p.pk,
                "produto_externo_id": pid,
                "nome": p.nome,
                "codigo_interno": p.codigo_interno,
                "codigo_nfe": p.codigo_nfe,
                "marca": p.marca,
                "ativo": p.ativo,
                "cadastro_inativo": p.cadastro_inativo,
                "preco_venda": str(p.preco_venda),
                "overlay": bool(ov),
                "overlay_ativo": getattr(ov, "ativo_exibicao", None) if ov else None,
            }
        )
    return JsonResponse({"ok": True, "n": len(rows), "produtos": rows})
