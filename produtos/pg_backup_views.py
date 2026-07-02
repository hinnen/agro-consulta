"""Views — painel backup/restore Postgres Agro (FL-048)."""
from __future__ import annotations

import json

from django.contrib.auth import authenticate
from django.contrib.auth.decorators import login_required, user_passes_test
from django.http import HttpResponse, HttpResponseForbidden
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from produtos.pg_backup_registry import PG_BACKUP_ALL_SLUGS, RESTORE_CONFIRM_PHRASE
from produtos.pg_backup_render_checklist import (
    CHECKLIST_REV,
    DISASTER_RECOVERY_STEPS,
    NOTAS_CURTAS,
    RENDER_ENV_ROWS,
    ROLLBACK_NOITE_STEPS,
)
from produtos.pg_backup_disaster_kit import build_disaster_kit_zip
from produtos.pg_backup_util import build_backup_zip, listar_categorias_stats, restore_backup_zip


def _superuser_ok(user) -> bool:
    return bool(user and user.is_authenticated and user.is_superuser)


def _verificar_senha_admin(request, senha: str) -> bool:
    senha = (senha or "").strip()
    if not senha:
        return False
    user = authenticate(request, username=request.user.get_username(), password=senha)
    return bool(user and user.is_superuser)


@login_required(login_url="/admin/login/")
@user_passes_test(_superuser_ok, login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def pg_backup_painel(request):
    ctx = {
        "categories": listar_categorias_stats(),
        "all_slugs": PG_BACKUP_ALL_SLUGS,
        "restore_confirm_phrase": RESTORE_CONFIRM_PHRASE,
        "checklist_rev": CHECKLIST_REV,
        "render_env_rows": RENDER_ENV_ROWS,
        "disaster_steps": DISASTER_RECOVERY_STEPS,
        "rollback_steps": ROLLBACK_NOITE_STEPS,
        "checklist_notas": NOTAS_CURTAS,
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
