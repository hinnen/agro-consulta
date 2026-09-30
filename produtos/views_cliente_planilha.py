"""API Excel clientes — /clientes/ (Excel ↓ / Excel ↑)."""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_GET, require_POST


def _tmp_upload(upload) -> Path:
    suf = Path(upload.name or "planilha.xlsx").suffix.lower()
    if suf not in (".xlsx", ".xls", ".csv"):
        suf = ".xlsx"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suf)
    try:
        for chunk in upload.chunks():
            tmp.write(chunk)
    finally:
        tmp.close()
    return Path(tmp.name)


@login_required(login_url="/entrar/")
@require_GET
def api_clientes_export_xlsx(request):
    from produtos.cliente_planilha_util import coletar_linhas_export_clientes, montar_xlsx_clientes

    rows = coletar_linhas_export_clientes()
    data = montar_xlsx_clientes(rows)
    nome = f"Clientes_{date.today().strftime('%Y%m%d')}.xlsx"
    resp = HttpResponse(
        data,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{nome}"'
    return resp


@login_required(login_url="/entrar/")
@require_POST
def api_clientes_import_preview(request):
    from produtos.cliente_planilha_util import preview_importacao_clientes

    upload = request.FILES.get("arquivo")
    if not upload:
        return JsonResponse({"ok": False, "erro": "Envie um arquivo .xlsx ou .csv."}, status=400)
    tmp_path = None
    try:
        tmp_path = _tmp_upload(upload)
        prev = preview_importacao_clientes(tmp_path)
        return JsonResponse({"ok": True, **prev})
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


@login_required(login_url="/entrar/")
@require_POST
def api_clientes_import_aplicar(request):
    from produtos.cliente_planilha_util import aplicar_importacao_clientes

    upload = request.FILES.get("arquivo")
    if not upload:
        return JsonResponse({"ok": False, "erro": "Envie um arquivo .xlsx ou .csv."}, status=400)
    tmp_path = None
    try:
        tmp_path = _tmp_upload(upload)
        nome = (request.POST.get("nome_arquivo") or upload.name or "")[:255]
        r = aplicar_importacao_clientes(tmp_path, request.user, nome_arquivo=nome)
        return JsonResponse({"ok": True, **r})
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
