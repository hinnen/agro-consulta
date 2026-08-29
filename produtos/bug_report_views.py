"""Views — reportar bug + lista gestão."""
from __future__ import annotations

import base64
import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.bug_report_util import notificar_report, sanitizar_print_base64, upsert_dispositivo
from produtos.caixa_util import (
    operador_label_request,
    ponto_operacao_browser,
    rotulo_caixa_loja_formo,
)
from produtos.models import BugReportAgro

logger = logging.getLogger(__name__)


def _usuario_nome(request) -> str:
    """Preferencia: PIN da sessao (caixa/PDV); senao login Django."""
    op = (operador_label_request(request) or "").strip()
    if op:
        return op[:120]
    u = getattr(request, "user", None)
    if u is None or not getattr(u, "is_authenticated", False):
        return ""
    full = (u.get_full_name() or "").strip()
    if full:
        return full[:120]
    return (getattr(u, "username", "") or "").strip()[:120]

def _versao_app() -> str:
    try:
        from config.app_build_util import read_app_version

        return (read_app_version() or "").strip()[:32]
    except Exception:
        try:
            from pathlib import Path

            return (Path(__file__).resolve().parents[1] / "VERSION").read_text(encoding="utf-8").strip()[:32]
        except Exception:
            return ""


@login_required(login_url="/admin/login/")
@require_POST
def api_bug_report_criar(request):
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)

    aconteceu = (data.get("o_que_aconteceu") or "").strip()
    if len(aconteceu) < 3:
        return JsonResponse({"ok": False, "erro": "Escreva o que aconteceu (mín. 3 letras)."}, status=400)

    esperava = (data.get("o_que_esperava") or "").strip()
    device_id = (data.get("device_id") or "").strip()[:64]
    dispositivo_nome = (data.get("dispositivo_nome") or "").strip()[:80]
    usuario_nome = (data.get("usuario_nome") or "").strip()[:120] or _usuario_nome(request) or "Sem nome"

    ponto = ""
    try:
        ponto = ponto_operacao_browser(request) or ""
    except Exception:
        ponto = ""
    ponto_rotulo = rotulo_caixa_loja_fixo(ponto) if ponto else ""

    print_b64, print_mime = sanitizar_print_base64(data.get("print_base64") or "")
    url_pagina = (data.get("url_pagina") or "")[:500]
    user_agent = (data.get("user_agent") or request.META.get("HTTP_USER_AGENT") or "")[:400]
    tela = (data.get("tela") or "")[:40]
    versao = (data.get("versao_app") or "").strip()[:32] or _versao_app()

    if device_id:
        upsert_dispositivo(
            device_id=device_id,
            nome=dispositivo_nome,
            ponto_caixa=ponto_rotulo or ponto,
            user_agent=user_agent,
            tela=tela,
        )

    report = BugReportAgro.objects.create(
        o_que_aconteceu=aconteceu[:8000],
        o_que_esperava=esperava[:4000],
        usuario_nome=usuario_nome,
        usuario=request.user if request.user.is_authenticated else None,
        device_id=device_id,
        dispositivo_nome=dispositivo_nome or ponto_rotulo,
        ponto_caixa=ponto_rotulo or ponto,
        url_pagina=url_pagina,
        versao_app=versao,
        user_agent=user_agent,
        tela=tela,
        print_base64=print_b64,
        print_mime=print_mime,
    )
    avisos = notificar_report(report, request)
    return JsonResponse({"ok": True, "id": report.pk, "mensagem": f"Recebido — #{report.pk}", "avisos": avisos})


@login_required(login_url="/admin/login/")
@ensure_csrf_cookie
@never_cache
@require_GET
def bug_reports_lista_view(request):
    qs = BugReportAgro.objects.all()[:200]
    return render(request, "produtos/bug_reports_lista.html", {"reports": qs, "total": BugReportAgro.objects.count()})


@login_required(login_url="/admin/login/")
@never_cache
@require_GET
def bug_report_detalhe_view(request, pk: int):
    from django.urls import reverse

    report = get_object_or_404(BugReportAgro, pk=pk)
    print_url = ""
    if (report.print_base64 or "").strip():
        try:
            print_url = request.build_absolute_uri(reverse("bug_report_print", kwargs={"pk": report.pk}))
        except Exception:
            print_url = reverse("bug_report_print", kwargs={"pk": report.pk})
    quando = ""
    try:
        quando = report.criado_em.strftime("%d/%m/%Y %H:%M") if report.criado_em else ""
    except Exception:
        quando = ""
    prompt_payload = {
        "pk": report.pk,
        "aconteceu": (report.o_que_aconteceu or "").strip(),
        "esperava": (report.o_que_esperava or "").strip(),
        "quem": (report.usuario_nome or "").strip(),
        "pc": (report.dispositivo_nome or "").strip(),
        "ponto": (report.ponto_caixa or "").strip(),
        "url": (report.url_pagina or "").strip(),
        "versao": (report.versao_app or "").strip(),
        "quando": quando,
        "status": (report.status or "").strip(),
        "tela": (report.tela or "").strip(),
        "print_url": print_url,
    }
    return render(
        request,
        "produtos/bug_report_detalhe.html",
        {
            "report": report,
            "bug_cursor_prompt": prompt_payload,
        },
    )


@login_required(login_url="/admin/login/")
@require_http_methods(["POST"])
def api_bug_report_status(request, pk: int):
    report = get_object_or_404(BugReportAgro, pk=pk)
    try:
        data = json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        data = {}
    st = (data.get("status") or "").strip().lower()
    valid = {c[0] for c in BugReportAgro.STATUS_CHOICES}
    if st not in valid:
        return JsonResponse({"ok": False, "erro": "Status inválido."}, status=400)
    report.status = st
    report.save(update_fields=["status"])
    return JsonResponse({"ok": True, "id": report.pk, "status": report.status})


@login_required(login_url="/admin/login/")
@require_GET
def bug_report_print_view(request, pk: int):
    report = get_object_or_404(BugReportAgro, pk=pk)
    if not (report.print_base64 or "").strip():
        return HttpResponse("Sem print.", status=404, content_type="text/plain")
    try:
        raw = base64.b64decode(report.print_base64)
    except Exception:
        return HttpResponse("Print inválido.", status=400, content_type="text/plain")
    mime = (report.print_mime or "image/jpeg").strip() or "image/jpeg"
    return HttpResponse(raw, content_type=mime)
