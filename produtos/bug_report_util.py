"""Bug report — WhatsApp (CallMeBot) e e-mail (SMTP se configurado)."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from decouple import config
from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone

if TYPE_CHECKING:
    from produtos.models import BugReportAgro

logger = logging.getLogger(__name__)
_PRINT_MAX_CHARS = 1_200_000


def sanitizar_print_base64(raw: str) -> tuple[str, str]:
    s = (raw or "").strip()
    mime = "image/jpeg"
    if not s:
        return "", mime
    if s.startswith("data:") and ";base64," in s:
        head, _, b64 = s.partition(";base64,")
        mime = head.replace("data:", "").strip() or mime
        s = b64.strip()
    if len(s) > _PRINT_MAX_CHARS:
        s = s[:_PRINT_MAX_CHARS]
    return s, mime


def upsert_dispositivo(*, device_id: str, nome: str = "", ponto_caixa: str = "", user_agent: str = "", tela: str = ""):
    from produtos.models import DispositivoLojaAgro

    did = (device_id or "").strip()[:64]
    if not did:
        return None
    obj, _ = DispositivoLojaAgro.objects.get_or_create(device_id=did)
    nome_limpo = (nome or "").strip()[:80]
    if nome_limpo:
        obj.nome = nome_limpo
    if ponto_caixa:
        obj.ponto_caixa_ultimo = (ponto_caixa or "").strip()[:32]
    if user_agent:
        obj.user_agent = (user_agent or "").strip()[:400]
    if tela:
        obj.tela = (tela or "").strip()[:40]
    obj.ultimo_visto_em = timezone.now()
    obj.save()
    return obj


def _url_lista(request=None) -> str:
    path = "/gestao/bugs/"
    try:
        origin = (getattr(settings, "AGRO_CANONICAL_ORIGIN", "") or "").strip().rstrip("/")
        if origin:
            return f"{origin}{path}"
    except Exception:
        pass
    if request is not None:
        try:
            return request.build_absolute_uri(path)
        except Exception:
            pass
    return path


def _mensagem(report: BugReportAgro, request=None) -> str:
    trecho = (report.o_que_aconteceu or "").strip().replace("\n", " ")
    if len(trecho) > 180:
        trecho = trecho[:177] + "…"
    pc = (report.dispositivo_nome or report.device_id[:8] or "?").strip()
    ponto = (report.ponto_caixa or "").strip()
    onde = f"{pc}" + (f" · {ponto}" if ponto else "")
    return (
        f"🐛 SisVale bug #{report.pk}\n"
        f"Quem: {report.usuario_nome or '?'}\n"
        f"PC: {onde}\n"
        f"Tela: {(report.url_pagina or '')[:120]}\n"
        f"v{report.versao_app or '?'}\n"
        f"{trecho}\n"
        f"Lista: {_url_lista(request)}"
    )


def notificar_whatsapp(report: BugReportAgro, request=None) -> bool:
    try:
        from integracoes.notificacao_whatsapp import enviar_whatsapp_callmebot

        ok, detalhe = enviar_whatsapp_callmebot(_mensagem(report, request))
        if ok:
            report.notificado_whatsapp = True
            report.save(update_fields=["notificado_whatsapp"])
            return True
        logger.info("Bug #%s WhatsApp não enviado: %s", report.pk, detalhe)
    except Exception:
        logger.exception("Bug #%s WhatsApp falhou", report.pk)
    return False


def notificar_email(report: BugReportAgro, request=None) -> bool:
    destino = (
        (getattr(settings, "AGRO_BUG_REPORT_EMAIL", None) or "")
        or (config("AGRO_BUG_REPORT_EMAIL", default="") or "")
    ).strip()
    if not destino:
        return False
    host = (getattr(settings, "EMAIL_HOST", "") or "").strip()
    if not host:
        return False
    remetente = (getattr(settings, "DEFAULT_FROM_EMAIL", "") or "").strip() or destino
    try:
        send_mail(
            subject=f"[SisVale] Bug #{report.pk} — {report.usuario_nome or '?'}",
            message=_mensagem(report, request),
            from_email=remetente,
            recipient_list=[destino],
            fail_silently=False,
        )
        report.notificado_email = True
        report.save(update_fields=["notificado_email"])
        return True
    except Exception:
        logger.exception("Bug #%s e-mail falhou", report.pk)
        return False


def notificar_report(report: BugReportAgro, request=None) -> dict:
    return {
        "whatsapp": notificar_whatsapp(report, request),
        "email": notificar_email(report, request),
    }
