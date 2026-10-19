"""Envio de NF-e autorizada (XML + DANFE HTML) ao e-mail do destinatário."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from django.conf import settings
from django.core.mail import EmailMultiAlternatives

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalizar_email_destinatario(dest: dict | None) -> str:
    if not isinstance(dest, dict):
        return ""
    return str(dest.get("email") or "").strip().lower()[:120]


def email_destinatario_valido(email: str) -> bool:
    e = (email or "").strip().lower()
    return bool(e) and bool(_EMAIL_RE.match(e))


def smtp_configurado() -> bool:
    return bool((getattr(settings, "EMAIL_HOST", None) or "").strip())


def mesclar_destinatario_emit(doc: dict, patch: dict | None) -> dict:
    """Garante e-mail e demais campos do formulário no doc antes da transmissão."""
    out = dict(doc)
    dest = dict(out.get("destinatario") or {}) if isinstance(out.get("destinatario"), dict) else {}
    if isinstance(patch, dict):
        for k in (
            "nome",
            "documento",
            "tipo_doc",
            "ie",
            "email",
            "ind_final",
            "ind_pres",
            "cep",
            "uf",
            "cidade",
            "bairro",
            "logradouro",
            "numero",
            "complemento",
            "cliente_agro_id",
        ):
            if k in patch and patch[k] is not None:
                dest[k] = patch[k]
    email = normalizar_email_destinatario(dest)
    if email:
        dest["email"] = email
    out["destinatario"] = dest
    return out


def enviar_nfe_saida_por_email(
    doc: dict,
    doc_id: str,
    *,
    site_base: str = "",
) -> dict[str, Any]:
    """
    Anexa XML autorizado e DANFE (HTML). Retorno: ok, enviado, email, erro, skip_motivo.
    """
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    email = normalizar_email_destinatario(dest)
    if not email_destinatario_valido(email):
        return {"ok": True, "enviado": False, "skip_motivo": "sem_email_destinatario"}
    if not smtp_configurado():
        return {
            "ok": False,
            "enviado": False,
            "erro": "Servidor de e-mail não configurado (EMAIL_HOST no Render/.env).",
        }
    xml = str(doc.get("xml_autorizado") or "").strip()
    if not xml:
        return {"ok": False, "enviado": False, "erro": "XML autorizado ausente no documento."}

    chave = re.sub(r"\D", "", str(doc.get("chave") or ""))[:44]
    nnf = doc.get("nNF") or "—"
    serie = doc.get("serie") or ""
    nome_dest = str(dest.get("nome") or "Destinatário").strip()

    from produtos.nfe_saida_danfe_html import carregar_emitente_danfe, montar_html_danfe

    emitente = carregar_emitente_danfe(doc=doc)
    danfe_html = montar_html_danfe(doc, emitente=emitente)

    base = (site_base or "").rstrip("/")
    link_danfe = f"{base}/api/nfe-saida/{doc_id}/danfe/" if base and doc_id else ""

    assunto = f"NF-e nº {nnf}" + (f" série {serie}" if serie else "") + " — GM Agro"
    texto = (
        f"Olá, {nome_dest}.\n\n"
        f"Sua Nota Fiscal Eletrônica foi autorizada.\n"
        f"Chave de acesso: {chave or '—'}\n"
    )
    if link_danfe:
        texto += f"Prévia / impressão DANFE: {link_danfe}\n"
    texto += "\nEm anexo: XML da NF-e e arquivo HTML do DANFE (abra no navegador ou imprima com Ctrl+P).\n"

    html_body = (
        f"<p>Olá, <strong>{nome_dest}</strong>.</p>"
        f"<p>Sua <strong>NF-e</strong> foi autorizada.</p>"
        f"<p>Chave: <code>{chave or '—'}</code></p>"
    )
    if link_danfe:
        html_body += f'<p><a href="{link_danfe}">Abrir DANFE no navegador</a></p>'
    html_body += "<p>Anexos: XML e DANFE (HTML).</p>"

    msg = EmailMultiAlternatives(
        subject=assunto[:180],
        body=texto,
        from_email=settings.DEFAULT_FROM_EMAIL,
        to=[email],
    )
    msg.attach_alternative(html_body, "text/html")
    nome_arq = chave or str(doc_id or "nfe")
    msg.attach(f"NFe-{nome_arq}.xml", xml.encode("utf-8"), "application/xml")
    msg.attach(f"DANFE-NFe-{nome_arq}.html", danfe_html.encode("utf-8"), "text/html")

    try:
        msg.send(fail_silently=False)
    except Exception as exc:
        logger.exception("Falha ao enviar NF-e por e-mail para %s", email)
        return {"ok": False, "enviado": False, "email": email, "erro": str(exc)[:500]}

    return {
        "ok": True,
        "enviado": True,
        "email": email,
        "enviado_em": datetime.now(timezone.utc).isoformat(),
    }
