"""URL pública do módulo WhatsApp (serviço Render dedicado)."""
from __future__ import annotations

from django.conf import settings
from django.urls import reverse


def agro_wa_public_origin() -> str:
    return (getattr(settings, "AGRO_WA_PUBLIC_ORIGIN", "") or "").strip().rstrip("/")


def agro_wa_path_is_split_target(path: str) -> bool:
    p = (path or "").lower()
    if p.startswith("/atendimento-whatsapp"):
        return True
    if p.startswith("/api/atendimento-whatsapp/"):
        return True
    return False


def agro_wa_absolute(request, url_name: str, *args, **kwargs) -> str:
    path = reverse(url_name, args=args, kwargs=kwargs)
    origin = agro_wa_public_origin()
    if origin:
        return origin + path
    if request is not None:
        return request.build_absolute_uri(path)
    return path


def agro_wa_absolute_path(path: str) -> str:
    p = path if path.startswith("/") else "/" + (path or "")
    origin = agro_wa_public_origin()
    if origin:
        return origin + p
    return p
