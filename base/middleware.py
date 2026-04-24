"""
Idempotência global para POST/PUT/PATCH (e DELETE com chave só em header).

Ativa quando o cliente envia uma das opções:
  - Cabeçalho ``Idempotency-Key`` (padrão da indústria) ou ``X-Agro-Client-Request-Id``
  - JSON: ``client_request_id`` ou ``idempotency_key``
  - Form: campos homónimos

A chave é combinada com sessão + método + path: o mesmo pedido repetido devolve a mesma
resposta JSON em cache (sem reexecutar a view). Requer cache partilhado entre workers (ex.: Redis)
para efeito pleno em multi-processo.

Desative rotas com ``AGRO_IDEMPOTENCY_EXEMPT_PREFIXES`` no settings.
"""

from __future__ import annotations

import json
import logging
import re
import time

from django.conf import settings
from django.core.cache import cache
from django.http import HttpResponse, StreamingHttpResponse

logger = logging.getLogger(__name__)

_IDEM_HEADER_KEYS = (
    "HTTP_IDEMPOTENCY_KEY",
    "HTTP_X_IDEMPOTENCY_KEY",
    "HTTP_X_AGRO_IDEMPOTENCY_KEY",
    "HTTP_X_AGRO_CLIENT_REQUEST_ID",
)


def _idem_enabled() -> bool:
    return bool(getattr(settings, "AGRO_IDEMPOTENCY_ENABLED", True))


def _idem_exempt_path(path: str) -> bool:
    p = path or ""
    for prefix in getattr(settings, "AGRO_IDEMPOTENCY_EXEMPT_PREFIXES", ()):
        if prefix and p.startswith(prefix):
            return True
    return False


def _sanitize_idempotency_key(raw) -> str | None:
    s = str(raw or "").strip()
    if not s or len(s) > 96:
        return None
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", s):
        return None
    return s


def _max_body_parse() -> int:
    return int(getattr(settings, "AGRO_IDEMPOTENCY_MAX_BODY_PARSE", 524288))


def _max_body_store() -> int:
    return int(getattr(settings, "AGRO_IDEMPOTENCY_MAX_RESPONSE_BYTES", 524288))


def _cache_ttl() -> int:
    return int(getattr(settings, "AGRO_IDEMPOTENCY_CACHE_TTL", 86400))


def _lock_ttl() -> int:
    return int(getattr(settings, "AGRO_IDEMPOTENCY_LOCK_TTL", 120))


def _cache_statuses() -> tuple[int, ...]:
    # Inclui 4xx/502 para o mesmo Idempotency-Key repetir a mesma resposta (evita segunda gravação).
    return tuple(
        getattr(
            settings,
            "AGRO_IDEMPOTENCY_CACHE_STATUSES",
            (200, 201, 202, 400, 404, 409, 422, 502),
        )
    )


def _extract_idempotency_key(request) -> str | None:
    if not _idem_enabled() or _idem_exempt_path(request.path):
        return None
    if request.method not in ("POST", "PUT", "PATCH", "DELETE"):
        return None

    for hk in _IDEM_HEADER_KEYS:
        k = _sanitize_idempotency_key(request.META.get(hk))
        if k:
            return k

    if request.method == "DELETE":
        return None

    try:
        cl = int(request.META.get("CONTENT_LENGTH") or 0)
    except (TypeError, ValueError):
        cl = 0
    if cl > _max_body_parse():
        return None

    ct = (request.META.get("CONTENT_TYPE") or "").split(";")[0].strip().lower()

    if ct == "application/json":
        try:
            raw = request.body
        except Exception:
            return None
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            return None
        if isinstance(data, dict):
            return _sanitize_idempotency_key(
                data.get("client_request_id") or data.get("idempotency_key")
            )
        return None

    if ct in ("application/x-www-form-urlencoded", "multipart/form-data"):
        try:
            post = request.POST
        except Exception:
            return None
        return _sanitize_idempotency_key(
            post.get("client_request_id") or post.get("idempotency_key")
        )

    return None


def _ensure_session_key(request) -> str:
    if not hasattr(request, "session"):
        return ""
    try:
        if not request.session.session_key:
            request.session.save()
    except Exception:
        logger.debug("idempotency: session.save falhou", exc_info=True)
    return getattr(request.session, "session_key", None) or ""


def _should_cache_response(response: HttpResponse) -> bool:
    if isinstance(response, StreamingHttpResponse):
        return False
    if response.status_code not in _cache_statuses():
        return False
    ct = (response.get("Content-Type") or "").lower()
    if "application/json" not in ct:
        return False
    try:
        if len(response.content) > _max_body_store():
            return False
    except Exception:
        return False
    return True


class AgroIdempotencyMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        idem_key = _extract_idempotency_key(request)
        if not idem_key:
            return self.get_response(request)

        sk = _ensure_session_key(request)
        if not sk:
            return self.get_response(request)

        path = request.path or ""
        rkey = f"agro:mw:idem:v1:{sk}:{request.method}:{path}:{idem_key}"
        lock_key = f"{rkey}:lock"

        hit = cache.get(rkey)
        if isinstance(hit, dict) and "content" in hit and "status" in hit:
            return HttpResponse(
                hit["content"],
                status=int(hit["status"]),
                content_type=str(hit.get("content_type") or "application/json"),
            )

        if not cache.add(lock_key, 1, timeout=_lock_ttl()):
            for _ in range(40):
                time.sleep(0.05)
                hit = cache.get(rkey)
                if isinstance(hit, dict) and "content" in hit and "status" in hit:
                    return HttpResponse(
                        hit["content"],
                        status=int(hit["status"]),
                        content_type=str(hit.get("content_type") or "application/json"),
                    )
            return HttpResponse(
                json.dumps(
                    {
                        "ok": False,
                        "erro": "Requisição em processamento. Aguarde um instante ou verifique se a operação já concluiu.",
                    },
                    ensure_ascii=False,
                ),
                status=409,
                content_type="application/json; charset=utf-8",
            )

        try:
            response = self.get_response(request)
        except Exception:
            raise
        finally:
            cache.delete(lock_key)

        if _should_cache_response(response):
            try:
                payload = {
                    "status": response.status_code,
                    "content": response.content,
                    "content_type": response.get("Content-Type") or "application/json",
                }
                cache.set(rkey, payload, timeout=_cache_ttl())
            except Exception:
                logger.warning("idempotency: falha ao gravar cache para %s", path, exc_info=True)

        return response
