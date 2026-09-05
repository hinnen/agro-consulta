"""Quando Mongo ERP está desligado: nunca mostrar a palavra «Mongo» ao usuário."""
from __future__ import annotations

import json
import re


_RE_MONGO = re.compile(r"(?i)\bmongo\b(\s+erp)?")
_RE_MONGO_INDISP = re.compile(
    r"(?i)mongo(\s+erp)?\s+indispon[ií]vel\.?"
)


def _scrub_texto(s: str) -> str:
    s2 = _RE_MONGO_INDISP.sub("serviço legado indisponível", s)
    if s2 == s:
        s2 = _RE_MONGO.sub("serviço legado", s)
    return s2


def _scrub_obj(obj):
    if isinstance(obj, dict):
        return {k: _scrub_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub_obj(x) for x in obj]
    if isinstance(obj, str):
        return _scrub_texto(obj)
    return obj


class AgroSemMencaoMongoMiddleware:
    """Com ``AGRO_MONGO_ERP_DESLIGADO`` / agro_pg: limpa «Mongo» de respostas JSON/texto."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        try:
            from produtos.agro_fonte_config import agro_mongo_erp_desligado

            if not agro_mongo_erp_desligado():
                return response
        except Exception:
            return response

        ctype = (response.get("Content-Type") or "").lower()
        if "json" not in ctype and "text/plain" not in ctype and "text/html" not in ctype:
            return response
        try:
            raw = response.content.decode(getattr(response, "charset", None) or "utf-8")
        except Exception:
            return response
        if "mongo" not in raw.lower():
            return response

        if "json" in ctype:
            try:
                data = json.loads(raw)
                scrubbed = _scrub_obj(data)
                new_body = json.dumps(scrubbed, ensure_ascii=False).encode("utf-8")
            except Exception:
                new_body = _scrub_texto(raw).encode("utf-8")
        else:
            new_body = _scrub_texto(raw).encode("utf-8")

        response.content = new_body
        if "Content-Length" in response:
            response["Content-Length"] = str(len(new_body))
        return response
