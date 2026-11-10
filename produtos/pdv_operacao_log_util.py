"""Registro de operações do PDV web (auditoria Gestão → LOG)."""

from __future__ import annotations

import logging
import re
from typing import Any

from django.http import HttpRequest

from produtos.caixa_util import operador_label_request
from produtos.models import PdvOperacaoLogAgro

logger = logging.getLogger(__name__)

_PDV_API_PREFIXES = (
    "/api/pdv/",
    "/api/fiado/baixa-pdv",
    "/api/fiado/cobranca-pdv",
)
_SKIP_RE = re.compile(
    r"(operador/?$|checkout-draft|catalog|buscar|heartbeat|ping|health)",
    re.I,
)


def _acao_de_path(path: str, metodo: str) -> str:
    p = (path or "").rstrip("/") or "/"
    m = (metodo or "GET").upper()
    low = p.lower()
    if "/mp-point/criar" in low:
        return "mp_point_criar"
    if "/mp-point/status" in low:
        return "mp_point_status"
    if "/mp-point/confirmar-tranche" in low:
        return "mp_point_confirmar_tranche"
    if "/mp-point/finalizar" in low:
        return "mp_point_finalizar"
    if "/mp-point/abandonar" in low:
        return "mp_point_abandonar"
    if "/mp-point/forcar-liberar" in low:
        return "mp_point_forcar_liberar"
    if "/mp-point/" in low:
        return f"mp_point_{m.lower()}"
    if "baixa-pdv" in low:
        return "fiado_baixa_pdv"
    if "cobranca-pdv" in low:
        return "fiado_cobranca_pdv"
    if "/api/pdv/venda" in low or low.endswith("/api/pdv/confirmar") or "finalizar-venda" in low:
        return "pdv_venda"
    if "/api/pdv/" in low:
        tail = low.rsplit("/", 1)[-1] or "pdv"
        return f"pdv_{tail}"[:80]
    return f"http_{m.lower()}"


def registrar_pdv_operacao_log(
    request: HttpRequest,
    acao: str,
    *,
    detalhe: dict[str, Any] | None = None,
    status_http: int = 0,
) -> None:
    try:
        user_login = ""
        user = getattr(request, "user", None)
        if user is not None and getattr(user, "is_authenticated", False):
            user_login = (getattr(user, "get_username", lambda: "")() or "")[:150]
        sk = ""
        try:
            sk = (getattr(request.session, "session_key", None) or "")[:50]
        except Exception:
            sk = ""
        PdvOperacaoLogAgro.objects.create(
            operador_nome=(operador_label_request(request) or "")[:120],
            usuario_login=user_login,
            acao=(acao or "acao")[:80],
            metodo=(request.method or "")[:12],
            path=(request.path or "")[:300],
            status_http=int(status_http or 0),
            detalhe_json=detalhe or {},
            django_session_key=sk,
        )
    except Exception:
        logger.exception("registrar_pdv_operacao_log %s", acao)


def deve_registrar_request_pdv(request: HttpRequest) -> bool:
    path = request.path or ""
    if not any(path.startswith(p) for p in _PDV_API_PREFIXES):
        return False
    if _SKIP_RE.search(path):
        return False
    # Status do Point polui demais (poll a cada 2s) — só erros / não-200.
    if "/mp-point/status" in path.lower():
        return False
    metodo = (request.method or "GET").upper()
    # GET de cobrança-pdv (montar tela) e status leves: skip
    if metodo == "GET" and "/mp-point/" not in path.lower():
        if "cobranca-pdv" in path.lower():
            return True  # útil ver quem abriu cobrança
        return False
    return True


def _detalhe_resumo_request(request: HttpRequest) -> dict[str, Any]:
    detalhe: dict[str, Any] = {}
    try:
        if request.GET:
            detalhe["query"] = {k: str(v)[:80] for k, v in list(request.GET.items())[:8]}
    except Exception:
        pass
    try:
        raw = getattr(request, "body", b"") or b""
        if raw and len(raw) < 8000:
            import json

            data = json.loads(raw.decode("utf-8") or "{}")
            if isinstance(data, dict):
                slim: dict[str, Any] = {}
                for k in (
                    "order_id",
                    "client_request_id",
                    "fiado_cobranca",
                    "modo",
                    "titulo_id",
                    "valor",
                    "valor_total",
                    "cliente",
                    "mp_point_conta",
                ):
                    if k in data and data[k] not in (None, "", [], {}):
                        slim[k] = data[k] if not isinstance(data[k], (dict, list)) else str(data[k])[:120]
                if "erp_payload" in data and isinstance(data["erp_payload"], dict):
                    ep = data["erp_payload"]
                    slim["erp_cliente"] = str(ep.get("cliente") or "")[:80]
                    slim["erp_fiado"] = bool(ep.get("fiado_cobranca"))
                    if ep.get("valor_total") is not None:
                        slim["erp_valor_total"] = ep.get("valor_total")
                if slim:
                    detalhe["body"] = slim
    except Exception:
        pass
    return detalhe


def registrar_request_pdv(request: HttpRequest, response) -> None:
    if not deve_registrar_request_pdv(request):
        return
    status = int(getattr(response, "status_code", 0) or 0)
    # Polls / GETs barulhentos já filtrados; POST sempre.
    acao = _acao_de_path(request.path or "", request.method or "GET")
    detalhe = _detalhe_resumo_request(request)
    try:
        # Espelha flags úteis da resposta JSON (sem corpo inteiro).
        content_type = (getattr(response, "get", lambda *_: "")("Content-Type") or "") if hasattr(response, "get") else ""
        if "json" in str(content_type).lower() and hasattr(response, "content"):
            import json

            raw = response.content
            if raw and len(raw) < 12000:
                data = json.loads(raw.decode("utf-8") or "{}")
                if isinstance(data, dict):
                    for k in (
                        "ok",
                        "erro",
                        "order_id",
                        "action",
                        "tinha_pago_point",
                        "pagamento_efetivado",
                        "retryable",
                        "venda_id",
                        "fiado_baixa",
                        "ja_finalizado",
                    ):
                        if k in data:
                            detalhe[f"resp_{k}"] = data[k]
    except Exception:
        pass
    registrar_pdv_operacao_log(request, acao, detalhe=detalhe, status_http=status)
