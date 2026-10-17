"""Registro de ações no PWA GM Lojas."""

from __future__ import annotations

import logging
import re
from typing import Any

from django.http import HttpRequest

from produtos.models import GmLojasAppLogAgro
from produtos.vendas_lojas_hub_sessao_util import operador_da_sessao

logger = logging.getLogger(__name__)

_LOG_PREFIX = "/vendas/lojas/"
_TAREFAS_PREFIX = "/vendas/lojas/tarefas/"
_SKIP_PATH_RE = re.compile(
    r"(/manifest\.webmanifest|/sw\.js|/foto/|/pin/$|service-worker)",
    re.I,
)


def _acao_de_path(path: str, metodo: str) -> str:
    p = path.rstrip("/") or "/"
    m = (metodo or "GET").upper()
    if p.endswith("/pin"):
        return "pin_tela" if m == "GET" else "pin_envio"
    if "/tarefas/api/" in p:
        if "criar" in p:
            return "tarefa_criar"
        if "status" in p:
            return "tarefa_status"
        if "comentar" in p:
            return "tarefa_comentar"
        if "atualizar" in p:
            return "tarefa_atualizar"
    if "/tarefas" in p:
        if p.endswith("/tarefas/nova"):
            return "tarefa_nova_tela"
        if "/tarefas/" in p and p != "/vendas/lojas/tarefas":
            return "tarefa_detalhe_tela"
        return "tarefas_lista"
    if "/fotos/api/" in p:
        return "foto_api"
    if p.endswith("/fotos"):
        return "fotos_app"
    if "/etiquetas/api/" in p or "solicitacao" in p and m == "POST":
        return "etiqueta_solicitacao_criar"
    if p.endswith("/etiquetas"):
        return "etiquetas_app"
    if "extras" in p or "resumo" in p:
        return "vendas_api" if "/api/" in p else "vendas_resumo"
    if p.endswith("/vendas/lojas") or p.endswith("/vendas/lojas/"):
        return "hub_inicio"
    if p.endswith("/sair"):
        return "logout"
    return f"http_{m.lower()}"


def registrar_gm_lojas_log(
    request: HttpRequest,
    acao: str,
    *,
    detalhe: dict[str, Any] | None = None,
    status_http: int = 0,
) -> None:
    try:
        GmLojasAppLogAgro.objects.create(
            operador_nome=(operador_da_sessao(request) or "")[:120],
            acao=(acao or "acao")[:80],
            metodo=(request.method or "")[:12],
            path=(request.path or "")[:300],
            status_http=int(status_http or 0),
            detalhe_json=detalhe or {},
        )
    except Exception:
        logger.exception("registrar_gm_lojas_log %s", acao)


def deve_registrar_request(request: HttpRequest) -> bool:
    path = request.path or ""
    if not path.startswith(_LOG_PREFIX):
        return False
    if _SKIP_PATH_RE.search(path):
        return False
    if path.endswith(".png") or path.endswith(".ico"):
        return False
    return True


def registrar_request_gm_lojas(request: HttpRequest, response) -> None:
    if not deve_registrar_request(request):
        return
    acao = _acao_de_path(request.path or "", request.method or "GET")
    detalhe: dict[str, Any] = {}
    if request.GET:
        detalhe["query"] = dict(list(request.GET.items())[:12])
    status = getattr(response, "status_code", 0) or 0
    if acao == "pin_envio" and status in (200, 302):
        detalhe["pin_ok"] = True
    elif acao == "pin_envio" and status >= 400:
        detalhe["pin_ok"] = False
    registrar_gm_lojas_log(request, acao, detalhe=detalhe, status_http=status)
