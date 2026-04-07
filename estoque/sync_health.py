"""
Heartbeat ERP→Agro (leitura Mongo): atualiza :class:`EstoqueSyncHealth` (singleton pk=1).
Não envia dados ao ERP; apenas registra sucesso/falha de leitura e rebuild do catálogo PDV.
"""

from __future__ import annotations

import logging

from django.utils import timezone

logger = logging.getLogger(__name__)


def _health():
    from estoque.models import EstoqueSyncHealth

    h, _ = EstoqueSyncHealth.objects.get_or_create(pk=1)
    return h


def registrar_ping_mongo(ok: bool, erro: str = "") -> None:
    """Chamar após tentativa de uso do Mongo (catálogo, saldos, busca)."""
    try:
        h = _health()
        h.mongo_ultimo_ping_em = timezone.now()
        h.mongo_ultimo_ok = bool(ok)
        h.mongo_ultimo_erro = (erro or "")[:4000]
        if ok:
            h.falhas_sequenciais_mongo = 0
        else:
            h.falhas_sequenciais_mongo = int(h.falhas_sequenciais_mongo or 0) + 1
        h.save(
            update_fields=[
                "mongo_ultimo_ping_em",
                "mongo_ultimo_ok",
                "mongo_ultimo_erro",
                "falhas_sequenciais_mongo",
                "atualizado_em",
            ]
        )
    except Exception:
        logger.exception("registrar_ping_mongo")


def registrar_catalogo_built(version: str) -> None:
    """Chamar quando o snapshot do catálogo PDV for montado (nova versão)."""
    try:
        h = _health()
        h.catalogo_ultimo_build_em = timezone.now()
        h.catalogo_ultima_versao = (version or "")[:80]
        h.save(update_fields=["catalogo_ultimo_build_em", "catalogo_ultima_versao", "atualizado_em"])
    except Exception:
        logger.exception("registrar_catalogo_built")


def snapshot_health_dict() -> dict:
    from estoque.models import EstoqueSyncHealth

    h = EstoqueSyncHealth.objects.filter(pk=1).first()
    if not h:
        return {
            "ok": True,
            "mongo_ultimo_ping_em": None,
            "mongo_ultimo_ok": True,
            "mongo_ultimo_erro": "",
            "catalogo_ultimo_build_em": None,
            "catalogo_ultima_versao": "",
            "falhas_sequenciais_mongo": 0,
            "alerta": None,
        }
    alerta = None
    if not h.mongo_ultimo_ok:
        alerta = "Último ping ao Mongo de estoque falhou."
    elif (h.falhas_sequenciais_mongo or 0) >= 3:
        alerta = "Várias falhas seguidas ao ler Mongo — verificar conexão."
    return {
        "ok": bool(h.mongo_ultimo_ok),
        "mongo_ultimo_ping_em": h.mongo_ultimo_ping_em.isoformat() if h.mongo_ultimo_ping_em else None,
        "mongo_ultimo_ok": h.mongo_ultimo_ok,
        "mongo_ultimo_erro": (h.mongo_ultimo_erro or "")[:500],
        "catalogo_ultimo_build_em": h.catalogo_ultimo_build_em.isoformat() if h.catalogo_ultimo_build_em else None,
        "catalogo_ultima_versao": h.catalogo_ultima_versao or "",
        "falhas_sequenciais_mongo": h.falhas_sequenciais_mongo,
        "alerta": alerta,
    }
