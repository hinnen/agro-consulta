"""Bloqueio de escrita no Mongo (staging lê espelho ERP, não envia alterações)."""
from __future__ import annotations

from django.conf import settings

_MSG_PADRAO = (
    "Ambiente de teste: gravação no Mongo bloqueada. "
    "Alteração salva só no Agro (Postgres deste site), não afeta a loja."
)


def agro_mongo_escrita_bloqueada() -> bool:
    return bool(getattr(settings, "AGRO_STAGING_READONLY", False))


def exigir_mongo_escrita_permitida(mensagem: str = "") -> None:
    if agro_mongo_escrita_bloqueada():
        raise ValueError(mensagem or _MSG_PADRAO)


def agro_mongo_guard_status() -> dict:
    blocked = agro_mongo_escrita_bloqueada()
    return {"mongo_escrita": not blocked}
