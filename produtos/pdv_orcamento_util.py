"""Chave de cliente dos orçamentos do PDV (lista / Postgres)."""

from __future__ import annotations

import re

ORCAMENTO_CLIENTE_KEY_MAX = 120
ORCAMENTO_CLIENTE_KEY_CONSUMIDOR = "consumidor_final"
_ORC_CONSUMIDOR_RE = re.compile(r"consumidor\s+n[aã]o\s+identificado", re.I)


def nome_eh_consumidor_final(nome: str | None) -> bool:
    return bool(_ORC_CONSUMIDOR_RE.search(str(nome or "")))


def normalizar_orcamento_cliente_key(key: str | None, nome: str | None = "") -> str:
    """Alinha aliases do consumidor não identificado para a chave canônica da lista."""
    k = str(key or "").strip()
    n = str(nome or "").strip()
    if k.lower() in ("", "null", "undefined"):
        k = ""
    if nome_eh_consumidor_final(n) or nome_eh_consumidor_final(k):
        return ORCAMENTO_CLIENTE_KEY_CONSUMIDOR
    if k.lower().startswith("tmp:") and nome_eh_consumidor_final(k[4:].split(":", 1)[0]):
        return ORCAMENTO_CLIENTE_KEY_CONSUMIDOR
    if k == ORCAMENTO_CLIENTE_KEY_CONSUMIDOR:
        return ORCAMENTO_CLIENTE_KEY_CONSUMIDOR
    return (k or ORCAMENTO_CLIENTE_KEY_CONSUMIDOR)[:ORCAMENTO_CLIENTE_KEY_MAX]


def carimbar_entry_orcamento_pdv(
    payload: dict | None,
    *,
    orc_local_id: int,
    cliente_key: str = "",
    cliente_nome: str = "",
    cliente_mode: str = "",
    total_texto: str = "",
    entrega: bool = False,
    forma_pagamento: str = "",
    usuario_registro: str = "",
    criado_em=None,
) -> dict:
    """Monta o JSON da lista: id/chave do modelo prevalecem sobre o payload."""
    entry = dict(payload) if isinstance(payload, dict) else {}
    nome = str(entry.get("cliente") or cliente_nome or "").strip()
    ck = normalizar_orcamento_cliente_key(cliente_key or entry.get("cliente_key"), nome)
    entry["id"] = int(orc_local_id)
    entry["orc_barcode"] = str(entry.get("orc_barcode") or f"GMORC{orc_local_id}")
    entry["cliente"] = nome
    entry["cliente_key"] = ck
    if cliente_mode:
        entry["cliente_mode"] = str(cliente_mode)
    elif not entry.get("cliente_mode"):
        entry["cliente_mode"] = (
            ORCAMENTO_CLIENTE_KEY_CONSUMIDOR
            if ck == ORCAMENTO_CLIENTE_KEY_CONSUMIDOR
            else "cliente"
        )
    if total_texto and not entry.get("total"):
        entry["total"] = total_texto
    if "entrega" not in entry:
        entry["entrega"] = bool(entrega)
    if forma_pagamento and not entry.get("forma_pagamento"):
        entry["forma_pagamento"] = forma_pagamento
    if usuario_registro and not entry.get("usuario"):
        entry["usuario"] = usuario_registro
    if criado_em and not entry.get("data"):
        try:
            entry["data"] = criado_em.strftime("%d/%m/%Y, %H:%M:%S")
        except Exception:
            pass
    return entry
