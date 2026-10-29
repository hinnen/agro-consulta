"""Unificação de fornecedores na Entrada NF — só daqui pra frente (alias Postgres; não altera CP antigo)."""

from __future__ import annotations

import re
from typing import Any

from django.db import transaction

_MONGO_ID_RE = re.compile(r"^[a-fA-F0-9]{24}$")


def _norm_id(pid: str | None) -> str:
    return str(pid or "").strip()


def fornecedor_id_mongo_valido(pid: str | None) -> bool:
    return bool(_MONGO_ID_RE.match(_norm_id(pid)))


def mapa_fornecedor_duplicado_para_canonico() -> dict[str, str]:
    """Id duplicado → id canônico (só aliases ativos no Postgres)."""
    from produtos.models import FornecedorEntradaNfeAliasAgro

    out: dict[str, str] = {}
    for dup, can in FornecedorEntradaNfeAliasAgro.objects.values_list(
        "fornecedor_duplicado_id",
        "fornecedor_canonico_id",
    ):
        d = _norm_id(dup)
        c = _norm_id(can)
        if d and c and d != c:
            out[d] = c
    return out


def resolver_fornecedor_canonico_id(pid: str | None) -> str:
    """Segue cadeia curta de aliases (A→B→C)."""
    cur = _norm_id(pid)
    if not cur:
        return ""
    mapping = mapa_fornecedor_duplicado_para_canonico()
    seen: set[str] = set()
    while cur in mapping and cur not in seen:
        seen.add(cur)
        cur = mapping[cur]
    return cur


def filtrar_itens_fornecedor_entrada_nfe(itens: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove duplicados da lista; reescreve id para canônico quando o item é o principal."""
    dup_map = mapa_fornecedor_duplicado_para_canonico()
    if not dup_map:
        return itens
    dup_ids = set(dup_map.keys())
    out: list[dict[str, Any]] = []
    seen_canon: set[str] = set()
    for row in itens:
        if not isinstance(row, dict):
            continue
        rid = _norm_id(row.get("id"))
        if rid in dup_ids:
            continue
        canon = resolver_fornecedor_canonico_id(rid) if rid else rid
        row2 = dict(row)
        if canon and canon != rid:
            row2["id"] = canon
        # Uma linha por id canônico após merge de origens
        ik = _norm_id(row2.get("id"))
        if ik:
            if ik in seen_canon:
                continue
            seen_canon.add(ik)
        out.append(row2)
    return out


@transaction.atomic
def registrar_unificacao_fornecedores_entrada_nfe(
    *,
    fornecedor_canonico_id: str,
    duplicados_ids: list[str],
    canonico_nome: str = "",
    usuario=None,
) -> dict[str, Any]:
    """
    Grava aliases duplicado→canônico. **Não** altera títulos / contas a pagar existentes.
    """
    from produtos.models import FornecedorEntradaNfeAliasAgro

    canon = _norm_id(fornecedor_canonico_id)
    if not fornecedor_id_mongo_valido(canon):
        return {"ok": False, "erro": "Escolha um fornecedor principal com cadastro válido (Id ERP)."}

    dups_in = [_norm_id(x) for x in (duplicados_ids or []) if _norm_id(x)]
    dups: list[str] = []
    for d in dups_in:
        if d == canon:
            continue
        if not fornecedor_id_mongo_valido(d):
            continue
        if d not in dups:
            dups.append(d)
    if not dups:
        return {"ok": False, "erro": "Marque ao menos um cadastro duplicado diferente do principal."}

    mapping = mapa_fornecedor_duplicado_para_canonico()
    criados = 0
    atualizados = 0
    for dup in dups:
        if dup in mapping and mapping[dup] != canon:
            return {
                "ok": False,
                "erro": (
                    f"O cadastro {dup[:8]}… já está unificado a outro principal. "
                    "Desfaça no Admin ou escolha outro duplicado."
                ),
            }
        obj, created = FornecedorEntradaNfeAliasAgro.objects.update_or_create(
            fornecedor_duplicado_id=dup,
            defaults={
                "fornecedor_canonico_id": canon,
                "canonico_nome": (canonico_nome or "")[:300],
                "criado_por": usuario if getattr(usuario, "pk", None) else None,
            },
        )
        if created:
            criados += 1
        else:
            atualizados += 1

    return {
        "ok": True,
        "canonico_id": canon,
        "duplicados": dups,
        "criados": criados,
        "atualizados": atualizados,
        "aviso": "Contas a pagar e títulos antigos não foram alterados — só novas entradas usam o cadastro único.",
    }
