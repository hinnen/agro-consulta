"""Unificação de fornecedores na Entrada NF — só daqui pra frente (alias Postgres; não altera CP antigo)."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from django.db import transaction

_MONGO_ID_RE = re.compile(r"^[a-fA-F0-9]{24}$")
_LOCAL_ID_RE = re.compile(r"^local:\d+$", re.IGNORECASE)
_NOME_ALIAS_ID_RE = re.compile(r"^n:[a-fA-F0-9]{16,28}$")


def _norm_id(pid: str | None) -> str:
    return str(pid or "").strip()


def fornecedor_id_mongo_valido(pid: str | None) -> bool:
    return bool(_MONGO_ID_RE.match(_norm_id(pid)))


def fornecedor_id_local_agro_valido(pid: str | None) -> bool:
    return bool(_LOCAL_ID_RE.match(_norm_id(pid)))


def fornecedor_id_nome_alias_valido(pid: str | None) -> bool:
    return bool(_NOME_ALIAS_ID_RE.match(_norm_id(pid)))


def fornecedor_id_unificavel(pid: str | None) -> bool:
    """Id que o modal de unificação aceita: Mongo ERP, cadastro Agro ou chave por nome."""
    s = _norm_id(pid)
    if not s:
        return False
    return (
        fornecedor_id_mongo_valido(s)
        or fornecedor_id_local_agro_valido(s)
        or fornecedor_id_nome_alias_valido(s)
    )


def chave_nome_alias_fornecedor(nome: str | None) -> str:
    """Id sintético estável p/ cadastro sem Id ERP (ex.: título «CNPJ — Nome»)."""
    nk = " ".join(str(nome or "").strip().lower().split())
    if not nk:
        return ""
    return "n:" + hashlib.sha1(nk.encode("utf-8")).hexdigest()[:28]


def id_estavel_fornecedor_entrada_nfe(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    rid = _norm_id(row.get("id"))
    if rid:
        return rid
    return chave_nome_alias_fornecedor(row.get("nome"))


def estabilizar_ids_itens_fornecedor_entrada_nfe(
    itens: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Garante id em toda linha (preenche chave por nome quando o Id ERP falta)."""
    out: list[dict[str, Any]] = []
    for row in itens or []:
        if not isinstance(row, dict):
            continue
        row2 = dict(row)
        if not _norm_id(row2.get("id")):
            cid = chave_nome_alias_fornecedor(row2.get("nome"))
            if cid:
                row2["id"] = cid
                row2["id_sintetico"] = True
        out.append(row2)
    return out


def tokens_busca_unificar_fornecedor(q: str | None) -> list[str]:
    """Tokens úteis p/ achar fantasia e razão social parecidas (ex.: «Sn - Rbs» → rbs)."""
    s = re.sub(r"[^a-zA-Z0-9]+", " ", str(q or "").lower())
    stop = {
        "ltda",
        "ltd",
        "sa",
        "me",
        "epp",
        "comercio",
        "comércio",
        "distribuidora",
        "distribuicao",
        "alimentos",
        "produtos",
        "industria",
        "indústria",
        "de",
        "da",
        "do",
        "das",
        "dos",
        "e",
        "sn",
        "cn",
    }
    out: list[str] = []
    seen: set[str] = set()
    for t in s.split():
        if len(t) < 3 or t in stop:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= 6:
            break
    return out


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
        rid = id_estavel_fornecedor_entrada_nfe(row)
        if rid in dup_ids:
            continue
        canon = resolver_fornecedor_canonico_id(rid) if rid else rid
        row2 = dict(row)
        if canon and canon != _norm_id(row2.get("id")):
            # Só reescreve id real; id vazio permanece vazio na busca normal
            if _norm_id(row2.get("id")):
                row2["id"] = canon
        # Uma linha por id canônico após merge de origens
        ik = id_estavel_fornecedor_entrada_nfe(row2)
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
    Aceita Id Mongo, ``local:<pk>`` (cadastro Agro) e ``n:<hash>`` (nome sem Id ERP).
    """
    from produtos.models import FornecedorEntradaNfeAliasAgro

    canon = _norm_id(fornecedor_canonico_id)
    if not fornecedor_id_unificavel(canon):
        return {
            "ok": False,
            "erro": "Escolha um fornecedor principal com cadastro válido (Id ERP, Agro ou nome).",
        }

    dups_in = [_norm_id(x) for x in (duplicados_ids or []) if _norm_id(x)]
    dups: list[str] = []
    for d in dups_in:
        if d == canon:
            continue
        if not fornecedor_id_unificavel(d):
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
