"""Cadastro de plano de contas Agro (Postgres)."""
from __future__ import annotations

from typing import Any

from django.db.models import Q

from produtos.models import PlanoContaAgro

ID_PREFIX = "agro:"


def id_publico_plano(pk: int | str) -> str:
    return f"{ID_PREFIX}{pk}"


def parse_id_publico(raw: str) -> int | None:
    s = str(raw or "").strip()
    if not s.lower().startswith(ID_PREFIX):
        return None
    try:
        return int(s[len(ID_PREFIX) :])
    except (TypeError, ValueError):
        return None


def serializar_plano(p: PlanoContaAgro) -> dict[str, Any]:
    return {
        "id": id_publico_plano(p.pk),
        "pk": p.pk,
        "nome": p.nome,
        "nome_exibicao": p.nome,
        "tipo": p.tipo or PlanoContaAgro.Tipo.OUTRA,
        "tipo_label": p.get_tipo_display(),
        "grupo": p.grupo or "",
        "ativo": bool(p.ativo),
        "observacao": p.observacao or "",
        "apelidos": p.aliases.count(),
        "fonte": "agro",
    }


def listar_planos_agro(*, q: str = "", incluir_inativos: bool = False) -> list[dict[str, Any]]:
    qs = PlanoContaAgro.objects.all()
    if not incluir_inativos:
        qs = qs.filter(ativo=True)
    qq = (q or "").strip()
    if qq:
        qs = qs.filter(Q(nome__icontains=qq) | Q(grupo__icontains=qq))
    return [serializar_plano(p) for p in qs[:300]]


def injetar_planos_agro_sugestao(
    itens: list[dict[str, Any]] | None,
    q: str | None,
    *,
    limit: int = 30,
) -> list[dict[str, Any]]:
    """Coloca planos Agro ativos no topo do autocomplete de plano."""
    out = [x for x in (itens or []) if isinstance(x, dict)]
    agro = listar_planos_agro(q=q or "", incluir_inativos=False)
    if not agro:
        return out
    seen_ids = {str(it.get("id") or "").strip() for it in out}
    seen_nomes = {str(it.get("nome") or "").strip().casefold() for it in out}
    inj: list[dict[str, Any]] = []
    for p in agro:
        pid = str(p.get("id") or "")
        nome = str(p.get("nome_exibicao") or p.get("nome") or "").strip()
        if pid in seen_ids or nome.casefold() in seen_nomes:
            continue
        inj.append(
            {
                "id": pid,
                "nome": nome,
                "fonte": "agro",
            }
        )
        seen_ids.add(pid)
        seen_nomes.add(nome.casefold())
        if len(inj) >= limit:
            break
    if not inj:
        return out
    return [*inj, *out][: max(limit, len(out))]
