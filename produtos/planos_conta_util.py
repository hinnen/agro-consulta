"""Cadastro de plano de contas Agro (Postgres)."""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from django.conf import settings
from django.db.models import Q

from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

ID_PREFIX = "agro:"

logger = logging.getLogger(__name__)


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


def _csv_niveis_path() -> Path:
    return Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_niveis_proposta.csv"


def _csv_mapa_path() -> Path:
    return Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_mapa_unificacao.csv"


def _tipo_from_csv(raw: str) -> str:
    t = (raw or "").strip().casefold()
    if t.startswith("fix"):
        return PlanoContaAgro.Tipo.FIXA
    if t.startswith("var"):
        return PlanoContaAgro.Tipo.VARIAVEL
    return PlanoContaAgro.Tipo.OUTRA


def seed_planos_padrao() -> dict[str, int]:
    """Cria os planos oficiais + apelidos a partir dos CSV. Idempotente (mesma lista da loja)."""
    stats = {"planos": 0, "apelidos": 0, "ja_existiam": 0}
    niveis = _csv_niveis_path()
    if not niveis.is_file():
        logger.warning("seed planos: CSV ausente %s", niveis)
        return stats

    with niveis.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            nome = (row.get("Plano oficial") or "").strip()[:200]
            if not nome:
                continue
            _, criado = PlanoContaAgro.objects.get_or_create(
                nome=nome,
                defaults={
                    "tipo": _tipo_from_csv(row.get("Tipo") or ""),
                    "grupo": (row.get("Grupo") or "").strip()[:120],
                    "observacao": (row.get("Observação") or row.get("Observacao") or "").strip()[
                        :400
                    ],
                    "ativo": True,
                },
            )
            stats["planos" if criado else "ja_existiam"] += 1

    mapa = _csv_mapa_path()
    if not mapa.is_file():
        return stats
    with mapa.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        cols = {(c or "").strip().lower(): c for c in (reader.fieldnames or [])}
        k_ant = cols.get("nome antigo (como está no cp)") or cols.get("nome antigo")
        k_ofi = cols.get("nome oficial")
        if not (k_ant and k_ofi):
            return stats
        for row in reader:
            antigo = (row.get(k_ant) or "").strip()[:200]
            oficial = (row.get(k_ofi) or "").strip()[:200]
            if not antigo or not oficial or antigo == oficial:
                continue
            plano = PlanoContaAgro.objects.filter(nome=oficial).first()
            if plano is None:
                plano = PlanoContaAgro.objects.create(
                    nome=oficial,
                    tipo=_tipo_from_csv(row.get("Tipo") or ""),
                    grupo=(row.get("Grupo") or "").strip()[:120],
                    ativo=True,
                )
                stats["planos"] += 1
            _, criado = PlanoContaAliasAgro.objects.get_or_create(
                grafia=antigo, defaults={"plano": plano}
            )
            if criado:
                stats["apelidos"] += 1
    return stats


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
