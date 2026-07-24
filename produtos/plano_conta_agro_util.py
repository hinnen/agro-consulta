"""Cadastro oficial de planos de conta (Postgres) — sem Mongo, sem renomear títulos.

- Registry: ``PlanoContaAgro``
- Alias: grafia no título → oficial (só mapeia; não altera ``TituloFinanceiroAgro``)
- Lançamento manual: só nome cadastrado (ou cria na hora)
"""
from __future__ import annotations

import csv
import logging
import unicodedata
from pathlib import Path
from typing import Any

from django.conf import settings
from django.db import transaction

logger = logging.getLogger(__name__)

_SEM_PLANO = "(sem plano)"


def _csv_niveis_path() -> Path:
    return Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_niveis_proposta.csv"


def _csv_mapa_path() -> Path:
    return Path(settings.BASE_DIR) / "docs" / "dados" / "plano_despesas_mapa_unificacao.csv"


def norm_plano_chave(nome: str) -> str:
    s = (nome or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.casefold().split())


def _tipo_from_csv(raw: str) -> str:
    t = (raw or "").strip().casefold()
    if t.startswith("fix"):
        return "fixa"
    if t.startswith("var"):
        return "variavel"
    return "outra"


def seed_planos_conta_agro(*, force: bool = False) -> dict[str, int]:
    """Cria planos oficiais + aliases a partir dos CSV (idempotente)."""
    from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

    stats = {"planos": 0, "aliases": 0, "ja_existiam": 0}
    niveis = _csv_niveis_path()
    if not niveis.is_file():
        logger.warning("seed planos: CSV níveis ausente %s", niveis)
        return stats

    with niveis.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            nome = (row.get("Plano oficial") or row.get("plano oficial") or "").strip()
            if not nome:
                continue
            tipo = _tipo_from_csv(row.get("Tipo") or row.get("tipo") or "")
            grupo = (row.get("Grupo") or row.get("grupo") or "").strip()[:120]
            obs = (row.get("Observação") or row.get("Observacao") or row.get("observação") or "").strip()[:400]
            obj, created = PlanoContaAgro.objects.get_or_create(
                nome=nome,
                defaults={"tipo": tipo, "grupo": grupo, "observacao": obs, "ativo": True},
            )
            if created:
                stats["planos"] += 1
            else:
                stats["ja_existiam"] += 1
                if force:
                    obj.tipo = tipo or obj.tipo
                    obj.grupo = grupo or obj.grupo
                    if obs:
                        obj.observacao = obs
                    obj.ativo = True
                    obj.save(update_fields=["tipo", "grupo", "observacao", "ativo", "atualizado_em"])

    mapa = _csv_mapa_path()
    if mapa.is_file():
        with mapa.open(encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            cols = {(c or "").strip().lower(): c for c in (reader.fieldnames or [])}
            k_ant = cols.get("nome antigo (como está no cp)") or cols.get("nome antigo")
            k_ofi = cols.get("nome oficial")
            if k_ant and k_ofi:
                for row in reader:
                    antigo = (row.get(k_ant) or "").strip()
                    oficial = (row.get(k_ofi) or "").strip()
                    if not antigo or not oficial or antigo == oficial:
                        continue
                    plano = PlanoContaAgro.objects.filter(nome=oficial).first()
                    if plano is None:
                        plano = PlanoContaAgro.objects.create(
                            nome=oficial,
                            tipo=_tipo_from_csv(row.get("Tipo") or row.get("tipo") or ""),
                            grupo=(row.get("Grupo") or row.get("grupo") or "").strip()[:120],
                            ativo=True,
                        )
                        stats["planos"] += 1
                    _, created = PlanoContaAliasAgro.objects.get_or_create(
                        grafia=antigo,
                        defaults={"plano": plano},
                    )
                    if created:
                        stats["aliases"] += 1
    return stats


def listar_planos_cadastro(*, ativos: bool = True, q: str | None = None) -> list[dict[str, Any]]:
    from produtos.models import PlanoContaAgro

    qs = PlanoContaAgro.objects.all()
    if ativos:
        qs = qs.filter(ativo=True)
    termo = (q or "").strip()
    if termo:
        chave = norm_plano_chave(termo)
        out: list[dict[str, Any]] = []
        for p in qs.order_by("nome")[:500]:
            if chave in norm_plano_chave(p.nome) or chave in norm_plano_chave(p.grupo):
                out.append(_plano_dict(p))
            if len(out) >= 80:
                break
        return out
    return [_plano_dict(p) for p in qs.order_by("nome")[:500]]


def _plano_dict(p) -> dict[str, Any]:
    return {
        "id": p.pk,
        "nome": p.nome,
        "tipo": p.tipo,
        "grupo": p.grupo,
        "ativo": p.ativo,
    }


def _mapa_resolucao() -> tuple[dict[str, str], set[str]]:
    """chave_norm → nome oficial; set de nomes oficiais ativos."""
    from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

    oficiais: dict[str, str] = {}
    nomes_oficiais: set[str] = set()
    try:
        for nome in PlanoContaAgro.objects.filter(ativo=True).values_list("nome", flat=True):
            n = (nome or "").strip()
            if not n:
                continue
            nomes_oficiais.add(n)
            oficiais[norm_plano_chave(n)] = n
        for grafia, oficial in PlanoContaAliasAgro.objects.select_related("plano").filter(
            plano__ativo=True
        ).values_list("grafia", "plano__nome"):
            g = (grafia or "").strip()
            o = (oficial or "").strip()
            if g and o:
                oficiais[norm_plano_chave(g)] = o
    except Exception:
        logger.debug("_mapa_resolucao: cadastro indisponível", exc_info=True)
        return {}, set()
    return oficiais, nomes_oficiais


def resolver_nome_oficial(grafia: str) -> str | None:
    """Retorna nome oficial se cadastrado/alias; senão None."""
    g = (grafia or "").strip()
    if not g or g == _SEM_PLANO:
        return None
    mapa, oficiais = _mapa_resolucao()
    if g in oficiais:
        return g
    return mapa.get(norm_plano_chave(g))


def expandir_nomes_exclusao(nomes: list[str] | None) -> list[str]:
    """Ao excluir um plano na UI, inclui grafias alias do mesmo oficial."""
    from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

    raw = [str(x).strip() for x in (nomes or []) if x and str(x).strip()]
    if not raw:
        return []
    out: set[str] = set(raw)
    mapa, oficiais = _mapa_resolucao()
    # nome → todas grafias conhecidas (oficial + aliases)
    por_oficial: dict[str, set[str]] = {n: {n} for n in oficiais}
    for grafia, oficial in PlanoContaAliasAgro.objects.filter(plano__ativo=True).values_list(
        "grafia", "plano__nome"
    ):
        g = (grafia or "").strip()
        o = (oficial or "").strip()
        if g and o:
            por_oficial.setdefault(o, {o}).add(g)

    for nome in list(raw):
        if nome == _SEM_PLANO or nome.lower() == "(sem plano)":
            continue
        oficial = mapa.get(norm_plano_chave(nome)) or (nome if nome in oficiais else None)
        if oficial and oficial in por_oficial:
            out.update(por_oficial[oficial])
        # também: se o checkbox é o oficial, ok; se é grafia órfã sem alias, só ela
    return list(out)[:400]


def mesclar_planos_distintos(planos: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Agrupa distinct de títulos pelo nome oficial (alias); marca órfãos."""
    from produtos.mongo_financeiro_util import EMPRESTIMO_DUAL_LABEL

    mapa, oficiais = _mapa_resolucao()
    grupos: dict[str, dict[str, Any]] = {}
    for item in planos or []:
        nome = (item.get("nome") or "").strip() or _SEM_PLANO
        if nome == _SEM_PLANO:
            chave_ui = _SEM_PLANO
            oficial = _SEM_PLANO
            orfao = False
        elif nome == EMPRESTIMO_DUAL_LABEL:
            chave_ui = nome
            oficial = nome
            orfao = False
        else:
            oficial = mapa.get(norm_plano_chave(nome))
            if oficial:
                chave_ui = oficial
                orfao = False
            elif nome in oficiais:
                chave_ui = nome
                orfao = False
            else:
                chave_ui = nome
                oficial = nome
                orfao = True
        g = grupos.get(chave_ui)
        if g is None:
            grupos[chave_ui] = {
                "nome": chave_ui,
                "oficial": oficial,
                "orfao": orfao,
                "grafias": [nome],
            }
        else:
            if nome not in g["grafias"]:
                g["grafias"].append(nome)
            if orfao:
                g["orfao"] = True
    return sorted(grupos.values(), key=lambda r: (r["nome"] or "").casefold())


def eh_plano_fora_cadastro(nome: str) -> bool:
    """True se o texto do título não está no cadastro nem em alias (e não é dual/sistema)."""
    from produtos.mongo_financeiro_util import EMPRESTIMO_DUAL_LABEL

    g = (nome or "").strip()
    if not g or g == _SEM_PLANO or g == EMPRESTIMO_DUAL_LABEL:
        return False
    if not cadastro_planos_disponivel():
        return False
    return resolver_nome_oficial(g) is None


def marcar_planos_orfaos_nas_linhas(itens: list[dict[str, Any]]) -> None:
    """Injeta ``plano_orfao`` nas linhas da lista CP (in-place)."""
    if not itens:
        return
    if not cadastro_planos_disponivel():
        for it in itens:
            if isinstance(it, dict):
                it["plano_orfao"] = False
        return
    from produtos.mongo_financeiro_util import EMPRESTIMO_DUAL_LABEL

    mapa, oficiais = _mapa_resolucao()
    for it in itens:
        if not isinstance(it, dict):
            continue
        g = str(it.get("plano_conta") or "").strip()
        if not g or g == EMPRESTIMO_DUAL_LABEL:
            it["plano_orfao"] = False
            continue
        if g in oficiais or mapa.get(norm_plano_chave(g)):
            it["plano_orfao"] = False
        else:
            it["plano_orfao"] = True


def listar_orfaos_cp(*, despesa: bool = True) -> list[dict[str, Any]]:
    """Planos que aparecem em títulos e não estão no cadastro nem em alias."""
    from produtos.models import TituloFinanceiroAgro
    from produtos.mongo_financeiro_util import EMPRESTIMO_DUAL_LABEL

    mapa, oficiais = _mapa_resolucao()
    qs = TituloFinanceiroAgro.objects.filter(despesa=despesa).exclude(plano_conta="")
    nomes = {
        str(p).strip()
        for p in qs.values_list("plano_conta", flat=True).distinct()
        if str(p).strip()
    }
    out: list[dict[str, Any]] = []
    for nome in sorted(nomes, key=str.casefold):
        if nome == EMPRESTIMO_DUAL_LABEL:
            continue
        if nome in oficiais:
            continue
        if mapa.get(norm_plano_chave(nome)):
            continue
        n = TituloFinanceiroAgro.objects.filter(despesa=despesa, plano_conta=nome).count()
        out.append({"grafia": nome, "titulos": n})
    return out


@transaction.atomic
def mapear_grafia_para_oficial(
    grafia: str,
    plano_oficial: str,
    *,
    usuario=None,
) -> dict[str, Any]:
    """Cria/atualiza alias. Não altera títulos."""
    from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

    g = (grafia or "").strip()
    oficial = (plano_oficial or "").strip()
    if not g or not oficial:
        return {"ok": False, "erro": "Informe a grafia e o plano oficial."}
    if g == oficial:
        return {"ok": False, "erro": "A grafia já é o nome oficial."}
    plano = PlanoContaAgro.objects.filter(nome=oficial, ativo=True).first()
    if plano is None:
        return {"ok": False, "erro": f"Plano oficial «{oficial}» não cadastrado."}
    alias, created = PlanoContaAliasAgro.objects.update_or_create(
        grafia=g,
        defaults={"plano": plano, "criado_por": usuario if getattr(usuario, "pk", None) else None},
    )
    return {
        "ok": True,
        "created": created,
        "grafia": alias.grafia,
        "oficial": plano.nome,
        "plano_id": plano.pk,
    }


@transaction.atomic
def criar_plano_cadastro(
    nome: str,
    *,
    tipo: str = "outra",
    grupo: str = "",
    observacao: str = "",
) -> dict[str, Any]:
    from produtos.models import PlanoContaAgro

    n = (nome or "").strip()[:200]
    if len(n) < 2:
        return {"ok": False, "erro": "Nome do plano muito curto."}
    existente = None
    for p in PlanoContaAgro.objects.all()[:500]:
        if norm_plano_chave(p.nome) == norm_plano_chave(n):
            existente = p
            break
    if existente:
        if not existente.ativo:
            existente.ativo = True
            existente.save(update_fields=["ativo", "atualizado_em"])
        return {"ok": True, "created": False, "plano": _plano_dict(existente)}
    t = _tipo_from_csv(tipo) if tipo else "outra"
    if t not in ("fixa", "variavel", "outra"):
        t = "outra"
    p = PlanoContaAgro.objects.create(
        nome=n,
        tipo=t,
        grupo=(grupo or "").strip()[:120],
        observacao=(observacao or "").strip()[:400],
        ativo=True,
    )
    return {"ok": True, "created": True, "plano": _plano_dict(p)}


def cadastro_planos_disponivel() -> bool:
    """True se o seed/migrate já populou o cadastro (senão fail-open)."""
    try:
        from produtos.models import PlanoContaAgro

        return PlanoContaAgro.objects.filter(ativo=True).exists()
    except Exception:
        return False


def validar_plano_para_lancamento_manual(plano_nome: str, plano_id: str | None = None) -> dict[str, Any]:
    """Aceita nome do cadastro, alias resolvido, ou pseudo empréstimo dual."""
    from produtos.mongo_financeiro_util import EMPRESTIMO_DUAL_LABEL, EMPRESTIMO_DUAL_PLANO_ID

    pn = (plano_nome or "").strip()
    pid = str(plano_id or "").strip()
    if pid == EMPRESTIMO_DUAL_PLANO_ID or pn == EMPRESTIMO_DUAL_LABEL:
        return {"ok": True, "nome": EMPRESTIMO_DUAL_LABEL, "plano_conta_id": EMPRESTIMO_DUAL_PLANO_ID}
    if not pn:
        return {"ok": False, "erro": "Plano de conta obrigatório."}
    if not cadastro_planos_disponivel():
        return {"ok": True, "nome": pn, "plano_conta_id": pid or None, "cadastro_vazio": True}
    oficial = resolver_nome_oficial(pn)
    if oficial:
        return {"ok": True, "nome": oficial, "plano_conta_id": pid or None}
    return {
        "ok": False,
        "erro": (
            f"Plano «{pn}» não está no cadastro. Escolha um plano da lista "
            "ou cadastre um novo antes de gravar."
        ),
        "orfao": True,
        "grafia": pn,
    }


def sugestoes_plano_cadastro(q: str | None = None, *, limit: int = 40) -> list[dict[str, str]]:
    """Autocomplete só do cadastro oficial (+ dual injetado na view)."""
    itens = listar_planos_cadastro(ativos=True, q=q)
    lim = min(max(int(limit or 40), 1), 80)
    return [{"id": str(p["id"]), "nome": p["nome"]} for p in itens[:lim]]
