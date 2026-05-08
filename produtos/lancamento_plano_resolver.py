"""Resolve linguagem livre → plano de contas já usado no Mongo (sugestões).

Duas estratégias:
- **Lista ancorada + LLM**: o modelo só escolhe um índice entre nomes reais (RAG curto).
- **Roteiro por pontuação**: sem chave de API ou quando a IA não responde — heurísticas + sinônimos.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

from .lancamento_llm_client import gerar_json_llm, resolver_credencial_llm
from .mongo_financeiro_util import lancamentos_sugestoes_campo

logger = logging.getLogger(__name__)


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in s if not unicodedata.combining(c))


def _tokens_palavra(s: str) -> set[str]:
    return {t for t in re.findall(r"[a-záàâãéêíóôõúç]+", _fold(s)) if len(t) >= 2}


def _queries_busca_plano(dica: str, frase_completa: str = "") -> list[str]:
    """Gera termos de busca (regex no Mongo) a partir da dica e da frase original."""
    out: list[str] = []
    seen: set[str] = set()

    _skip_q = frozenset({"de", "da", "do", "das", "dos", "e", "em", "a", "o", "os", "as"})

    def add(q: str) -> None:
        q = (q or "").strip()
        if len(q) < 2 or q.lower() in seen or q.lower() in _skip_q:
            return
        seen.add(q.lower())
        out.append(q)

    base = (dica or "").strip()
    fc = (frase_completa or "").strip()
    add(base)

    # Última palavra costuma ser o substantivo (conta de luz → luz).
    parts = _fold(base).split()
    if len(parts) >= 2:
        add(parts[-1])
    if len(parts) >= 1 and parts[0] != parts[-1]:
        add(parts[0])

    # Sinônimos / linguagem coloquial (pt-BR)
    bag = _fold(f"{base} {fc}")
    if "luz" in bag or "eletrica" in bag:
        add("energia")
        add("elétrica")
        add("luz")
    if "agua" in bag:
        add("água")
        add("saneamento")
    if "internet" in bag or "telefone" in bag or "celular" in bag:
        add("telecom")
        add("internet")
    if "aluguel" in bag:
        add("aluguel")
    if "frete" in bag or "transporte" in bag or "correio" in bag:
        add("frete")
        add("transporte")

    return out[:10]


def _score_match(nome_plano: str, dica: str, frase_completa: str) -> float:
    nl = nome_plano.lower()
    nf = _fold(nome_plano)
    dica_f = _fold(dica)
    frase_f = _fold(frase_completa)
    bag = f"{dica_f} {frase_f}"
    toks = _tokens_palavra(dica) | _tokens_palavra(frase_completa)
    sc = 0.0
    for t in toks:
        if len(t) < 2:
            continue
        if t in nl or t in nf:
            sc += 3.0
    if dica_f and len(dica_f) >= 3 and dica_f in nl:
        sc += 8.0
    # Coloquial: luz / conta de luz → energia
    if "luz" in bag:
        if any(k in nl for k in ("energia", "elétrica", "eletrica", "cee", "cosern", "cpfl", "enel")):
            sc += 14.0
        if "luz" in nl:
            sc += 6.0
    if "agua" in bag or "água" in (dica + frase_completa).lower():
        if "agua" in nl or "água" in nome_plano.lower() or "saneamento" in nl:
            sc += 10.0
    return sc


def coletar_candidatos_plano_ordenados(
    db: Any | None,
    dica: str,
    frase_completa: str,
    *,
    max_n: int = 28,
) -> list[tuple[str, str, float]]:
    """Planos únicos (nome, id, score_heurístico) ordenados do melhor para o pior."""
    out: list[tuple[str, str, float]] = []
    if db is None or not (dica or "").strip():
        return out
    qs = _queries_busca_plano(dica.strip(), frase_completa)
    aggregated: dict[tuple[str, str], tuple[str, str, float]] = {}
    for q in qs:
        try:
            itens = lancamentos_sugestoes_campo(db, "plano", q=q, limit=35)
        except Exception:
            continue
        for it in itens:
            nome = str(it.get("nome") or "").strip()
            pid = str(it.get("id") or "").strip()
            if not nome:
                continue
            key = (nome.lower(), pid)
            sc = _score_match(nome, dica, frase_completa)
            prev = aggregated.get(key)
            if prev is None or sc > prev[2]:
                aggregated[key] = (nome, pid, sc)
    ranked = sorted(aggregated.values(), key=lambda x: -x[2])
    return ranked[:max_n]


def _prompt_escolher_plano_lista(frase: str, cands: list[tuple[str, str]]) -> str:
    linhas = [f"{i}. {nome}" for i, (nome, _pid) in enumerate(cands, start=1)]
    bloco = "\n".join(linhas)
    nmax = len(cands)
    return (
        "Você trabalha para um ERP de contas a pagar/receber no Brasil.\n"
        "Sua tarefa: entender o que o usuário quis dizer em linguagem coloquial e "
        "escolher exatamente UMA linha da lista numerada abaixo.\n"
        "Cada linha é um nome oficial de PLANO DE CONTAS já utilizado nos lançamentos do sistema.\n"
        "Regras: não invente nome fora da lista; não misture dados; "
        'se ficar duvidoso ou nenhuma opção servir bem, devolva "indice" null.\n\n'
        f"Lista ({nmax} opções válidas):\n{bloco}\n\n"
        f"Frase do usuário sobre o gasto ou receita:\n---\n{frase.strip()}\n---\n\n"
        "Responda somente este JSON compacto "
        '(ex.: {"indice": 3} ou {"indice": null}):\n'
        '{"indice": número inteiro de 1 até '
        + str(nmax)
        + " ou null}. Sem texto extra."
    )


def eleger_plano_via_llm_lista_ancorada(
    frase: str, candidatos: list[tuple[str, str]]
) -> int | None:
    """Índice 0-based nos ``candidatos`` ou ``None``. Uma chamada ao LLM configurado."""
    if len(candidatos) < 2:
        return None
    cred = resolver_credencial_llm()
    if cred is None:
        return None
    prompt = _prompt_escolher_plano_lista(frase, candidatos)
    parsed, _prov = gerar_json_llm(prompt)
    if not parsed:
        return None
    raw_ix = parsed.get("indice")
    if raw_ix is None:
        return None
    try:
        j = int(raw_ix)
    except (TypeError, ValueError):
        return None
    if j < 1 or j > len(candidatos):
        return None
    return j - 1


def resolver_plano_por_dica(
    db: Any | None,
    dica: str,
    *,
    frase_completa: str = "",
    min_score: float = 5.5,
    min_gap: float = 2.0,
) -> dict[str, Any] | None:
    """
    Busca vários ``q`` em planos já presentes nos lançamentos e escolhe o melhor scoring.

    Retorna ``{"nome", "id", "score"}`` ou ``None`` se ambíguo ou fraco.
    """
    if db is None or not (dica or "").strip():
        return None

    qs = _queries_busca_plano(dica.strip(), frase_completa)
    if not qs:
        return None

    aggregated: dict[tuple[str, str], tuple[str, str, float]] = {}
    for q in qs:
        try:
            itens = lancamentos_sugestoes_campo(db, "plano", q=q, limit=30)
        except Exception:
            continue
        for it in itens:
            nome = str(it.get("nome") or "").strip()
            pid = str(it.get("id") or "").strip()
            if not nome:
                continue
            key = (nome.lower(), pid)
            sc = _score_match(nome, dica, frase_completa)
            prev = aggregated.get(key)
            if prev is None or sc > prev[2]:
                aggregated[key] = (nome, pid, sc)

    if not aggregated:
        return None

    ranked = sorted(aggregated.values(), key=lambda x: -x[2])
    best_n, best_id, best_s = ranked[0]
    second_s = ranked[1][2] if len(ranked) > 1 else 0.0
    if best_s < min_score:
        return None
    if second_s >= min_score and (best_s - second_s) < min_gap:
        return None
    return {"nome": best_n, "id": best_id, "score": best_s}


def enriquecer_interpretacao_com_plano_mongo(
    payload: dict[str, Any],
    db: Any | None,
    *,
    frase_original: str,
    permitir_llm: bool = True,
) -> dict[str, Any]:
    """Acrescenta ``plano_sugerido_nome`` / ``plano_sugerido_id`` na primeira linha quando seguro."""
    if not payload.get("ok") or db is None:
        return payload
    linhas = payload.get("linhas")
    if not isinstance(linhas, list) or not linhas:
        return payload
    lin0 = dict(linhas[0]) if isinstance(linhas[0], dict) else {}
    dica = str(lin0.get("plano_hint") or "").strip()
    if not dica:
        dica = re.sub(
            r"\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{1,2}-\d{1,2}|\d+[.,]\d{2}|\d{1,3}(?:\.\d{3})*,\d{2}",
            " ",
            frase_original,
            flags=re.I,
        )
        dica = " ".join(dica.split()).strip()

    ranked = coletar_candidatos_plano_ordenados(db, dica, frase_original, max_n=28)
    hit: dict[str, Any] | None = None
    modo = ""

    if len(ranked) == 1:
        nome, pid, sc = ranked[0]
        if sc >= 3.0:
            hit = {"nome": nome, "id": pid}
            modo = "unico_candidato_mongo"
    elif len(ranked) >= 2 and permitir_llm and resolver_credencial_llm() is not None:
        cands = [(n, p) for n, p, _s in ranked]
        try:
            ix = eleger_plano_via_llm_lista_ancorada(frase_original, cands)
        except Exception:
            logger.exception("eleger_plano_via_llm_lista_ancorada")
            ix = None
        if ix is not None and 0 <= ix < len(cands):
            nome, pid = cands[ix]
            hit = {"nome": nome, "id": pid}
            modo = "ia_lista_ancorada"

    if hit is None:
        script = resolver_plano_por_dica(db, dica, frase_completa=frase_original)
        if script:
            hit = script
            modo = "roteiro_pontuacao"

    if not hit:
        return payload

    out = dict(payload)
    nl = list(out.get("linhas") or [])
    row = dict(lin0)
    row["plano_sugerido_nome"] = hit["nome"]
    row["plano_sugerido_id"] = hit["id"]
    nl[0] = row
    out["linhas"] = nl
    if modo:
        out["plano_resolvido_modo"] = modo
    return out
