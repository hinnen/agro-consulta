"""
Folha Compras × família saco (custo_familia).

- Produto composto (filho) **não** aparece como linha na Folha.
- Vendas do filho entram no saco (pai) × fator kg_filho/kg_pai
  (ex.: 0,1 do saco → 5 vendas do granel = +0,5 no saco).
"""
from __future__ import annotations

import logging
import math
from typing import Any

from produtos.custo_familia_util import (
    extrair_custo_familia,
    qtd_baixa_saco_por_unidade,
)

logger = logging.getLogger(__name__)


def _pid(v: Any) -> str:
    return str(v or "").strip()


def indice_filhos_custo_familia() -> dict[str, dict[str, Any]]:
    """
    filho_id → {pai_id, fator, pai_nome, filho_nome}.
    Só vínculos ativos com fator válido (kg_filho/kg_pai).
    """
    out: dict[str, dict[str, Any]] = {}
    try:
        from produtos.models import ProdutoGestaoOverlayAgro
    except Exception:
        logger.warning("folha familia: import overlay falhou", exc_info=True)
        return out

    try:
        qs = ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={}).only(
            "produto_externo_id", "nome", "cadastro_extras"
        )
        for ov in qs.iterator(chunk_size=250):
            cf = extrair_custo_familia(ov.cadastro_extras)
            if not cf:
                continue
            filho = _pid(ov.produto_externo_id)
            pai = _pid(cf.get("pai_produto_id"))
            if not filho or not pai or filho == pai:
                continue
            fator = qtd_baixa_saco_por_unidade(cf.get("kg_pai"), cf.get("kg_filho"))
            if fator is None or fator <= 0:
                continue
            out[filho] = {
                "pai_id": pai,
                "fator": float(fator),
                "pai_nome": str(cf.get("pai_nome") or "").strip()[:200],
                "filho_nome": str(ov.nome or "").strip()[:200],
            }
            if filho.isdigit():
                out.setdefault(
                    str(int(filho)),
                    out[filho],
                )
    except Exception:
        logger.warning("folha familia: indice filhos falhou", exc_info=True)
    return out


def preparar_pids_folha_familia(
    p_ids: list[str],
    *,
    nomes_hints: dict[str, str] | None = None,
) -> tuple[list[str], list[str], dict[str, dict[str, Any]], dict[str, str]]:
    """
    Retorna:
    - display_pids: linhas a imprimir (sem filhos; inclui pais necessários)
    - sales_pids: IDs para buscar vendas (display + filhos dos pais)
    - filhos_map: indice completo filho→pai/fator
    - nomes_extra: nomes de pais injetados
    """
    hints = dict(nomes_hints or {})
    filhos_map = indice_filhos_custo_familia()
    if not filhos_map:
        base = [_pid(x) for x in p_ids if _pid(x)]
        return base, list(base), {}, hints

    seen_in: set[str] = set()
    ordered_in: list[str] = []
    for raw in p_ids:
        p = _pid(raw)
        if not p or p in seen_in:
            continue
        seen_in.add(p)
        ordered_in.append(p)

    # Pais que precisam aparecer porque o filho estava na lista
    pais_forcar: list[str] = []
    for p in ordered_in:
        info = filhos_map.get(p)
        if not info:
            continue
        pai = _pid(info.get("pai_id"))
        if pai and pai not in seen_in:
            pais_forcar.append(pai)
            hints.setdefault(pai, str(info.get("pai_nome") or "").strip() or pai)

    display: list[str] = []
    seen_d: set[str] = set()
    for p in ordered_in + pais_forcar:
        if p in filhos_map:
            continue  # filho nunca vira linha
        if p in seen_d:
            continue
        seen_d.add(p)
        display.append(p)

    # Filhos de qualquer pai da folha (mesmo que o granel não estivesse no filtro)
    filhos_dos_pais: list[str] = []
    pais_set = set(display)
    for filho, info in filhos_map.items():
        pai = _pid(info.get("pai_id"))
        if pai in pais_set:
            filhos_dos_pais.append(filho)

    sales_seen: set[str] = set()
    sales: list[str] = []
    for p in display + filhos_dos_pais:
        if p in sales_seen:
            continue
        sales_seen.add(p)
        sales.append(p)

    return display, sales, filhos_map, hints


def _resolve_pai_canon(
    pai_id: str,
    variant_to_canon: dict[str, str] | None,
) -> str:
    pai = _pid(pai_id)
    if not pai:
        return ""
    if variant_to_canon:
        c = variant_to_canon.get(pai) or variant_to_canon.get(
            str(int(pai)) if pai.isdigit() else pai
        )
        if c:
            return str(c)
    return pai


def rollup_qtds_filhos_no_pai(
    qtd_por_canon: dict[str, float],
    filhos_map: dict[str, dict[str, Any]],
    *,
    variant_to_canon: dict[str, str] | None = None,
    display_pais: set[str] | None = None,
) -> dict[str, float]:
    """Soma no pai: qtd_filho × fator. Mutável cópia do dict."""
    out = {str(k): float(v or 0.0) for k, v in (qtd_por_canon or {}).items()}
    if not filhos_map:
        return out

    pais_ok = {_pid(x) for x in (display_pais or set()) if _pid(x)}

    for filho, info in filhos_map.items():
        fator = float(info.get("fator") or 0.0)
        if fator <= 0:
            continue
        pai_raw = _pid(info.get("pai_id"))
        if not pai_raw:
            continue
        pai = _resolve_pai_canon(pai_raw, variant_to_canon)
        if pais_ok:
            # Aceita pai canônico ou id cru
            if pai not in pais_ok and pai_raw not in pais_ok:
                # também checa se algum display resolve para este pai
                ok = False
                for d in pais_ok:
                    if _resolve_pai_canon(d, variant_to_canon) == pai:
                        ok = True
                        break
                if not ok:
                    continue

        # qtd do filho: tenta várias chaves
        keys = [filho]
        if filho.isdigit():
            keys.append(str(int(filho)))
        if variant_to_canon:
            for k in list(keys):
                c = variant_to_canon.get(k)
                if c:
                    keys.append(str(c))
        q_filho = 0.0
        for k in keys:
            if k in out:
                q_filho = float(out.get(k) or 0.0)
                break
            # também procurar por canon do filho
        if q_filho <= 0:
            continue
        add = q_filho * fator
        out[pai] = float(out.get(pai) or 0.0) + add
        if pai_raw != pai:
            out[pai_raw] = float(out.get(pai_raw) or 0.0) + add
    return out


def rollup_first_dt_filhos_no_pai(
    first_por_canon: dict[str, Any],
    filhos_map: dict[str, dict[str, Any]],
    *,
    variant_to_canon: dict[str, str] | None = None,
    display_pais: set[str] | None = None,
    qtd_por_canon: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Antecipa a 1ª venda do pai se o filho vendeu antes (média/semana)."""
    out = dict(first_por_canon or {})
    if not filhos_map:
        return out
    qmap = qtd_por_canon or {}
    pais_ok = {_pid(x) for x in (display_pais or set()) if _pid(x)}

    for filho, info in filhos_map.items():
        pai_raw = _pid(info.get("pai_id"))
        if not pai_raw:
            continue
        pai = _resolve_pai_canon(pai_raw, variant_to_canon)
        if pais_ok and pai not in pais_ok and pai_raw not in pais_ok:
            ok = False
            for d in pais_ok:
                if _resolve_pai_canon(d, variant_to_canon) == pai:
                    ok = True
                    break
            if not ok:
                continue
        keys = [filho]
        if filho.isdigit():
            keys.append(str(int(filho)))
        if variant_to_canon:
            for k in list(keys):
                c = variant_to_canon.get(k)
                if c:
                    keys.append(str(c))
        # só conta se o filho teve venda
        if qmap and not any(float(qmap.get(k) or 0.0) > 0 for k in keys):
            continue
        fd = None
        for k in keys:
            if k in out and out[k] is not None:
                fd = out[k]
                break
        if fd is None:
            continue
        for pk in (pai, pai_raw):
            cur = out.get(pk)
            if cur is None or fd < cur:
                out[pk] = fd
    return out


def arred_qtd_folha_compras(x: float | int | None, *, casas: int = 1) -> float:
    """Arredonda para N casas (meio para cima). Permite 5,5."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(v) or math.isinf(v):
        return 0.0
    if v < 0:
        v = 0.0
    mult = 10 ** max(0, int(casas))
    return float(math.floor(v * mult + 0.5) / mult)
