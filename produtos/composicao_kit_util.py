"""Composição / kit local no Agro (overlay) + leitura do espelho Mongo."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any


def _pid(v: Any) -> str:
    return str(v or "").strip()[:64]


def _qtd(v: Any, default: float = 1.0) -> float:
    try:
        q = float(str(v).replace(",", ".").strip() or default)
    except (TypeError, ValueError):
        q = float(default)
    if q <= 0:
        q = float(default)
    return q


def normalizar_item_composicao(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    pid = _pid(raw.get("produto_id") or raw.get("id") or raw.get("ProdutoID"))
    if not pid:
        return None
    dep = str(raw.get("deposito") or "").strip().lower()
    if dep in ("1", "centro", "loja central"):
        dep = "centro"
    elif dep in ("2", "vila"):
        dep = "vila"
    elif dep in ("3", "dinamico", "dinâmico", ""):
        dep = ""
    else:
        dep = dep[:40]
    out: dict[str, Any] = {
        "produto_id": pid,
        "nome": str(raw.get("nome") or raw.get("Nome") or "").strip()[:200],
        "codigo": str(raw.get("codigo") or "").strip()[:80],
        "quantidade": _qtd(raw.get("quantidade") or raw.get("qtd") or 1),
        "deposito": dep,
    }
    origem = str(raw.get("origem") or "").strip()[:40]
    if origem:
        out["origem"] = origem
    for k in ("custo_unitario_agro", "custo_unitario"):
        if raw.get(k) is not None:
            try:
                out["custo_unitario_agro"] = float(
                    Decimal(str(raw.get(k)).replace(",", ".")).quantize(
                        Decimal("0.0001"), rounding=ROUND_HALF_UP
                    )
                )
            except Exception:
                pass
            break
    return out


def normalizar_composicao_lista(raw: Any, *, max_itens: int = 40) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for it in raw:
        n = normalizar_item_composicao(it)
        if not n:
            continue
        if n["produto_id"] in seen:
            # soma qtd se repetir o mesmo produto
            for prev in out:
                if prev["produto_id"] == n["produto_id"]:
                    prev["quantidade"] = _qtd(prev["quantidade"] + n["quantidade"])
                    break
            continue
        seen.add(n["produto_id"])
        out.append(n)
        if len(out) >= max_itens:
            break
    return out


def extrair_composicao_overlay(extras: Any) -> list[dict[str, Any]]:
    if not isinstance(extras, dict):
        return []
    if "composicao" not in extras:
        return []
    return normalizar_composicao_lista(extras.get("composicao"))


def mesclar_composicao_no_extras(ex: dict, raw: Any) -> dict:
    """Se ``raw`` é lista (mesmo vazia), grava; ``None`` não mexe."""
    if raw is None:
        return ex
    if not isinstance(raw, list):
        ex.pop("composicao", None)
        return ex
    lista = normalizar_composicao_lista(raw)
    if lista:
        # não guardar custo no JSON (sempre recalcular na leitura)
        slim = []
        for it in lista:
            slim.append(
                {
                    "produto_id": it["produto_id"],
                    "nome": it.get("nome") or "",
                    "codigo": it.get("codigo") or "",
                    "quantidade": it.get("quantidade") or 1,
                    "deposito": it.get("deposito") or "",
                    **(
                        {"origem": str(it.get("origem") or "").strip()[:40]}
                        if str(it.get("origem") or "").strip()
                        else {}
                    ),
                }
            )
        ex["composicao"] = slim
    else:
        ex["composicao"] = []
    return ex


def custo_total_composicao(itens: list[dict[str, Any]]) -> Decimal | None:
    if not itens:
        return None
    tot = Decimal(0)
    ok = False
    for it in itens:
        cu = it.get("custo_unitario_agro")
        if cu is None:
            continue
        try:
            c = Decimal(str(cu))
            q = Decimal(str(it.get("quantidade") or 1))
        except Exception:
            continue
        tot += c * q
        ok = True
    if not ok:
        return None
    return tot.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
