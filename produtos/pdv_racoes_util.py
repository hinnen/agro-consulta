"""PDV atalho Rações — match Categoria / Sub 1 / Sub 2 / Peso (etiqueta)."""
from __future__ import annotations

import re
import unicodedata
from typing import Any

TIPOS_RACOES: list[dict[str, str]] = [
    {"id": "gato_filhote", "label": "Gato filhote", "sub1": "Gato", "sub2": "Filhote"},
    {"id": "gato_adulto", "label": "Gato adulto", "sub1": "Gato", "sub2": "Adulto"},
    {"id": "gato_castrado", "label": "Gato castrado", "sub1": "Gato", "sub2": "Castrado"},
    {"id": "cao_adulto_rp", "label": "Cão Adulto RP", "sub1": "Cão", "sub2": "Adulto RP"},
    {"id": "cao_adulto", "label": "Cão adulto", "sub1": "Cão", "sub2": "Adulto"},
    {"id": "cao_filhote_rp", "label": "Cão Filhote RP", "sub1": "Cão", "sub2": "Filhote RP"},
    {"id": "cao_filhote", "label": "Cão Filhote", "sub1": "Cão", "sub2": "Filhote"},
    {"id": "cao_senior", "label": "Cão Sênior", "sub1": "Cão", "sub2": "Sênior"},
]

PESOS_KG_RACOES = (1, 5, 10, 15, 20, 25)


def norm_txt_racoes(s: Any) -> str:
    t = str(s or "").strip().lower()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()


def eh_categoria_racoes(cat: Any) -> bool:
    return norm_txt_racoes(cat) == "racoes"


def parse_peso_racoes(raw: Any) -> str | None:
    """Chave: ``pacote`` | ``kg:1`` … ``kg:25`` | None se não reconhecer."""
    t = norm_txt_racoes(raw)
    if not t:
        return None
    if t.startswith("pacote") or t in ("pct", "p10"):
        return "pacote"
    t = t.replace(",", ".")
    t = re.sub(r"\s*k\s*g\s*$", "", t)
    t = re.sub(r"\s*quilos?\s*$", "", t).strip()
    try:
        n = float(t)
    except ValueError:
        return None
    ni = int(round(n))
    if abs(n - ni) > 0.05:
        return None
    if ni in PESOS_KG_RACOES:
        return f"kg:{ni}"
    return None


def tipo_racoes_por_id(tipo_id: str) -> dict[str, str] | None:
    want = str(tipo_id or "").strip()
    for t in TIPOS_RACOES:
        if t["id"] == want:
            return t
    return None


def produto_racoes_ativo(row: dict | None) -> bool:
    if not row:
        return False
    if row.get("inativo") or row.get("cadastro_inativo"):
        return False
    return True


def produto_passa_tipo_racoes(row: dict | None, tipo: dict[str, str] | None) -> bool:
    if not produto_racoes_ativo(row) or not tipo:
        return False
    if not eh_categoria_racoes(row.get("categoria")):
        return False
    if norm_txt_racoes(row.get("subcategoria")) != norm_txt_racoes(tipo.get("sub1")):
        return False
    if norm_txt_racoes(row.get("subcategoria_2")) != norm_txt_racoes(tipo.get("sub2")):
        return False
    return True


def produto_passa_marca_racoes(row: dict | None, marca: str | None) -> bool:
    """``marca is None`` = todas. ``''`` = sem marca."""
    if not row:
        return False
    if marca is None:
        return True
    return norm_txt_racoes(row.get("marca")) == norm_txt_racoes(marca)


def produto_passa_peso_racoes(row: dict | None, peso_key: str | None) -> bool:
    """``peso_key is None`` = todos os tamanhos reconhecidos."""
    if not row:
        return False
    parsed = parse_peso_racoes(row.get("peso_etiqueta"))
    if peso_key is None:
        return parsed is not None
    return parsed == peso_key


def filtrar_racoes(
    rows: list[dict] | None,
    tipo: dict[str, str] | None,
    marca: str | None = None,
    peso_key: str | None = None,
) -> list[dict]:
    out: list[dict] = []
    for row in rows or []:
        if not produto_passa_tipo_racoes(row, tipo):
            continue
        if not produto_passa_marca_racoes(row, marca):
            continue
        if not produto_passa_peso_racoes(row, peso_key):
            continue
        out.append(row)
    return out
