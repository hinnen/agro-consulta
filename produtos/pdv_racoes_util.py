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

PESOS_KG_RACOES = (1, 2.5, 5, 10, 15, 20, 25)


def norm_txt_racoes(s: Any) -> str:
    t = str(s or "").strip().lower()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()


def eh_categoria_racoes(cat: Any) -> bool:
    return norm_txt_racoes(cat) == "racoes"


def peso_racoes_key(n: float) -> str:
    if abs(n - round(n)) < 0.001:
        return f"kg:{int(round(n))}"
    return f"kg:{n:g}"


def parse_peso_racoes(raw: Any) -> str | None:
    """Chave: ``pacote`` | ``kg:1`` · ``kg:2.5`` … ``kg:25`` | None se não reconhecer."""
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
    for p in PESOS_KG_RACOES:
        if abs(n - float(p)) <= 0.05:
            return peso_racoes_key(float(p))
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


def patch_racoes_de_campos(
    *,
    pid: Any,
    categoria: Any = "",
    sub1: Any = "",
    sub2: Any = "",
    peso: Any = "",
    marca: Any = "",
) -> dict | None:
    """Patch mínimo p/ o PDV (id + cat/sub/peso). None se não for ração com Sub 2."""
    if not eh_categoria_racoes(categoria):
        return None
    s2 = str(sub2 or "").strip()
    if not s2:
        return None
    ident = str(pid or "").strip()[:64]
    if not ident:
        return None
    return {
        "id": ident,
        "categoria": str(categoria or "").strip(),
        "subcategoria": str(sub1 or "").strip(),
        "subcategoria_2": s2,
        "peso_etiqueta": str(peso or "").strip(),
        "marca": str(marca or "").strip(),
    }


def listar_patches_racoes_pdv() -> list[dict]:
    """Cadastro Agro ao vivo — não depende do cache diário do PDV."""
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    seen: set[str] = set()
    out: list[dict] = []
    ov_qs = (
        ProdutoGestaoOverlayAgro.objects.exclude(categoria="")
        .exclude(subcategoria_2="")
        .only(
            "produto_externo_id",
            "categoria",
            "subcategoria",
            "subcategoria_2",
            "peso_etiqueta",
            "marca",
        )
    )
    for ov in ov_qs.iterator():
        patch = patch_racoes_de_campos(
            pid=ov.produto_externo_id,
            categoria=ov.categoria,
            sub1=ov.subcategoria,
            sub2=ov.subcategoria_2,
            peso=ov.peso_etiqueta,
            marca=ov.marca,
        )
        if not patch:
            continue
        seen.add(patch["id"])
        out.append(patch)
    prod_qs = (
        Produto.objects.filter(cadastro_inativo=False, ativo=True)
        .exclude(categoria="")
        .exclude(subcategoria_2="")
        .only(
            "pk",
            "produto_externo_id",
            "erp_produto_id",
            "categoria",
            "subcategoria",
            "subcategoria_2",
            "marca",
        )
    )
    for p in prod_qs.iterator():
        pid = str(p.produto_externo_id or p.erp_produto_id or p.pk).strip()[:64]
        if not pid or pid in seen:
            continue
        patch = patch_racoes_de_campos(
            pid=pid,
            categoria=p.categoria,
            sub1=p.subcategoria,
            sub2=p.subcategoria_2,
            peso="",
            marca=p.marca,
        )
        if not patch:
            continue
        seen.add(patch["id"])
        out.append(patch)
    return out


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
