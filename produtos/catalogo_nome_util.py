"""Resolve nomes corrompidos (ObjectId Mongo no campo ``nome`` do Postgres)."""
from __future__ import annotations

import re

from django.db.models import Q

from produtos.models import Produto, ProdutoGestaoOverlayAgro

_RE_OID = re.compile(r"^[0-9a-f]{24}$", re.I)
_RE_GM_BASE = re.compile(r"^(GM\d{4})-(.+)$", re.I)


def nome_parece_objectid_corrupto(nome: str, pid: str = "") -> bool:
    s = (nome or "").strip()
    if not _RE_OID.fullmatch(s):
        return False
    p = (pid or "").strip().lower()
    if p and s.lower() == p:
        return True
    return True


def _nome_limpo_mongo(doc: dict | None, pid: str) -> str:
    if not isinstance(doc, dict):
        return ""
    n = str(doc.get("Nome") or "").strip()
    if n and not nome_parece_objectid_corrupto(n, pid):
        return n
    return ""


def _codigo_nfe_mongo(doc: dict | None) -> str:
    if not isinstance(doc, dict):
        return ""
    for k in ("CodigoNFe", "Codigo", "codigo_nfe", "codigo"):
        v = str(doc.get(k) or "").strip()
        if v and not nome_parece_objectid_corrupto(v, ""):
            return v
    return ""


def _nome_base_de_irmao(nome_irmao: str) -> str:
    n = (nome_irmao or "").strip()
    n = re.sub(r"\s*\(\s*ensacado na loja\s*\)\s*$", "", n, flags=re.I).strip()
    n = re.sub(r"\s*\d+\s*kg\s*$", "", n, flags=re.I).strip()
    return n


def _sufixo_gm_para_nome(suffix: str) -> str:
    s = (suffix or "").strip().upper()
    if s in ("25", "25S"):
        return "25kg"
    if s in ("5", "5S"):
        return "5kg"
    if s in ("1", "1S"):
        return "1kg"
    if s.endswith("S") and s[:-1].isdigit():
        return f"{s[:-1]}kg"
    if s.isdigit():
        return f"{s}kg"
    return s


def inferir_campos_por_codigo_nfe_irmaos(codigo_nfe: str) -> dict[str, str] | None:
    cn = (codigo_nfe or "").strip().upper()
    m = _RE_GM_BASE.match(cn)
    if not m:
        return None
    base, suffix = m.group(1).upper(), m.group(2).upper()
    siblings = list(
        Produto.objects.filter(codigo_nfe__istartswith=f"{base}-")
        .exclude(nome__iregex=r"^[0-9a-f]{24}$")
        .only("nome", "marca", "categoria", "subcategoria", "codigo_nfe")[:24]
    )
    if not siblings:
        return None
    s0 = siblings[0]
    nome_base = _nome_base_de_irmao(s0.nome or "")
    if len(nome_base) < 3:
        return None
    tail = _sufixo_gm_para_nome(suffix)
    nome = f"{nome_base} {tail}".strip() if tail else nome_base
    return {
        "nome": nome[:300],
        "codigo_nfe": cn[:64],
        "marca": (s0.marca or "").strip()[:120],
        "categoria": (s0.categoria or "").strip()[:200],
        "subcategoria": (s0.subcategoria or "").strip()[:200],
    }


def _inferir_por_preco_familia_25kg(p: Produto) -> dict[str, str] | None:
    """Quando falta ``codigo_nfe``, tenta casar preço com família GM sem variante 25 boa."""
    pv = round(float(p.preco_venda or 0), 2)
    if pv <= 0:
        return None
    pid = (p.produto_externo_id or "").strip()
    bases: set[str] = set()
    for cn in (
        Produto.objects.filter(codigo_nfe__iregex=r"^GM\d{4}-")
        .exclude(nome__iregex=r"^[0-9a-f]{24}$")
        .values_list("codigo_nfe", flat=True)
        .iterator(chunk_size=500)
    ):
        m = _RE_GM_BASE.match(str(cn or "").strip().upper())
        if m:
            bases.add(m.group(1).upper())

    candidatos: list[dict[str, str]] = []
    for base in sorted(bases):
        cnfe_25 = f"{base}-25"
        if (
            Produto.objects.filter(codigo_nfe__iexact=cnfe_25)
            .exclude(Q(produto_externo_id=pid) | Q(nome__iregex=r"^[0-9a-f]{24}$"))
            .exists()
        ):
            continue
        inf = inferir_campos_por_codigo_nfe_irmaos(cnfe_25)
        if inf:
            candidatos.append(inf)

    if len(candidatos) == 1:
        return candidatos[0]
    if not candidatos:
        return None

    scored: list[tuple[float, dict[str, str]]] = []
    for inf in candidatos:
        m = _RE_GM_BASE.match(inf["codigo_nfe"])
        if not m:
            continue
        base = m.group(1).upper()
        ref5 = (
            Produto.objects.filter(codigo_nfe__iregex=rf"^{base}-(5|5S)$")
            .exclude(nome__iregex=r"^[0-9a-f]{24}$")
            .only("preco_venda")
            .first()
        )
        if ref5 and ref5.preco_venda:
            exp = float(ref5.preco_venda) * 3.6
            scored.append((abs(pv - exp), inf))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0])
    if scored[0][0] <= 10:
        return scored[0][1]
    return None


def resolver_campos_catalogo_produto(
    p: Produto,
    ov: ProdutoGestaoOverlayAgro | None = None,
    *,
    mongo_doc: dict | None = None,
) -> dict[str, str]:
    pid = (p.produto_externo_id or "").strip()
    out = {
        "nome": (p.nome or "").strip(),
        "marca": (p.marca or "").strip(),
        "codigo_nfe": (p.codigo_nfe or p.codigo_interno or "").strip(),
        "categoria": (p.categoria or "").strip(),
        "subcategoria": (p.subcategoria or "").strip(),
    }
    if ov and ov.nome.strip():
        out["nome"] = ov.nome.strip()
    if ov and ov.marca.strip():
        out["marca"] = ov.marca.strip()
    if ov and ov.codigo_nfe.strip():
        out["codigo_nfe"] = ov.codigo_nfe.strip()
    if not nome_parece_objectid_corrupto(out["nome"], pid):
        return out

    if mongo_doc is None and pid:
        try:
            from produtos.views import _produto_mongo_por_id_externo, obter_conexao_mongo

            client, db = obter_conexao_mongo()
            if db is not None and client is not None:
                mongo_doc = _produto_mongo_por_id_externo(db, client, pid)
        except Exception:
            mongo_doc = None

    mn = _nome_limpo_mongo(mongo_doc, pid)
    if mn:
        out["nome"] = mn
        cnfe_m = _codigo_nfe_mongo(mongo_doc)
        if cnfe_m:
            out["codigo_nfe"] = cnfe_m
        return out

    for cnfe in (_codigo_nfe_mongo(mongo_doc), out["codigo_nfe"]):
        if not cnfe or nome_parece_objectid_corrupto(cnfe, pid):
            continue
        inf = inferir_campos_por_codigo_nfe_irmaos(cnfe)
        if inf:
            out.update(inf)
            return out
        out["codigo_nfe"] = cnfe
        out["nome"] = cnfe
        return out

    inf = _inferir_por_preco_familia_25kg(p)
    if inf:
        out.update(inf)
        return out

    if out["codigo_nfe"] and out["codigo_nfe"].upper().startswith("GM"):
        out["nome"] = out["codigo_nfe"]
    else:
        out["nome"] = "—"
    return out


def aplicar_nome_resolvido_em_row(
    row: dict,
    p: Produto,
    ov: ProdutoGestaoOverlayAgro | None = None,
) -> dict:
    pid = (p.produto_externo_id or "").strip()
    if not nome_parece_objectid_corrupto(str(row.get("nome") or ""), pid):
        return row
    patch = resolver_campos_catalogo_produto(p, ov)
    for k in ("nome", "marca", "codigo_nfe", "categoria", "subcategoria"):
        v = str(patch.get(k) or "").strip()
        if v:
            row[k] = v
    if row.get("codigo_nfe") and not row.get("codigo"):
        row["codigo"] = row["codigo_nfe"]
    row["busca_texto"] = " ".join(
        x
        for x in (
            row.get("nome"),
            row.get("marca"),
            row.get("codigo"),
            row.get("codigo_nfe"),
            row.get("codigo_barras"),
            row.get("categoria"),
            row.get("subcategoria"),
            row.get("fornecedor"),
        )
        if x
    ).strip()
    return row


def reparar_produto_nome_corrupto_persist(p: Produto, *, dry_run: bool = False) -> dict[str, str] | None:
    pid = (p.produto_externo_id or "").strip()
    if not nome_parece_objectid_corrupto(p.nome or "", pid):
        return None
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
    patch = resolver_campos_catalogo_produto(p, ov)
    novo_nome = str(patch.get("nome") or "").strip()
    if not novo_nome or nome_parece_objectid_corrupto(novo_nome, pid):
        return None
    if dry_run:
        return patch
    changed = False
    if p.nome != novo_nome:
        p.nome = novo_nome[:300]
        changed = True
    for fld, key in (
        ("marca", "marca"),
        ("codigo_nfe", "codigo_nfe"),
        ("categoria", "categoria"),
        ("subcategoria", "subcategoria"),
    ):
        v = str(patch.get(key) or "").strip()
        if v and getattr(p, fld) != v:
            setattr(p, fld, v[: (64 if fld == "codigo_nfe" else 200)])
            changed = True
    if changed:
        p.save(update_fields=["nome", "marca", "codigo_nfe", "categoria", "subcategoria"])
    if ov:
        ov_changed = False
        if not ov.nome.strip() and novo_nome:
            ov.nome = novo_nome[:300]
            ov_changed = True
        cnfe = str(patch.get("codigo_nfe") or "").strip()
        if cnfe and not ov.codigo_nfe.strip():
            ov.codigo_nfe = cnfe[:64]
            ov_changed = True
        if ov_changed:
            ov.save(update_fields=["nome", "codigo_nfe"])
    return patch
