"""Resolve nomes corrompidos (ObjectId Mongo no campo ``nome`` do Postgres)."""
from __future__ import annotations

import re

from django.db.models import Q

from produtos.models import Produto, ProdutoGestaoOverlayAgro

_RE_OID = re.compile(r"^[0-9a-f]{24}$", re.I)
_RE_GM_BASE = re.compile(r"^(GM\d{4})-(.+)$", re.I)


def nome_parece_objectid_corrupto(nome: str, pid: str = "") -> bool:
    s = (nome or "").strip()
    if _RE_OID.fullmatch(s):
        return True
    p = (pid or "").strip()
    if not _RE_OID.fullmatch(p):
        return False
    if s.lower() == p.lower():
        return True
    # Import Mongo sem Nome grava "—" mas codigo_interno = Id
    if s in ("—", "-", "–", "---", "..."):
        return True
    if len(s) < 3:
        return True
    return False


def queryset_produtos_nome_corrupto(qs=None):
    """Produtos com nome ObjectId ou fantasma (— + Id Mongo 24 hex)."""
    if qs is None:
        qs = Produto.objects.all()
    return qs.filter(
        Q(nome__iregex=r"^[0-9a-f]{24}$")
        | Q(nome__in=["—", "-", "–", "---"])
        | Q(nome="")
    ).filter(produto_externo_id__iregex=r"^[0-9a-f]{24}$")


def iter_fantasmas_catalogo(*, ativos_apenas: bool = False):
    """Todos os ``Produto`` que batem ``produto_fantasma_catalogo``."""
    qs = Produto.objects.all().order_by("nome", "pk")
    if ativos_apenas:
        qs = qs.filter(cadastro_inativo=False, ativo=True)
    for p in qs.iterator(chunk_size=200):
        if produto_fantasma_catalogo(p):
            yield p


def auditar_fantasmas_catalogo(*, ativos_apenas: bool = False) -> list[dict]:
    """Lista fantasmas + campos resolvidos (para Shell/comando de auditoria)."""
    out: list[dict] = []
    for p in iter_fantasmas_catalogo(ativos_apenas=ativos_apenas):
        pid = (p.produto_externo_id or "").strip()
        patch = resolver_campos_catalogo_produto(p)
        out.append(
            {
                "produto_externo_id": pid,
                "nome_pg": (p.nome or "").strip(),
                "codigo_nfe_pg": (p.codigo_nfe or "").strip(),
                "preco_venda": float(p.preco_venda or 0),
                "nome_resolvido": str(patch.get("nome") or "").strip(),
                "codigo_nfe_resolvido": str(patch.get("codigo_nfe") or "").strip(),
                "marca_resolvida": str(patch.get("marca") or "").strip(),
                "categoria_resolvida": str(patch.get("categoria") or "").strip(),
            }
        )
    return out


def deve_ignorar_import_mongo_fantasma(doc: dict, pid: str) -> bool:
    """
    Não importar duplicata Mongo sem ``Nome`` quando o GM já existe no Postgres.

    Fantasmas típicos: ``_id`` 24 hex, sem cadastro, mesmo preço/GM que variante boa.
    """
    nome_m = str(doc.get("Nome") or "").strip()
    if nome_m and not nome_parece_objectid_corrupto(nome_m, pid):
        return False
    cnfe = str(doc.get("CodigoNFe") or doc.get("Codigo") or "").strip().upper()
    if cnfe:
        tem_bom = (
            Produto.objects.filter(codigo_nfe__iexact=cnfe)
            .exclude(produto_externo_id=pid)
            .exclude(nome__iregex=r"^[0-9a-f]{24}$")
            .exclude(nome__in=["—", "-", "–", "---", ""])
            .exists()
        )
        if tem_bom:
            return True
    if _RE_OID.fullmatch((pid or "").strip()) and not nome_m:
        return True
    return False


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


def produto_fantasma_catalogo(p: Produto) -> bool:
    """Registro importado do Mongo sem cadastro completo (Id 24 hex)."""
    pid = (p.produto_externo_id or "").strip()
    if not _RE_OID.fullmatch(pid):
        return False
    if nome_parece_objectid_corrupto(p.nome or "", pid):
        return True
    ci = (p.codigo_interno or "").strip().lower()
    if ci == pid.lower():
        return True
    cn = (p.codigo_nfe or "").strip().lower()
    if cn == pid.lower() or nome_parece_objectid_corrupto(cn, pid):
        return True
    return False


def _qs_irmaos_gm_validos(base: str):
    """Variantes da mesma família GM com cadastro legível (não fantasma)."""
    return (
        Produto.objects.filter(codigo_nfe__istartswith=f"{base}-")
        .exclude(nome__iregex=r"^[0-9a-f]{24}$")
        .exclude(nome__in=["—", "-", "–", "---", ""])
        .exclude(nome__isnull=True)
        .order_by("codigo_nfe", "pk")
    )


def _melhor_irmao_gm(base: str) -> Produto | None:
    qs = _qs_irmaos_gm_validos(base)
    for pref in (f"{base}-5", f"{base}-5S", f"{base}-1S", f"{base}-1"):
        hit = qs.filter(codigo_nfe__iexact=pref).first()
        if hit:
            return hit
    return qs.first()


def inferir_campos_por_codigo_nfe_irmaos(codigo_nfe: str) -> dict[str, str] | None:
    cn = (codigo_nfe or "").strip().upper()
    m = _RE_GM_BASE.match(cn)
    if not m:
        return None
    base, suffix = m.group(1).upper(), m.group(2).upper()
    s0 = _melhor_irmao_gm(base)
    if s0 is None:
        return None
    nome_base = _nome_base_de_irmao(s0.nome or "")
    if len(nome_base) < 3:
        return None
    tail = _sufixo_gm_para_nome(suffix)
    nome = f"{nome_base} {tail}".strip() if tail else nome_base
    ci = (s0.codigo_interno or "").strip()
    if _RE_OID.fullmatch(ci):
        ci = ""
    cb = (s0.codigo_barras or "").strip()
    if _RE_OID.fullmatch(cb):
        cb = ""
    return {
        "nome": nome[:300],
        "codigo_nfe": cn[:64],
        "codigo_interno": ci[:50],
        "codigo_barras": cb[:50],
        "marca": (s0.marca or "").strip()[:120],
        "categoria": (s0.categoria or "").strip()[:200],
        "subcategoria": (s0.subcategoria or "").strip()[:200],
        "fornecedor_texto": (s0.fornecedor_texto or "").strip()[:300],
        "unidade": (s0.unidade or "UN").strip()[:20] or "UN",
        "ncm": (s0.ncm or "").strip()[:16],
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


def _enriquecer_out_com_inferencia(out: dict, cnfe: str) -> dict:
    cnfe = (cnfe or "").strip()
    if not cnfe or nome_parece_objectid_corrupto(cnfe, ""):
        return out
    if not str(out.get("codigo_nfe") or "").strip() or nome_parece_objectid_corrupto(
        str(out.get("codigo_nfe") or ""), ""
    ):
        out["codigo_nfe"] = cnfe
    inf = inferir_campos_por_codigo_nfe_irmaos(cnfe.upper())
    if not inf:
        return out
    for k, v in inf.items():
        vs = str(v or "").strip()
        if not vs:
            continue
        cur = str(out.get(k) or "").strip()
        if not cur or cur in ("—", "-") or nome_parece_objectid_corrupto(cur, ""):
            out[k] = vs
    return out


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
        "codigo_nfe": (p.codigo_nfe or "").strip(),
        "codigo_interno": (p.codigo_interno or "").strip(),
        "codigo_barras": (p.codigo_barras or "").strip(),
        "categoria": (p.categoria or "").strip(),
        "subcategoria": (p.subcategoria or "").strip(),
        "fornecedor_texto": (p.fornecedor_texto or "").strip(),
        "unidade": (p.unidade or "UN").strip() or "UN",
        "ncm": (p.ncm or "").strip(),
    }
    if nome_parece_objectid_corrupto(out["codigo_nfe"], pid):
        out["codigo_nfe"] = ""
    if _RE_OID.fullmatch((out["codigo_interno"] or "").strip()):
        out["codigo_interno"] = ""
    if ov and ov.nome.strip():
        out["nome"] = ov.nome.strip()
    if ov and ov.marca.strip():
        out["marca"] = ov.marca.strip()
    if ov and ov.codigo_nfe.strip():
        out["codigo_nfe"] = ov.codigo_nfe.strip()
    if ov and ov.categoria.strip():
        out["categoria"] = ov.categoria.strip()
    if ov and ov.subcategoria.strip():
        out["subcategoria"] = ov.subcategoria.strip()
    if ov and ov.codigo_barras.strip():
        out["codigo_barras"] = ov.codigo_barras.strip()
    if not produto_fantasma_catalogo(p) and not nome_parece_objectid_corrupto(out["nome"], pid):
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
            out = _enriquecer_out_com_inferencia(out, cnfe_m)
        return out

    for cnfe in (_codigo_nfe_mongo(mongo_doc), out["codigo_nfe"]):
        if not cnfe or nome_parece_objectid_corrupto(cnfe, pid):
            continue
        out = _enriquecer_out_com_inferencia(out, cnfe)
        if out.get("nome") and not nome_parece_objectid_corrupto(out["nome"], pid):
            return out

    inf = _inferir_por_preco_familia_25kg(p)
    if inf:
        for k, v in inf.items():
            vs = str(v or "").strip()
            if vs:
                out[k] = vs
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
    if not produto_fantasma_catalogo(p):
        return row
    patch = resolver_campos_catalogo_produto(p, ov)
    for row_k, patch_k in (
        ("nome", "nome"),
        ("marca", "marca"),
        ("codigo_nfe", "codigo_nfe"),
        ("codigo_interno", "codigo_interno"),
        ("codigo_barras", "codigo_barras"),
        ("categoria", "categoria"),
        ("subcategoria", "subcategoria"),
        ("fornecedor", "fornecedor_texto"),
        ("unidade", "unidade"),
        ("ncm", "ncm"),
    ):
        v = str(patch.get(patch_k) or "").strip()
        if v:
            row[row_k] = v
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
    if not produto_fantasma_catalogo(p):
        return None
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64]).first()
    patch = resolver_campos_catalogo_produto(p, ov)
    novo_nome = str(patch.get("nome") or "").strip()
    if not novo_nome or nome_parece_objectid_corrupto(novo_nome, pid):
        return None
    if dry_run:
        return patch
    changed = False
    updates: list[str] = []
    for fld, key in (
        ("nome", "nome"),
        ("marca", "marca"),
        ("codigo_nfe", "codigo_nfe"),
        ("codigo_interno", "codigo_interno"),
        ("codigo_barras", "codigo_barras"),
        ("categoria", "categoria"),
        ("subcategoria", "subcategoria"),
        ("fornecedor_texto", "fornecedor_texto"),
        ("unidade", "unidade"),
        ("ncm", "ncm"),
    ):
        v = str(patch.get(key) or "").strip()
        if not v:
            continue
        mx = 64 if fld in ("codigo_nfe",) else (50 if fld in ("codigo_interno", "codigo_barras") else 200)
        if fld == "nome":
            mx = 300
        if fld == "marca":
            mx = 120
        if fld == "fornecedor_texto":
            mx = 300
        if fld == "unidade":
            mx = 20
        if fld == "ncm":
            mx = 16
        cur = str(getattr(p, fld) or "").strip()
        if cur == v:
            continue
        if cur and fld not in ("codigo_interno", "codigo_nfe", "codigo_barras") and cur not in ("—", "-"):
            continue
        if fld in ("codigo_interno", "codigo_nfe") and nome_parece_objectid_corrupto(cur, pid):
            pass
        elif cur and fld == "codigo_barras":
            continue
        setattr(p, fld, v[:mx])
        changed = True
        updates.append(fld)
    if changed:
        p.save(update_fields=updates)
    if ov:
        ov_changed = False
        ov_fields: list[str] = []
        for fld, key in (
            ("nome", "nome"),
            ("marca", "marca"),
            ("codigo_nfe", "codigo_nfe"),
            ("codigo_barras", "codigo_barras"),
            ("categoria", "categoria"),
            ("subcategoria", "subcategoria"),
            ("fornecedor_texto", "fornecedor_texto"),
            ("unidade", "unidade"),
        ):
            cur = str(getattr(ov, fld) or "").strip()
            v = str(patch.get(key) or "").strip()
            if v and not cur:
                setattr(ov, fld, v[: (300 if fld == "nome" else 200)])
                ov_changed = True
                ov_fields.append(fld)
        if ov_changed:
            ov.save(update_fields=ov_fields)
    return patch
