"""Busca por código GM / barras no cadastro SisVale (Postgres e fallback Mongo)."""
from __future__ import annotations

import re
from typing import Any

from django.db.models import Q

from produtos.mongo_index_codigos import (
    mongo_query_so_index_codigo,
    produto_termo_bate_campos_principais,
    somente_alnum,
)

_RE_NAO_ALNUM = re.compile(r"[^a-zA-Z0-9]")


def termo_eh_codigo_gm(termo: str) -> bool:
    """Etiqueta GM (ex. GM9503) — não usar ``icontains`` só-dígitos (9503 ≠ GM9503)."""
    return bool(re.match(r"^gm", str(termo or "").strip(), re.I))


def parece_codigo_cadastro(termo: str) -> bool:
    t = str(termo or "").strip()
    if not t:
        return False
    lim = _RE_NAO_ALNUM.sub("", t)
    if not lim:
        return False
    if lim.isdigit():
        return len(lim) >= 8
    if re.match(r"^gm", lim, re.I):
        return len(lim) >= 5
    tem_l = bool(re.search(r"[a-zA-Z]", lim))
    tem_n = bool(re.search(r"\d", lim))
    return tem_l and tem_n and len(lim) >= 3 and " " not in t


def variantes_gm_literal(raw: str) -> set[str]:
    n = str(raw or "").strip().lower()
    out: set[str] = set()
    if not n or not n.startswith("gm"):
        return out
    out.add(n)
    al = somente_alnum(n).lower()
    if len(al) >= 5:
        out.add(al)
    m = re.match(r"^gm(\d{3,})([a-z]?)$", al)
    if m:
        digits, suf = m.group(1), m.group(2) or ""
        if len(digits) >= 4:
            out.add(f"gm{digits[:4]}-{digits[4:]}{suf}")
            if suf:
                out.add(f"gm{digits}{suf}")
            out.add(f"gm{digits}")
    return out


def _norm_cmp(val: Any) -> str:
    return str(val or "").strip().lower()


def _alnum_cmp(val: Any) -> str:
    return somente_alnum(str(val or "")).lower()


def termo_bate_valor_codigo(termo: str, val: Any) -> bool:
    if val is None or str(val).strip() == "":
        return False
    t = _norm_cmp(termo)
    tn = _alnum_cmp(termo)
    vn = _norm_cmp(val)
    va = _alnum_cmp(val)
    if not t:
        return False
    if vn == t:
        return True
    if tn and va and tn == va:
        return True
    if t.isdigit():
        vd = _RE_NAO_ALNUM.sub("", str(val))
        if vd == t:
            return True
    if tn and len(tn) >= 5 and va and va == tn:
        return True
    if t.startswith("gm") and len(t) >= 3:
        if vn.startswith(t):
            return True
        if tn and len(tn) >= 3 and va.startswith(tn):
            return True
        for v in variantes_gm_literal(t):
            if vn == v or va == somente_alnum(v).lower():
                return True
    return False


def termo_bate_codigos_produto(
    termo: str,
    *,
    codigo_interno: str | None = None,
    codigo_nfe: str | None = None,
    codigo_barras: str | None = None,
    extras: tuple[str | None, ...] = (),
) -> bool:
    campos = (codigo_nfe, codigo_barras, codigo_interno, *extras)
    return any(termo_bate_valor_codigo(termo, c) for c in campos if c)


def q_nome_tokens_cadastro(termo: str) -> Q | None:
    """Busca textual leve (nome/marca por token) — evita OR em 8 colunas."""
    parts = [p.strip() for p in (termo or "").split() if len(p.strip()) >= 2]
    if not parts:
        return None
    q_obj = Q()
    for pl in parts:
        q_obj &= Q(nome__icontains=pl) | Q(marca__icontains=pl)
    return q_obj


def q_codigo_exato_cadastro(termo: str) -> Q | None:
    """Match indexável (barras, GM, ids) — sem varrer o catálogo inteiro."""
    termo = (termo or "").strip()
    if not termo:
        return None
    q_obj: Q | None = None
    digits = _RE_NAO_ALNUM.sub("", termo)

    def _or(q_new: Q) -> None:
        nonlocal q_obj
        q_obj = q_new if q_obj is None else (q_obj | q_new)

    if digits and len(digits) >= 4:
        _or(Q(codigo_barras=digits) | Q(codigo_barras__iexact=termo.strip()))
        if not termo_eh_codigo_gm(termo):
            _or(Q(codigo_interno__iexact=digits) | Q(codigo_nfe__iexact=digits))
    for v in variantes_gm_literal(termo):
        _or(Q(codigo_interno__iexact=v) | Q(codigo_nfe__iexact=v))
    if termo_eh_codigo_gm(termo):
        esc = termo.strip()
        _or(Q(codigo_interno__iexact=esc) | Q(codigo_nfe__iexact=esc))
        _or(Q(codigo_interno__istartswith=esc) | Q(codigo_nfe__istartswith=esc))
        al_gm = somente_alnum(termo).lower()
        if al_gm.startswith("gm") and len(al_gm) >= 5:
            _or(Q(codigo_nfe__icontains=al_gm) | Q(codigo_interno__icontains=al_gm))
    tl = somente_alnum(termo).lower()
    if tl and len(tl) >= 3:
        _or(Q(produto_externo_id__iexact=termo) | Q(erp_produto_id__iexact=termo))
    return q_obj


def q_icontains_cadastro(termo: str) -> Q:
    termo = (termo or "").strip()
    digits = _RE_NAO_ALNUM.sub("", termo)
    q_obj = (
        Q(nome__icontains=termo)
        | Q(marca__icontains=termo)
        | Q(categoria__icontains=termo)
        | Q(codigo_interno__icontains=termo)
        | Q(codigo_nfe__icontains=termo)
        | Q(codigo_barras__icontains=termo)
        | Q(produto_externo_id__icontains=termo)
        | Q(erp_produto_id__icontains=termo)
    )
    for v in variantes_gm_literal(termo):
        q_obj |= Q(codigo_interno__iexact=v) | Q(codigo_nfe__iexact=v)
    if digits and len(digits) >= 4 and not termo_eh_codigo_gm(termo):
        q_obj |= Q(codigo_barras__icontains=digits) | Q(codigo_interno__icontains=digits)
        if digits.isdigit():
            q_obj |= Q(codigo_barras=digits) | Q(codigo_barras__iexact=digits)
    return q_obj


def overlay_pids_por_codigo(termo: str, *, limit: int = 80) -> list[str]:
    from produtos.models import ProdutoGestaoOverlayAgro

    termo = (termo or "").strip()
    if not termo:
        return []
    al = somente_alnum(termo).lower()
    if termo_eh_codigo_gm(termo) and len(al) < 5:
        return []
    digits = _RE_NAO_ALNUM.sub("", termo)
    if digits.isdigit() and len(digits) < 8:
        return []

    q_obj: Q | None = None

    def _or(q_new: Q) -> None:
        nonlocal q_obj
        q_obj = q_new if q_obj is None else (q_obj | q_new)

    if termo_eh_codigo_gm(termo):
        for v in variantes_gm_literal(termo):
            _or(Q(codigo_nfe__iexact=v) | Q(codigo_barras__iexact=v))
        esc = termo.strip()
        _or(Q(codigo_nfe__istartswith=esc) | Q(codigo_barras__istartswith=esc))
    elif digits.isdigit() and len(digits) >= 8:
        _or(Q(codigo_barras=digits) | Q(codigo_barras__iexact=digits) | Q(codigo_nfe__iexact=digits))
    else:
        _or(Q(codigo_nfe__icontains=termo) | Q(codigo_barras__icontains=termo))
        if digits and len(digits) >= 4:
            _or(Q(codigo_barras=digits) | Q(codigo_barras__iexact=digits))

    if q_obj is None:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for ov in ProdutoGestaoOverlayAgro.objects.filter(q_obj).only("produto_externo_id", "codigo_nfe", "codigo_barras")[: max(limit * 3, 120)]:
        pid = str(ov.produto_externo_id or "").strip()
        if not pid or pid in seen:
            continue
        if termo_bate_codigos_produto(termo, codigo_nfe=ov.codigo_nfe, codigo_barras=ov.codigo_barras):
            seen.add(pid)
            out.append(pid)
            if len(out) >= limit:
                break
    return out


def index_codigos_de_campos(
    *,
    codigo: str | None = None,
    codigo_nfe: str | None = None,
    codigo_barras: str | None = None,
) -> list[str]:
    from produtos.mongo_index_codigos import extrair_index_codigos_de_documento_mongo

    doc = {
        "Codigo": codigo or "",
        "CodigoNFe": codigo_nfe or codigo or "",
        "CodigoBarras": codigo_barras or "",
    }
    return extrair_index_codigos_de_documento_mongo(doc)


def cadastro_mongo_busca_por_codigo(
    db,
    client_m,
    termo: str,
    *,
    limit: int = 80,
    include_inactive: bool = False,
    projection: dict | None = None,
) -> list[dict]:
    """Fallback quando o motor de busca não acha GM/barras (ex.: ``index_codigos`` desatualizado)."""
    termo = str(termo or "").strip()
    if not termo or db is None or client_m is None:
        return []
    base: dict[str, Any] = {} if include_inactive else {"CadastroInativo": {"$ne": True}}
    col = db[client_m.col_p]
    lim = max(1, min(int(limit or 80), 160))
    proj = projection or {"Id": 1, "_id": 1, "Nome": 1}

    out: list[dict] = []
    seen: set[str] = set()

    def _push(doc: dict | None) -> None:
        if not doc:
            return
        pid = str(doc.get("Id") or doc.get("_id") or "").strip()
        if not pid or pid in seen:
            return
        seen.add(pid)
        out.append(doc)

    try:
        q_ix = mongo_query_so_index_codigo(termo)
        for doc in col.find({**base, **q_ix}, proj).limit(lim):
            _push(doc)
    except Exception:
        pass

    if len(out) < lim and parece_codigo_cadastro(termo):
        tl = somente_alnum(termo).lower()
        ors: list[dict] = []
        if tl.startswith("gm") and len(tl) >= 3:
            esc = re.escape(termo.strip())
            ors.append({"CodigoNFe": {"$regex": f"^{esc}", "$options": "i"}})
            ors.append({"Codigo": {"$regex": f"^{esc}", "$options": "i"}})
        digits = _RE_NAO_ALNUM.sub("", termo)
        if digits and len(digits) >= 4:
            for fld in ("CodigoBarras", "EAN_NFe", "EAN", "CodigoDeBarras", "GTIN"):
                ors.append({fld: digits})
                ors.append({fld: termo.strip()})
        if ors:
            try:
                for doc in col.find({**base, "$or": ors}, proj).limit(lim):
                    _push(doc)
                    if len(out) >= lim:
                        break
            except Exception:
                pass

    if len(out) < lim:
        try:
            scan_cap = min(4000, max(lim * 40, 800))
            for doc in col.find(base, proj).limit(scan_cap):
                if produto_termo_bate_campos_principais(doc, termo):
                    _push(doc)
                    if len(out) >= lim:
                        break
        except Exception:
            pass

    return out[:lim]
