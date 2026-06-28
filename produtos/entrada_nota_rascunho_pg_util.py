"""Persistência Postgres dos rascunhos Entrada NF — adaptador compatível com operações Mongo usadas."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from django.db import transaction
from django.utils.dateparse import parse_datetime

logger = logging.getLogger(__name__)


@dataclass
class _InsertOneResult:
    inserted_id: str


@dataclass
class _UpdateResult:
    matched_count: int = 0
    modified_count: int = 0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware_dt(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        dt = parse_datetime(val.strip())
        if dt:
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _sanitize_json_value(val: Any) -> Any:
    """Garante JSONField Postgres serializável (sem ``datetime`` bruto dentro de ``extra``)."""
    if isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(val, dict):
        return {str(k): _sanitize_json_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_sanitize_json_value(v) for v in val]
    if isinstance(val, tuple):
        return [_sanitize_json_value(v) for v in val]
    return val


def _id_str(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if s.lower() in ("none", "null", "undefined"):
        return ""
    return s


def _rascunho_id_hex_valido(rid: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-fA-F]{24}", str(rid or "").strip()))


def gerar_rascunho_id_entrada_nfe() -> str:
    from bson.objectid import ObjectId

    return str(ObjectId())


def _get_nested(doc: dict, path: str) -> Any:
    cur: Any = doc
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _set_nested(doc: dict, path: str, value: Any) -> None:
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _unset_nested(doc: dict, path: str) -> None:
    parts = path.split(".")
    cur: Any = doc
    for part in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.get(part)
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def _valor_total_linhas(linhas: list | None) -> float:
    total = 0.0
    for ln in linhas or []:
        if not isinstance(ln, dict):
            continue
        try:
            qe = float(str(ln.get("q_estoque") or "").replace(",", ".") or 0)
        except (TypeError, ValueError):
            qe = 0.0
        try:
            qc = float(str(ln.get("q_com") or "").replace(",", ".") or 0)
        except (TypeError, ValueError):
            qc = 0.0
        q = max(qe, qc)
        try:
            vu = float(str(ln.get("v_un_com") or "").replace(",", ".") or 0)
        except (TypeError, ValueError):
            vu = 0.0
        if q > 0 and vu > 0:
            total += q * vu
    return total


def row_to_doc(row, *, projection: dict | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {
        "_id": row.rascunho_id,
        "status": row.status,
        "modo": row.modo,
        "usuario": row.usuario,
        "usuario_ultima_alteracao": row.usuario_ultima_alteracao,
        "usuario_estoque_aplicado": row.usuario_estoque_aplicado,
        "xml_chave": row.xml_chave or None,
        "cabecalho": _sanitize_json_value(row.cabecalho if isinstance(row.cabecalho, dict) else {}),
        "linhas": _sanitize_json_value(row.linhas if isinstance(row.linhas, list) else []),
        "extra": _sanitize_json_value(row.extra if isinstance(row.extra, dict) else {}),
        "criado_em": row.criado_em,
        "atualizado_em": row.atualizado_em,
        "estoque_aplicado_em": row.estoque_aplicado_em,
    }
    linhas = d["linhas"]
    d["linhas_count"] = len(linhas)
    d["_valor_total_nota"] = _valor_total_linhas(linhas)
    if projection:
        return _project_doc(d, projection)
    return d


def _project_doc(doc: dict[str, Any], projection: dict) -> dict[str, Any]:
    if not projection or projection == {"_id": 1}:
        return {"_id": doc.get("_id")}
    if 1 in projection.values() or True in projection.values():
        out: dict[str, Any] = {}
        for k, v in projection.items():
            if v:
                if k == "linhas_count":
                    out[k] = doc.get("linhas_count", 0)
                elif k == "_valor_total_nota":
                    out[k] = doc.get("_valor_total_nota", 0)
                elif k in doc:
                    out[k] = doc[k]
        if "_id" not in out and projection.get("_id", 1):
            out["_id"] = doc.get("_id")
        return out
    # exclusion projection (rare)
    out = dict(doc)
    for k, v in projection.items():
        if v == 0 and k in out:
            del out[k]
    return out


def _doc_to_row_fields(doc: dict[str, Any]) -> dict[str, Any]:
    criado = _as_aware_dt(doc.get("criado_em")) or _utcnow()
    atualizado = _as_aware_dt(doc.get("atualizado_em"))
    estoque_ap = _as_aware_dt(doc.get("estoque_aplicado_em"))
    return {
        "status": str(doc.get("status") or "com_pendencias")[:40],
        "modo": str(doc.get("modo") or "manual")[:40],
        "usuario": str(doc.get("usuario") or "")[:200],
        "usuario_ultima_alteracao": str(doc.get("usuario_ultima_alteracao") or "")[:200],
        "usuario_estoque_aplicado": str(doc.get("usuario_estoque_aplicado") or "")[:200],
        "xml_chave": str(doc.get("xml_chave") or "")[:44],
        "cabecalho": _sanitize_json_value(doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}),
        "linhas": _sanitize_json_value(doc.get("linhas") if isinstance(doc.get("linhas"), list) else []),
        "extra": _sanitize_json_value(doc.get("extra") if isinstance(doc.get("extra"), dict) else {}),
        "criado_em": criado,
        "atualizado_em": atualizado,
        "estoque_aplicado_em": estoque_ap,
    }


def importar_doc_mongo_para_pg(doc: dict[str, Any]) -> str | None:
    """Upsert de documento Mongo legado → Postgres. Retorna ``rascunho_id``."""
    from produtos.models import EntradaNotaRascunhoAgro

    if not isinstance(doc, dict):
        return None
    rid = _id_str(doc.get("_id"))
    if not rid or len(rid) != 24:
        return None
    fields = _doc_to_row_fields(doc)
    EntradaNotaRascunhoAgro.objects.update_or_create(rascunho_id=rid, defaults=fields)
    return rid


class EntradaNotaRascunhoPgCollection:
    """Adaptador mínimo da API pymongo usada por ``nfe_entrada_util``."""

    def insert_one(self, doc: dict[str, Any]) -> _InsertOneResult:
        from produtos.models import EntradaNotaRascunhoAgro

        rid = _id_str(doc.get("_id"))
        if not _rascunho_id_hex_valido(rid):
            rid = gerar_rascunho_id_entrada_nfe()
        fields = _doc_to_row_fields(doc)
        EntradaNotaRascunhoAgro.objects.create(rascunho_id=rid, **fields)
        return _InsertOneResult(inserted_id=rid)

    def find_one(
        self,
        filt: dict[str, Any],
        projection: dict | None = None,
        sort: list | tuple | None = None,
    ) -> dict[str, Any] | None:
        from produtos.models import EntradaNotaRascunhoAgro

        rid = _id_from_filter(filt)
        if rid and not sort and len(filt) == 1:
            row = EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).first()
            if row:
                return row_to_doc(row, projection=projection)
            return None
        cur = _PgFindCursor(filt, projection)
        if sort:
            cur.sort(sort[0], sort[1] if len(sort) > 1 else -1)
        cur.limit(1)
        for doc in cur:
            return doc
        return None

    def update_one(self, filt: dict[str, Any], update: dict[str, Any]) -> _UpdateResult:
        from produtos.models import EntradaNotaRascunhoAgro

        rid = _id_from_filter(filt)
        if not rid:
            return _UpdateResult()
        row = EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).first()
        if not row:
            return _UpdateResult()
        doc = row_to_doc(row)
        if not _doc_matches_filter(doc, filt):
            return _UpdateResult(matched_count=0, modified_count=0)
        changed = _apply_update_to_doc(doc, update)
        if not changed:
            return _UpdateResult(matched_count=1, modified_count=0)
        fields = _doc_to_row_fields(doc)
        for k, v in fields.items():
            setattr(row, k, v)
        row.save()
        return _UpdateResult(matched_count=1, modified_count=1)

    def delete_one(self, filt: dict[str, Any]) -> _UpdateResult:
        from produtos.models import EntradaNotaRascunhoAgro

        rid = _id_from_filter(filt)
        if not rid:
            return _UpdateResult()
        n, _ = EntradaNotaRascunhoAgro.objects.filter(rascunho_id=rid).delete()
        return _UpdateResult(matched_count=n, modified_count=n)

    def find(
        self,
        filt: dict[str, Any] | None = None,
        projection: dict | None = None,
    ) -> _PgFindCursor:
        return _PgFindCursor(filt or {}, projection)

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from produtos.models import EntradaNotaRascunhoAgro

        sort_desc = True
        limit = 100
        include_linhas_slice: int | None = None
        for stage in pipeline:
            if "$sort" in stage:
                criado = stage["$sort"].get("criado_em", -1)
                sort_desc = int(criado) < 0
            if "$limit" in stage:
                limit = int(stage["$limit"])
            proj = stage.get("$project") or {}
            if isinstance(proj.get("linhas"), dict) and "$slice" in proj["linhas"]:
                sl = proj["linhas"]["$slice"]
                if isinstance(sl, list) and sl:
                    include_linhas_slice = int(sl[-1])

        qs = EntradaNotaRascunhoAgro.objects.all().order_by(
            "-criado_em" if sort_desc else "criado_em"
        )[:limit]
        out: list[dict[str, Any]] = []
        for row in qs:
            d = row_to_doc(row)
            if include_linhas_slice is not None and isinstance(d.get("linhas"), list):
                d["linhas"] = d["linhas"][:include_linhas_slice]
            out.append(d)
        return out


class _PgFindCursor:
    def __init__(self, filt: dict[str, Any], projection: dict | None) -> None:
        self._filt = filt
        self._projection = projection
        self._sort_field = "-atualizado_em"
        self._limit: int | None = None
        self._max_time_ms: int | None = None

    def sort(self, key: str | list, direction: int | None = None) -> _PgFindCursor:
        if isinstance(key, (list, tuple)) and len(key) >= 2:
            field, direction = key[0], key[1]
        else:
            field = str(key)
            if direction is None:
                direction = -1 if field.startswith("-") else 1
        field = field.lstrip("-+")
        self._sort_field = f"-{field}" if int(direction) < 0 else field
        return self

    def limit(self, n: int) -> _PgFindCursor:
        self._limit = int(n)
        return self

    def max_time_ms(self, _ms: int) -> _PgFindCursor:
        return self

    def _rows(self):
        from produtos.models import EntradaNotaRascunhoAgro

        scan = min(max(self._limit or 500, 50) * 4, 8000)
        return EntradaNotaRascunhoAgro.objects.all().order_by(self._sort_field)[:scan]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        n = 0
        lim = self._limit
        for row in self._rows():
            doc = row_to_doc(row, projection=self._projection)
            if not _doc_matches_mongo_filter(doc, self._filt):
                continue
            yield doc
            n += 1
            if lim is not None and n >= lim:
                break

    def __getitem__(self, sl: slice) -> list[dict[str, Any]]:
        return list(self)[sl]


def _doc_matches_mongo_filter(doc: dict[str, Any], filt: dict[str, Any]) -> bool:
    if not filt:
        return True
    if "$and" in filt:
        return all(_doc_matches_mongo_filter(doc, clause) for clause in filt["$and"])
    if "$or" in filt:
        return any(_doc_matches_mongo_filter(doc, clause) for clause in filt["$or"])

    for key, cond in filt.items():
        if key in ("$and", "$or"):
            continue
        val = _get_nested(doc, key) if "." in key else doc.get(key)
        if isinstance(cond, dict):
            if "$ne" in cond:
                if val == cond["$ne"]:
                    return False
            if "$nin" in cond:
                if val in cond["$nin"]:
                    return False
            if "$in" in cond:
                if isinstance(val, list):
                    if not set(val).intersection(set(cond["$in"])):
                        return False
                elif val not in cond["$in"]:
                    return False
            if "$exists" in cond:
                exists = val is not None and val != ""
                if bool(cond["$exists"]) != exists:
                    return False
            if "$type" in cond and cond.get("$regex"):
                if not re.search(str(cond["$regex"]), str(val or "")):
                    return False
            if cond.get("$ne") is True and val is True:
                return False
        elif val != cond:
            return False
    return True


def _id_from_filter(filt: dict[str, Any]) -> str | None:
    if not isinstance(filt, dict):
        return None
    raw = filt.get("_id")
    if raw is None:
        return None
    return _id_str(raw)


def _apply_sort(qs, sort: list | tuple):
    if not sort:
        return qs
    field, direction = sort[0], sort[1] if len(sort) > 1 else -1
    f = str(field)
    if int(direction) < 0:
        f = f"-{f.lstrip('-+')}"
    return qs.order_by(f)


def _apply_find_filter(qs, filt: dict[str, Any]):
    from produtos.models import EntradaNotaRascunhoAgro
    from produtos.nfe_entrada_util import ENTRADA_NFE_STATUS_DESCARTADA

    if not filt:
        return qs

    if "$or" in filt:
        from django.db.models import Q

        or_q = Q()
        for clause in filt["$or"]:
            sub = _apply_find_filter(EntradaNotaRascunhoAgro.objects.all(), clause)
            ids = list(sub.values_list("rascunho_id", flat=True)[:5000])
            if ids:
                or_q |= Q(rascunho_id__in=ids)
        if or_q:
            qs = qs.filter(or_q)
        else:
            qs = qs.none()

    st = filt.get("status")
    if isinstance(st, dict):
        if "$ne" in st:
            qs = qs.exclude(status=str(st["$ne"]))
        if "$nin" in st:
            qs = qs.exclude(status__in=[str(x) for x in st["$nin"]])

    fin_ids = filt.get("extra.financeiro_ids")
    if isinstance(fin_ids, dict) and "$in" in fin_ids:
        wanted = {str(x).strip() for x in fin_ids["$in"] if str(x).strip()}
        if wanted:
            matched: list[str] = []
            for row in qs.order_by("-atualizado_em")[:2500]:
                ex = row.extra if isinstance(row.extra, dict) else {}
                ids_raw = ex.get("financeiro_ids")
                if not isinstance(ids_raw, list):
                    continue
                if wanted.intersection({str(x).strip() for x in ids_raw}):
                    matched.append(row.rascunho_id)
            qs = EntradaNotaRascunhoAgro.objects.filter(rascunho_id__in=matched)

    lote = filt.get("extra.financeiro_lote")
    if lote is not None:
        lote_s = str(lote).strip().upper()
        matched_lote: list[str] = []
        for row in qs.order_by("-atualizado_em")[:2500]:
            ex = row.extra if isinstance(row.extra, dict) else {}
            if str(ex.get("financeiro_lote") or "").strip().upper() == lote_s:
                matched_lote.append(row.rascunho_id)
        qs = EntradaNotaRascunhoAgro.objects.filter(rascunho_id__in=matched_lote)

    fin_lanc = filt.get("extra.financeiro_lancado")
    if fin_lanc is True:
        matched_fin: list[str] = []
        for row in qs.order_by("-atualizado_em")[:2500]:
            ex = row.extra if isinstance(row.extra, dict) else {}
            if ex.get("financeiro_lancado"):
                matched_fin.append(row.rascunho_id)
        qs = EntradaNotaRascunhoAgro.objects.filter(rascunho_id__in=matched_fin)

    if filt.get("status") == {"$ne": ENTRADA_NFE_STATUS_DESCARTADA}:
        qs = qs.exclude(status=ENTRADA_NFE_STATUS_DESCARTADA)

    return qs


def _doc_matches_filter(doc: dict[str, Any], filt: dict[str, Any]) -> bool:
    from produtos.nfe_entrada_util import ENTRADA_NFE_STATUS_CONGELADOS

    rid = _id_from_filter(filt)
    if rid and _id_str(doc.get("_id")) != rid:
        return False

    st_f = filt.get("status")
    if isinstance(st_f, dict):
        st_doc = str(doc.get("status") or "").strip().lower()
        if "$nin" in st_f:
            if st_doc in {str(x).strip().lower() for x in st_f["$nin"]}:
                return False
        if "$ne" in st_f:
            if st_doc == str(st_f["$ne"]).strip().lower():
                return False

    if "$or" in filt:
        if not any(_doc_matches_clause(doc, clause) for clause in filt["$or"]):
            return False

    return True


def _doc_matches_clause(doc: dict[str, Any], clause: dict[str, Any]) -> bool:
    for key, cond in clause.items():
        val = _get_nested(doc, key.replace("extra.", "extra.")) if "." in key else doc.get(key)
        if key.startswith("extra."):
            val = _get_nested(doc, key)
        if isinstance(cond, dict):
            if "$exists" in cond:
                exists = val is not None and val != ""
                if bool(cond["$exists"]) != exists:
                    return False
            if "$lte" in cond:
                dt_val = _as_aware_dt(val)
                dt_lim = _as_aware_dt(cond["$lte"])
                if dt_val is None or dt_lim is None or dt_val > dt_lim:
                    return False
            if "$in" in cond:
                if val not in cond["$in"]:
                    return False
        elif val != cond:
            return False
    return True


def _apply_update_to_doc(doc: dict[str, Any], update: dict[str, Any]) -> bool:
    changed = False
    for path, val in (update.get("$set") or {}).items():
        val = _sanitize_json_value(val)
        if "." in path:
            prev = _get_nested(doc, path)
            if prev != val:
                _set_nested(doc, path, val)
                changed = True
        else:
            if doc.get(path) != val:
                doc[path] = val
                changed = True
    for path in (update.get("$unset") or {}):
        if "." in path:
            if _get_nested(doc, path) is not None:
                _unset_nested(doc, path)
                changed = True
        elif path in doc:
            doc.pop(path, None)
            changed = True
    return changed


def rascunho_entrada_col(db):
    """Coleção Mongo ou adaptador Postgres conforme ``agro_entrada_nota_rascunho_postgres()``."""
    from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres

    if agro_entrada_nota_rascunho_postgres():
        return EntradaNotaRascunhoPgCollection()
    if db is None:
        return None
    from produtos.nfe_entrada_util import COL_ENTRADA_RASCUNHO

    return db[COL_ENTRADA_RASCUNHO]


def importar_rascunhos_mongo_batch(db, *, limit: int = 5000) -> dict[str, int]:
    """Importa rascunhos do Mongo para Postgres (comando de gestão / bootstrap)."""
    from produtos.nfe_entrada_util import COL_ENTRADA_RASCUNHO

    if db is None:
        return {"ok": 0, "erro": 1}
    n = 0
    try:
        cur = db[COL_ENTRADA_RASCUNHO].find({}).sort("criado_em", -1).limit(max(1, min(limit, 20_000)))
        with transaction.atomic():
            for doc in cur:
                if importar_doc_mongo_para_pg(doc):
                    n += 1
    except Exception as exc:
        logger.exception("importar_rascunhos_mongo_batch: %s", exc)
        return {"ok": n, "erro": 1}
    return {"ok": n, "erro": 0}


def maybe_bootstrap_rascunhos_entrada_nota_pg(
    db,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Import Mongo→PG na 1ª listagem ou boot (loja/staging). Idempotente."""
    from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres
    from produtos.models import EntradaNotaRascunhoAgro

    if not agro_entrada_nota_rascunho_postgres():
        return {"ok": True, "skipped": True, "motivo": "mongo_store"}

    n = EntradaNotaRascunhoAgro.objects.count()
    if n > 0 and not force:
        return {"ok": True, "skipped": True, "motivo": "pg_ja_populado", "pg_depois": n}

    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}

    antes = n
    r = importar_rascunhos_mongo_batch(db)
    depois = EntradaNotaRascunhoAgro.objects.count()
    ok = not r.get("erro") and depois >= antes
    return {
        "ok": ok,
        "importados": int(r.get("ok") or 0),
        "pg_antes": antes,
        "pg_depois": depois,
        "erro": None if ok else "import_parcial_ou_falhou",
    }


def lazy_import_rascunho_mongo(db, oid: str) -> dict[str, Any] | None:
    """Se PG ativo e rascunho ausente, tenta copiar do Mongo legado."""
    from bson.objectid import ObjectId
    from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres
    from produtos.nfe_entrada_util import COL_ENTRADA_RASCUNHO

    if not agro_entrada_nota_rascunho_postgres() or db is None:
        return None
    try:
        doc = db[COL_ENTRADA_RASCUNHO].find_one({"_id": ObjectId(str(oid).strip())})
    except Exception:
        doc = None
    if not doc:
        return None
    importar_doc_mongo_para_pg(doc)
    col = EntradaNotaRascunhoPgCollection()
    return col.find_one({"_id": oid})
