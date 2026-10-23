"""Rascunhos NF-e saída 55 no Postgres (loja sem Mongo ERP)."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from django.utils.dateparse import parse_datetime

_DOC_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")


def gerar_nfe_saida_doc_id() -> str:
    from bson.objectid import ObjectId

    return str(ObjectId())


def nfe_saida_doc_id_valido(doc_id: str) -> bool:
    return bool(_DOC_ID_RE.fullmatch(str(doc_id or "").strip()))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sanitize_json(val: Any) -> Any:
    if isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    if isinstance(val, dict):
        return {str(k): _sanitize_json(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_sanitize_json(v) for v in val]
    return val


def _hydrate_timestamps(doc: dict) -> dict:
    out = dict(doc)
    for k in ("created_at", "updated_at", "pin_ok_em"):
        v = out.get(k)
        if isinstance(v, str):
            dt = parse_datetime(v.strip())
            if dt:
                out[k] = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return out


def _corpo_from_doc(doc: dict) -> dict:
    base = dict(doc)
    base.pop("_id", None)
    return _sanitize_json(base)


def row_to_mongo_doc(row) -> dict:
    corpo = row.corpo if isinstance(row.corpo, dict) else {}
    doc = _hydrate_timestamps(dict(corpo))
    doc["_id"] = row.doc_id
    if not doc.get("status"):
        doc["status"] = row.status
    if doc.get("venda_agro_id") is None and row.venda_agro_id is not None:
        doc["venda_agro_id"] = row.venda_agro_id
    return doc


def pg_insert(doc: dict) -> str:
    from produtos.models import NfeSaidaRascunhoAgro

    rid = gerar_nfe_saida_doc_id()
    now = _utcnow()
    corpo = _corpo_from_doc(doc)
    if not corpo.get("created_at"):
        corpo["created_at"] = now.isoformat()
    corpo["updated_at"] = now.isoformat()
    vid = corpo.get("venda_agro_id")
    venda_id = int(vid) if vid is not None and str(vid).strip().isdigit() else None
    NfeSaidaRascunhoAgro.objects.create(
        doc_id=rid,
        status=str(corpo.get("status") or "rascunho")[:40],
        venda_agro_id=venda_id,
        usuario=str(corpo.get("usuario") or "")[:200],
        corpo=corpo,
        criado_em=now,
        atualizado_em=now,
    )
    return rid


def pg_find_one(doc_id: str) -> dict | None:
    from produtos.models import NfeSaidaRascunhoAgro

    if not nfe_saida_doc_id_valido(doc_id):
        return None
    row = NfeSaidaRascunhoAgro.objects.filter(doc_id=doc_id.strip()).first()
    if not row:
        return None
    return row_to_mongo_doc(row)


def pg_update_set(doc_id: str, fields: dict) -> bool:
    from produtos.models import NfeSaidaRascunhoAgro

    if not nfe_saida_doc_id_valido(doc_id):
        return False
    row = NfeSaidaRascunhoAgro.objects.filter(doc_id=doc_id.strip()).first()
    if not row:
        return False
    corpo = dict(row.corpo) if isinstance(row.corpo, dict) else {}
    corpo = _hydrate_timestamps(corpo)
    for k, v in fields.items():
        if k == "_id":
            continue
        corpo[k] = v
    corpo = _corpo_from_doc({**corpo, **fields})
    now = _utcnow()
    corpo["updated_at"] = now.isoformat()
    row.corpo = corpo
    row.status = str(corpo.get("status") or row.status)[:40]
    vid = corpo.get("venda_agro_id")
    if vid is not None and str(vid).strip().isdigit():
        row.venda_agro_id = int(vid)
    row.usuario = str(corpo.get("usuario") or row.usuario or "")[:200]
    row.atualizado_em = now
    row.save(
        update_fields=["corpo", "status", "venda_agro_id", "usuario", "atualizado_em"]
    )
    return True


def pg_replace(doc_id: str, doc: dict) -> bool:
    from produtos.models import NfeSaidaRascunhoAgro

    if not nfe_saida_doc_id_valido(doc_id):
        return False
    row = NfeSaidaRascunhoAgro.objects.filter(doc_id=doc_id.strip()).first()
    if not row:
        return False
    corpo = _corpo_from_doc(doc)
    now = _utcnow()
    corpo["updated_at"] = now.isoformat()
    vid = corpo.get("venda_agro_id")
    venda_id = int(vid) if vid is not None and str(vid).strip().isdigit() else row.venda_agro_id
    row.corpo = corpo
    row.status = str(corpo.get("status") or "rascunho")[:40]
    row.venda_agro_id = venda_id
    row.usuario = str(corpo.get("usuario") or row.usuario or "")[:200]
    row.atualizado_em = now
    row.save(
        update_fields=["corpo", "status", "venda_agro_id", "usuario", "atualizado_em"]
    )
    return True


def pg_find_filter(
    *,
    status: str | None = None,
    status_nin: list[str] | None = None,
    venda_agro_id: int | None = None,
    status_in: list[str] | None = None,
    sort_updated_desc: bool = True,
    limit: int = 40,
) -> list[dict]:
    from produtos.models import NfeSaidaRascunhoAgro

    qs = NfeSaidaRascunhoAgro.objects.all()
    if status is not None:
        qs = qs.filter(status=status)
    if status_in:
        qs = qs.filter(status__in=status_in)
    if status_nin:
        qs = qs.exclude(status__in=status_nin)
    if venda_agro_id is not None:
        qs = qs.filter(venda_agro_id=int(venda_agro_id))
    qs = qs.order_by("-atualizado_em" if sort_updated_desc else "-criado_em")
    out: list[dict] = []
    for row in qs[: max(1, min(limit, 100))]:
        out.append(row_to_mongo_doc(row))
    return out
