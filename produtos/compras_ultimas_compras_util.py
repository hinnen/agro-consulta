"""Últimas compras Compras — Entrada NF Agro (Mongo) + fallback ERP (views)."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


def _parse_data_entrada_flex(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.replace(tzinfo=None) if raw.tzinfo else raw
    s = str(raw).strip()
    if not s:
        return None
    if "T" in s:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except ValueError:
            pass
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


def _data_doc_entrada_nf_agro(cab: dict, doc: dict) -> datetime | None:
    if isinstance(cab, dict):
        for k in ("data_entrada", "data_emissao", "data"):
            dt = _parse_data_entrada_flex(cab.get(k))
            if dt:
                return dt
    for k in ("estoque_aplicado_em", "criado_em", "atualizado_em"):
        dt = _parse_data_entrada_flex(doc.get(k))
        if dt:
            return dt
    return None


def _qtd_linha_entrada_nf_agro(ln: dict) -> float:
    from produtos.nfe_entrada_util import _entrada_nfe_qtd_linha

    return float(_entrada_nfe_qtd_linha(ln))


def _preco_unit_linha_entrada_nf_agro(ln: dict) -> tuple[float, bool]:
    if not isinstance(ln, dict):
        return 0.0, False
    try:
        vu = float(Decimal(str(ln.get("v_un_com") or "0").replace(",", ".").strip() or "0"))
    except Exception:
        vu = 0.0
    return vu, False


def _doc_conta_como_compra_entrada_nf(doc: dict) -> bool:
    from produtos.nfe_entrada_util import (
        ENTRADA_NFE_STATUS_DESCARTADA,
        _entrada_nfe_extra_correcao_sistemica,
        _entrada_nfe_extra_finalizacao_ok,
        entrada_nfe_status_efetivo,
    )

    extra = doc.get("extra") if isinstance(doc.get("extra"), dict) else {}
    if _entrada_nfe_extra_correcao_sistemica(extra):
        return False
    try:
        if entrada_nfe_status_efetivo(doc) == ENTRADA_NFE_STATUS_DESCARTADA:
            return False
    except Exception:
        pass
    if _entrada_nfe_extra_finalizacao_ok(extra):
        return True
    if doc.get("estoque_aplicado_em"):
        return True
    if str(extra.get("estoque_agro_registrado_em") or "").strip():
        return True
    return False


def _numero_doc_entrada_nf_agro(cab: dict) -> str:
    if not isinstance(cab, dict):
        return ""
    num = str(cab.get("numero") or "").strip()
    ser = str(cab.get("serie") or "").strip()
    chave = str(cab.get("chave") or "").strip()
    if num and ser:
        return f"{ser}/{num}"[:120]
    if num:
        return num[:120]
    if chave:
        return chave[:44]
    return ""


def _normalizar_pid_compra(raw: Any) -> str:
    return str(raw or "").strip()


def _mapa_pid_busca(p_ids: list[str]) -> dict[str, str]:
    """Chaves alternativas (ObjectId str, int…) → pid canônico da busca."""
    out: dict[str, str] = {}
    for raw in p_ids:
        canon = _normalizar_pid_compra(raw)
        if not canon:
            continue
        out[canon] = canon
        if canon.isdigit():
            try:
                n = int(canon)
                out[str(n)] = canon
            except ValueError:
                pass
        if len(canon) == 24 and all(c in "0123456789abcdefABCDEF" for c in canon):
            try:
                from bson import ObjectId

                oid = ObjectId(canon)
                out[str(oid)] = canon
            except Exception:
                pass
    return out


def _codigo_alnum_compra(val: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(val or "").strip().lower())


def _mapa_codigo_para_pid(produtos_por_id: dict | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(produtos_por_id, dict):
        return out
    for pid, p in produtos_por_id.items():
        if not isinstance(p, dict):
            continue
        canon = _normalizar_pid_compra(pid)
        if not canon:
            continue
        for k in ("Codigo", "CodigoNFe", "codigo", "codigo_nfe", "codigo_barras"):
            al = _codigo_alnum_compra(p.get(k))
            if al and len(al) >= 3:
                out[al] = canon
    return out


def _resolver_pid_linha(
    ln: dict,
    mapa: dict[str, str],
    codigo_map: dict[str, str] | None = None,
) -> str | None:
    if not isinstance(ln, dict):
        return None
    key = _normalizar_pid_compra(ln.get("produto_id"))
    if key:
        if key in mapa:
            return mapa[key]
        if key.isdigit():
            k2 = str(int(key))
            if k2 in mapa:
                return mapa[k2]
        try:
            from bson import ObjectId

            if len(key) == 24:
                k3 = str(ObjectId(key))
                if k3 in mapa:
                    return mapa[k3]
        except Exception:
            pass
    cm = codigo_map or {}
    for field in ("c_prod", "codigo", "Codigo", "CodigoNFe"):
        al = _codigo_alnum_compra(ln.get(field))
        if al and al in cm:
            return cm[al]
    return None


def append_eventos_entrada_nf_agro(
    db,
    *,
    eventos: dict[str, list[dict]],
    pid_ok: set[str],
    since: datetime,
    produtos_por_id: dict | None = None,
    mongo_max_time_ms: int | None = 45_000,
) -> None:
    """
    Acrescenta eventos de compra a partir de ``AgroEntradaNotaRascunho`` (Entrada NF Agro).
    Mesmo formato interno que ``_ultimas_compras_por_produto_ids`` (Mongo ERP).
    """
    if not pid_ok:
        return
    from produtos.nfe_entrada_util import _entrada_nota_rascunho_store

    col = _entrada_nota_rascunho_store(db)
    if col is None:
        return

    pid_map = _mapa_pid_busca(list(pid_ok))
    codigo_map = _mapa_codigo_para_pid(produtos_por_id)
    if not pid_map:
        return

    filtro: dict[str, Any] = {
        "$or": [
            {"extra.aprovacao_wizard_em": {"$exists": True, "$nin": [None, ""]}},
            {"extra.estoque_agro_registrado_em": {"$exists": True, "$nin": [None, ""]}},
            {"estoque_aplicado_em": {"$exists": True}},
        ]
    }

    try:
        cur = col.find(
            filtro,
            {
                "cabecalho": 1,
                "linhas": 1,
                "extra": 1,
                "criado_em": 1,
                "estoque_aplicado_em": 1,
                "status": 1,
            },
        ).sort("criado_em", -1)
        if mongo_max_time_ms is not None:
            cur = cur.max_time_ms(int(mongo_max_time_ms))
        docs = list(cur[:8000])
    except Exception as exc:
        logger.warning("ultimas_compras entrada_nf_agro find: %s", exc)
        return

    for doc in docs:
        if not isinstance(doc, dict) or not _doc_conta_como_compra_entrada_nf(doc):
            continue
        cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
        dt = _data_doc_entrada_nf_agro(cab, doc)
        if dt is None or dt < since:
            continue
        forn = str(cab.get("emit_nome") or cab.get("fornecedor_nome") or "").strip()[:200] or "—"
        numero_doc = _numero_doc_entrada_nf_agro(cab)
        linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
        for ln in linhas:
            if not isinstance(ln, dict):
                continue
            pid = _resolver_pid_linha(ln, pid_map, codigo_map)
            if not pid:
                continue
            qtd = _qtd_linha_entrada_nf_agro(ln)
            if qtd <= 0:
                continue
            unit, ja_final = _preco_unit_linha_entrada_nf_agro(ln)
            eventos.setdefault(pid, []).append(
                {
                    "dt": dt,
                    "fornecedor": forn,
                    "qtd": qtd,
                    "unit_base": unit,
                    "unit_ja_final": ja_final,
                    "numero_doc": numero_doc,
                    "tipo_fonte": "entrada_nf_agro",
                }
            )
