"""Ãšltimas compras Compras â€” Entrada NF Agro (Mongo) + fallback ERP (views)."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
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
    for k in ("v_un_com", "custo_unitario_nota", "preco_custo", "v_unit", "vUnCom"):
        raw = ln.get(k)
        if raw in (None, ""):
            continue
        try:
            vu = float(Decimal(str(raw).replace(",", ".").strip() or "0"))
        except Exception:
            continue
        if vu > 0:
            return vu, False
    return 0.0, False


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
    """Chaves alternativas (ObjectId str, intâ€¦) â†’ pid canÃ´nico da busca."""
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
    excluir_rascunho_ids: set[str] | None = None,
) -> None:
    """
    Acrescenta eventos de compra a partir de ``EntradaNotaRascunhoAgro`` (Entrada NF).
    Mesmo formato interno que ``_ultimas_compras_por_produto_ids``.

    **Importante (loja PG):** nÃ£o usa ``col.find($or $exists)`` â€” no adaptador PG isso
    expandia a tabela inteira 3Ã— (atÃ© 5000 ids) e travava o worker na prÃ©via de custo.
    """
    if not pid_ok:
        return

    pid_map = _mapa_pid_busca(list(pid_ok))
    codigo_map = _mapa_codigo_para_pid(produtos_por_id)
    if not pid_map:
        return

    excluir = {str(x).strip() for x in (excluir_rascunho_ids or set()) if str(x).strip()}

    try:
        from django.db.models import Q

        from produtos.entrada_nota_rascunho_pg_util import row_to_doc
        from produtos.models import EntradaNotaRascunhoAgro
        from produtos.nfe_entrada_util import (
            ENTRADA_NFE_STATUS_DESCARTADA,
            ENTRADA_NFE_STATUS_ENCERRADA,
            ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        )

        # Filtra por produto_id no JSON + limite baixo (evita carregar centenas de notas).
        lim = 80
        if mongo_max_time_ms is not None and int(mongo_max_time_ms) <= 8_000:
            lim = 40
        pid_list = [str(x).strip() for x in pid_ok if str(x).strip()][:60]
        pid_q = Q()
        for pid in pid_list:
            pid_q |= Q(linhas__contains=[{"produto_id": pid}])
        qs = (
            EntradaNotaRascunhoAgro.objects.exclude(status=ENTRADA_NFE_STATUS_DESCARTADA)
            .filter(
                Q(status__in=[ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO])
                | Q(estoque_aplicado_em__isnull=False)
            )
            .filter(pid_q)
            .only(
                "rascunho_id",
                "cabecalho",
                "linhas",
                "extra",
                "criado_em",
                "estoque_aplicado_em",
                "status",
            )
            .order_by("-criado_em")[:lim]
        )
        proj = {
            "cabecalho": 1,
            "linhas": 1,
            "extra": 1,
            "criado_em": 1,
            "estoque_aplicado_em": 1,
            "status": 1,
        }
        docs = [row_to_doc(row, projection=proj) for row in qs]
    except Exception as exc:
        # Com rascunho PG: nÃ£o cair em find amplo (risco de varrer tabela).
        logger.warning("ultimas_compras entrada_nf_agro ORM: %s", exc)
        try:
            from produtos.agro_fonte_config import agro_entrada_nota_rascunho_postgres

            if agro_entrada_nota_rascunho_postgres():
                return
        except Exception:
            pass
        try:
            from produtos.nfe_entrada_util import _entrada_nota_rascunho_store

            col = _entrada_nota_rascunho_store(db)
            if col is None:
                return
            cur = (
                col.find(
                    {"status": {"$nin": ["descartada"]}},
                    {
                        "cabecalho": 1,
                        "linhas": 1,
                        "extra": 1,
                        "criado_em": 1,
                        "estoque_aplicado_em": 1,
                        "status": 1,
                    },
                )
                .sort("criado_em", -1)
                .limit(200)
            )
            docs = list(cur)
        except Exception as exc2:
            logger.warning("ultimas_compras entrada_nf_agro find: %s", exc2)
            return

    for doc in docs:
        if not isinstance(doc, dict) or not _doc_conta_como_compra_entrada_nf(doc):
            continue
        doc_id = str(doc.get("_id") or doc.get("rascunho_id") or "").strip()
        if doc_id and doc_id in excluir:
            continue
        cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
        dt = _data_doc_entrada_nf_agro(cab, doc)
        if dt is None or dt < since:
            continue
        forn = str(cab.get("emit_nome") or cab.get("fornecedor_nome") or "").strip()[:200] or "â€”"
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
                    "rascunho_id": doc_id,
                }
            )


def _compras_entrada_cutoff_dt() -> datetime:
    return datetime.utcnow() - timedelta(days=800)


def ultima_entrada_nf_agro_por_produto_ids(
    db,
    p_ids: list[str],
    produtos_por_id: dict | None = None,
    *,
    since: datetime | None = None,
    mongo_max_time_ms: int | None = 20_000,
) -> dict[str, dict[str, Any]]:
    """
    Ãšltima entrada NF Agro concluÃ­da por produto.
    Retorno: ``{ pid: {"data": iso str, "qtd": float} }``.
    """
    out: dict[str, dict[str, Any]] = {}
    if not p_ids:
        return out
    pid_ok = {str(x) for x in p_ids if str(x).strip()}
    if not pid_ok:
        return out
    since_dt = since or _compras_entrada_cutoff_dt()
    eventos: dict[str, list[dict]] = {str(pid): [] for pid in pid_ok}
    append_eventos_entrada_nf_agro(
        db,
        eventos=eventos,
        pid_ok=pid_ok,
        since=since_dt,
        produtos_por_id=produtos_por_id,
        mongo_max_time_ms=mongo_max_time_ms,
    )
    for pid in pid_ok:
        evs = [e for e in (eventos.get(pid) or []) if e.get("dt")]
        if not evs:
            continue
        best = max(evs, key=lambda e: e["dt"])
        dt = best.get("dt")
        iso = dt.isoformat()[:19] if isinstance(dt, datetime) else ""
        try:
            qtd = round(float(best.get("qtd") or 0), 4)
        except (TypeError, ValueError):
            qtd = 0.0
        out[pid] = {"data": iso, "qtd": qtd}
    return out


def _docs_entrada_nf_agro_por_fornecedor(
    fornecedor_nome: str,
    fornecedor_id: str | None = None,
    *,
    scan_limit: int = 400,
) -> list[dict]:
    """Rascunhos PG concluídos do fornecedor (pré-filtro id **ou** nome — não exclusivo)."""
    fn = str(fornecedor_nome or "").strip()
    fid = str(fornecedor_id or "").strip()
    if not fn and not fid:
        return []
    try:
        from django.db.models import Q

        from produtos.entrada_nota_rascunho_pg_util import row_to_doc
        from produtos.models import EntradaNotaRascunhoAgro
        from produtos.nfe_entrada_util import (
            ENTRADA_NFE_STATUS_DESCARTADA,
            ENTRADA_NFE_STATUS_ENCERRADA,
            ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        )
    except Exception as exc:
        logger.warning("docs entrada_nf forn import: %s", exc)
        return []

    lim = max(50, min(int(scan_limit or 400), 800))
    try:
        qs = (
            EntradaNotaRascunhoAgro.objects.exclude(status=ENTRADA_NFE_STATUS_DESCARTADA)
            .filter(
                Q(status__in=[ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO])
                | Q(estoque_aplicado_em__isnull=False)
            )
        )
        # Pré-filtro: id **ou** nome (antes só id quando vinha do autocomplete → zero NF).
        pre = Q()
        if fid:
            pre |= (
                Q(cabecalho__emit_fornecedor_id=fid)
                | Q(cabecalho__fornecedor_id=fid)
                | Q(cabecalho__emit_id=fid)
            )
        if fn:
            token = fn.split()[0][:40]
            pre |= Q(cabecalho__emit_nome__icontains=token) | Q(
                cabecalho__fornecedor_nome__icontains=token
            )
        if pre:
            qs = qs.filter(pre)
        qs = qs.only(
            "rascunho_id",
            "cabecalho",
            "linhas",
            "extra",
            "criado_em",
            "estoque_aplicado_em",
            "status",
        ).order_by("-criado_em")[:lim]
        proj = {
            "cabecalho": 1,
            "linhas": 1,
            "extra": 1,
            "criado_em": 1,
            "estoque_aplicado_em": 1,
            "status": 1,
        }
        return [row_to_doc(row, projection=proj) for row in qs]
    except Exception as exc:
        logger.warning("docs entrada_nf forn query: %s", exc)
        return []


def _fornecedor_casa_doc_entrada_nf(
    cab: dict,
    *,
    fornecedor_nome: str,
    fornecedor_id: str,
    nomes_batem,
) -> bool:
    emit = str(cab.get("emit_nome") or cab.get("fornecedor_nome") or "").strip()
    emit_id = str(
        cab.get("emit_fornecedor_id") or cab.get("fornecedor_id") or cab.get("emit_id") or ""
    ).strip()
    if fornecedor_id and emit_id and fornecedor_id == emit_id:
        return True
    if fornecedor_nome and emit and nomes_batem(fornecedor_nome, emit):
        return True
    return False


def produto_ids_entrada_nf_agro_por_fornecedor(
    fornecedor_nome: str,
    fornecedor_id: str | None = None,
    *,
    scan_limit: int = 800,
    limit: int = 800,
) -> tuple[list[str], dict[str, str]]:
    """
    Todos os produto_id que já entraram em NF Agro deste fornecedor (não só o último pedido).
    Retorna (ids, nomes_hint da linha quando houver).
    """
    fn = str(fornecedor_nome or "").strip()
    fid = str(fornecedor_id or "").strip()
    if not fn and not fid:
        return [], {}
    try:
        from produtos.nfe_entrada_util import _entrada_nfe_nomes_fornecedor_batem
    except Exception as exc:
        logger.warning("produto_ids entrada_nf forn import: %s", exc)
        return [], {}

    lim_out = max(1, min(int(limit or 800), 1200))
    docs = _docs_entrada_nf_agro_por_fornecedor(fn, fid, scan_limit=scan_limit)
    seen: set[str] = set()
    ids: list[str] = []
    nomes: dict[str, str] = {}

    def _add(pid: str, nome: str = "") -> None:
        p = str(pid or "").strip()
        if not p or p == "None" or p in seen:
            return
        if len(ids) >= lim_out:
            return
        seen.add(p)
        ids.append(p)
        nm = str(nome or "").strip()
        if nm:
            nomes[p] = nm[:500]
            if p.isdigit():
                nomes[str(int(p))] = nm[:500]

    for doc in docs:
        if not isinstance(doc, dict) or not _doc_conta_como_compra_entrada_nf(doc):
            continue
        cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
        if not _fornecedor_casa_doc_entrada_nf(
            cab,
            fornecedor_nome=fn,
            fornecedor_id=fid,
            nomes_batem=_entrada_nfe_nomes_fornecedor_batem,
        ):
            continue
        for ln in doc.get("linhas") if isinstance(doc.get("linhas"), list) else []:
            if not isinstance(ln, dict):
                continue
            pid = str(ln.get("produto_id") or ln.get("ProdutoID") or "").strip()
            if not pid or pid == "None":
                continue
            qtd = _qtd_linha_entrada_nf_agro(ln)
            if qtd <= 0:
                continue
            nome_ln = str(
                ln.get("produto_nome")
                or ln.get("nome")
                or ln.get("xProd")
                or ln.get("descricao")
                or ""
            ).strip()
            _add(pid, nome_ln)
            if len(ids) >= lim_out:
                return ids, nomes
    return ids, nomes


def ultimo_documento_entrada_nf_agro_por_fornecedor(
    fornecedor_nome: str,
    fornecedor_id: str | None = None,
    *,
    scan_limit: int = 400,
) -> dict | None:
    """
    Ultima Entrada NF Agro concluida do fornecedor (Postgres).
    Retorno: dt, documento, origem, linhas_qtd {pid: float}, hist_pids set[str].
    """
    fn = str(fornecedor_nome or "").strip()
    fid = str(fornecedor_id or "").strip()
    if not fn and not fid:
        return None
    try:
        from produtos.nfe_entrada_util import _entrada_nfe_nomes_fornecedor_batem
    except Exception as exc:
        logger.warning("ultimo_doc entrada_nf forn import: %s", exc)
        return None

    docs = _docs_entrada_nf_agro_por_fornecedor(fn, fid, scan_limit=scan_limit)
    best: dict | None = None
    best_dt: datetime | None = None
    hist_pids: set[str] = set()
    for doc in docs:
        if not isinstance(doc, dict) or not _doc_conta_como_compra_entrada_nf(doc):
            continue
        cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
        if not _fornecedor_casa_doc_entrada_nf(
            cab,
            fornecedor_nome=fn,
            fornecedor_id=fid,
            nomes_batem=_entrada_nfe_nomes_fornecedor_batem,
        ):
            continue
        dt = _data_doc_entrada_nf_agro(cab, doc)
        if dt is None:
            continue
        linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
        linhas_qtd: dict[str, float] = {}
        for ln in linhas:
            if not isinstance(ln, dict):
                continue
            pid = str(ln.get("produto_id") or ln.get("ProdutoID") or "").strip()
            if not pid or pid == "None":
                continue
            qtd = _qtd_linha_entrada_nf_agro(ln)
            if qtd <= 0:
                continue
            hist_pids.add(pid)
            linhas_qtd[pid] = linhas_qtd.get(pid, 0.0) + qtd
            if pid.isdigit():
                hist_pids.add(str(int(pid)))
        if best_dt is None or dt > best_dt:
            best_dt = dt
            best = {
                "dt": dt,
                "documento": _numero_doc_entrada_nf_agro(cab),
                "origem": "entrada_nf_agro",
                "linhas_qtd": linhas_qtd,
            }
    if not best or best_dt is None:
        return None
    best["hist_pids"] = hist_pids
    return best

