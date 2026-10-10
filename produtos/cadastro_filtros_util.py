"""Filtros avançados da lista Cadastro ERP (`/produtos/cadastro-erp/`)."""
from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any

from django.db.models import Q
from django.utils import timezone

logger = logging.getLogger(__name__)

_GETLIST_CAMPOS = (
    "marca",
    "categoria",
    "subcategoria",
    "subcategoria_2",
    "subcategoria_3",
    "subcategoria_4",
    "fornecedor",
    "unidade",
    "modelo",
)


def _as_lista(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        out: list[str] = []
        for x in raw:
            t = str(x or "").strip()
            if t and t not in out:
                out.append(t)
        return out
    t = str(raw or "").strip()
    if not t:
        return []
    parts = [p.strip() for p in t.split(",")]
    out = []
    for p in parts:
        if p and p not in out:
            out.append(p)
    return out


def _parse_date_br(raw: str) -> date | None:
    s = str(raw or "").strip()
    if not s:
        return None
    if "-" in s and len(s) >= 10:
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            pass
    m = s.replace(".", "/").split("/")
    if len(m) == 3:
        try:
            d, mo, y = int(m[0]), int(m[1]), int(m[2])
            if y < 100:
                y += 2000
            return date(y, mo, d)
        except ValueError:
            return None
    return None


def _parse_money(raw: str) -> Decimal | None:
    s = str(raw or "").strip().replace("R$", "").replace(" ", "")
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def parse_filtros_cadastro(request) -> dict[str, Any]:
    """Lê GET da lista/busca cadastro (multi getlist + estoque + datas + extras)."""
    get = request.GET
    out: dict[str, Any] = {
        "incluir_saldo": str(get.get("incluir_saldo", "1")).strip().lower()
        not in ("0", "false", "no"),
    }
    for campo in _GETLIST_CAMPOS:
        brutos: list[str] = []
        for raw in get.getlist(campo):
            brutos.extend(_as_lista(raw))
        vistos: list[str] = []
        for b in brutos:
            if b not in vistos:
                vistos.append(b)
        out[campo] = vistos

    loja = str(get.get("estoque_loja") or get.get("loja") or "total").strip().lower()
    if loja not in ("total", "centro", "vila"):
        loja = "total"
    sinal = str(get.get("estoque_sinal") or "").strip().lower()
    if sinal in ("positivo", "pos", "+"):
        sinal = "positivo"
    elif sinal in ("negativo", "neg", "-"):
        sinal = "negativo"
    elif sinal in ("zero", "zerado", "0"):
        sinal = "zero"
    else:
        sinal = ""
    out["estoque_loja"] = loja
    out["estoque_sinal"] = sinal

    data_tipo = str(get.get("data_tipo") or "").strip().lower()
    if data_tipo not in ("cadastro", "primeira_nf", "ultima_nf"):
        data_tipo = "cadastro" if (get.get("data_de") or get.get("data_ate")) else ""
    out["data_tipo"] = data_tipo
    out["data_de"] = _parse_date_br(str(get.get("data_de") or ""))
    out["data_ate"] = _parse_date_br(str(get.get("data_ate") or ""))

    out["sem_marca"] = get.get("sem_marca") in ("1", "true", "yes")
    out["sem_categoria"] = get.get("sem_categoria") in ("1", "true", "yes")
    out["somente_agro"] = get.get("somente_agro") in ("1", "true", "yes")
    out["pendente_pdv"] = get.get("pendente_pdv") in ("1", "true", "yes") or get.get(
        "pendente_conferencia_pdv"
    ) in ("1", "true", "yes")
    ncm = str(get.get("ncm") or "").strip().lower()
    out["ncm"] = ncm if ncm in ("com", "sem") else ""
    out["custo_min"] = _parse_money(str(get.get("custo_min") or ""))
    out["custo_max"] = _parse_money(str(get.get("custo_max") or ""))
    out["venda_min"] = _parse_money(str(get.get("venda_min") or ""))
    out["venda_max"] = _parse_money(str(get.get("venda_max") or ""))
    return out


def filtros_cadastro_ativos(f: dict[str, Any]) -> bool:
    if not f:
        return False
    for campo in _GETLIST_CAMPOS:
        if f.get(campo):
            return True
    if f.get("estoque_sinal"):
        return True
    if f.get("data_tipo") and (f.get("data_de") or f.get("data_ate")):
        return True
    if (
        f.get("sem_marca")
        or f.get("sem_categoria")
        or f.get("somente_agro")
        or f.get("pendente_pdv")
        or f.get("ncm")
    ):
        return True
    if any(f.get(k) is not None for k in ("custo_min", "custo_max", "venda_min", "venda_max")):
        return True
    return False


def filtros_cadastro_cache_key(f: dict[str, Any]) -> str:
    parts: list[str] = []
    for k in sorted(f.keys()):
        if k == "incluir_saldo":
            continue
        v = f.get(k)
        if v in (None, "", [], False):
            continue
        parts.append(f"{k}={v}")
    raw = "|".join(parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def aplicar_filtros_cadastro_qs(qs, f: dict[str, Any]):
    """Aplica filtros dimensionais / preço / flags no QuerySet Produto."""
    if not f:
        return qs

    marcas = f.get("marca") or []
    if marcas:
        qs = qs.filter(marca__in=marcas)
    if f.get("sem_marca"):
        qs = qs.filter(Q(marca__isnull=True) | Q(marca=""))

    cats = f.get("categoria") or []
    if cats:
        qs = qs.filter(categoria__in=cats)
    if f.get("sem_categoria"):
        qs = qs.filter(Q(categoria__isnull=True) | Q(categoria=""))

    for campo in ("subcategoria", "subcategoria_2", "subcategoria_3", "subcategoria_4"):
        vals = f.get(campo) or []
        if vals:
            qs = qs.filter(**{f"{campo}__in": vals})

    unidades = f.get("unidade") or []
    if unidades:
        qs = qs.filter(unidade__in=unidades)

    modelos = f.get("modelo") or []
    if modelos:
        q_mod = Q()
        for m in modelos:
            q_mod |= Q(modelo__iexact=m)
        qs = qs.filter(q_mod)

    forns = f.get("fornecedor") or []
    if forns:
        q_f = Q()
        for fr in forns:
            q_f |= Q(fornecedor_texto__icontains=fr)
        qs = qs.filter(q_f)

    if f.get("somente_agro"):
        qs = qs.filter(cadastro_somente_agro=True)

    if f.get("pendente_pdv"):
        from produtos.pdv_cadastro_rapido_util import ids_pendentes_pdv

        pids = ids_pendentes_pdv(2000)
        f["_pendente_pdv_ids"] = set(pids)
        if not pids:
            qs = qs.none()
        else:
            qs = qs.filter(produto_externo_id__in=pids)

    ncm = f.get("ncm") or ""
    if ncm == "com":
        qs = qs.exclude(Q(ncm__isnull=True) | Q(ncm=""))
    elif ncm == "sem":
        qs = qs.filter(Q(ncm__isnull=True) | Q(ncm=""))

    if f.get("custo_min") is not None:
        qs = qs.filter(custo__gte=f["custo_min"])
    if f.get("custo_max") is not None:
        qs = qs.filter(custo__lte=f["custo_max"])
    if f.get("venda_min") is not None:
        qs = qs.filter(preco_venda__gte=f["venda_min"])
    if f.get("venda_max") is not None:
        qs = qs.filter(preco_venda__lte=f["venda_max"])

    data_tipo = f.get("data_tipo") or ""
    d0: date | None = f.get("data_de")
    d1: date | None = f.get("data_ate")
    if data_tipo == "cadastro" and (d0 or d1):
        if d0:
            start = timezone.make_aware(datetime.combine(d0, time.min))
            qs = qs.filter(criado_em__gte=start)
        if d1:
            end = timezone.make_aware(datetime.combine(d1, time.max))
            qs = qs.filter(criado_em__lte=end)

    if data_tipo in ("primeira_nf", "ultima_nf") and (d0 or d1):
        ids = produto_ids_por_data_entrada_nf(
            tipo=data_tipo,
            data_de=d0,
            data_ate=d1,
        )
        if not ids:
            return qs.none()
        qs = qs.filter(produto_externo_id__in=list(ids))

    sinal = f.get("estoque_sinal") or ""
    if sinal:
        from produtos.estoque_saldo_agro_util import filtro_ids_estoque_sinal

        modo, id_set = filtro_ids_estoque_sinal(
            loja=str(f.get("estoque_loja") or "total"),
            sinal=sinal,
        )
        if modo == "in":
            if not id_set:
                return qs.none()
            qs = qs.filter(produto_externo_id__in=list(id_set))
        elif modo == "exclude":
            if id_set:
                qs = qs.exclude(produto_externo_id__in=list(id_set))

    return qs


def row_passa_filtros_cadastro(r: dict, f: dict[str, Any]) -> bool:
    """Filtro em dict (busca Mongo / pós-processamento)."""
    if not f:
        return True

    def _val_in(campo_row: str, key_f: str) -> bool:
        vals = f.get(key_f) or []
        if not vals:
            return True
        v = str(r.get(campo_row) or "").strip().casefold()
        return any(v == x.casefold() for x in vals)

    if not _val_in("marca", "marca"):
        return False
    if f.get("sem_marca") and str(r.get("marca") or "").strip():
        return False
    if not _val_in("categoria", "categoria"):
        return False
    if f.get("sem_categoria") and str(r.get("categoria") or "").strip():
        return False
    for campo in ("subcategoria", "subcategoria_2", "subcategoria_3", "subcategoria_4"):
        if not _val_in(campo, campo):
            return False
    if not _val_in("unidade", "unidade"):
        return False
    modelos = f.get("modelo") or []
    if modelos:
        m = str(r.get("modelo") or "").strip().casefold()
        if not any(m == x.casefold() for x in modelos):
            return False
    forns = f.get("fornecedor") or []
    if forns:
        fr = str(r.get("fornecedor") or "").strip().casefold()
        if not any(x.casefold() in fr for x in forns):
            return False
    if f.get("somente_agro") and not r.get("cadastro_somente_agro") and not r.get("somente_agro"):
        return False
    if f.get("pendente_pdv"):
        pid = str(r.get("id") or r.get("produto_externo_id") or "").strip()
        if not pid:
            return False
        # Conjunto cacheado no request/filtro se disponível; fallback por flag na row.
        pids = f.get("_pendente_pdv_ids")
        if pids is not None:
            if pid not in pids and pid[:64] not in pids:
                return False
        elif not r.get("pendente_conferencia") and not r.get("origem_pdv_pendente"):
            from produtos.pdv_cadastro_rapido_util import ids_pendentes_pdv

            if pid not in set(ids_pendentes_pdv(2000)):
                return False
    ncm_flag = f.get("ncm") or ""
    ncm_v = str(r.get("ncm") or "").strip()
    if ncm_flag == "com" and not ncm_v:
        return False
    if ncm_flag == "sem" and ncm_v:
        return False

    def _money_ok(key_row: str, vmin, vmax) -> bool:
        try:
            val = Decimal(str(r.get(key_row) or 0))
        except Exception:
            val = Decimal("0")
        if vmin is not None and val < vmin:
            return False
        if vmax is not None and val > vmax:
            return False
        return True

    if not _money_ok("preco_custo", f.get("custo_min"), f.get("custo_max")):
        return False
    if not _money_ok("preco_venda", f.get("venda_min"), f.get("venda_max")):
        return False

    data_tipo = f.get("data_tipo") or ""
    d0 = f.get("data_de")
    d1 = f.get("data_ate")
    if data_tipo == "cadastro" and (d0 or d1):
        raw = str(r.get("criado_em") or "")[:10]
        try:
            d = date.fromisoformat(raw) if raw else None
        except ValueError:
            d = None
        if d is None:
            return False
        if d0 and d < d0:
            return False
        if d1 and d > d1:
            return False
    if data_tipo in ("primeira_nf", "ultima_nf") and (d0 or d1):
        ids_ok = f.get("_nf_ids_cache")
        if ids_ok is None:
            ids_ok = produto_ids_por_data_entrada_nf(tipo=data_tipo, data_de=d0, data_ate=d1)
            f["_nf_ids_cache"] = ids_ok
        pid = str(r.get("id") or "").strip()
        if pid not in ids_ok:
            return False

    sinal = f.get("estoque_sinal") or ""
    if sinal:
        loja = str(f.get("estoque_loja") or "total")
        try:
            sc = float(r.get("saldo_centro") or 0)
            sv = float(r.get("saldo_vila") or 0)
        except (TypeError, ValueError):
            sc, sv = 0.0, 0.0
        if loja == "centro":
            saldo = sc
        elif loja == "vila":
            saldo = sv
        else:
            saldo = sc + sv
        if sinal == "positivo" and not (saldo > 0):
            return False
        if sinal == "negativo" and not (saldo < 0):
            return False
        if sinal == "zero" and not (abs(saldo) < 1e-9):
            return False

    return True


def mapa_primeira_ultima_entrada_nf(*, force: bool = False) -> dict[str, dict[str, date]]:
    """
    {pid: {"primeira": date, "ultima": date}} a partir de Entrada NF Agro (PG).
    Cache curto — usado só quando o filtro de data NF está ativo.
    """
    from django.core.cache import cache

    cache_key = "cadastro_mapa_entrada_nf_v1"
    if not force:
        hit = cache.get(cache_key)
        if isinstance(hit, dict):
            return hit

    out: dict[str, dict[str, date]] = {}
    try:
        from produtos.compras_ultimas_compras_util import (
            _data_doc_entrada_nf_agro,
            _doc_conta_como_compra_entrada_nf,
            _qtd_linha_entrada_nf_agro,
        )
        from produtos.models import EntradaNotaRascunhoAgro
        from produtos.nfe_entrada_util import (
            ENTRADA_NFE_STATUS_DESCARTADA,
            ENTRADA_NFE_STATUS_ENCERRADA,
            ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        )

        qs = (
            EntradaNotaRascunhoAgro.objects.exclude(status=ENTRADA_NFE_STATUS_DESCARTADA)
            .filter(
                Q(status__in=[ENTRADA_NFE_STATUS_ENCERRADA, ENTRADA_NFE_STATUS_ESTOQUE_APLICADO])
                | Q(estoque_aplicado_em__isnull=False)
            )
            .only("cabecalho", "linhas", "extra", "criado_em", "estoque_aplicado_em", "status")
            .order_by("-criado_em")[:2500]
        )
        for row in qs.iterator(chunk_size=100):
            doc = {
                "cabecalho": row.cabecalho if isinstance(row.cabecalho, dict) else {},
                "linhas": row.linhas if isinstance(row.linhas, list) else [],
                "extra": row.extra if isinstance(row.extra, dict) else {},
                "criado_em": row.criado_em,
                "estoque_aplicado_em": row.estoque_aplicado_em,
                "status": row.status,
            }
            if not _doc_conta_como_compra_entrada_nf(doc):
                continue
            cab = doc["cabecalho"]
            dt = _data_doc_entrada_nf_agro(cab, doc)
            if dt is None:
                continue
            d = dt.date() if isinstance(dt, datetime) else dt
            if not isinstance(d, date):
                continue
            for ln in doc["linhas"]:
                if not isinstance(ln, dict):
                    continue
                if _qtd_linha_entrada_nf_agro(ln) <= 0:
                    continue
                pid = str(ln.get("produto_id") or "").strip()
                if not pid:
                    continue
                cur = out.get(pid)
                if not cur:
                    out[pid] = {"primeira": d, "ultima": d}
                else:
                    if d < cur["primeira"]:
                        cur["primeira"] = d
                    if d > cur["ultima"]:
                        cur["ultima"] = d
    except Exception as exc:
        logger.warning("mapa_primeira_ultima_entrada_nf: %s", exc)

    try:
        cache.set(cache_key, out, 300)
    except Exception:
        pass
    return out


def produto_ids_por_data_entrada_nf(
    *,
    tipo: str,
    data_de: date | None,
    data_ate: date | None,
) -> set[str]:
    mapa = mapa_primeira_ultima_entrada_nf()
    chave = "primeira" if tipo == "primeira_nf" else "ultima"
    out: set[str] = set()
    for pid, datas in mapa.items():
        d = datas.get(chave)
        if not isinstance(d, date):
            continue
        if data_de and d < data_de:
            continue
        if data_ate and d > data_ate:
            continue
        out.add(pid)
    return out
