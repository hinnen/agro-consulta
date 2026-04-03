"""
Agregações e operações financeiras a partir do Mongo (DtoLancamento), alinhadas ao ERP.
Baixa via Mongo: o ERP pode resincronizar e sobrescrever — use API dedicada quando existir (ver VendaERPAPIClient).
"""
from __future__ import annotations

import copy
import logging
import re
from collections import defaultdict
import secrets
import unicodedata
from datetime import date, datetime, timedelta, time as dtime
from decimal import Decimal
from typing import Any

from bson import ObjectId
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

_SENTINEL = datetime(1, 1, 1, 0, 0)
COL_DTO_LANCAMENTO = "DtoLancamento"

# Campos que o DTO C# do ERP espera como string no BSON (não ObjectId).
_COERCE_OID_CAMPOS_ERP = (
    "BancoID",
    "FormaPagamentoID",
    "EmpresaID",
    "ClienteID",
    "LancamentoGrupoID",
    "PlanoDeContaID",
    "CentroDeCustoID",
)


def _financeiro_id_para_string(v: Any) -> str:
    """Converte valor de ID (JSON, ObjectId, etc.) em string para gravação no DtoLancamento."""
    if v is None:
        return ""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, dict):
        oid = v.get("$oid")
        if isinstance(oid, str):
            return oid.strip()
        return ""
    return str(v).strip()


def _financeiro_doc_coerce_ids_oid_para_string(doc: dict[str, Any]) -> None:
    """Evita herdar ObjectId do documento modelo ao inserir — o ERP deserializa *ID como string."""
    for k in _COERCE_OID_CAMPOS_ERP:
        val = doc.get(k)
        if isinstance(val, ObjectId):
            doc[k] = str(val)


def _sanear_dto_lancamento_ids_erp_string(col, oid_lancamento: ObjectId) -> None:
    """
    Corrige no Mongo campos *ID que o BorlanV2.DTO.DtoLancamento espera como string no BSON.
    Documentos antigos podem ter PlanoDeContaID como ObjectId (quebrava visualização no ERP).
    """
    if col is None or oid_lancamento is None:
        return
    try:
        doc = col.find_one({"_id": oid_lancamento}, {"PlanoDeContaID": 1, "CentroDeCustoID": 1})
        if not doc:
            return
        set_doc: dict[str, Any] = {}
        for key in ("PlanoDeContaID", "CentroDeCustoID"):
            v = doc.get(key)
            if isinstance(v, ObjectId):
                set_doc[key] = str(v)
        if set_doc:
            col.update_one({"_id": oid_lancamento}, {"$set": set_doc})
    except Exception:
        logger.exception("_sanear_dto_lancamento_ids_erp_string")


def _dec(v) -> Decimal:
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _restante_a_pagar(doc: dict) -> Decimal:
    saida = _dec(doc.get("Saida"))
    valor_pago = _dec(doc.get("ValorPago"))
    r = saida - valor_pago
    return r if r > 0 else Decimal("0")


def _valor_realizado_receita_dec(entrada: Decimal, rec: Decimal, vp: Decimal) -> Decimal:
    """
    Valor realizado no título a receber. O ERP às vezes grava o mesmo recebimento em Recebido e
    ValorPago; somar os dois infla o DRE. Quando Entrada > 0, usa min(Entrada, Recebido+ValorPago).
    """
    s = rec + vp
    if entrada > 0:
        return min(entrada, s)
    return s


def _restante_a_receber(doc: dict) -> Decimal:
    """
    Saldo a receber. Movimento realizado = ``_valor_realizado_receita_dec`` (sem dupla contagem).
    """
    entrada = _dec(doc.get("Entrada"))
    rec = _dec(doc.get("Recebido"))
    vp = _dec(doc.get("ValorPago"))
    mov = _valor_realizado_receita_dec(entrada, rec, vp)
    r = entrada - mov
    return r if r > 0 else Decimal("0")


def _mongo_expr_valor_realizado_receita() -> dict[str, Any]:
    """Expressão Mongo (aggregate) equivalente a ``_valor_realizado_receita_dec``."""
    soma_rv: dict[str, Any] = {
        "$add": [
            {"$ifNull": ["$Recebido", 0]},
            {"$ifNull": ["$ValorPago", 0]},
        ]
    }
    return {
        "$cond": [
            {"$gt": [{"$ifNull": ["$Entrada", 0]}, 0]},
            {"$min": [{"$ifNull": ["$Entrada", 0]}, soma_rv]},
            soma_rv,
        ]
    }


def _mongo_expr_dre_dedup_key() -> dict[str, Any]:
    """
    Chave para o primeiro ``$group`` do DRE. Com Id/LancamentoID do ERP, usa-os.
    Sem ambos: se ``DRE_DEDUP_ASSINATURA_SEM_ID``, monta assinatura estável com ``vl_dre`` (exige
    estágio ``$addFields`` anterior com ``vl_dre``); senão usa ``_id`` (cada cópia duplicada soma de novo).
    """
    id_trim = {"$trim": {"input": {"$toString": {"$ifNull": ["$Id", ""]}}}}
    lid_trim = {"$trim": {"input": {"$toString": {"$ifNull": ["$LancamentoID", ""]}}}}
    dstr = {
        "$cond": [
            {"$ne": ["$DataPagamento", None]},
            {"$dateToString": {"format": "%Y-%m-%d", "date": "$DataPagamento"}},
            "nod",
        ]
    }
    vl_round = {"$toString": {"$round": [{"$toDouble": {"$ifNull": ["$vl_dre", 0]}}, 2]}}
    assinatura: dict[str, Any] = {
        "$concat": [
            "SIG|",
            {"$ifNull": [{"$toString": "$EmpresaID"}, ""]},
            "|",
            {"$ifNull": ["$Empresa", ""]},
            "|",
            dstr,
            "|",
            {"$ifNull": ["$PlanoDeConta", ""]},
            "|",
            {"$ifNull": [{"$toString": {"$ifNull": ["$PlanoDeContaID", ""]}}, ""]},
            "|",
            {"$toString": "$Despesa"},
            "|",
            vl_round,
            "|",
            {"$ifNull": [{"$toString": "$NumeroDocumento"}, ""]},
            "|",
            {"$toString": {"$ifNull": ["$NumeroParcela", 0]}},
        ]
    }
    fallback_oid = {"$toString": "$_id"}
    if not getattr(settings, "DRE_DEDUP_ASSINATURA_SEM_ID", True):
        assinatura = fallback_oid
    return {
        "$cond": [
            {"$gt": [{"$strLenCP": id_trim}, 0]},
            {"$concat": ["ID|", {"$toString": "$Id"}]},
            {
                "$cond": [
                    {"$gt": [{"$strLenCP": lid_trim}, 0]},
                    {"$concat": ["LID|", {"$toString": "$LancamentoID"}]},
                    assinatura,
                ]
            },
        ]
    }


def _filtro_sem_quitacao_registrada():
    return {
        "$or": [
            {"DataPagamento": {"$exists": False}},
            {"DataPagamento": None},
            {"DataPagamento": {"$lte": _SENTINEL}},
        ]
    }


def obter_vencimentos_abertos_dia_mongo(db, dia=None) -> tuple[Decimal, Decimal]:
    if db is None:
        return Decimal("0"), Decimal("0")

    dia = dia or timezone.localdate()
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(dia, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(dia, dtime.max), tz)

    q_base = {
        "DataVencimento": {"$gte": inicio, "$lte": fim, "$gt": _SENTINEL},
        "Pago": False,
        **_filtro_sem_quitacao_registrada(),
    }

    total_pagar = Decimal("0")
    total_receber = Decimal("0")

    try:
        for doc in db[COL_DTO_LANCAMENTO].find({**q_base, "Despesa": True}):
            total_pagar += _restante_a_pagar(doc)
        for doc in db[COL_DTO_LANCAMENTO].find({**q_base, "Despesa": False}):
            total_receber += _restante_a_receber(doc)
    except Exception as exc:
        logger.exception("obter_vencimentos_abertos_dia_mongo: %s", exc)
        return Decimal("0"), Decimal("0")

    return total_pagar.quantize(Decimal("0.01")), total_receber.quantize(Decimal("0.01"))


def _filtro_quitado():
    return {
        "$or": [
            {"Pago": True},
            {"DataPagamento": {"$exists": True, "$gt": _SENTINEL}},
        ]
    }


def _dt_efetiva(v) -> bool:
    return v is not None and isinstance(v, datetime) and v > _SENTINEL


_SEM_PLANO_MARKER = "__SEM_PLANO__"


def _fragmento_exclusao_planos(excluir_planos_nomes: list[str] | None) -> dict[str, Any] | None:
    """Restringe lançamentos excluindo nomes exatos de PlanoDeConta e/ou títulos sem plano."""
    raw = [str(x).strip() for x in (excluir_planos_nomes or []) if x and str(x).strip()]
    if not raw:
        return None
    exclui_sem = _SEM_PLANO_MARKER in raw or any(x.lower() == "(sem plano)" for x in raw)
    nomes = [
        x
        for x in raw
        if x != _SEM_PLANO_MARKER and x.lower() != "(sem plano)"
    ]
    partes: list[dict[str, Any]] = []
    if nomes:
        partes.append({"PlanoDeConta": {"$nin": nomes[:200]}})
    if exclui_sem:
        partes.append({"PlanoDeConta": {"$regex": r".", "$options": "s"}})
    if not partes:
        return None
    if len(partes) == 1:
        return partes[0]
    return {"$and": partes}


def _data_vencimento_local_doc(doc: dict) -> date | None:
    dv = doc.get("DataVencimento")
    if dv is None or not isinstance(dv, datetime):
        return None
    if dv.replace(tzinfo=None) <= _SENTINEL.replace(tzinfo=None):
        return None
    tz = timezone.get_current_timezone()
    if timezone.is_naive(dv):
        dv = timezone.make_aware(dv, tz)
    return timezone.localtime(dv).date()


def lancamentos_montar_query_mongo(
    *,
    despesa: bool,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    texto: str | None = None,
    excluir_planos_nomes: list[str] | None = None,
) -> dict[str, Any]:
    base: dict[str, Any] = {"Despesa": bool(despesa)}

    st = (status or "abertos").strip().lower()
    if st == "abertos":
        base["Pago"] = False
        base.update(_filtro_sem_quitacao_registrada())
    elif st == "quitados":
        base.update(_filtro_quitado())
    elif st != "todos":
        st = "abertos"
        base["Pago"] = False
        base.update(_filtro_sem_quitacao_registrada())

    tz = timezone.get_current_timezone()
    if vencimento_de is not None:
        ini = timezone.make_aware(datetime.combine(vencimento_de, dtime.min), tz)
        base.setdefault("DataVencimento", {})
        if not isinstance(base["DataVencimento"], dict):
            base["DataVencimento"] = {}
        base["DataVencimento"]["$gte"] = ini
    if vencimento_ate is not None:
        fim = timezone.make_aware(datetime.combine(vencimento_ate, dtime.max), tz)
        base.setdefault("DataVencimento", {})
        if not isinstance(base["DataVencimento"], dict):
            base["DataVencimento"] = {}
        base["DataVencimento"]["$lte"] = fim

    if base.get("DataVencimento") == {}:
        del base["DataVencimento"]

    t = (texto or "").strip()
    if t:
        esc = re.escape(t[:120])
        rx = re.compile(esc, re.IGNORECASE)
        texto_or = {
            "$or": [
                {"Descricao": rx},
                {"Cliente": rx},
                {"NumeroDocumento": rx},
                {"Observacoes": rx},
                {"PlanoDeConta": rx},
                {"LancamentoGrupo": rx},
                {"FormaPagamento": rx},
                {"Banco": rx},
            ]
        }
        q: dict[str, Any] = {"$and": [base, texto_or]}
    else:
        q = base

    frag = _fragmento_exclusao_planos(excluir_planos_nomes)
    if frag is not None:
        q = {"$and": [q, frag]}
    return q


def contas_pagar_montar_query_mongo(**kwargs) -> dict[str, Any]:
    """Compatível: apenas Despesa=True."""
    kwargs.pop("despesa", None)
    return lancamentos_montar_query_mongo(despesa=True, **kwargs)


def _lancamentos_sort_spec_list(ordenacao: str = "vencimento_asc") -> list[tuple[str, int]]:
    ord_ = (ordenacao or "vencimento_asc").strip().lower()
    if ord_ == "vencimento_desc":
        return [("DataVencimento", -1), ("_id", -1)]
    if ord_ == "fluxo_desc":
        return [("DataFluxo", -1), ("_id", -1)]
    return [("DataVencimento", 1), ("_id", 1)]


def _lancamentos_mongo_stages_dedup_por_titulo_erp(sort_spec: list[tuple[str, int]]) -> list[dict[str, Any]]:
    """
    Um documento por título no ERP: evita linhas repetidas quando o Mongo recebeu o mesmo
    DtoLancamento duas vezes (ex.: resync). Sem LancamentoID, cada BSON _id permanece único.
    """
    lid_trim = {"$trim": {"input": {"$toString": {"$ifNull": ["$LancamentoID", ""]}}}}
    return [
        {
            "$addFields": {
                "_dupKey": {
                    "$cond": [
                        {"$gt": [{"$strLenCP": lid_trim}, 0]},
                        {
                            "$concat": [
                                "L|",
                                lid_trim,
                                "|P|",
                                {"$toString": {"$ifNull": ["$NumeroParcela", 0]}},
                            ]
                        },
                        {"$concat": ["O|", {"$toString": "$_id"}]},
                    ]
                }
            }
        },
        {"$sort": dict(sort_spec)},
        {"$group": {"_id": "$_dupKey", "_dedup": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$_dedup"}},
        {"$sort": dict(sort_spec)},
    ]


def _lancamentos_mongo_group_totais_stage(despesa: bool) -> dict[str, Any]:
    if despesa:
        return {
            "$group": {
                "_id": None,
                "n": {"$sum": 1},
                "bruto": {"$sum": {"$ifNull": ["$Saida", 0]}},
                "movimentado": {"$sum": {"$ifNull": ["$ValorPago", 0]}},
                "saldo_aberto": {
                    "$sum": {
                        "$max": [
                            0,
                            {
                                "$subtract": [
                                    {"$ifNull": ["$Saida", 0]},
                                    {"$ifNull": ["$ValorPago", 0]},
                                ]
                            },
                        ]
                    }
                },
            }
        }
    _mov_rec = _mongo_expr_valor_realizado_receita()
    return {
        "$group": {
            "_id": None,
            "n": {"$sum": 1},
            "bruto": {"$sum": {"$ifNull": ["$Entrada", 0]}},
            "movimentado": {"$sum": _mov_rec},
            "saldo_aberto": {
                "$sum": {
                    "$max": [
                        0,
                        {
                            "$subtract": [
                                {"$ifNull": ["$Entrada", 0]},
                                _mov_rec,
                            ]
                        },
                    ]
                }
            },
        }
    }


def _lancamentos_totais_dict_from_group_doc(a: dict | None) -> dict[str, float]:
    if not a:
        return {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}
    return {
        "quantidade": int(a.get("n") or 0),
        "bruto": round(float(a.get("bruto") or 0), 2),
        "movimentado": round(float(a.get("movimentado") or 0), 2),
        "saldo_aberto": round(float(a.get("saldo_aberto") or 0), 2),
    }


def lancamentos_totais_filtrados(db, query: dict, despesa: bool) -> dict[str, float]:
    if db is None:
        return {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}
    try:
        sort_dedup = _lancamentos_sort_spec_list("vencimento_asc")
        dedup = _lancamentos_mongo_stages_dedup_por_titulo_erp(sort_dedup)
        pipe: list[dict[str, Any]] = [
            {"$match": query},
            *dedup,
            _lancamentos_mongo_group_totais_stage(despesa),
        ]
        agg = list(db[COL_DTO_LANCAMENTO].aggregate(pipe))
        return _lancamentos_totais_dict_from_group_doc(agg[0] if agg else None)
    except Exception as exc:
        logger.exception("lancamentos_totais_filtrados: %s", exc)
        return {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}


def contas_pagar_totais_filtrados(db, query: dict) -> dict[str, float]:
    t = lancamentos_totais_filtrados(db, query, True)
    return {
        "quantidade": t["quantidade"],
        "previsto": t["bruto"],
        "pago": t["movimentado"],
        "a_pagar": t["saldo_aberto"],
    }


def _serializar_dt(v) -> str | None:
    if v is None or not isinstance(v, datetime):
        return None
    if v.replace(tzinfo=None) <= _SENTINEL:
        return None
    try:
        if timezone.is_naive(v):
            v = timezone.make_aware(v, timezone.get_current_timezone())
        return timezone.localtime(v).isoformat()
    except Exception:
        return v.isoformat(sep=" ")


def _lancamento_e_manual_agro(doc: dict) -> bool:
    obs = str(doc.get("Observacoes") or "")
    mod = str(doc.get("ModificadoPor") or "")
    return "Lote manual Agro" in obs or "inclusão manual em lote Agro" in mod


def _lancamento_tem_vinculo_erp(doc: dict) -> bool:
    if str(doc.get("LancamentoID") or "").strip():
        return True
    x = str(doc.get("Id") or "").strip()
    if not x:
        return False
    if len(x) == 24 and re.match(r"^[a-fA-F0-9]{24}$", x):
        return False
    return True


def _lancamento_pode_excluir_agro(doc: dict, quitado: bool, valor_mov: float) -> bool:
    if quitado or valor_mov > 0.02:
        return False
    if _lancamento_e_manual_agro(doc):
        return True
    return not _lancamento_tem_vinculo_erp(doc)


def lancamento_para_api(doc: dict, despesa: bool) -> dict[str, Any]:
    dp = doc.get("DataPagamento")
    quitado = bool(doc.get("Pago")) or _dt_efetiva(dp)
    if despesa:
        restante = _restante_a_pagar(doc)
        bruto = float(_dec(doc.get("Saida")))
        mov = float(_dec(doc.get("ValorPago")))
    else:
        restante = _restante_a_receber(doc)
        bruto = float(_dec(doc.get("Entrada")))
        mov = float(
            _valor_realizado_receita_dec(
                _dec(doc.get("Entrada")),
                _dec(doc.get("Recebido")),
                _dec(doc.get("ValorPago")),
            )
        )
    mov_r = round(float(mov), 2)
    return {
        "id": str(doc.get("_id", "")),
        "despesa": despesa,
        "descricao": doc.get("Descricao") or "",
        "cliente": doc.get("Cliente") or "",
        "cliente_id": str(doc.get("ClienteID") or ""),
        "numero_documento": str(doc.get("NumeroDocumento") or ""),
        "parcela": int(doc.get("NumeroParcela") or 0),
        "plano_conta": doc.get("PlanoDeConta") or "",
        "plano_conta_id": _financeiro_id_para_string(doc.get("PlanoDeContaID")),
        "grupo": doc.get("LancamentoGrupo") or "",
        "forma_pagamento": doc.get("FormaPagamento") or "",
        "forma_pagamento_id": str(doc.get("FormaPagamentoID") or ""),
        "banco": doc.get("Banco") or "",
        "banco_id": str(doc.get("BancoID") or ""),
        "centro_custo": doc.get("CentroDeCusto") or "",
        "empresa": doc.get("Empresa") or "",
        "observacoes": (doc.get("Observacoes") or "")[:500],
        "valor_bruto": round(bruto, 2),
        "valor_movimentado": round(float(mov), 2),
        "restante": float(restante.quantize(Decimal("0.01"))),
        "pago": quitado,
        "data_vencimento": _serializar_dt(doc.get("DataVencimento")),
        "data_competencia": _serializar_dt(doc.get("DataCompetencia")),
        "data_fluxo": _serializar_dt(doc.get("DataFluxo")),
        "data_pagamento": _serializar_dt(dp) if quitado else None,
        # aliases para compatibilidade com tela antiga
        "valor_previsto": round(bruto, 2),
        "valor_pago": round(float(mov), 2),
        "pode_editar": not quitado,
        "pode_editar_valor": (not quitado) and mov_r <= 0.02,
        "pode_excluir": _lancamento_pode_excluir_agro(doc, quitado, mov_r),
    }


def lancamento_contas_pagar_para_api(doc: dict) -> dict[str, Any]:
    return lancamento_para_api(doc, True)


def lancamentos_buscar_pagina(
    db,
    query: dict,
    despesa: bool,
    *,
    page: int = 1,
    page_size: int = 50,
    ordenacao: str = "vencimento_asc",
    limite_max: int = 200,
) -> tuple[list[dict], int, dict[str, float]]:
    if db is None:
        return [], 0, {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}

    page = max(1, page)
    cap = max(1, int(limite_max) if limite_max else 200)
    page_size = min(cap, max(1, page_size))
    skip = (page - 1) * page_size

    sort_spec = _lancamentos_sort_spec_list(ordenacao)

    try:
        col = db[COL_DTO_LANCAMENTO]
        dedup = _lancamentos_mongo_stages_dedup_por_titulo_erp(sort_spec)
        group_tot = _lancamentos_mongo_group_totais_stage(despesa)
        facet_stage: dict[str, Any] = {
            "$facet": {
                "total_count": [{"$count": "n"}],
                "page_slice": [{"$skip": skip}, {"$limit": page_size}],
                "totais_agg": [group_tot],
            }
        }
        pipe: list[dict[str, Any]] = [{"$match": query}, *dedup, facet_stage]
        agg = list(col.aggregate(pipe))
        total = 0
        page_docs: list[dict[str, Any]] = []
        totais = {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}
        if agg:
            facet = agg[0]
            tc = facet.get("total_count") or []
            if tc:
                total = int(tc[0].get("n") or 0)
            page_docs = list(facet.get("page_slice") or [])
            ta = facet.get("totais_agg") or []
            totais = _lancamentos_totais_dict_from_group_doc(ta[0] if ta else None)
        linhas = []
        for d in page_docs:
            d.pop("_dupKey", None)
            linhas.append(lancamento_para_api(d, despesa))
        return linhas, total, totais
    except Exception as exc:
        logger.exception("lancamentos_buscar_pagina: %s", exc)
        return [], 0, {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}


def contas_pagar_buscar_pagina(db, query: dict, **kwargs) -> tuple[list[dict], int, dict[str, float]]:
    linhas, total, totais = lancamentos_buscar_pagina(db, query, True, **kwargs)
    tot_legacy = {
        "quantidade": totais["quantidade"],
        "previsto": totais["bruto"],
        "pago": totais["movimentado"],
        "a_pagar": totais["saldo_aberto"],
    }
    return linhas, total, tot_legacy


def _maybe_oid(s: str | None) -> ObjectId | str | None:
    if not s or not str(s).strip():
        return None
    s = str(s).strip()
    try:
        if len(s) == 24 and re.match(r"^[a-fA-F0-9]{24}$", s):
            return ObjectId(s)
    except Exception:
        pass
    return s


def _listar_formas_e_bancos_modo_historico(db, limit: int) -> tuple[list[dict], list[dict]]:
    """Uma linha por combinação nome+ID vista em títulos (inclui digitação livre e duplicatas de rótulo)."""
    formas: list[dict] = []
    bancos: list[dict] = []
    seen_f: set[str] = set()
    seen_b: set[str] = set()
    col = db[COL_DTO_LANCAMENTO]
    pipe_f = [
        {"$match": {"FormaPagamento": {"$nin": [None, ""]}}},
        {
            "$group": {
                "_id": {
                    "nome": "$FormaPagamento",
                    "fid": "$FormaPagamentoID",
                }
            }
        },
        {"$limit": limit},
    ]
    for r in col.aggregate(pipe_f):
        i = r.get("_id") or {}
        nome = str(i.get("nome") or "").strip()
        if not nome or nome in seen_f:
            continue
        seen_f.add(nome)
        fid = i.get("fid")
        formas.append({"id": _financeiro_id_para_string(fid), "nome": nome})
    formas.sort(key=lambda x: x["nome"].lower())

    pipe_b = [
        {"$match": {"Banco": {"$nin": [None, "", "ADICIONAR BANCO", "Adicionar banco"]}}},
        {"$group": {"_id": {"nome": "$Banco", "bid": "$BancoID"}}},
        {"$limit": limit},
    ]
    for r in col.aggregate(pipe_b):
        i = r.get("_id") or {}
        nome = str(i.get("nome") or "").strip()
        if not nome or nome in seen_b:
            continue
        seen_b.add(nome)
        bid = i.get("bid")
        bancos.append({"id": _financeiro_id_para_string(bid), "nome": nome})
    bancos.sort(key=lambda x: x["nome"].lower())
    return formas, bancos


# Rótulos alinhados ao cadastro de contas no ERP (Agro Mais) quando o BancoID identifica uma conta única.
_BANCO_ID_ROTULO_ERP: dict[str, str] = {
    "323": "CPF | Mercado Pago ( Renan )",
    "001": "CPF | Banco Brasil ( Renan )",
    "237": "CPF | Bradesco ( Geraldinho )",
    "197": "CNPJ | Stone ( Cartões )",
    "756": "CPF | Sicoob ( Geraldinho )",
}


def normalizar_rotulo_banco_erp(banco_id: str, nome: str) -> str:
    """Uniformiza grafia (Santander, Sicredi, Sicoob) e aplica nome canônico por ID quando cadastrado."""
    bid = str(banco_id or "").strip()
    if bid in _BANCO_ID_ROTULO_ERP:
        return _BANCO_ID_ROTULO_ERP[bid]
    n = (nome or "").strip()
    if not n:
        return n
    n = n.replace("Satander", "Santander")
    n = re.sub(r"\bSicob\b", "Sicoob", n, flags=re.IGNORECASE)
    n = re.sub(r"\bSicred(?!i)([a-zA-ZÀ-ÿ]*)", r"Sicredi\1", n)
    return n


def _listar_formas_e_bancos_modo_erp(db, limit: int) -> tuple[list[dict], list[dict]]:
    """
    Agrupa por ID de cadastro (FormaPagamentoID / BancoID) convertido para string.
    Omite títulos sem ID (baixas só com texto livre) e entradas típicas de rascunho do ERP.
    """
    formas: list[dict] = []
    bancos: list[dict] = []
    col = db[COL_DTO_LANCAMENTO]
    pipe_f = [
        {
            "$match": {
                "$and": [
                    {"FormaPagamento": {"$nin": [None, ""]}},
                    {"FormaPagamento": {"$not": {"$regex": r"^criar\s+novo", "$options": "i"}}},
                ]
            }
        },
        {
            "$addFields": {
                "fidStr": {
                    "$convert": {
                        "input": "$FormaPagamentoID",
                        "to": "string",
                        "onError": "",
                        "onNull": "",
                    }
                }
            }
        },
        {"$match": {"fidStr": {"$ne": ""}}},
        {"$sort": {"FormaPagamento": 1}},
        {"$group": {"_id": "$fidStr", "nome": {"$first": "$FormaPagamento"}}},
        {"$limit": limit},
    ]
    for r in col.aggregate(pipe_f):
        fid = str(r.get("_id") or "").strip()
        nome = str(r.get("nome") or "").strip()
        if fid and nome:
            formas.append({"id": fid, "nome": nome})
    formas.sort(key=lambda x: x["nome"].lower())

    pipe_b = [
        {
            "$match": {
                "$and": [
                    {"Banco": {"$nin": [None, "", "ADICIONAR BANCO", "Adicionar banco"]}},
                ]
            }
        },
        {
            "$addFields": {
                "bidStr": {
                    "$convert": {
                        "input": "$BancoID",
                        "to": "string",
                        "onError": "",
                        "onNull": "",
                    }
                }
            }
        },
        {"$match": {"bidStr": {"$ne": ""}}},
        {"$sort": {"Banco": 1}},
        {"$group": {"_id": "$bidStr", "nome": {"$first": "$Banco"}}},
        {"$limit": limit},
    ]
    for r in col.aggregate(pipe_b):
        bid = str(r.get("_id") or "").strip()
        nome = str(r.get("nome") or "").strip()
        if bid and nome:
            bancos.append({"id": bid, "nome": normalizar_rotulo_banco_erp(bid, nome)})
    bancos.sort(key=lambda x: x["nome"].lower())
    return formas, bancos


def listar_formas_e_bancos_distintos(
    db, limit: int = 400, *, modo: str = "erp"
) -> tuple[list[dict], list[dict]]:
    """
    Listas para selects na baixa a partir do Mongo.

    - ``erp`` (padrão): uma entrada por ID de forma/conta (como no cadastro do ERP).
    - ``historico``: todas as combinações nome+ID já usadas em títulos (comportamento antigo).
    """
    formas: list[dict] = []
    bancos: list[dict] = []
    if db is None:
        return formas, bancos
    modo_n = (modo or "erp").strip().lower()
    if modo_n not in ("erp", "historico"):
        modo_n = "erp"
    try:
        if modo_n == "historico":
            return _listar_formas_e_bancos_modo_historico(db, limit)
        return _listar_formas_e_bancos_modo_erp(db, limit)
    except Exception as exc:
        logger.exception("listar_formas_e_bancos_distintos: %s", exc)
    return formas, bancos


def baixar_lancamentos_mongo(
    db,
    ids: list[str],
    *,
    despesa: bool,
    data_movimento: datetime,
    forma_nome: str,
    forma_id: str | None,
    banco_nome: str,
    banco_id: str | None,
    usuario_label: str,
) -> dict[str, Any]:
    """
    Quitação **total** de cada título (saldo restante) no Mongo, sobrescrevendo forma e conta bancária
    conforme escolha no ato da baixa.
    """
    if db is None:
        return {"ok": False, "atualizados": [], "erros": [{"id": "", "erro": "Mongo indisponível"}]}
    forma_nome = (forma_nome or "").strip()
    banco_nome = (banco_nome or "").strip()
    if not forma_nome or not banco_nome:
        return {"ok": False, "atualizados": [], "erros": [{"id": "", "erro": "Informe forma de pagamento e conta/banco."}]}

    now = timezone.now()
    mod = (usuario_label or "Agro")[:80] + " — baixa Agro Consulta"
    mod = mod[:200]
    col = db[COL_DTO_LANCAMENTO]
    res_ok: list[str] = []
    res_err: list[dict] = []

    # ERP espera string em FormaPagamentoID/BancoID; mantemos sempre string aqui.
    fid = _financeiro_id_para_string(forma_id)
    bid = _financeiro_id_para_string(banco_id)
    banco_nome = normalizar_rotulo_banco_erp(bid, banco_nome)[:200]

    for sid in (ids or [])[:80]:
        try:
            oid = ObjectId(str(sid).strip())
        except Exception:
            res_err.append({"id": sid, "erro": "ID inválido"})
            continue
        doc = col.find_one({"_id": oid})
        if not doc:
            res_err.append({"id": sid, "erro": "Lançamento não encontrado"})
            continue
        if bool(doc.get("Despesa")) != bool(despesa):
            res_err.append({"id": sid, "erro": "Tipo de lançamento divergente (pagar/receber)"})
            continue

        if despesa:
            if doc.get("Pago") or _dt_efetiva(doc.get("DataPagamento")):
                res_err.append({"id": sid, "erro": "Já quitado"})
                continue
            saida = float(_dec(doc.get("Saida")))
            rest = float(_restante_a_pagar(doc))
            if rest <= 0 or saida <= 0:
                res_err.append({"id": sid, "erro": "Sem saldo a pagar"})
                continue
            col.update_one(
                {"_id": oid},
                {
                    "$set": {
                        "Pago": True,
                        "DataPagamento": data_movimento,
                        "ValorPago": saida,
                        "FormaPagamento": forma_nome[:200],
                        "FormaPagamentoID": fid,
                        "Banco": banco_nome[:200],
                        "BancoID": bid,
                        "LastUpdate": now,
                        "ModificadoPor": mod,
                    }
                },
            )
        else:
            if doc.get("Pago") or _dt_efetiva(doc.get("DataPagamento")):
                res_err.append({"id": sid, "erro": "Já recebido/quitado"})
                continue
            entrada = float(_dec(doc.get("Entrada")))
            rest = float(_restante_a_receber(doc))
            if rest <= 0 or entrada <= 0:
                res_err.append({"id": sid, "erro": "Sem saldo a receber"})
                continue
            col.update_one(
                {"_id": oid},
                {
                    "$set": {
                        "Pago": True,
                        "DataPagamento": data_movimento,
                        "Recebido": entrada,
                        "ValorPago": entrada,
                        "FormaPagamento": forma_nome[:200],
                        "FormaPagamentoID": fid,
                        "Banco": banco_nome[:200],
                        "BancoID": bid,
                        "LastUpdate": now,
                        "ModificadoPor": mod,
                    }
                },
            )
        res_ok.append(str(oid))
        _sanear_dto_lancamento_ids_erp_string(col, oid)

    return {
        "ok": len(res_err) == 0,
        "atualizados": res_ok,
        "erros": res_err,
    }


def baixar_lancamento_parcial_mongo(
    db,
    lancamento_id: str,
    *,
    despesa: bool,
    data_movimento: datetime,
    parcelas: list[dict[str, Any]],
    usuario_label: str,
) -> dict[str, Any]:
    """
    Uma ou mais parcelas no mesmo título (várias formas/contas). Soma em ValorPago (a pagar)
    ou incrementa ValorPago (a receber). Quita quando o saldo zera.
    """
    if db is None:
        return {"ok": False, "id": None, "erro": "Mongo indisponível", "quitado": False}
    raw = [p for p in (parcelas or []) if isinstance(p, dict)]
    if not raw or len(raw) > 24:
        return {"ok": False, "id": None, "erro": "Informe de 1 a 24 parcelas (valor + forma + banco).", "quitado": False}

    now = timezone.now()
    mod = (usuario_label or "Agro")[:80] + " — baixa parcial Agro"
    mod = mod[:200]
    col = db[COL_DTO_LANCAMENTO]

    try:
        oid = ObjectId(str(lancamento_id).strip())
    except Exception:
        return {"ok": False, "id": None, "erro": "ID inválido", "quitado": False}

    for par in raw:
        forma_nome = str(par.get("forma_pagamento") or par.get("forma_nome") or "").strip()
        banco_nome = str(par.get("banco") or par.get("banco_nome") or "").strip()
        try:
            valor_par = float(str(par.get("valor", "")).replace(",", ".").strip())
        except (ValueError, TypeError):
            return {"ok": False, "id": str(oid), "erro": "Valor inválido em uma das parcelas.", "quitado": False}
        if valor_par <= 0:
            return {"ok": False, "id": str(oid), "erro": "Cada parcela deve ter valor maior que zero.", "quitado": False}
        if not forma_nome or not banco_nome:
            return {"ok": False, "id": str(oid), "erro": "Cada parcela precisa de forma de pagamento e banco/conta.", "quitado": False}

    doc = col.find_one({"_id": oid})
    if not doc:
        return {"ok": False, "id": None, "erro": "Lançamento não encontrado", "quitado": False}
    if bool(doc.get("Despesa")) != bool(despesa):
        return {"ok": False, "id": str(oid), "erro": "Tipo de lançamento divergente (pagar/receber)", "quitado": False}

    soma_par = sum(
        float(str(p.get("valor", "")).replace(",", ".").strip())
        for p in raw
    )
    if despesa:
        if doc.get("Pago") or _dt_efetiva(doc.get("DataPagamento")):
            return {"ok": False, "id": str(oid), "erro": "Título já quitado", "quitado": False}
        rest_ini = float(_restante_a_pagar(doc))
        if rest_ini <= 0 or soma_par > rest_ini + 0.02:
            return {
                "ok": False,
                "id": str(oid),
                "erro": f"Soma das parcelas (R$ {soma_par:.2f}) não pode exceder o saldo (R$ {rest_ini:.2f}).",
                "quitado": False,
            }
    else:
        if doc.get("Pago") or _dt_efetiva(doc.get("DataPagamento")):
            return {"ok": False, "id": str(oid), "erro": "Título já quitado/recebido", "quitado": False}
        rest_ini = float(_restante_a_receber(doc))
        entrada = float(_dec(doc.get("Entrada")))
        if rest_ini <= 0 or soma_par > rest_ini + 0.02:
            return {
                "ok": False,
                "id": str(oid),
                "erro": f"Soma das parcelas (R$ {soma_par:.2f}) não pode exceder o saldo (R$ {rest_ini:.2f}).",
                "quitado": False,
            }
        if entrada <= 0:
            return {"ok": False, "id": str(oid), "erro": "Sem valor de entrada no título", "quitado": False}

    quitado_final = False
    ultima_forma = ""
    ultima_banco = ""
    ultima_fid = ""
    ultima_bid = ""

    for par in raw:
        doc = col.find_one({"_id": oid})
        if not doc:
            return {"ok": False, "id": str(oid), "erro": "Lançamento sumiu durante a baixa", "quitado": False}
        forma_nome = str(par.get("forma_pagamento") or par.get("forma_nome") or "").strip()
        fid = _financeiro_id_para_string(par.get("forma_pagamento_id") or par.get("forma_id"))
        bid = _financeiro_id_para_string(par.get("banco_id"))
        banco_nome = str(par.get("banco") or par.get("banco_nome") or "").strip()
        banco_nome = normalizar_rotulo_banco_erp(bid, banco_nome)[:200]
        valor_par = float(str(par.get("valor", "")).replace(",", ".").strip())
        ultima_forma = forma_nome[:200]
        ultima_banco = banco_nome[:200]
        ultima_fid = fid
        ultima_bid = bid

        obs_ant = str(doc.get("Observacoes") or "")[:1800]
        linha_obs = (
            f"Agro parc. {timezone.localtime(data_movimento).strftime('%d/%m/%Y')} "
            f"{forma_nome[:50]}/{banco_nome[:50]} R$ {valor_par:.2f}"
        )
        obs_nova = (obs_ant + (" | " if obs_ant else "") + linha_obs)[:2000]

        if despesa:
            vp_atual = float(_dec(doc.get("ValorPago")))
            saida = float(_dec(doc.get("Saida")))
            novo_vp = vp_atual + valor_par
            rest_apos = saida - novo_vp
            if rest_apos <= 0.02:
                novo_vp = saida
                quitado_final = True
                col.update_one(
                    {"_id": oid},
                    {
                        "$set": {
                            "Pago": True,
                            "DataPagamento": data_movimento,
                            "ValorPago": novo_vp,
                            "FormaPagamento": ultima_forma,
                            "FormaPagamentoID": ultima_fid,
                            "Banco": ultima_banco,
                            "BancoID": ultima_bid,
                            "Observacoes": obs_nova,
                            "LastUpdate": now,
                            "ModificadoPor": mod,
                        }
                    },
                )
            else:
                col.update_one(
                    {"_id": oid},
                    {
                        "$set": {
                            "Pago": False,
                            "ValorPago": novo_vp,
                            "FormaPagamento": ultima_forma,
                            "FormaPagamentoID": ultima_fid,
                            "Banco": ultima_banco,
                            "BancoID": ultima_bid,
                            "Observacoes": obs_nova,
                            "LastUpdate": now,
                            "ModificadoPor": mod,
                        }
                    },
                )
        else:
            rec = float(_dec(doc.get("Recebido")))
            vp_atual = float(_dec(doc.get("ValorPago")))
            entrada = float(_dec(doc.get("Entrada")))
            novo_vp = vp_atual + valor_par
            rest_apos = entrada - rec - novo_vp
            if rest_apos <= 0.02:
                quitado_final = True
                col.update_one(
                    {"_id": oid},
                    {
                        "$set": {
                            "Pago": True,
                            "DataPagamento": data_movimento,
                            "ValorPago": novo_vp,
                            "FormaPagamento": ultima_forma,
                            "FormaPagamentoID": ultima_fid,
                            "Banco": ultima_banco,
                            "BancoID": ultima_bid,
                            "Observacoes": obs_nova,
                            "LastUpdate": now,
                            "ModificadoPor": mod,
                        }
                    },
                )
            else:
                col.update_one(
                    {"_id": oid},
                    {
                        "$set": {
                            "Pago": False,
                            "ValorPago": novo_vp,
                            "FormaPagamento": ultima_forma,
                            "FormaPagamentoID": ultima_fid,
                            "Banco": ultima_banco,
                            "BancoID": ultima_bid,
                            "Observacoes": obs_nova,
                            "LastUpdate": now,
                            "ModificadoPor": mod,
                        }
                    },
                )

    _sanear_dto_lancamento_ids_erp_string(col, oid)
    return {"ok": True, "id": str(oid), "erro": None, "quitado": bool(quitado_final)}


# Padrão “contas de resultado”: exclui planos claramente patrimoniais (balanço).
# Ajuste fino via DRE_RESULTADO_EXCLUIR_REGEX_EXTRA no .env (padrões extras separados por ||).
#
# Não excluir por “empréstimo” sozinho: no ERP, “Pagamento de Emprestimos” e “Juros de …”
# entram em Despesas (resultado). Exclua só captação/passivo (entrada de empréstimo, etc.).
_DRE_REGEXES_EXCLUIR_CONTA_PATRIMONIAL: tuple[str, ...] = (
    r"(?i)\b(ativo|passivos?|patrim[oô]nio)(\s|$|/|-)",
    r"(?i)\b(circulante|imobilizado|investimentos)\b",
    r"(?i)\b(estoques?|estoque)\b",
    r"(?i)\b(caixa e bancos|bancos)\b",
    r"(?i)\b(duplicatas)\b",
    r"(?i)\b(contas a pagar|contas a receber)\b",
    r"(?i)\bentrada\s+de\s+empr[eé]stimos?\b",
    r"(?i)\b(empr[eé]stimos?|financiamentos?)\s+a\s+(captar|obter|contratar)\b",
    r"(?i)\b(aplica[cç][oõ]es?)\b",
    r"(?i)\b(realiz[aá]vel)\b",
    r"(?i)^\s*\(sem plano\)\s*$",
)


def _dre_regexes_excluir_resultado(extra: str | None) -> list[str]:
    out = list(_DRE_REGEXES_EXCLUIR_CONTA_PATRIMONIAL)
    raw = (extra or "").strip()
    if raw:
        for part in raw.split("||"):
            p = part.strip()
            if p:
                out.append(p)
    return out


def _sanitizar_nome_plano_dre(nome: str) -> str:
    """
    Unifica variações do mesmo texto de plano (espaços duplos, NBSP, zero-width, NFKC).
    Evita duas linhas ``Aluguel`` por diferença invisível no Mongo.
    """
    s = unicodedata.normalize("NFKC", nome or "")
    s = s.replace("\xa0", " ").replace("\u200b", "").replace("\ufeff", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _profundidade_codigo_plano(nome: str) -> int:
    """Quantidade de níveis no código inicial (ex.: ``1.1.1`` → 3; sem código → 0)."""
    c = _parse_codigo_hierarquia_plano(nome)
    if not c:
        return 0
    segs = _segmentos_codigo_plano(c)
    return len(segs) if segs else 0


def _normalizar_chave_plano_dre(nome: str) -> str:
    """
    Remove prefixo de código do plano (ex.: ``1.1.1 —``, ``2.2.1 -``) para agrupar a mesma conta
    gravada com textos diferentes no Mongo (comum no ERP: hierarquia + nome curto).
    """
    s = _sanitizar_nome_plano_dre(nome)
    if not s:
        return "(sem plano)"
    # Hífen ASCII e Unicode en dash (U+2013) / em dash (U+2014)
    s2 = re.sub(r"^\s*\d+(?:\.\d+)*\s*[\u2013\u2014\-]\s*", "", s, count=1)
    s2 = s2.strip()
    return s2 if s2 else "(sem plano)"


def _mesclar_por_plano_normalizado(
    por_plano: dict[str, dict[str, Decimal]],
) -> dict[str, dict[str, Decimal]]:
    """
    Agrupa por nome sem prefixo de código. Se várias linhas caem na mesma chave e há código
    numérico no nome, mantém só as de **maior profundidade** (folha), para não somar pai + filho
    quando o ERP grava ``1.1 - Vendas Pdv`` e ``1.1.1 - Vendas Pdv`` (ambos viram ``Vendas Pdv``).
    """
    grupos: dict[str, list[tuple[str, dict[str, Decimal]]]] = defaultdict(list)
    for nome, row in por_plano.items():
        k = _normalizar_chave_plano_dre(nome)
        grupos[k].append((nome, row))

    profundidade_ok = getattr(settings, "DRE_MESCLAR_PLANO_PROFUNDIDADE_MAX", True)
    out: dict[str, dict[str, Decimal]] = {}
    for k, items in grupos.items():
        selecionados = items
        if profundidade_ok and len(items) > 1:
            depths = [_profundidade_codigo_plano(nome) for nome, _ in items]
            md = max(depths) if depths else 0
            if md > 0:
                selecionados = [(n, r) for (n, r), d in zip(items, depths) if d == md]
        slot = out.setdefault(k, {"despesa": Decimal("0"), "receita": Decimal("0")})
        for _, row in selecionados:
            slot["despesa"] += row["despesa"]
            slot["receita"] += row["receita"]
    return out


def _parse_codigo_hierarquia_plano(nome: str) -> str | None:
    """Extrai o código inicial tipo ``1``, ``1.1``, ``1.01``, ``1.1.1`` do nome do plano (PlanoDeConta)."""
    s = _sanitizar_nome_plano_dre(nome)
    if not s:
        return None
    m = re.match(r"^\s*(\d+(?:\.\d+)*)\b", s)
    return m.group(1) if m else None


def _segmentos_codigo_plano(codigo: str) -> list[int] | None:
    """Segmentos numéricos do código (``1.01`` e ``1.1`` → ``[1, 1]``)."""
    if not codigo or not str(codigo).strip():
        return None
    out: list[int] = []
    for p in str(codigo).split("."):
        p = p.strip()
        if not p.isdigit():
            return None
        out.append(int(p))
    return out if out else None


def _eh_ancestral_estrito(cod_ance: str, cod_desc: str) -> bool:
    """True se ``cod_ance`` é nível acima de ``cod_desc`` (ex.: 1.1 é ancestral de 1.1.1)."""
    pa = _segmentos_codigo_plano(cod_ance)
    pb = _segmentos_codigo_plano(cod_desc)
    if not pa or not pb or len(pa) >= len(pb):
        return False
    return pb[: len(pa)] == pa


def _filtrar_planos_pais_dre(por_plano: dict[str, dict[str, Decimal]]) -> dict[str, dict[str, Decimal]]:
    """
    Alinha ao DRE do ERP por **código** (ex.: ``2`` > ``2.1`` > ``2.1.1`` > ``2.1.1.1`` > ``2.1.1.1.1``).

    No ERP o **pai** já agrega os **filhos** na apresentação; nos lançamentos (Mongo) o mesmo período
    pode ter movimento no pai **e** no filho (ex.: Salários e Adiantamento). Somar os dois **duplica**.

    Regra: **receita** e **despesa** são tratadas à parte — só entram no conjunto hierárquico contas
    com valor > 0 naquela dimensão; zera-se o valor na dimensão do **pai** se existir **qualquer**
    descendente estrito com valor na mesma dimensão. Sem código numérico no início do nome, não há
    árvore (linha mantida).

    Segmentos numéricos: ``1.01`` e ``1.1`` equivalem (``int`` por parte).
    """
    def _segmentos_com_valor(dim: str) -> set[tuple[int, ...]]:
        s: set[tuple[int, ...]] = set()
        for nome, row in por_plano.items():
            if dim == "receita" and row["receita"] <= 0:
                continue
            if dim == "despesa" and row["despesa"] <= 0:
                continue
            c = _parse_codigo_hierarquia_plano(nome)
            if not c:
                continue
            segs = _segmentos_codigo_plano(c)
            if segs is not None:
                s.add(tuple(segs))
        return s

    conj_rec = _segmentos_com_valor("receita")
    conj_desp = _segmentos_com_valor("despesa")

    def _tem_filho_no_conjunto(pa: list[int], conj: set[tuple[int, ...]]) -> bool:
        for pb in conj:
            if len(pb) <= len(pa):
                continue
            if list(pb[: len(pa)]) == pa:
                return True
        return False

    out: dict[str, dict[str, Decimal]] = {}
    for nome, row in por_plano.items():
        c = _parse_codigo_hierarquia_plano(nome)
        if c is None:
            out[nome] = {"receita": row["receita"], "despesa": row["despesa"]}
            continue
        pa = _segmentos_codigo_plano(c)
        if pa is None:
            out[nome] = {"receita": row["receita"], "despesa": row["despesa"]}
            continue

        rec = row["receita"]
        des = row["despesa"]
        if rec > 0 and _tem_filho_no_conjunto(pa, conj_rec):
            rec = Decimal("0")
        if des > 0 and _tem_filho_no_conjunto(pa, conj_desp):
            des = Decimal("0")
        if rec == 0 and des == 0:
            continue
        out[nome] = {"receita": rec, "despesa": des}
    return out


def _dre_texto_base_nome(s: str) -> str:
    """Sanitizado, minúsculo, ``de`` opcional colapsado (comparação de rótulos de plano)."""
    t = _sanitizar_nome_plano_dre(s).casefold()
    return re.sub(r"\s+de\s+", " ", t, flags=re.I).strip()


def _dre_nomes_plano_equivalentes(a: str, b: str) -> bool:
    """Igualdade visual de nome de plano (espaços, caixa, ``de`` opcional entre palavras)."""
    return _dre_texto_base_nome(a) == _dre_texto_base_nome(b)


def _dre_remover_sem_codigo_se_nome_igual_plano_codificado(
    por_plano: dict[str, dict[str, Decimal]],
    normas_sem_prefixo_de_planos_com_codigo: set[str],
) -> dict[str, dict[str, Decimal]]:
    """
    Remove linha **sem** código no texto quando:

    1. O nome (normalizado) é **igual** ao de algum plano **com** código no período; ou
    2. Existe plano codificado cujo nome (sem prefixo) **continua** o da linha sem código
       (ex.: ``Compra de Mercadoria`` sombra de ``2.2.1.1 — Compra Mercadoria CN`` quando **não**
       há movimento na conta ``2.2.1 —`` no Mongo). Exige **≥ 2** palavras no rótulo sem código para
       evitar remover só ``Compra`` por engano.
    """
    if not getattr(settings, "DRE_ZERAR_SEM_CODIGO_REPETE_PAI", True):
        return por_plano
    remover: set[str] = set()
    for nome in por_plano:
        if _parse_codigo_hierarquia_plano(nome):
            continue
        alvo = _normalizar_chave_plano_dre(nome)
        if not alvo or alvo == "(sem plano)":
            continue
        ba = _dre_texto_base_nome(alvo)
        palavras = len(ba.split())
        hit = False
        for nc in normas_sem_prefixo_de_planos_com_codigo:
            if _dre_nomes_plano_equivalentes(nc, alvo):
                hit = True
                break
            if palavras >= 2:
                bc = _dre_texto_base_nome(nc)
                if bc.startswith(ba + " "):
                    hit = True
                    break
        if hit:
            remover.add(nome)
    if not remover:
        return por_plano
    return {k: v for k, v in por_plano.items() if k not in remover}


def _dre_fragmento_classificacao_colunas_erp() -> dict[str, Any]:
    """
    Alinha ao PDF de síntese do Venda ERP (colunas Receitas / Despesas):

    - Título **a pagar** (``Despesa`` true): só planos cujo código inicial é **2**, **10**, **11** ou **12**
      (ex.: ``2.5.1``, ``10 Outro``), que entram no **Total Despesas** do relatório.
    - Título **a receber** (``Despesa`` false): códigos **1** e **5** (receita operacional + passivos/entrada
      de empréstimo na coluna de receitas).

    Exclui da DRE lançamentos com plano **3**, **4**, etc. ou sem código no texto — que o ERP mostra fora
    dessas colunas e costumam inflar o total Agro (ex.: ~30k a mais no Centro).
    """
    frag_desp = {
        "$or": [
            {"PlanoDeConta": {"$regex": r"^\s*2(\.\d+)*\b"}},
            {"PlanoDeConta": {"$regex": r"^\s*10(\.\d+)*\b"}},
            {"PlanoDeConta": {"$regex": r"^\s*11(\.\d+)*\b"}},
            {"PlanoDeConta": {"$regex": r"^\s*12(\.\d+)*\b"}},
        ]
    }
    frag_rec = {
        "$or": [
            {"PlanoDeConta": {"$regex": r"^\s*1(\.\d+)*\b"}},
            {"PlanoDeConta": {"$regex": r"^\s*5(\.\d+)*\b"}},
        ]
    }
    return {
        "$or": [
            {"$and": [{"Despesa": True}, frag_desp]},
            {"$and": [{"Despesa": {"$ne": True}}, frag_rec]},
        ]
    }


def _filtro_empresa_dre(empresa: str | None, empresa_id: str | None) -> dict[str, Any] | None:
    """
    Restringe por ``Empresa`` e/ou ``EmpresaID`` (filtro de loja, como no ERP).

    Quando ambos existem, usa ``$or``: basta bater o ID **ou** o nome (evita zero linhas quando
    o texto em ``Empresa`` difere do cadastro Django mas o ``EmpresaID`` coincide).
    """
    eid = (empresa_id or "").strip()
    en = (empresa or "").strip()
    parts: list[dict[str, Any]] = []
    if eid:
        id_conds: list[dict[str, Any]] = [{"EmpresaID": eid}]
        if eid.isdigit():
            id_conds.append({"EmpresaID": int(eid)})
        parts.append(id_conds[0] if len(id_conds) == 1 else {"$or": id_conds})
    if en:
        tokens = [re.escape(t) for t in re.split(r"\s+", en.strip()) if t]
        if tokens:
            rpat = r"^\s*" + r"\s+".join(tokens) + r"\s*$"
            parts.append({"Empresa": {"$regex": rpat, "$options": "i"}})
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return {"$or": parts}


def _mongo_filtro_jsonish_for_log(obj: Any) -> Any:
    """Serializa filtro Mongo para log (datetime, ObjectId, aninhados)."""
    if obj is None:
        return None
    if isinstance(obj, datetime):
        try:
            if timezone.is_naive(obj):
                obj = timezone.make_aware(obj, timezone.get_current_timezone())
            return timezone.localtime(obj).isoformat()
        except Exception:
            return obj.isoformat(sep=" ")
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): _mongo_filtro_jsonish_for_log(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_mongo_filtro_jsonish_for_log(v) for v in obj[:80]]
    return obj


def _dre_distinct_sample_strings(
    col,
    query: dict[str, Any],
    field: str,
    *,
    limit: int = 30,
) -> list[str]:
    """Valores distintos não vazios de ``field`` no conjunto ``query`` (amostra ordenada)."""
    out: list[str] = []
    try:
        pipe: list[dict[str, Any]] = [
            {"$match": query},
            {"$group": {"_id": f"${field}"}},
            {"$match": {"_id": {"$nin": [None, ""]}}},
            {"$sort": {"_id": 1}},
            {"$limit": limit},
        ]
        for r in col.aggregate(pipe):
            v = r.get("_id")
            if v is None:
                continue
            s = str(v).strip()
            if s:
                out.append(s[:220])
    except Exception as exc:
        logger.exception("_dre_distinct_sample_strings: %s", exc)
    return out


def debug_resumo_mongo_lens(
    db,
    *,
    data_de: date,
    data_ate: date,
    por: str = "competencia",
    filtro_contas: str = "resultado",
    regex_excluir_extra: str | None = None,
    empresa: str | None = None,
    empresa_id: str | None = None,
) -> dict[str, Any]:
    """
    Contagens encadeadas para diagnosticar resumo gerencial zerado (mesma lógica de filtro do DRE).

    - ``total_documentos_periodo``: só intervalo de data (DataCompetencia / Vencimento / Pagamento).
    - ``total_documentos_empresa``: período + filtro de loja (nome e/ou EmpresaID, ``$or``).
    - ``total_documentos_resultado``: período + loja + regras de plano do modo ``resultado``/``resultado_erp``/``todas``.
    """
    empty = {
        "total_documentos_periodo": 0,
        "total_documentos_empresa": 0,
        "total_documentos_resultado": 0,
        "exemplos_empresa_distinta": [],
        "exemplos_planos_conta": [],
        "ok": False,
        "erro": "Mongo indisponível",
        "campo_data": None,
    }
    if db is None:
        return empty

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_de, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(data_ate, dtime.max), tz)
    modo_por = (por or "").strip().lower()
    if modo_por == "vencimento":
        campo = "DataVencimento"
    elif modo_por == "pagamento":
        campo = "DataPagamento"
    else:
        campo = "DataCompetencia"

    fc = (filtro_contas or "resultado").strip().lower()
    if fc not in ("resultado", "resultado_erp", "todas"):
        fc = "resultado"

    if campo == "DataPagamento":
        match_data: dict[str, Any] = {
            "DataPagamento": {"$gte": ini, "$lte": fim, "$gt": _SENTINEL},
        }
    else:
        match_data = {campo: {"$gte": ini, "$lte": fim}}

    em_filtro = _filtro_empresa_dre(empresa, empresa_id)
    if em_filtro is not None:
        match_empresa: dict[str, Any] = {"$and": [match_data, em_filtro]}
    else:
        match_empresa = match_data

    if fc in ("resultado", "resultado_erp"):
        pats = _dre_regexes_excluir_resultado(regex_excluir_extra)
        match_resultado_base: dict[str, Any] = {
            "$and": [
                match_data,
                {"PlanoDeConta": {"$regex": r"\S"}},
                {"$nor": [{"PlanoDeConta": {"$regex": pat}} for pat in pats]},
            ]
        }
        if fc == "resultado_erp":
            match_resultado_base["$and"].append(_dre_fragmento_classificacao_colunas_erp())
    else:
        match_resultado_base = match_data

    if em_filtro is not None:
        if isinstance(match_resultado_base, dict) and "$and" in match_resultado_base:
            match_final = {**match_resultado_base, "$and": [*match_resultado_base["$and"], em_filtro]}
        else:
            match_final = {"$and": [match_resultado_base, em_filtro]}
    else:
        match_final = match_resultado_base

    col = db[COL_DTO_LANCAMENTO]
    try:
        n_periodo = col.count_documents(match_data)
        n_empresa = col.count_documents(match_empresa)
        n_resultado = col.count_documents(match_final)
    except Exception as exc:
        logger.exception("debug_resumo_mongo_lens count: %s", exc)
        return {
            "total_documentos_periodo": 0,
            "total_documentos_empresa": 0,
            "total_documentos_resultado": 0,
            "exemplos_empresa_distinta": [],
            "exemplos_planos_conta": [],
            "ok": False,
            "erro": str(exc)[:400],
            "campo_data": campo,
        }

    exemplos_empresa = _dre_distinct_sample_strings(col, match_data, "Empresa", limit=30)
    exemplos_planos = _dre_distinct_sample_strings(col, match_empresa, "PlanoDeConta", limit=30)

    return {
        "total_documentos_periodo": n_periodo,
        "total_documentos_empresa": n_empresa,
        "total_documentos_resultado": n_resultado,
        "exemplos_empresa_distinta": exemplos_empresa,
        "exemplos_planos_conta": exemplos_planos,
        "ok": True,
        "erro": None,
        "campo_data": campo,
        "filtro_contas": fc,
        "empresa_filtro_nome": (empresa or "").strip() or None,
        "empresa_id_filtro": (empresa_id or "").strip() or None,
    }


def dre_resumo_simples_mongo(
    db,
    *,
    data_de: date,
    data_ate: date,
    por: str = "competencia",
    valor: str = "bruto",
    filtro_contas: str = "resultado",
    regex_excluir_extra: str | None = None,
    empresa: str | None = None,
    empresa_id: str | None = None,
    diagnostico: bool = False,
) -> dict[str, Any]:
    """
    Base simples para DRE: totais por PlanoDeConta no período (lançamentos DtoLancamento).

    ``por`` (data base do filtro):
      - ``competencia``: DataCompetencia
      - ``vencimento``: DataVencimento
      - ``pagamento``: DataPagamento (só títulos com pagamento efetivo no período; alinha ao ERP em "Filtrar por: Data Pagamento")

    ``valor``:
      - ``bruto``: receita = ``Entrada``, despesa = ``Saida`` (valor do título).
      - ``realizado``: receita = min(``Entrada``, ``Recebido``+``ValorPago``) quando ``Entrada``>0
        (evita dupla contagem se o ERP repetir o valor em ambos os campos); despesa = ``ValorPago``.

    filtro_contas:
      - ``resultado`` (default): exclui planos que parecem patrimoniais/balanço (ver regexes no código).
      - ``resultado_erp``: como ``resultado`` + só contas nas colunas do PDF (despesa: cód. 2/10/11/12;
        receita: 1/5). Use para bater o **Total Despesas / Receitas** do síntese.
      - ``todas``: sem filtro por nome de plano (comportamento antigo).

    ``empresa`` / ``empresa_id`` (opcional): filtram lançamentos pela loja cadastrada no título,
    alinhado ao relatório do ERP por empresa. Nome: match exato ignorando maiúsculas e com espaços
    flexíveis entre as palavras.
    """
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível", "linhas": [], "totais": {}}
    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_de, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(data_ate, dtime.max), tz)
    modo_por = (por or "").strip().lower()
    if modo_por == "vencimento":
        campo = "DataVencimento"
    elif modo_por == "pagamento":
        campo = "DataPagamento"
    else:
        campo = "DataCompetencia"
    col = db[COL_DTO_LANCAMENTO]
    linhas: list[dict[str, Any]] = []
    fc = (filtro_contas or "resultado").strip().lower()
    if fc not in ("resultado", "resultado_erp", "todas"):
        fc = "resultado"
    modo_valor = (valor or "bruto").strip().lower()
    if modo_valor not in ("bruto", "realizado"):
        modo_valor = "bruto"
    if modo_valor == "realizado":
        soma_receita_expr = _mongo_expr_valor_realizado_receita()
        soma_despesa_expr: dict[str, Any] = {"$ifNull": ["$ValorPago", 0]}
    else:
        soma_receita_expr = {"$ifNull": ["$Entrada", 0]}
        soma_despesa_expr = {"$ifNull": ["$Saida", 0]}
    try:
        if campo == "DataPagamento":
            # Ignora datas sentinela (1/1/1) e títulos sem pagamento no período
            match_data = {
                "DataPagamento": {"$gte": ini, "$lte": fim, "$gt": _SENTINEL},
            }
        else:
            match_data = {campo: {"$gte": ini, "$lte": fim}}
        if fc in ("resultado", "resultado_erp"):
            pats = _dre_regexes_excluir_resultado(regex_excluir_extra)
            match0 = {
                "$and": [
                    match_data,
                    {"PlanoDeConta": {"$regex": r"\S"}},
                    {"$nor": [{"PlanoDeConta": {"$regex": pat}} for pat in pats]},
                ]
            }
            if fc == "resultado_erp":
                match0["$and"].append(_dre_fragmento_classificacao_colunas_erp())
        else:
            match0 = match_data

        em_filtro = _filtro_empresa_dre(empresa, empresa_id)
        if em_filtro is not None:
            if isinstance(match0, dict) and "$and" in match0:
                match0["$and"].append(em_filtro)
            else:
                match0 = {"$and": [match0, em_filtro]}

        if diagnostico:
            try:
                n_docs = col.count_documents(match0)
            except Exception as exc:
                n_docs = f"erro_count:{exc!s}"[:120]
            proj = {
                "Empresa": 1,
                "EmpresaID": 1,
                "PlanoDeConta": 1,
                "Despesa": 1,
                "Entrada": 1,
                "Saida": 1,
                "DataCompetencia": 1,
                "DataVencimento": 1,
                "DataPagamento": 1,
            }
            amostras: list[dict[str, Any]] = []
            try:
                for doc in col.find(match0, projection=proj).limit(5):
                    amostras.append(
                        {
                            "_id": str(doc.get("_id", "")),
                            "Empresa": doc.get("Empresa"),
                            "EmpresaID": doc.get("EmpresaID"),
                            "PlanoDeConta": (str(doc.get("PlanoDeConta") or ""))[:100],
                            "Despesa": doc.get("Despesa"),
                            "Entrada": doc.get("Entrada"),
                            "Saida": doc.get("Saida"),
                            "DataCompetencia": _mongo_filtro_jsonish_for_log(
                                doc.get("DataCompetencia")
                            ),
                            "DataVencimento": _mongo_filtro_jsonish_for_log(
                                doc.get("DataVencimento")
                            ),
                            "DataPagamento": _mongo_filtro_jsonish_for_log(
                                doc.get("DataPagamento")
                            ),
                        }
                    )
            except Exception as exc:
                amostras = [{"erro_amostra": str(exc)[:200]}]
            logger.info(
                "[FINANCEIRO_RESUMO_DIAG] collection=%s campo_data=%s intervalo_local=[%s .. %s] "
                "filtro_contas=%s valor_modo=%s empresa_filtro_nome=%r empresa_id_filtro=%r",
                COL_DTO_LANCAMENTO,
                campo,
                ini.isoformat(),
                fim.isoformat(),
                fc,
                modo_valor,
                (empresa or "").strip() or None,
                (empresa_id or "").strip() or None,
            )
            logger.info(
                "[FINANCEIRO_RESUMO_DIAG] match0_jsonish=%s",
                _mongo_filtro_jsonish_for_log(match0),
            )
            logger.info(
                "[FINANCEIRO_RESUMO_DIAG] documentos_no_match_antes_agregacao=%s amostras=%s",
                n_docs,
                amostras,
            )

        vl_linha = {"$cond": ["$Despesa", soma_despesa_expr, soma_receita_expr]}
        dedup_id = getattr(settings, "DRE_DEDUP_LANCAMENTO_ID", True)
        if dedup_id:
            pipe = [
                {"$match": match0},
                {"$addFields": {"vl_dre": vl_linha}},
                {"$addFields": {"dk_dre": _mongo_expr_dre_dedup_key()}},
                {
                    "$group": {
                        "_id": {
                            "dk": "$dk_dre",
                            "plano": {"$ifNull": ["$PlanoDeConta", ""]},
                            "desp": "$Despesa",
                        },
                        "soma": {"$max": "$vl_dre"},
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "plano": "$_id.plano",
                            "desp": "$_id.desp",
                        },
                        "soma": {"$sum": "$soma"},
                    }
                },
            ]
        else:
            pipe = [
                {"$match": match0},
                {
                    "$group": {
                        "_id": {
                            "plano": {"$ifNull": ["$PlanoDeConta", ""]},
                            "desp": "$Despesa",
                        },
                        "soma": {"$sum": vl_linha},
                    }
                },
            ]
        agg = list(col.aggregate(pipe))
        por_plano: dict[str, dict[str, Decimal]] = {}
        for r in agg:
            pid = r.get("_id") or {}
            nome_raw = str(pid.get("plano") or "").strip()
            nome = _sanitizar_nome_plano_dre(nome_raw) or "(sem plano)"
            is_desp = bool(pid.get("desp"))
            val = _dec(r.get("soma"))
            slot = por_plano.setdefault(nome, {"despesa": Decimal("0"), "receita": Decimal("0")})
            if is_desp:
                slot["despesa"] += val
            else:
                slot["receita"] += val
        if diagnostico:
            totais_plano_preview: list[tuple[str, float, float]] = []
            for nome, row in por_plano.items():
                totais_plano_preview.append(
                    (
                        (nome or "")[:120],
                        float(row["receita"].quantize(Decimal("0.01"))),
                        float(row["despesa"].quantize(Decimal("0.01"))),
                    )
                )
            totais_plano_preview.sort(
                key=lambda t: max(t[1], t[2]),
                reverse=True,
            )
            logger.info(
                "[FINANCEIRO_RESUMO_DIAG] apos_group_plano n_planos=%s totais_parciais_top15=%s",
                len(por_plano),
                totais_plano_preview[:15],
            )
        normas_codificadas_pre_pais = {
            _normalizar_chave_plano_dre(nome)
            for nome in por_plano
            if _parse_codigo_hierarquia_plano(nome)
        }
        if getattr(settings, "DRE_EXCLUIR_PLANOS_PAI_HIERARQUIA", True):
            por_plano = _filtrar_planos_pais_dre(por_plano)
        por_plano = _dre_remover_sem_codigo_se_nome_igual_plano_codificado(
            por_plano,
            normas_codificadas_pre_pais,
        )
        if getattr(settings, "DRE_MESCLAR_PLANO_PREFIXO_CODIGO", True):
            por_plano = _mesclar_por_plano_normalizado(por_plano)
        tot_rec = sum((row["receita"] for row in por_plano.values()), Decimal("0"))
        tot_desp = sum((row["despesa"] for row in por_plano.values()), Decimal("0"))
        for nome in sorted(por_plano.keys(), key=lambda x: x.lower()):
            row = por_plano[nome]
            d = row["despesa"]
            r_ = row["receita"]
            linhas.append(
                {
                    "plano": nome,
                    "despesa": float(d.quantize(Decimal("0.01"))),
                    "receita": float(r_.quantize(Decimal("0.01"))),
                    "saldo": float((r_ - d).quantize(Decimal("0.01"))),
                }
            )
    except Exception as exc:
        logger.exception("dre_resumo_simples_mongo: %s", exc)
        return {"ok": False, "erro": str(exc)[:300], "linhas": [], "totais": {}}

    ef = (empresa or "").strip()
    eidf = (empresa_id or "").strip()
    out_tot = {
        "total_despesa": float(tot_desp.quantize(Decimal("0.01"))),
        "total_receita": float(tot_rec.quantize(Decimal("0.01"))),
        "resultado": float((tot_rec - tot_desp).quantize(Decimal("0.01"))),
    }
    if diagnostico:
        logger.info(
            "[FINANCEIRO_RESUMO_DIAG] dre_saida_final n_linhas_dre=%s totais=%s",
            len(linhas),
            out_tot,
        )
    return {
        "ok": True,
        "campo_data": campo,
        "valor_modo": modo_valor,
        "filtro_contas": fc,
        "empresa_filtro": ef or None,
        "empresa_id_filtro": eidf or None,
        "periodo": {"de": data_de.isoformat(), "ate": data_ate.isoformat()},
        "linhas": linhas,
        "totais": out_tot,
        "dedup_assinatura_sem_id": getattr(settings, "DRE_DEDUP_ASSINATURA_SEM_ID", True),
    }


# Campos usuais do DtoLancamento (WL / Venda ERP) enviados ao POST de integração — evita payload gigante.
_ERP_DOC_KEYS_EXPORT = frozenset(
    {
        "Id",
        "Despesa",
        "Cliente",
        "ClienteID",
        "Empresa",
        "EmpresaID",
        "NumeroDocumento",
        "NumeroParcela",
        "Descricao",
        "Observacoes",
        "PlanoDeConta",
        "PlanoDeContaID",
        "Saida",
        "Entrada",
        "ValorPago",
        "Recebido",
        "Pago",
        "DataVencimento",
        "DataPagamento",
        "DataCompetencia",
        "DataFluxo",
        "DataVencimentoOriginal",
        "LastUpdate",
        "FormaPagamento",
        "FormaPagamentoID",
        "Banco",
        "BancoID",
        "LancamentoGrupo",
        "LancamentoGrupoID",
        "NumeroLancamento",
        "LancamentoID",
        "CategoriaLancamento",
        "CentroDeCusto",
        "CentroDeCustoID",
        "ValorLiquido",
        "SaldoAtual",
        "CriadoPor",
        "ModificadoPor",
    }
)


def _json_safe_erp_value(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        s = _serializar_dt(v)
        return s
    if isinstance(v, (list, tuple)):
        return [_json_safe_erp_value(x) for x in v[:300]]
    if isinstance(v, dict):
        return {str(k)[:120]: _json_safe_erp_value(val) for k, val in list(v.items())[:80]}
    return str(v)[:500]


def lancamento_doc_subset_erp(doc: dict) -> dict[str, Any]:
    """Recorte JSON-safe do documento Mongo para o ERP mapear baixa / inclusão."""
    out: dict[str, Any] = {"_id_mongo": str(doc.get("_id", ""))}
    for k in _ERP_DOC_KEYS_EXPORT:
        if k not in doc:
            continue
        out[k] = _json_safe_erp_value(doc[k])
    return out


def lancamentos_carregar_por_ids(db, ids: list[str]) -> list[dict]:
    """Recarrega DtoLancamento após gravação, na ordem dos ids solicitados."""
    if db is None:
        return []
    col = db[COL_DTO_LANCAMENTO]
    out: list[dict] = []
    for sid in ids or []:
        try:
            oid = ObjectId(str(sid).strip())
        except Exception:
            continue
        doc = col.find_one({"_id": oid})
        if doc:
            out.append(doc)
    return out


def _payload_indica_baixa_parcial_lancamentos(payload_ui: dict | None) -> bool:
    """POST da tela de baixa parcial traz ``parcelas``; baixa total não."""
    if not payload_ui or not isinstance(payload_ui, dict):
        return False
    p = payload_ui.get("parcelas")
    return isinstance(p, list) and len(p) > 0


def normalizar_parcelas_baixa_ui_erp(parcelas: list[Any] | None) -> list[dict[str, Any]]:
    """Parcelas vindas do POST da tela → lista estável para o corpo enviado ao ERP (baixa parcial)."""
    out: list[dict[str, Any]] = []
    for p in parcelas or []:
        if not isinstance(p, dict):
            continue
        try:
            val = float(str(p.get("valor", "")).replace(",", ".").strip())
        except (ValueError, TypeError):
            continue
        if val <= 0:
            continue
        out.append(
            {
                "valor": round(val, 2),
                "forma_pagamento": str(p.get("forma_pagamento") or p.get("forma_nome") or "").strip()[:200],
                "forma_pagamento_id": str(p.get("forma_pagamento_id") or p.get("forma_id") or "").strip()[:80],
                "banco": str(p.get("banco") or p.get("banco_nome") or "").strip()[:200],
                "banco_id": str(p.get("banco_id") or "").strip()[:80],
            }
        )
        if len(out) >= 24:
            break
    return out


def _montar_vinculo_root_baixa_erp(t0: dict[str, Any], mongo_id: str | None) -> dict[str, Any]:
    """
    Chaves para o servidor WL amarrar pagamento(s) ao título certo (evita realizado “solto” no
    totalizador sem atualizar a linha do lançamento, ex. Previsto 3.140,94 / código 550).
    """
    v: dict[str, Any] = {}
    mid = (mongo_id or "").strip()
    if mid:
        v["mongodb_id"] = mid
    erp_id = t0.get("Id")
    if erp_id is not None and str(erp_id).strip() != "":
        v["id"] = erp_id
    nl = t0.get("NumeroLancamento")
    if nl is not None and str(nl).strip() != "":
        v["numero_lancamento"] = nl
    lid = t0.get("LancamentoID")
    if lid is not None and str(lid).strip() != "":
        v["lancamento_id"] = lid
    return v


def pagamentos_detalhe_formato_sisvale(
    parcelas_norm: list[dict[str, Any]],
    *,
    data_pagamento_iso: str | None,
    documento_base_titulo: str | None,
    quitar_lancamento: bool = False,
    vinculo_titulo: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Estrutura análoga à aba **Pagamentos** do cadastro de lançamento no SisVale/Venda (linhas de
    pagamento relacionadas). O endpoint WL pode ignorar campos que não mapear — ``titulos`` segue
    sendo o snapshot do DtoLancamento.
    """
    base = (documento_base_titulo or "").strip()[:80]
    data_s = (data_pagamento_iso or "").strip()[:10] or None
    rows: list[dict[str, Any]] = []
    vin = vinculo_titulo or {}
    vid = vin.get("id")
    vid_s = str(vid).strip() if vid is not None and str(vid).strip() else ""
    for idx, p in enumerate(parcelas_norm or [], start=1):
        val = float(p.get("valor") or 0)
        if val <= 0:
            continue
        doc_linha = f"{base}-P{idx}" if base else f"P{idx}"
        row: dict[str, Any] = {
            "valor": round(val, 2),
            "multa": 0.0,
            "juros_percent": 0,
            "juros": 0.0,
            "data_pagamento": data_s,
            "forma_pagamento": str(p.get("forma_pagamento") or "")[:200],
            "forma_pagamento_id": str(p.get("forma_pagamento_id") or "")[:80],
            "conta_bancaria": str(p.get("banco") or "")[:200],
            "conta_bancaria_id": str(p.get("banco_id") or "")[:80],
            "banco": str(p.get("banco") or "")[:200],
            "banco_id": str(p.get("banco_id") or "")[:80],
            "documento": doc_linha[:120],
            "total": round(val, 2),
            "quitar_lancamento": bool(quitar_lancamento),
        }
        if vid_s:
            row["lancamento_vinculo_id"] = vid_s
            row["dto_lancamento_id"] = vid_s
            row["LancamentoPaiId"] = vid_s
        if vin.get("numero_lancamento") is not None and str(vin.get("numero_lancamento")).strip() != "":
            row["numero_lancamento_pai"] = vin["numero_lancamento"]
        if vin.get("lancamento_id") is not None and str(vin.get("lancamento_id")).strip() != "":
            row["lancamento_codigo"] = vin["lancamento_id"]
        if vin.get("mongodb_id"):
            row["mongodb_id_titulo"] = vin["mongodb_id"]
        rows.append(row)
    return rows


def lancamento_titulos_payload_baixa_erp(
    db,
    mongo_ids: list[str],
    payload_ui: dict | None = None,
) -> list[dict[str, Any]]:
    """
    Snapshot pós-baixa para o ERP, com saldo/realizado explícitos e fallback de LancamentoID.

    Alguns ambientes SisVale/Venda só exibem ``NumeroLancamento`` na grade; se ``LancamentoID`` vier
    vazio no Mongo, repetimos o número para facilitar o match no endpoint de baixa.

    Em **baixa parcial** de conta a pagar, há builds que não atualizam a coluna "Realizado" só com
    ``ValorPago``; espelhamos o acumulado pago em ``Recebido`` no JSON (sem alterar o Mongo) e
    fixamos ``SaldoAtual`` como saldo em aberto para o título.
    """
    parcial = _payload_indica_baixa_parcial_lancamentos(payload_ui)
    titulos: list[dict[str, Any]] = []
    for doc in lancamentos_carregar_por_ids(db, mongo_ids):
        sub = lancamento_doc_subset_erp(doc)
        desp = bool(doc.get("Despesa"))
        try:
            if desp:
                rest = float(_restante_a_pagar(doc))
                sub["SaldoAberto"] = round(rest, 2)
                real = float(_dec(doc.get("ValorPago")))
                saida = float(_dec(doc.get("Saida")))
                sub["ValorPago"] = round(real, 2)
                sub["Saida"] = round(saida, 2)
                sub["SaldoAtual"] = round(max(0.0, rest), 2)
                if parcial and real > 0.005 and not bool(doc.get("Pago")):
                    sub["Recebido"] = round(real, 2)
            else:
                sub["SaldoAberto"] = round(float(_restante_a_receber(doc)), 2)
                real = float(
                    _valor_realizado_receita_dec(
                        _dec(doc.get("Entrada")),
                        _dec(doc.get("Recebido")),
                        _dec(doc.get("ValorPago")),
                    )
                )
            sub["ValorRealizadoAgro"] = round(real, 2)
        except Exception:
            pass
        lid_s = str(sub.get("LancamentoID") or "").strip()
        if not lid_s and doc.get("NumeroLancamento") is not None:
            sub["LancamentoID"] = _json_safe_erp_value(doc.get("NumeroLancamento"))
        titulos.append(sub)
    return titulos


def montar_payload_erp_baixa(
    db,
    mongo_ids: list[str],
    despesa: bool,
    payload_ui: dict,
    *,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Corpo sugerido para VENDA_ERP_API_FINANCEIRO_BAIXA_PATH.
    Mantém ``ids`` / ``payload`` / ``tipo`` (compatível com integrações antigas) e acrescenta ``titulos``
    com snapshot pós-baixa (inclui Id/LancamentoID do ERP quando existirem no Mongo).
    """
    titulos = lancamento_titulos_payload_baixa_erp(db, mongo_ids, payload_ui)
    tipo = "pagar" if despesa else "receber"
    out: dict[str, Any] = {
        "ids": mongo_ids,
        "mongodb_ids": mongo_ids,
        "tipo": tipo,
        "despesa": despesa,
        "payload": payload_ui or {},
        "origem": "agro_consulta",
        "titulos": titulos,
    }
    vinculo_root: dict[str, Any] = {}
    if titulos:
        mid0 = str(mongo_ids[0]).strip() if mongo_ids else ""
        vinculo_root = _montar_vinculo_root_baixa_erp(titulos[0], mid0 or None)
        if vinculo_root:
            out["vinculo_lancamento"] = vinculo_root
            eid = vinculo_root.get("id")
            if eid is not None and str(eid).strip():
                sid = str(eid).strip()
                out["lancamentoId"] = sid
                out["dtoLancamentoId"] = sid
    if _payload_indica_baixa_parcial_lancamentos(payload_ui):
        plist = normalizar_parcelas_baixa_ui_erp(
            payload_ui.get("parcelas") if isinstance(payload_ui, dict) else None
        )
        out["baixa_parcial"] = True
        out["parcelas_baixa"] = plist
        docs_head = lancamentos_carregar_por_ids(db, mongo_ids[:1])
        num_doc = str((docs_head[0] or {}).get("NumeroDocumento") or "").strip()[:80] if docs_head else ""
        data_iso = (
            str(payload_ui.get("data_movimento") or "").strip()[:10]
            if isinstance(payload_ui, dict)
            else ""
        ) or None
        pag = pagamentos_detalhe_formato_sisvale(
            plist,
            data_pagamento_iso=data_iso,
            documento_base_titulo=num_doc or None,
            quitar_lancamento=False,
            vinculo_titulo=vinculo_root if vinculo_root else None,
        )
        if pag:
            out["pagamentos"] = pag
            out["pagamentos_relacionados"] = pag
    if extras:
        for k, v in extras.items():
            if v is not None:
                out[k] = v
    return out


def montar_payload_erp_lancamentos_novos(
    db,
    mongo_ids: list[str],
    lote: str,
    despesa: bool,
) -> dict[str, Any]:
    """Corpo sugerido para VENDA_ERP_API_FINANCEIRO_LANCAMENTO_PATH após inclusão manual no Agro."""
    titulos = [lancamento_doc_subset_erp(d) for d in lancamentos_carregar_por_ids(db, mongo_ids)]
    return {
        "origem": "agro_consulta",
        "operacao": "inclusao_manual",
        "lote": (lote or "")[:40],
        "despesa": despesa,
        "mongodb_ids": mongo_ids,
        "titulos": titulos,
    }


_SUGESTOES_CAMPOS = {
    "empresa": ("Empresa", "EmpresaID"),
    "cliente": ("Cliente", "ClienteID"),
    "plano": ("PlanoDeConta", "PlanoDeContaID"),
    "forma": ("FormaPagamento", "FormaPagamentoID"),
    "banco": ("Banco", "BancoID"),
    "grupo": ("LancamentoGrupo", "LancamentoGrupoID"),
    "centro": ("CentroDeCusto", "CentroDeCustoID"),
}

# Conta padrão do cadastro ERP (WL) — oferecida nas sugestões mesmo sem histórico no Mongo.
_BANCO_ADICIONAR_ERP_FIXO = {"nome": "ADICIONAR BANCO", "id": "6990cf726c4d856abaa670c6"}


def lancamentos_sugestoes_campo(
    db,
    campo: str,
    q: str | None = None,
    limit: int = 30,
) -> list[dict[str, str]]:
    """Sugestões (nome + id) a partir de lançamentos existentes no Mongo — alinhado ao cadastro ERP."""
    out: list[dict[str, str]] = []
    if db is None or campo not in _SUGESTOES_CAMPOS:
        return out
    nome_f, id_f = _SUGESTOES_CAMPOS[campo]
    cap = 500 if campo == "plano" else 80
    lim = min(max(int(limit or 30), 1), cap)
    qq = (q or "").strip()
    try:
        col = db[COL_DTO_LANCAMENTO]
        if campo == "banco":
            conds = [
                {nome_f: {"$nin": [None, "", "ADICIONAR BANCO", "Adicionar banco"]}},
            ]
            if qq:
                conds.append({nome_f: {"$regex": re.escape(qq[:100]), "$options": "i"}})
            match: dict[str, Any] = {"$and": conds}
        elif qq:
            match = {nome_f: {"$regex": re.escape(qq[:100]), "$options": "i"}}
        else:
            match = {nome_f: {"$nin": [None, ""]}}
        pipe = [
            {"$match": match},
            {"$group": {"_id": {"n": f"${nome_f}", "i": f"${id_f}"}}},
            {"$limit": 200},
        ]
        seen: set[str] = set()
        for r in col.aggregate(pipe):
            i = r.get("_id") or {}
            nome = str(i.get("n") or "").strip()
            if not nome or nome.lower() in seen:
                continue
            seen.add(nome.lower())
            rid = i.get("i")
            out.append({"nome": nome, "id": str(rid) if rid is not None else ""})
            if len(out) >= lim:
                break
        out.sort(key=lambda x: x["nome"].lower())
        if campo == "banco":
            fid = str(_BANCO_ADICIONAR_ERP_FIXO.get("id") or "")
            if fid and not any(str(x.get("id") or "") == fid for x in out):
                out.insert(0, dict(_BANCO_ADICIONAR_ERP_FIXO))
    except Exception as exc:
        logger.exception("lancamentos_sugestoes_campo: %s", exc)
    return out[:lim]


def _obter_template_lancamento(db, despesa: bool) -> dict | None:
    doc = db[COL_DTO_LANCAMENTO].find_one({"Despesa": despesa})
    if not doc:
        doc = db[COL_DTO_LANCAMENTO].find_one({})
    return copy.deepcopy(doc) if doc else None


def _dt_naive_meia_noite_erp(d: date) -> datetime:
    """Mesmo padrão de hora visto nos documentos do WL (03:00 local, armazenado naive)."""
    return datetime.combine(d, dtime(3, 0, 0))


def inserir_lancamentos_manual_lote(
    db,
    *,
    despesa: bool,
    empresa_nome: str,
    empresa_id: str | None,
    pessoa_nome: str,
    pessoa_id: str | None,
    data_competencia: date,
    data_vencimento: date,
    banco_nome: str,
    banco_id: str | None,
    forma_nome: str,
    forma_id: str | None,
    grupo_nome: str | None,
    grupo_id: str | None,
    usuario_label: str,
    linhas: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Vários títulos compartilhando cabeçalho (empresa, favorecido, datas, banco; forma opcional);
    cada linha: plano de conta, valor, descrição, observação.
    """
    if db is None:
        return {"ok": False, "ids": [], "erros": [{"erro": "Mongo indisponível"}]}
    empresa_nome = (empresa_nome or "").strip()
    pessoa_nome = (pessoa_nome or "").strip()
    banco_nome = (banco_nome or "").strip()
    forma_nome = (forma_nome or "").strip()
    if not empresa_nome or not pessoa_nome or not banco_nome:
        return {
            "ok": False,
            "ids": [],
            "erros": [{"erro": "Preencha empresa, cliente/fornecedor e conta bancária."}],
        }
    linhas = [x for x in (linhas or []) if isinstance(x, dict)]
    if not linhas or len(linhas) > 50:
        return {"ok": False, "ids": [], "erros": [{"erro": "Informe de 1 a 50 linhas de detalhe."}]}

    tpl = _obter_template_lancamento(db, despesa)
    if not tpl:
        return {"ok": False, "ids": [], "erros": [{"erro": "Não há lançamento modelo no Mongo para clonar."}]}

    tpl.pop("_id", None)
    tpl["PagamentoRemessa"] = {}

    now = timezone.now()
    dc = _dt_naive_meia_noite_erp(data_competencia)
    dv = _dt_naive_meia_noite_erp(data_vencimento)
    lote = f"AG{secrets.token_hex(4).upper()}"
    user = (usuario_label or "Agro")[:200]

    # IDs originais vindos do ERP geralmente são strings; não converter para ObjectId
    eid = _financeiro_id_para_string(empresa_id)
    pid = _financeiro_id_para_string(pessoa_id)
    bid = _financeiro_id_para_string(banco_id)
    fid = _financeiro_id_para_string(forma_id)
    gid = _financeiro_id_para_string(grupo_id)

    col = db[COL_DTO_LANCAMENTO]
    inserted: list[str] = []
    erros: list[dict] = []

    for idx, ln in enumerate(linhas):
        n = idx + 1
        try:
            valor = float(str(ln.get("valor", "")).replace(",", ".").strip())
        except (ValueError, TypeError):
            erros.append({"linha": n, "erro": "Valor inválido"})
            continue
        if valor <= 0:
            erros.append({"linha": n, "erro": "Valor deve ser maior que zero"})
            continue
        plano_nome = (ln.get("plano_conta") or ln.get("plano_nome") or "").strip()
        plano_id_raw = ln.get("plano_conta_id") or ln.get("plano_id")
        if not plano_nome:
            erros.append({"linha": n, "erro": "Plano de conta obrigatório"})
            continue

        doc = copy.deepcopy(tpl)
        doc.pop("_id", None)
        doc["Despesa"] = bool(despesa)
        doc["Empresa"] = empresa_nome[:200]
        doc["EmpresaID"] = eid
        doc["Cliente"] = pessoa_nome[:300]
        doc["ClienteID"] = pid
        doc["Banco"] = banco_nome[:200]
        doc["BancoID"] = bid
        doc["FormaPagamento"] = forma_nome[:200]
        doc["FormaPagamentoID"] = fid
        if (grupo_nome or "").strip():
            doc["LancamentoGrupo"] = grupo_nome.strip()[:200]
            doc["LancamentoGrupoID"] = gid
        doc["PlanoDeConta"] = plano_nome[:200]
        # DtoLancamento (C#): PlanoDeContaID deve ser string no BSON, nunca ObjectId.
        doc["PlanoDeContaID"] = _financeiro_id_para_string(plano_id_raw) if plano_id_raw else ""
        doc["Descricao"] = (ln.get("descricao") or f"Lançamento manual {n}").strip()[:500]
        obs_linha = (ln.get("observacao") or ln.get("observacoes") or "").strip()
        doc["Observacoes"] = " | ".join(
            p for p in (obs_linha, f"Lote manual Agro {lote}") if p
        )[:2000]
        doc["DataCompetencia"] = dc
        doc["DataVencimento"] = dv
        doc["DataVencimentoOriginal"] = dv
        doc["DataFluxo"] = now
        doc["DataModificacao"] = now
        doc["LastUpdate"] = now
        doc["DataPagamento"] = _SENTINEL
        doc["Pago"] = False
        doc["NumeroDocumento"] = f"{lote}-{n:02d}"[:80]
        doc["NumeroParcela"] = idx
        doc["CriadoPor"] = user
        doc["ModificadoPor"] = f"{user} — inclusão manual em lote Agro"
        doc["ValorLiquido"] = 0.0
        doc["SaldoAtual"] = 0.0
        if despesa:
            doc["Saida"] = valor
            doc["Entrada"] = 0.0
            doc["ValorPago"] = 0.0
            doc["Recebido"] = 0.0
        else:
            doc["Entrada"] = valor
            doc["Saida"] = 0.0
            doc["Recebido"] = 0.0
            doc["ValorPago"] = 0.0
        _financeiro_doc_coerce_ids_oid_para_string(doc)
        try:
            ins = col.insert_one(doc)
            inserted.append(str(ins.inserted_id))
        except Exception as exc:
            logger.exception("insert manual lote linha %s", n)
            erros.append({"linha": n, "erro": str(exc)[:300]})

    return {
        "ok": len(inserted) == len(linhas) and not erros,
        "lote": lote,
        "ids": inserted,
        "erros": erros,
    }


def lancamentos_planos_distintos_no_filtro(
    db,
    *,
    despesa: bool,
    status: str,
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    texto: str | None = None,
    limit: int = 400,
) -> list[dict[str, str]]:
    """Nomes distintos de PlanoDeConta no conjunto filtrado (sem exclusão de planos)."""
    if db is None:
        return []
    q = lancamentos_montar_query_mongo(
        despesa=despesa,
        status=status,
        vencimento_de=vencimento_de,
        vencimento_ate=vencimento_ate,
        texto=texto,
    )
    lim = min(max(int(limit or 400), 1), 500)
    out: list[dict[str, str]] = []
    try:
        pipe = [
            {"$match": q},
            {"$group": {"_id": {"$ifNull": ["$PlanoDeConta", ""]}}},
            {"$sort": {"_id": 1}},
            {"$limit": lim},
        ]
        for r in db[COL_DTO_LANCAMENTO].aggregate(pipe):
            raw = r.get("_id")
            nome = str(raw).strip() if raw is not None else ""
            label = nome if nome else "(sem plano)"
            out.append({"nome": label})
    except Exception as exc:
        logger.exception("lancamentos_planos_distintos_no_filtro: %s", exc)
    return out


def financeiro_projecao_fluxo_diario(
    db,
    *,
    dias_media_vendas: int = 30,
    horizonte_dias: int = 60,
    incluir_media_vendas: bool = True,
) -> dict[str, Any]:
    """
    Projeção dia a dia: média de vendas (Mongo) + títulos a pagar/receber em aberto por vencimento.
    Saldo acumulado parte de zero (indicador de tendência, não saldo bancário).
    """
    from .mongo_vendas_util import (
        faixa_dia_mes_mongo,
        fatores_vendas_por_calendario,
        media_vendas_diaria_ultimos_n_dias,
    )

    if db is None:
        return {"erro": "Mongo indisponível", "dias": [], "meta": {}}

    hoje = timezone.localdate()
    horizonte_dias = max(1, min(int(horizonte_dias or 60), 120))
    fim = hoje + timedelta(days=horizonte_dias)
    dias_media_vendas = max(1, min(int(dias_media_vendas or 30), 365))

    media_dec = (
        media_vendas_diaria_ultimos_n_dias(db, dias_media_vendas) if incluir_media_vendas else Decimal("0")
    )
    media_f = float(media_dec)
    lookback_fatores = max(84, min(dias_media_vendas * 3, 126))
    fatores_cal = (
        fatores_vendas_por_calendario(db, dias_lookback=lookback_fatores) if incluir_media_vendas else None
    )

    q_pagar = lancamentos_montar_query_mongo(
        despesa=True,
        status="abertos",
        vencimento_de=hoje,
        vencimento_ate=fim,
    )
    q_receb = lancamentos_montar_query_mongo(
        despesa=False,
        status="abertos",
        vencimento_de=hoje,
        vencimento_ate=fim,
    )

    pagar_m: dict[date, dict[str, Any]] = {}
    rec_m: dict[date, dict[str, Any]] = {}

    try:
        col = db[COL_DTO_LANCAMENTO]
        for doc in col.find(q_pagar):
            dkey = _data_vencimento_local_doc(doc)
            if dkey is None or dkey < hoje or dkey > fim:
                continue
            r = _restante_a_pagar(doc)
            if r <= 0:
                continue
            e = pagar_m.setdefault(dkey, {"valor": Decimal("0"), "n": 0})
            e["valor"] += r
            e["n"] += 1
        for doc in col.find(q_receb):
            dkey = _data_vencimento_local_doc(doc)
            if dkey is None or dkey < hoje or dkey > fim:
                continue
            r = _restante_a_receber(doc)
            if r <= 0:
                continue
            e = rec_m.setdefault(dkey, {"valor": Decimal("0"), "n": 0})
            e["valor"] += r
            e["n"] += 1
    except Exception as exc:
        logger.exception("financeiro_projecao_fluxo_diario: %s", exc)
        return {"erro": str(exc)[:300], "dias": [], "meta": {}}

    dias_out: list[dict[str, Any]] = []
    cum = Decimal("0")
    d = hoje
    nomes_dow_curto = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    while d <= fim:
        vp = pagar_m.get(d, {"valor": Decimal("0"), "n": 0})
        vr = rec_m.get(d, {"valor": Decimal("0"), "n": 0})
        np = int(vp["n"])
        nr = int(vr["n"])
        wd = d.weekday()
        fx = faixa_dia_mes_mongo(d.day)
        if (
            incluir_media_vendas
            and media_f > 0
            and fatores_cal
            and fatores_cal.get("suficiente")
        ):
            r_w = float(fatores_cal["mult_dow"][wd])
            r_f = float(fatores_cal["mult_faixa"][fx])
            r_comb_f = max(0.45, min(1.65, r_w * r_f))
        else:
            r_w = 1.0
            r_f = 1.0
            r_comb_f = 1.0
        ent_media = (
            (media_dec * Decimal(str(r_comb_f))).quantize(Decimal("0.01"))
            if incluir_media_vendas
            else Decimal("0")
        )
        ent_titulos = vr["valor"]
        entradas = ent_media + ent_titulos
        saidas = vp["valor"]
        liquido = entradas - saidas
        cum += liquido

        motivos: list[dict[str, Any]] = []
        if incluir_media_vendas and media_f > 0:
            fx_labels = (fatores_cal or {}).get("faixa_labels") or {}
            fx_lab = fx_labels.get(fx, fx)
            nome_dow = nomes_dow_curto[wd]
            if fatores_cal and fatores_cal.get("suficiente"):
                det_mv = (
                    f"Base: média dos últimos {dias_media_vendas} dias = R$ {media_f:.2f}/dia (Mongo). "
                    f"Ajuste histórico (~{lookback_fatores} dias): {nome_dow} ×{r_w:.2f}, {fx_lab} ×{r_f:.2f} "
                    f"→ estimativa do dia R$ {float(ent_media):.2f}."
                )
                rot_mv = "Faturamento estimado (dia da semana + faixa do mês)"
            else:
                det_mv = (
                    f"Média dos últimos {dias_media_vendas} dias = R$ {media_f:.2f}/dia (Mongo). "
                    "Ajuste por dia da semana / faixa do mês não aplicado (histórico insuficiente ou média zero)."
                )
                rot_mv = f"Média de faturamento ({dias_media_vendas} dias)"
            motivos.append(
                {
                    "tipo": "media_vendas",
                    "rotulo": rot_mv,
                    "detalhe": det_mv,
                    "valor": round(float(ent_media), 2),
                    "sinal": 1,
                }
            )
        if nr > 0:
            motivos.append(
                {
                    "tipo": "receber",
                    "rotulo": "Contas a receber (vencimento nesta data)",
                    "detalhe": f"{nr} título(s) em aberto com saldo a receber nesta data.",
                    "valor": round(float(ent_titulos), 2),
                    "sinal": 1,
                }
            )
        if np > 0:
            motivos.append(
                {
                    "tipo": "pagar",
                    "rotulo": "Contas a pagar (vencimento nesta data)",
                    "detalhe": f"{np} título(s) em aberto com saldo a pagar nesta data.",
                    "valor": round(float(saidas), 2),
                    "sinal": -1,
                }
            )
        if not motivos:
            motivos.append(
                {
                    "tipo": "vazio",
                    "rotulo": "Sem títulos nesta data",
                    "detalhe": "Não há contas a pagar nem a receber em aberto com vencimento neste dia. "
                    + (
                        "A linha de média de vendas ainda entra como estimativa de entrada."
                        if incluir_media_vendas and media_f > 0
                        else "Sem média de vendas ativa ou média igual a zero."
                    ),
                    "valor": 0.0,
                    "sinal": 0,
                }
            )

        fx_labels_row = (fatores_cal or {}).get("faixa_labels") or {}
        dias_out.append(
            {
                "data": d.isoformat(),
                "entradas_previstas": round(float(entradas), 2),
                "saidas_previstas": round(float(saidas), 2),
                "liquido_dia": round(float(liquido), 2),
                "saldo_acumulado_proj": round(float(cum), 2),
                "n_a_receber": nr,
                "n_a_pagar": np,
                "media_vendas_diaria": round(media_f, 2) if incluir_media_vendas else 0.0,
                "media_vendas_ajustada_dia": round(float(ent_media), 2) if incluir_media_vendas else 0.0,
                "media_vendas_base": round(media_f, 2) if incluir_media_vendas else 0.0,
                "fator_dia_semana": round(r_w, 4),
                "fator_faixa_mes": round(r_f, 4),
                "fator_combinado": round(r_comb_f, 4),
                "dia_semana": nomes_dow_curto[wd],
                "faixa_mes": fx,
                "faixa_mes_label": fx_labels_row.get(fx, fx),
                "a_receber_titulos": round(float(ent_titulos), 2),
                "motivos": motivos,
            }
        )
        d += timedelta(days=1)

    fc_meta: dict[str, Any] | None = None
    if fatores_cal is not None:
        fc_meta = {
            "lookback_dias": fatores_cal.get("lookback_dias"),
            "suficiente": bool(fatores_cal.get("suficiente")),
            "mult_dow": fatores_cal.get("mult_dow"),
            "mult_faixa": fatores_cal.get("mult_faixa"),
            "faixa_labels": fatores_cal.get("faixa_labels"),
            "dia_semana_nomes_curto": fatores_cal.get("dia_semana_nomes_curto"),
            "n_amostras_dow": fatores_cal.get("n_amostras_dow"),
            "n_amostras_faixa": fatores_cal.get("n_amostras_faixa"),
        }
    meta = {
        "hoje": hoje.isoformat(),
        "fim": fim.isoformat(),
        "dias_media_vendas": dias_media_vendas,
        "horizonte_dias": horizonte_dias,
        "incluir_media_vendas": incluir_media_vendas,
        "media_vendas_diaria": round(media_f, 2),
        "lookback_fatores_calendario": lookback_fatores if incluir_media_vendas else None,
        "fatores_vendas_calendario": fc_meta,
        "aviso_saldo": "O saldo acumulado é uma projeção a partir de zero (não é saldo bancário). "
        "Serve para enxergar tendência de caixa a partir de vencimentos conhecidos e da média recente de vendas. "
        "As entradas por vendas usam a média base e um ajuste por dia da semana e por faixa do mês (início/meio/final), "
        "calculado sobre o histórico de faturamento no Mongo quando há dados suficientes.",
    }
    return {"dias": dias_out, "meta": meta}


def excluir_lancamento_mongo_agro(db, lancamento_id: str, usuario_label: str) -> dict[str, Any]:
    """Remove título no Mongo apenas quando permitido (manual Agro ou sem vínculo ERP, sem pagamento)."""
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    col = db[COL_DTO_LANCAMENTO]
    try:
        oid = ObjectId(str(lancamento_id).strip())
    except Exception:
        return {"ok": False, "erro": "ID inválido"}
    doc = col.find_one({"_id": oid})
    if not doc:
        return {"ok": False, "erro": "Lançamento não encontrado"}
    dp = doc.get("DataPagamento")
    quitado = bool(doc.get("Pago")) or _dt_efetiva(dp)
    mov = float(_dec(doc.get("ValorPago")))
    if not bool(doc.get("Despesa")):
        mov = float(
            _valor_realizado_receita_dec(
                _dec(doc.get("Entrada")),
                _dec(doc.get("Recebido")),
                _dec(doc.get("ValorPago")),
            )
        )
    if not _lancamento_pode_excluir_agro(doc, quitado, round(mov, 2)):
        return {
            "ok": False,
            "erro": "Exclusão não permitida: quitado, com movimento ou vinculado ao ERP (use o ERP para excluir).",
        }
    col.delete_one({"_id": oid})
    logger.info(
        "excluir_lancamento_mongo_agro: _id=%s por=%s",
        oid,
        (usuario_label or "")[:80],
    )
    return {"ok": True}


def atualizar_lancamento_mongo_agro(
    db,
    lancamento_id: str,
    patch: dict[str, Any],
    usuario_label: str,
) -> dict[str, Any]:
    """Atualiza campos cadastrais de um título em aberto (Mongo)."""
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}
    col = db[COL_DTO_LANCAMENTO]
    try:
        oid = ObjectId(str(lancamento_id).strip())
    except Exception:
        return {"ok": False, "erro": "ID inválido"}
    doc = col.find_one({"_id": oid})
    if not doc:
        return {"ok": False, "erro": "Lançamento não encontrado"}
    dp = doc.get("DataPagamento")
    quitado = bool(doc.get("Pago")) or _dt_efetiva(dp)
    if quitado:
        return {"ok": False, "erro": "Não é possível alterar título quitado."}
    despesa = bool(doc.get("Despesa"))
    mov = float(_dec(doc.get("ValorPago")))
    if not despesa:
        mov = float(
            _valor_realizado_receita_dec(
                _dec(doc.get("Entrada")),
                _dec(doc.get("Recebido")),
                _dec(doc.get("ValorPago")),
            )
        )
    mov_r = round(mov, 2)
    now = timezone.now()
    mod = ((usuario_label or "Agro")[:80] + " — edição lançamento Agro")[:200]
    set_doc: dict[str, Any] = {"LastUpdate": now, "ModificadoPor": mod, "DataModificacao": now}

    if "descricao" in patch:
        set_doc["Descricao"] = str(patch.get("descricao") or "").strip()[:500]
    if "cliente" in patch:
        set_doc["Cliente"] = str(patch.get("cliente") or "").strip()[:300]
    if "cliente_id" in patch and patch.get("cliente_id") is not None:
        set_doc["ClienteID"] = _financeiro_id_para_string(patch.get("cliente_id"))[:80]
    if "plano_conta" in patch:
        set_doc["PlanoDeConta"] = str(patch.get("plano_conta") or "").strip()[:200]
    if "plano_conta_id" in patch and patch.get("plano_conta_id") is not None:
        set_doc["PlanoDeContaID"] = _financeiro_id_para_string(patch.get("plano_conta_id"))
    dv = patch.get("data_vencimento")
    if dv is not None:
        ds = str(dv).strip()[:10]
        try:
            d = date.fromisoformat(ds)
        except ValueError:
            return {"ok": False, "erro": "data_vencimento inválida (AAAA-MM-DD)."}
        dtn = _dt_naive_meia_noite_erp(d)
        set_doc["DataVencimento"] = dtn
        set_doc["DataVencimentoOriginal"] = dtn
    if "valor_bruto" in patch and patch.get("valor_bruto") is not None:
        if mov_r > 0.02:
            return {"ok": False, "erro": "Não é possível alterar o valor com pagamento já registrado."}
        try:
            vb = float(str(patch.get("valor_bruto")).replace(",", ".").strip())
        except (TypeError, ValueError):
            return {"ok": False, "erro": "valor_bruto inválido."}
        if vb <= 0:
            return {"ok": False, "erro": "valor_bruto deve ser maior que zero."}
        if despesa:
            set_doc["Saida"] = vb
        else:
            set_doc["Entrada"] = vb
    if "banco" in patch:
        bn = str(patch.get("banco") or "").strip()
        if bn:
            bid_e = _financeiro_id_para_string(patch.get("banco_id"))
            set_doc["Banco"] = normalizar_rotulo_banco_erp(bid_e, bn)[:200]
            set_doc["BancoID"] = bid_e
    if "forma_pagamento" in patch:
        fn = str(patch.get("forma_pagamento") or "").strip()
        if fn:
            set_doc["FormaPagamento"] = fn[:200]
            set_doc["FormaPagamentoID"] = _financeiro_id_para_string(patch.get("forma_pagamento_id"))

    if len(set_doc) <= 3:
        return {"ok": False, "erro": "Nenhum campo para atualizar."}

    _financeiro_doc_coerce_ids_oid_para_string(set_doc)
    col.update_one({"_id": oid}, {"$set": set_doc})
    return {"ok": True, "id": str(oid)}
