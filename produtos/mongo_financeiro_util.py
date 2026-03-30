"""
Agregações e operações financeiras a partir do Mongo (DtoLancamento), alinhadas ao ERP.
Baixa via Mongo: o ERP pode resincronizar e sobrescrever — use API dedicada quando existir (ver VendaERPAPIClient).
"""
from __future__ import annotations

import copy
import logging
import re
import secrets
from datetime import date, datetime, timedelta, time as dtime
from decimal import Decimal
from typing import Any

from bson import ObjectId
from django.utils import timezone

logger = logging.getLogger(__name__)

_SENTINEL = datetime(1, 1, 1, 0, 0)
COL_DTO_LANCAMENTO = "DtoLancamento"


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


def _restante_a_receber(doc: dict) -> Decimal:
    """
    Saldo a receber. No Mongo do Venda ERP o recebido pode constar em Recebido e/ou ValorPago
    (ex.: título quitado com Recebido=0 e ValorPago=Entrada).
    """
    entrada = _dec(doc.get("Entrada"))
    rec = _dec(doc.get("Recebido"))
    vp = _dec(doc.get("ValorPago"))
    r = entrada - rec - vp
    return r if r > 0 else Decimal("0")


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


def lancamentos_totais_filtrados(db, query: dict, despesa: bool) -> dict[str, float]:
    if db is None:
        return {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}
    try:
        if despesa:
            pipe = [
                {"$match": query},
                {
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
                },
            ]
        else:
            pipe = [
                {"$match": query},
                {
                    "$group": {
                        "_id": None,
                        "n": {"$sum": 1},
                        "bruto": {"$sum": {"$ifNull": ["$Entrada", 0]}},
                        "movimentado": {
                            "$sum": {
                                "$add": [
                                    {"$ifNull": ["$Recebido", 0]},
                                    {"$ifNull": ["$ValorPago", 0]},
                                ]
                            }
                        },
                        "saldo_aberto": {
                            "$sum": {
                                "$max": [
                                    0,
                                    {
                                        "$subtract": [
                                            {"$ifNull": ["$Entrada", 0]},
                                            {
                                                "$add": [
                                                    {"$ifNull": ["$Recebido", 0]},
                                                    {"$ifNull": ["$ValorPago", 0]},
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            }
                        },
                    }
                },
            ]
        agg = list(db[COL_DTO_LANCAMENTO].aggregate(pipe))
        if not agg:
            return {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}
        a = agg[0]
        return {
            "quantidade": int(a.get("n") or 0),
            "bruto": round(float(a.get("bruto") or 0), 2),
            "movimentado": round(float(a.get("movimentado") or 0), 2),
            "saldo_aberto": round(float(a.get("saldo_aberto") or 0), 2),
        }
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
        mov = float(_dec(doc.get("Recebido")) + _dec(doc.get("ValorPago")))
    return {
        "id": str(doc.get("_id", "")),
        "despesa": despesa,
        "descricao": doc.get("Descricao") or "",
        "cliente": doc.get("Cliente") or "",
        "numero_documento": str(doc.get("NumeroDocumento") or ""),
        "parcela": int(doc.get("NumeroParcela") or 0),
        "plano_conta": doc.get("PlanoDeConta") or "",
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
) -> tuple[list[dict], int, dict[str, float]]:
    if db is None:
        return [], 0, {"quantidade": 0, "bruto": 0.0, "movimentado": 0.0, "saldo_aberto": 0.0}

    page = max(1, page)
    page_size = min(200, max(1, page_size))
    skip = (page - 1) * page_size

    ord = (ordenacao or "vencimento_asc").strip().lower()
    if ord == "vencimento_desc":
        sort_spec = [("DataVencimento", -1), ("_id", -1)]
    elif ord == "fluxo_desc":
        sort_spec = [("DataFluxo", -1), ("_id", -1)]
    else:
        sort_spec = [("DataVencimento", 1), ("_id", 1)]

    try:
        col = db[COL_DTO_LANCAMENTO]
        total = col.count_documents(query)
        totais = lancamentos_totais_filtrados(db, query, despesa)
        cur = col.find(query).sort(sort_spec).skip(skip).limit(page_size)
        linhas = [lancamento_para_api(d, despesa) for d in cur]
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


def listar_formas_e_bancos_distintos(db, limit: int = 400) -> tuple[list[dict], list[dict]]:
    """Listas para selects na baixa (a partir de lançamentos já existentes no Mongo)."""
    formas: list[dict] = []
    bancos: list[dict] = []
    if db is None:
        return formas, bancos
    seen_f: set[str] = set()
    seen_b: set[str] = set()
    try:
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
            formas.append({"id": str(fid) if fid is not None else "", "nome": nome})
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
            bancos.append({"id": str(bid) if bid is not None else "", "nome": nome})
        bancos.sort(key=lambda x: x["nome"].lower())
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

    fid = _maybe_oid(forma_id)
    bid = _maybe_oid(banco_id)

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

    return {
        "ok": len(res_err) == 0,
        "atualizados": res_ok,
        "erros": res_err,
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


def montar_payload_erp_baixa(
    db,
    mongo_ids: list[str],
    despesa: bool,
    payload_ui: dict,
) -> dict[str, Any]:
    """
    Corpo sugerido para VENDA_ERP_API_FINANCEIRO_BAIXA_PATH.
    Mantém ``ids`` / ``payload`` / ``tipo`` (compatível com integrações antigas) e acrescenta ``titulos``
    com snapshot pós-baixa (inclui Id/LancamentoID do ERP quando existirem no Mongo).
    """
    titulos = [lancamento_doc_subset_erp(d) for d in lancamentos_carregar_por_ids(db, mongo_ids)]
    tipo = "pagar" if despesa else "receber"
    return {
        "ids": mongo_ids,
        "mongodb_ids": mongo_ids,
        "tipo": tipo,
        "despesa": despesa,
        "payload": payload_ui or {},
        "origem": "agro_consulta",
        "titulos": titulos,
    }


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
    lim = min(max(int(limit or 30), 1), 80)
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
    Vários títulos compartilhando cabeçalho (empresa, favorecido, datas, banco, forma);
    cada linha: plano de conta, valor, descrição, observação.
    """
    if db is None:
        return {"ok": False, "ids": [], "erros": [{"erro": "Mongo indisponível"}]}
    empresa_nome = (empresa_nome or "").strip()
    pessoa_nome = (pessoa_nome or "").strip()
    banco_nome = (banco_nome or "").strip()
    forma_nome = (forma_nome or "").strip()
    if not empresa_nome or not pessoa_nome or not banco_nome or not forma_nome:
        return {
            "ok": False,
            "ids": [],
            "erros": [{"erro": "Preencha empresa, cliente/fornecedor, banco e forma de pagamento."}],
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

    eid = _maybe_oid(empresa_id) if empresa_id else None
    pid = _maybe_oid(pessoa_id) if pessoa_id else None
    bid = _maybe_oid(banco_id) if banco_id else None
    fid = _maybe_oid(forma_id) if forma_id else None
    gid = _maybe_oid(grupo_id) if grupo_id else None

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
        plano_oid = _maybe_oid(str(plano_id_raw).strip()) if plano_id_raw else None

        doc = copy.deepcopy(tpl)
        doc.pop("_id", None)
        doc["Despesa"] = bool(despesa)
        doc["Empresa"] = empresa_nome[:200]
        doc["EmpresaID"] = eid if eid is not None else (empresa_id or "")
        doc["Cliente"] = pessoa_nome[:300]
        doc["ClienteID"] = pid if pid is not None else (pessoa_id or "")
        doc["Banco"] = banco_nome[:200]
        doc["BancoID"] = bid if bid is not None else (banco_id or "")
        doc["FormaPagamento"] = forma_nome[:200]
        doc["FormaPagamentoID"] = fid if fid is not None else (forma_id or "")
        if (grupo_nome or "").strip():
            doc["LancamentoGrupo"] = grupo_nome.strip()[:200]
            doc["LancamentoGrupoID"] = gid if gid is not None else (grupo_id or "")
        doc["PlanoDeConta"] = plano_nome[:200]
        doc["PlanoDeContaID"] = plano_oid if plano_oid is not None else (str(plano_id_raw).strip() if plano_id_raw else "")
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
