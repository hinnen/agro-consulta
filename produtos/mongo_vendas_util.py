"""
Agregações de vendas a partir do Mongo (DtoVenda / DtoVendaProduto), alinhadas ao uso em views.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, time as dtime
from decimal import Decimal
from typing import Any

from bson import ObjectId

from django.utils import timezone

logger = logging.getLogger(__name__)

# Dashboard: agregações com limite de tempo no servidor (evita OOM / SIGKILL no worker).
_MONGO_DASHBOARD_AGG_OPTS: dict[str, Any] = {"maxTimeMS": 120_000, "allowDiskUse": True}


def _mongo_conv_double(field_ref: str | dict) -> dict:
    """``field_ref`` = campo ``"$ValorTotal"`` ou expressão (ex.: ``$ifNull``)."""
    return {"$convert": {"input": field_ref, "to": "double", "onError": 0.0, "onNull": 0.0}}


def _mongo_expr_linha_dto_venda_produto() -> dict:
    """
    Valor da linha de DtoVendaProduto alinhado a ``_valor_linha_item`` (primeiro total > 0; senão preço × qtd).
    """
    qtd = _mongo_conv_double({"$ifNull": ["$Quantidade", "$quantidade"]})
    pu_in = {
        "$ifNull": [
            "$PrecoUnitario",
            {"$ifNull": ["$ValorUnitario", {"$ifNull": ["$Preco", "$precoUnitario"]}]},
        ]
    }
    pu = _mongo_conv_double(pu_in)
    fallback: dict = {"$multiply": [pu, qtd]}
    chain = fallback
    for fld in ("TotalLinha", "valorTotal", "ValorLiquido", "SubTotal", "Total", "ValorTotal"):
        v = _mongo_conv_double(f"${fld}")
        chain = {"$cond": [{"$gt": [v, 0]}, v, chain]}
    return chain


def _match_venda_ids_clause(venda_ids_obj: list, venda_ids_str: list[str]) -> dict:
    return {
        "$or": [
            {"VendaID": {"$in": venda_ids_obj}},
            {"VendaID": {"$in": venda_ids_str}},
        ]
    }


def _aggregate_soma_linhas_por_venda_id(
    db, venda_ids_obj: list, venda_ids_str: list[str]
) -> dict[str, Decimal] | None:
    """
    Soma valor das linhas por VendaID em uma agregação (sem trazer todos os itens para o Python).
    Retorna None se a agregação falhar.
    """
    if not venda_ids_str:
        return {}
    coll = db["DtoVendaProduto"]
    match = _match_venda_ids_clause(venda_ids_obj, venda_ids_str)
    pipeline = [
        {"$match": match},
        {
            "$addFields": {
                "linha": _mongo_expr_linha_dto_venda_produto(),
                "vid": {"$toString": "$VendaID"},
            }
        },
        {"$group": {"_id": "$vid", "soma": {"$sum": "$linha"}}},
    ]
    try:
        rows = list(coll.aggregate(pipeline, **_MONGO_DASHBOARD_AGG_OPTS))
    except Exception as exc:
        logger.warning("aggregate soma linhas por venda: %s", exc)
        return None
    out: dict[str, Decimal] = {}
    for row in rows:
        k = str(row.get("_id") or "")
        if not k or k == "None":
            continue
        out[k] = _decimal_seguro(row.get("soma"))
    return out


def _aggregate_top_produtos_por_produto(
    db, venda_ids_obj: list, venda_ids_str: list[str], limite: int
) -> tuple[list[tuple[str, Decimal, Decimal]], dict[str, str]] | None:
    """
    Top produtos por soma de linha (servidor). Retorna lista (produto_id, total, qtd) ordenada
    e mapa produto_id → primeira descrição de linha encontrada.
    """
    if not venda_ids_str:
        return [], {}
    coll = db["DtoVendaProduto"]
    match = {"$and": [_match_venda_ids_clause(venda_ids_obj, venda_ids_str), {"ProdutoID": {"$ne": None}}]}
    pipeline = [
        {"$match": match},
        {
            "$addFields": {
                "linha": _mongo_expr_linha_dto_venda_produto(),
                "qtd": _mongo_conv_double({"$ifNull": ["$Quantidade", "$quantidade"]}),
                "pid": {"$toString": "$ProdutoID"},
                "dsc": {
                    "$ifNull": [
                        "$Descricao",
                        {"$ifNull": ["$descricao", {"$ifNull": ["$NomeProduto", "$Produto"]}]},
                    ]
                },
            }
        },
        {
            "$group": {
                "_id": "$pid",
                "total": {"$sum": "$linha"},
                "qtd_total": {"$sum": "$qtd"},
                "desc_any": {"$first": "$dsc"},
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": int(limite)},
    ]
    try:
        rows = list(coll.aggregate(pipeline, **_MONGO_DASHBOARD_AGG_OPTS))
    except Exception as exc:
        logger.warning("aggregate top produtos: %s", exc)
        return None
    ranked: list[tuple[str, Decimal, Decimal]] = []
    desc: dict[str, str] = {}
    for row in rows:
        pid = str(row.get("_id") or "")
        if not pid or pid == "None":
            continue
        tot = _decimal_seguro(row.get("total"))
        qtd = _decimal_seguro(row.get("qtd_total"))
        ranked.append((pid, tot, qtd))
        d = row.get("desc_any")
        if d is not None:
            s = str(d).strip()
            if s:
                desc[pid] = s[:200]
    return ranked, desc


def _filtro_venda_ativa_mongo():
    """
    Exclui vendas canceladas e status que não entram no faturamento do dia
    (orçamento, cancelado em qualquer redação — ex.: "Pedido Cancelado", "Orçamento" com ç).
    Alinha o total ao relatório Pedidos Faturados do ERP.
    """
    return {
        "Cancelada": {"$ne": True},
        "$nor": [
            {"Status": {"$regex": r"cancel", "$options": "i"}},
            {"Status": {"$regex": r"orçamento", "$options": "i"}},
            {"Status": {"$regex": r"orcamento", "$options": "i"}},
        ],
    }


def _decimal_seguro(v) -> Decimal:
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _valor_cabecalho_venda(v: dict) -> Decimal | None:
    for k in (
        "ValorTotal",
        "Total",
        "ValorLiquido",
        "valorTotal",
        "total",
        "TotalGeral",
        "ValorFinal",
    ):
        if v.get(k) is not None:
            return _decimal_seguro(v.get(k))
    return None


def _valor_linha_item(item: dict) -> Decimal:
    for k in (
        "ValorTotal",
        "Total",
        "SubTotal",
        "ValorLiquido",
        "valorTotal",
        "TotalLinha",
    ):
        if item.get(k) is not None:
            d = _decimal_seguro(item.get(k))
            if d != 0:
                return d
    pu = (
        item.get("PrecoUnitario")
        or item.get("ValorUnitario")
        or item.get("precoUnitario")
        or item.get("Preco")
    )
    qtd = item.get("Quantidade") or item.get("quantidade") or 0
    try:
        if pu is not None:
            return _decimal_seguro(pu) * _decimal_seguro(qtd)
    except Exception:
        pass
    return Decimal("0")


def _total_vendas_de_documentos_mongo(db, vendas: list) -> Decimal:
    """
    Soma faturamento a partir de uma lista de documentos DtoVenda já filtrados.
    Cabeçalho quando existir; senão agrega DtoVendaProduto por VendaID.
    """
    if not vendas:
        return Decimal("0")

    venda_ids_obj = []
    venda_ids_str = []
    total = Decimal("0")
    precisa_itens: list[str] = []

    for v in vendas:
        vt = _valor_cabecalho_venda(v)
        if vt is not None:
            total += vt
            continue
        vid = str(v.get("Id") or v.get("_id"))
        precisa_itens.append(vid)
        venda_ids_str.append(vid)
        if len(vid) == 24:
            try:
                venda_ids_obj.append(ObjectId(vid))
            except Exception:
                pass

    if not precisa_itens:
        return total.quantize(Decimal("0.01"))

    query_itens = {
        "$or": [
            {"VendaID": {"$in": venda_ids_obj}},
            {"VendaID": {"$in": venda_ids_str}},
        ]
    }
    try:
        itens = db["DtoVendaProduto"].find(query_itens)
    except Exception as exc:
        logger.exception("_total_vendas_de_documentos_mongo: itens: %s", exc)
        return total.quantize(Decimal("0.01"))

    soma_por_venda: dict[str, Decimal] = {}
    for item in itens:
        vid_raw = item.get("VendaID")
        vid = str(vid_raw) if vid_raw is not None else ""
        if not vid or vid == "None":
            continue
        linha = _valor_linha_item(item)
        soma_por_venda[vid] = soma_por_venda.get(vid, Decimal("0")) + linha

    for vid in precisa_itens:
        for k in (vid, str(vid)):
            if k in soma_por_venda:
                total += soma_por_venda[k]
                break

    return total.quantize(Decimal("0.01"))


def obter_valor_total_vendas_dia_mongo(db, dia=None) -> Decimal:
    """
    Soma o faturamento do dia (timezone Django) a partir de DtoVenda / DtoVendaProduto.
    Usa valor do cabeçalho quando existir; senão soma linhas de DtoVendaProduto.
    """
    if db is None:
        return Decimal("0")

    dia = dia or timezone.localdate()
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(dia, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(dia, dtime.max), tz)

    q = {"Data": {"$gte": inicio, "$lte": fim}, **_filtro_venda_ativa_mongo()}

    try:
        vendas = list(db["DtoVenda"].find(q))
    except Exception as exc:
        logger.exception("obter_valor_total_vendas_dia_mongo: find DtoVenda: %s", exc)
        return Decimal("0")

    return _total_vendas_de_documentos_mongo(db, vendas)


def obter_valor_total_vendas_periodo_mongo(db, data_de: date, data_ate: date) -> Decimal:
    """
    Faturamento no intervalo [data_de, data_ate] pela data do pedido (DtoVenda.Data),
    mesmas regras que o total diário (Pedidos Faturados / PDV).
    """
    if db is None or data_de is None or data_ate is None:
        return Decimal("0")
    if data_de > data_ate:
        data_de, data_ate = data_ate, data_de
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(data_de, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(data_ate, dtime.max), tz)
    q = {"Data": {"$gte": inicio, "$lte": fim}, **_filtro_venda_ativa_mongo()}
    try:
        vendas = list(db["DtoVenda"].find(q))
    except Exception as exc:
        logger.exception("obter_valor_total_vendas_periodo_mongo: find DtoVenda: %s", exc)
        return Decimal("0")
    return _total_vendas_de_documentos_mongo(db, vendas)


def media_vendas_diaria_ultimos_n_dias(db, n: int = 30) -> Decimal:
    """
    Média diária = soma do faturamento de cada um dos últimos n dias corridos (incluindo hoje) ÷ n.
    Dias sem venda entram com zero no numerador.
    """
    if db is None or n < 1:
        return Decimal("0")
    n = min(int(n), 365)
    hoje = timezone.localdate()
    total = Decimal("0")
    for k in range(n):
        d = hoje - timedelta(days=k)
        total += obter_valor_total_vendas_dia_mongo(db, d)
    return (total / Decimal(n)).quantize(Decimal("0.01"))


def faixa_dia_mes_mongo(day_of_month: int) -> str:
    """Segmenta o mês: início (1–10), meio (11–20), final (21+)."""
    d = int(day_of_month)
    if d <= 10:
        return "inicio"
    if d <= 20:
        return "meio"
    return "final"


def fatores_vendas_por_calendario(db, dias_lookback: int = 84) -> dict[str, Any]:
    """
    Multiplicadores em relação à média diária global do período:
    - por dia da semana (0=segunda … 6=domingo);
    - por faixa do mês (início / meio / final).

    Usa histórico de faturamento por dia civil (Mongo). Poucas amostras → fator 1.0.
    """
    nomes_curto = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    faixa_labels = {
        "inicio": "Início do mês (1–10)",
        "meio": "Meio do mês (11–20)",
        "final": "Final do mês (21+)",
    }
    out: dict[str, Any] = {
        "lookback_dias": dias_lookback,
        "media_global_dia": 0.0,
        "mult_dow": [1.0] * 7,
        "mult_faixa": {"inicio": 1.0, "meio": 1.0, "final": 1.0},
        "n_amostras_dow": [0] * 7,
        "n_amostras_faixa": {"inicio": 0, "meio": 0, "final": 0},
        "suficiente": False,
        "dia_semana_nomes_curto": nomes_curto,
        "faixa_labels": faixa_labels,
    }
    if db is None:
        return out
    dias_lookback = max(21, min(int(dias_lookback or 84), 366))
    hoje = timezone.localdate()
    totais_dia: list[float] = []
    by_dow_sum = [0.0] * 7
    by_dow_cnt = [0] * 7
    by_fx_sum = {"inicio": 0.0, "meio": 0.0, "final": 0.0}
    by_fx_cnt = {"inicio": 0, "meio": 0, "final": 0}

    for k in range(dias_lookback):
        d = hoje - timedelta(days=k)
        val = float(obter_valor_total_vendas_dia_mongo(db, d))
        totais_dia.append(val)
        wd = d.weekday()
        by_dow_sum[wd] += val
        by_dow_cnt[wd] += 1
        fx = faixa_dia_mes_mongo(d.day)
        by_fx_sum[fx] += val
        by_fx_cnt[fx] += 1

    media_g = sum(totais_dia) / dias_lookback if dias_lookback else 0.0
    out["media_global_dia"] = round(media_g, 4)
    if media_g <= 0:
        return out

    min_wd = 3
    min_fx = 5
    mult_dow: list[float] = []
    for w in range(7):
        c = by_dow_cnt[w]
        out["n_amostras_dow"][w] = c
        if c >= min_wd:
            m = by_dow_sum[w] / c
            r = m / media_g
            mult_dow.append(round(max(0.55, min(1.5, r)), 4))
        else:
            mult_dow.append(1.0)
    out["mult_dow"] = mult_dow

    mult_fx: dict[str, float] = {}
    for fx in ("inicio", "meio", "final"):
        c = by_fx_cnt[fx]
        out["n_amostras_faixa"][fx] = c
        if c >= min_fx:
            m = by_fx_sum[fx] / c
            r = m / media_g
            mult_fx[fx] = round(max(0.55, min(1.5, r)), 4)
        else:
            mult_fx[fx] = 1.0
    out["mult_faixa"] = mult_fx
    out["suficiente"] = True
    return out


def _q_dto_venda_janela_grafico(data_ini: date, data_fim: date) -> dict:
    """
    Mesmo filtro de DtoVenda que o gráfico do dashboard (DataFaturamento ou Data na janela).
    Alinha rankings a ``_dashboard_mongo_vendas_serie`` / faturamento ERP.
    """
    dt_ini = datetime.combine(data_ini, dtime.min)
    dt_fim = datetime.combine(data_fim, dtime.max)
    return {
        "$and": [
            _filtro_venda_ativa_mongo(),
            {
                "$or": [
                    {"DataFaturamento": {"$gte": dt_ini, "$lte": dt_fim}},
                    {"Data": {"$gte": dt_ini, "$lte": dt_fim}},
                ]
            },
        ]
    }


def _doc_data_venda_espelho(doc: dict) -> datetime | None:
    for campo in ("DataFaturamento", "Data", "data", "CriadoEm", "criado_em"):
        dt = doc.get(campo)
        if isinstance(dt, datetime):
            return dt
    return None


def _float_seguro(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _doc_total_venda_espelho(doc: dict) -> float:
    for campo in ("ValorTotal", "ValorLiquido", "Total", "Valor", "total", "ValorFinal"):
        v = doc.get(campo)
        if v is not None:
            f = _float_seguro(v)
            if f > 0:
                return f
    return 0.0


def _nome_vendedor_dto_venda(doc: dict) -> str:
    for chave in (
        "VendedorNome",
        "NomeVendedor",
        "Vendedor",
        "vendedor",
        "UsuarioNome",
        "NomeUsuario",
        "Usuario",
        "usuario",
        "UserName",
        "userName",
        "Atendente",
        "Funcionario",
        "Operador",
        "LancamentoUsuario",
    ):
        v = doc.get(chave)
        if v is not None:
            s = str(v).strip()
            if s and s.lower() not in ("none", "null", "0"):
                return s[:120]
    for chave in ("VendedorID", "UsuarioID", "FuncionarioID"):
        v = doc.get(chave)
        if v is not None and str(v).strip() not in ("", "None", "null", "0"):
            return f"ID {v}"[:120]
    return "Não informado"


def _nome_da_linha_produto_erp(item: dict) -> str:
    for k in ("Descricao", "descricao", "Produto", "NomeProduto", "nome", "Nome"):
        v = item.get(k)
        if v is not None:
            s = str(v).strip()
            if len(s) >= 1:
                return s[:200]
    return ""


def _chaves_produto_mapa_mongo(p: dict) -> list[str]:
    keys: list[str] = []
    vid = p.get("Id")
    if vid is not None:
        keys.append(str(vid))
    keys.append(str(p.get("_id")))
    cod = p.get("Codigo")
    if cod is not None and str(cod).strip() != "":
        keys.append(str(cod))
    return [k for k in keys if k and k != "None"]


def _coletar_vendas_dto_capri(
    db, data_ini: date, data_fim: date
) -> tuple[list[dict] | None, list[dict] | None]:
    """
    Vendas no espelho ERP para o período (mesma regra de inclusão do gráfico: ignora Data inválida).
    """
    try:
        q = _q_dto_venda_janela_grafico(data_ini, data_fim)
        proj = {
            "Id": 1,
            "_id": 1,
            "DataFaturamento": 1,
            "Data": 1,
            "data": 1,
            "CriadoEm": 1,
            "criado_em": 1,
            "ValorTotal": 1,
            "ValorLiquido": 1,
            "Total": 1,
            "Valor": 1,
            "total": 1,
            "ValorFinal": 1,
            "VendedorNome": 1,
            "NomeVendedor": 1,
            "Vendedor": 1,
            "vendedor": 1,
            "UsuarioNome": 1,
            "NomeUsuario": 1,
            "Usuario": 1,
            "usuario": 1,
            "UserName": 1,
            "userName": 1,
            "Atendente": 1,
            "Funcionario": 1,
            "Operador": 1,
            "LancamentoUsuario": 1,
            "VendedorID": 1,
            "UsuarioID": 1,
            "FuncionarioID": 1,
        }
        vendas = list(db["DtoVenda"].find(q, proj).max_time_ms(120_000))
    except Exception as exc:
        logger.exception("coletar_vendas_dto_capri: %s", exc)
        return None, None
    if not vendas:
        return [], []
    filtrado: list[dict] = []
    for v in vendas:
        if _doc_data_venda_espelho(v) is None:
            continue
        filtrado.append(v)
    return filtrado, vendas


def dashboard_top_produtos_mongo(
    client: Any, db, data_ini: date, data_fim: date, *, limite: int = 8
) -> list[dict] | None:
    """
    Top produtos por faturamento (linhas DtoVendaProduto), mesma janela do gráfico.
    Nomes via cadastro (DtoProduto) quando possível.
    """
    if db is None or client is None:
        return None
    limite = max(1, min(int(limite or 8), 30))
    filtrado, _raw = _coletar_vendas_dto_capri(db, data_ini, data_fim)
    if filtrado is None:
        return None
    if not filtrado:
        return []

    venda_ids_obj: list = []
    venda_ids_str: list[str] = []
    for v in filtrado:
        vid = str(v.get("Id") or v.get("_id"))
        if not vid or vid == "None":
            continue
        venda_ids_str.append(vid)
        if len(vid) == 24:
            try:
                venda_ids_obj.append(ObjectId(vid))
            except Exception:
                pass

    if not venda_ids_str:
        return []

    agg = _aggregate_top_produtos_por_produto(db, venda_ids_obj, venda_ids_str, limite)
    if agg is None:
        return None
    ranked, desc_por_id = agg
    if not ranked:
        return []

    ids_top = [r[0] for r in ranked]
    ors: list[dict] = []
    for pid in ids_top:
        ors.append({"Id": pid})
        if pid.isdigit():
            try:
                n = int(pid)
                ors.append({"Id": n})
                ors.append({"Codigo": n})
            except (TypeError, ValueError):
                pass
        if len(pid) == 24:
            try:
                oid = ObjectId(pid)
                ors.append({"Id": oid})
                ors.append({"_id": oid})
            except Exception:
                pass

    pmap: dict[str, dict] = {}
    if ors:
        try:
            col = db[client.col_p]
            prods = list(
                col.find(
                    {"$or": ors},
                    {"Id": 1, "_id": 1, "Nome": 1, "ValorVenda": 1, "PrecoVenda": 1, "Codigo": 1},
                )
            )
        except Exception as exc:
            logger.exception("dashboard_top_produtos_mongo: DtoProduto: %s", exc)
            prods = []
        for p in prods:
            for k in _chaves_produto_mapa_mongo(p):
                pmap[k] = p

    out: list[dict] = []
    for pid, total, qtd in ranked:
        p = pmap.get(pid)
        nome_cat = (p or {}).get("Nome")
        nome = (str(nome_cat).strip() if nome_cat else "")
        if not nome:
            nome = desc_por_id.get(pid, "")
        if not nome:
            nome = f"Produto {pid}"
        out.append(
            {
                "nome": nome[:50].strip() or f"Produto {pid}"[:50],
                "total": round(float(total), 2),
                "qtd_total": round(float(qtd), 3),
            }
        )
    return out


def dashboard_ranking_vendedores_mongo(
    client: Any, db, data_ini: date, data_fim: date, *, limite: int = 8
) -> list[dict] | None:
    """
    Ranking de vendedor por faturamento (DtoVenda), mesma janela do gráfico.
    """
    if db is None or client is None:
        return None
    limite = max(1, min(int(limite or 8), 30))
    filtrado, _ = _coletar_vendas_dto_capri(db, data_ini, data_fim)
    if filtrado is None:
        return None
    if not filtrado:
        return []
    venda_ids_obj: list = []
    venda_ids_str: list[str] = []
    for v in filtrado:
        vid = str(v.get("Id") or v.get("_id"))
        if not vid or vid == "None":
            continue
        venda_ids_str.append(vid)
        if len(vid) == 24:
            try:
                venda_ids_obj.append(ObjectId(vid))
            except Exception:
                pass
    soma_linhas = _aggregate_soma_linhas_por_venda_id(db, venda_ids_obj, venda_ids_str)
    if soma_linhas is None:
        logger.warning("ranking_vendedores_mongo: agregação de linhas falhou; usando só totais de cabeçalho")
        soma_linhas = {}

    def _faturamento_venda_cached(v: dict) -> float:
        t = _doc_total_venda_espelho(v)
        if t > 0:
            return t
        vid = str(v.get("Id") or v.get("_id"))
        for k in (vid, str(v.get("Id")), str(v.get("_id"))):
            if k and k != "None" and k in soma_linhas:
                return float(soma_linhas[k])
        return 0.0

    ac: dict[str, tuple[float, int]] = {}
    for v in filtrado:
        nome = _nome_vendedor_dto_venda(v)
        t = _faturamento_venda_cached(v)
        tot, n = ac.get(nome, (0.0, 0))
        ac[nome] = (tot + t, n + 1)
    ranked = sorted(ac.items(), key=lambda x: x[1][0], reverse=True)[:limite]
    return [
        {
            "nome": nome[:120],
            "total": round(val, 2),
            "n_vendas": int(n_c),
        }
        for nome, (val, n_c) in ranked
    ]
