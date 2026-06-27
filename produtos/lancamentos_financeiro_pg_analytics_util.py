"""Analytics financeiro / gráficos — Postgres (TituloFinanceiroAgro)."""
from __future__ import annotations

import logging
import re
import unicodedata
from collections import defaultdict
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.db.models import Q, Sum
from django.utils import timezone

from produtos.lancamentos_financeiro_pg_util import (
    _dec2,
    _titulo_aberto,
    dedup_titulos,
    titulos_financeiro_montar_qs,
)
from produtos.models import TituloFinanceiroAgro, VendaAgro

logger = logging.getLogger(__name__)


def _plano_excluido_dre(nome: str, extra_rx: str | None = None) -> bool:
    from produtos.mongo_financeiro_util import _dre_regexes_excluir_resultado

    n = unicodedata.normalize("NFKD", str(nome or ""))
    n = "".join(c for c in n if not unicodedata.combining(c))
    if not n.strip():
        return True
    for pat in _dre_regexes_excluir_resultado(extra_rx):
        try:
            if re.search(pat, n):
                return True
        except re.error:
            continue
    return False


def _campo_data_titulo(t: TituloFinanceiroAgro, por: str) -> date | None:
    modo = (por or "competencia").strip().lower()
    if modo == "vencimento":
        return t.data_vencimento
    if modo == "pagamento":
        return t.data_pagamento
    return t.data_competencia or t.data_vencimento


def _valor_titulo_dre(t: TituloFinanceiroAgro, valor: str) -> Decimal:
    modo = (valor or "bruto").strip().lower()
    if modo == "realizado":
        return _dec2(t.valor_pago if t.despesa else min(t.valor_pago, t.valor_bruto))
    return _dec2(t.valor_bruto)


def _valor_titulo_grafico(t: TituloFinanceiroAgro, valor: str, *, as_of: date | None) -> Decimal:
    modo = (valor or "bruto").strip().lower()
    if modo == "pago":
        return _dec2(t.valor_pago)
    if modo == "saldo":
        if not _titulo_aberto(t):
            return Decimal("0")
        return _dec2(t.valor_restante)
    return _dec2(t.valor_bruto)


def _titulos_no_periodo_pg(
    *,
    data_de: date,
    data_ate: date,
    por: str,
    despesa: bool | None = None,
    status: str = "todos",
) -> list[TituloFinanceiroAgro]:
    modo = (por or "competencia").strip().lower()
    kwargs: dict[str, Any] = dict(status=status)
    if despesa is not None:
        kwargs["despesa"] = despesa
    if modo == "vencimento":
        kwargs["vencimento_de"] = data_de
        kwargs["vencimento_ate"] = data_ate
    elif modo == "pagamento":
        kwargs["pagamento_de"] = data_de
        kwargs["pagamento_ate"] = data_ate
    else:
        kwargs["competencia_de"] = data_de
        kwargs["competencia_ate"] = data_ate
    qs = titulos_financeiro_montar_qs(**kwargs)
    return dedup_titulos(list(qs))


def dre_resumo_simples_pg(
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
    from produtos.mongo_financeiro_util import _sanitizar_nome_plano_dre

    fc = (filtro_contas or "resultado").strip().lower()
    st = "todos" if (por or "").strip().lower() == "pagamento" else "todos"
    titulos = _titulos_no_periodo_pg(data_de=data_de, data_ate=data_ate, por=por, status=st)
    por_plano: dict[str, dict[str, Decimal]] = {}
    for t in titulos:
        if fc in ("resultado", "resultado_erp") and _plano_excluido_dre(t.plano_conta, regex_excluir_extra):
            continue
        if empresa and str(t.empresa or "").strip().lower() != str(empresa).strip().lower():
            continue
        dt = _campo_data_titulo(t, por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        if (por or "").strip().lower() == "pagamento" and _dec2(t.valor_pago) <= 0:
            continue
        nome = _sanitizar_nome_plano_dre(t.plano_conta) or "(sem plano)"
        slot = por_plano.setdefault(nome, {"despesa": Decimal("0"), "receita": Decimal("0")})
        val = _valor_titulo_dre(t, valor)
        if t.despesa:
            slot["despesa"] += val
        else:
            slot["receita"] += val

    linhas = []
    tot_d = tot_r = Decimal("0")
    for nome, slot in sorted(por_plano.items(), key=lambda x: x[0].casefold()):
        d = float(slot["despesa"].quantize(Decimal("0.01")))
        r = float(slot["receita"].quantize(Decimal("0.01")))
        if d <= 0 and r <= 0:
            continue
        linhas.append({"plano": nome, "despesa": d, "receita": r, "saldo": round(r - d, 2)})
        tot_d += slot["despesa"]
        tot_r += slot["receita"]

    out: dict[str, Any] = {
        "ok": True,
        "fonte": "postgres",
        "por": por,
        "valor": valor,
        "filtro_contas": fc,
        "linhas": linhas,
        "totais": {
            "despesa": float(tot_d.quantize(Decimal("0.01"))),
            "receita": float(tot_r.quantize(Decimal("0.01"))),
            "resultado": float((tot_r - tot_d).quantize(Decimal("0.01"))),
        },
    }
    if diagnostico:
        out["diagnostico"] = {"titulos_periodo": len(titulos), "linhas": len(linhas)}
    return out


def dashboard_despesas_plano_totais_pg(
    *,
    data_de: date,
    data_ate: date,
    por: str = "vencimento",
) -> dict[str, Any]:
    from produtos.mongo_financeiro_util import _dashboard_plano_excluido_gastos_chart

    modo = (por or "vencimento").strip().lower()
    if modo == "competencia":
        campo_label = "Competência"
    elif modo == "pagamento":
        campo_label = "Pagamento"
    else:
        campo_label = "Vencimento"

    st = "todos" if modo == "pagamento" else "abertos" if modo != "competencia" else "todos"
    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=por, despesa=True, status=st
    )
    por_plano: dict[str, Decimal] = defaultdict(Decimal)
    for t in titulos:
        plano = str(t.plano_conta or "").strip() or "(sem plano)"
        if _dashboard_plano_excluido_gastos_chart(plano):
            continue
        dt = _campo_data_titulo(t, por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        if modo == "pagamento":
            val = _dec2(t.valor_pago)
        elif modo == "competencia":
            val = _dec2(t.valor_bruto)
        else:
            val = _dec2(t.valor_restante) if _titulo_aberto(t) else Decimal("0")
        if val <= 0:
            continue
        por_plano[plano] += val

    totais_plano = [
        {"plano": k, "total": float(v.quantize(Decimal("0.01")))}
        for k, v in sorted(por_plano.items(), key=lambda x: (-x[1], x[0].casefold()))
    ]
    total_periodo = float(sum(por_plano.values()).quantize(Decimal("0.01")))
    return {
        "ok": True,
        "fonte": "postgres",
        "campo_data": por,
        "campo_data_label": campo_label,
        "totais_plano": totais_plano,
        "total_periodo": total_periodo,
    }


def financeiro_projecao_fluxo_diario_pg(
    *,
    dias_media_vendas: int = 30,
    horizonte_dias: int = 60,
    incluir_media_vendas: bool = True,
) -> dict[str, Any]:
    hoje = timezone.localdate()
    horizonte_dias = max(1, min(int(horizonte_dias or 60), 120))
    fim = hoje + timedelta(days=horizonte_dias)
    dias_media_vendas = max(1, min(int(dias_media_vendas or 30), 365))

    media_f = 0.0
    if incluir_media_vendas:
        ini_m = hoje - timedelta(days=dias_media_vendas)
        from django.db.models import Count

        agg = VendaAgro.objects.filter(
            criado_em__date__gte=ini_m,
            criado_em__date__lte=hoje,
        ).aggregate(total=Sum("total"), n=Count("id"))
        try:
            n_dias = max(1, (hoje - ini_m).days + 1)
            total_v = float(agg.get("total") or 0)
            media_f = total_v / n_dias
        except Exception:
            media_f = 0.0

    pagar_m: dict[date, dict[str, Any]] = {}
    rec_m: dict[date, dict[str, Any]] = {}
    for t in dedup_titulos(
        list(
            TituloFinanceiroAgro.objects.filter(
                quitado=False,
                data_vencimento__gte=hoje,
                data_vencimento__lte=fim,
            )
        )
    ):
        dkey = t.data_vencimento
        if dkey is None:
            continue
        rest = _dec2(t.valor_restante)
        if rest <= 0:
            continue
        bucket = pagar_m if t.despesa else rec_m
        e = bucket.setdefault(dkey, {"valor": Decimal("0"), "n": 0})
        e["valor"] += rest
        e["n"] += 1

    dias_out: list[dict[str, Any]] = []
    cum = Decimal("0")
    d = hoje
    nomes_dow = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    while d <= fim:
        vp = pagar_m.get(d, {"valor": Decimal("0"), "n": 0})
        vr = rec_m.get(d, {"valor": Decimal("0"), "n": 0})
        vendas_d = round(media_f, 2) if incluir_media_vendas else 0.0
        ent = float(vr["valor"])
        sai = float(vp["valor"])
        liq = ent + vendas_d - sai
        cum += Decimal(str(liq))
        dias_out.append(
            {
                "data": d.isoformat(),
                "dia_semana": nomes_dow[d.weekday()],
                "vendas_estimadas": vendas_d,
                "a_receber": ent,
                "a_pagar": sai,
                "liquido_dia": round(liq, 2),
                "saldo_acumulado": float(cum.quantize(Decimal("0.01"))),
                "n_pagar": int(vp["n"]),
                "n_receber": int(vr["n"]),
            }
        )
        d += timedelta(days=1)

    return {
        "fonte": "postgres",
        "dias": dias_out,
        "meta": {
            "hoje": hoje.isoformat(),
            "horizonte_dias": horizonte_dias,
            "media_vendas_dia": round(media_f, 2),
            "dias_media_vendas": dias_media_vendas,
        },
    }


def grafico_gastos_serie_pg(
    *,
    data_de: date,
    data_ate: date,
    agrupamento: str = "mes",
    plano_ids: list[str] | None = None,
    planos_excluir_nomes: list[str] | None = None,
    todos_planos: bool = False,
    individual: bool = False,
    por: str = "vencimento",
    valor: str = "bruto",
    data_referencia: date | None = None,
) -> dict[str, Any]:
    from produtos.mongo_financeiro_util import (
        _GRAFICO_GASTOS_CORES,
        _dashboard_plano_excluido_gastos_chart,
        _grafico_gastos_bucket_key,
        _grafico_gastos_bucket_label,
        _grafico_gastos_iter_bucket_keys,
    )

    vazio = {"ok": False, "erro": "Sem dados", "labels": [], "bucket_keys": [], "datasets": []}
    modo_por = (por or "vencimento").strip().lower()
    modo_valor = (valor or "bruto").strip().lower()
    from produtos.mongo_financeiro_util import _grafico_gastos_status_para_lista_planos

    st = _grafico_gastos_status_para_lista_planos(
        modo_por, modo_valor, data_referencia=data_referencia
    )

    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=modo_por, despesa=True, status=st
    )

    excl = set(str(x).strip() for x in (planos_excluir_nomes or []) if str(x).strip())
    incluir_individual: set[str] | None = None
    if individual and plano_ids:
        nomes = {str(x).strip() for x in plano_ids if str(x).strip()}
        if nomes:
            incluir_individual = nomes

    agr = (agrupamento or "mes").strip().lower()
    bucket_keys = _grafico_gastos_iter_bucket_keys(data_de, data_ate, agr)
    labels = [_grafico_gastos_bucket_label(k, agr) for k in bucket_keys]

    por_plano: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    totais: dict[str, float] = defaultdict(float)

    for t in titulos:
        plano = str(t.plano_conta or "").strip() or "(sem plano)"
        if _dashboard_plano_excluido_gastos_chart(plano):
            continue
        if excl and plano in excl:
            continue
        if incluir_individual is not None and plano not in incluir_individual:
            continue
        dt = _campo_data_titulo(t, modo_por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        val = _valor_titulo_grafico(t, modo_valor, as_of=data_referencia)
        if val <= Decimal("0.02"):
            continue
        bkey = _grafico_gastos_bucket_key(dt, agr)
        vf = float(val.quantize(Decimal("0.01")))
        if individual:
            por_plano[plano][bkey] += vf
        else:
            totais[bkey] += vf

    datasets = []
    if individual:
        for i, (plano, vals) in enumerate(sorted(por_plano.items(), key=lambda x: x[0].casefold())):
            cor_borda, cor_fundo = _GRAFICO_GASTOS_CORES[i % len(_GRAFICO_GASTOS_CORES)]
            datasets.append(
                {
                    "label": plano,
                    "data": [round(vals.get(k, 0.0), 2) for k in bucket_keys],
                    "borderColor": cor_borda,
                    "backgroundColor": cor_fundo,
                    "fill": True,
                }
            )
    else:
        label = "Total Selecionado"
        if incluir_nomes and len(incluir_nomes) == 1:
            label = next(iter(incluir_nomes))
        cor_borda, cor_fundo = _GRAFICO_GASTOS_CORES[0]
        datasets = [
            {
                "label": label,
                "data": [round(totais.get(k, 0.0), 2) for k in bucket_keys],
                "borderColor": cor_borda,
                "backgroundColor": cor_fundo,
                "fill": True,
            }
        ]

    out = {
        "ok": True,
        "erro": None,
        "fonte": "postgres",
        "labels": labels,
        "bucket_keys": bucket_keys,
        "datasets": datasets,
        "modo_tempo": "historico" if data_referencia else "real",
    }
    if data_referencia:
        out["data_referencia"] = data_referencia.isoformat()
    if not datasets:
        out["ok"] = False
        out["erro"] = "Sem valores no período"
    return out
