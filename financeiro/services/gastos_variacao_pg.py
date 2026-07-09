"""Comparativo de gastos por plano — últimos 3 meses ou 3 semanas (Postgres)."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from financeiro.services.resumo_operacional_mongo import get_object_or_none_empresa
from produtos.lancamentos_financeiro_pg_analytics_util import (
    _campo_data_titulo,
    _dec2,
    _titulo_aberto,
    _titulos_no_periodo_pg,
)


def _ultimo_dia_mes(ano: int, mes: int) -> date:
    if mes == 12:
        return date(ano + 1, 1, 1) - timedelta(days=1)
    return date(ano, mes + 1, 1) - timedelta(days=1)


def _primeiro_dia_mes(d: date) -> date:
    return d.replace(day=1)


def _semana_seg_dom(ref: date) -> tuple[date, date]:
    ini = ref - timedelta(days=ref.weekday())
    return ini, ini + timedelta(days=6)


_MESES_PT = (
    "jan", "fev", "mar", "abr", "mai", "jun",
    "jul", "ago", "set", "out", "nov", "dez",
)


def buckets_ultimos_meses(hoje: date, n: int = 3) -> list[dict[str, Any]]:
    """n buckets mensais: do mais antigo ao mais recente (último = mês corrente até hoje)."""
    out: list[dict[str, Any]] = []
    cur = _primeiro_dia_mes(hoje)
    for offset in range(n - 1, -1, -1):
        primeiro = cur
        for _ in range(offset):
            primeiro = _primeiro_dia_mes(primeiro - timedelta(days=1))
        if offset == 0:
            fim = hoje
        else:
            fim = _ultimo_dia_mes(primeiro.year, primeiro.month)
        label = f"{_MESES_PT[primeiro.month - 1]}/{str(primeiro.year)[2:]}"
        out.append(
            {
                "key": primeiro.strftime("%Y-%m"),
                "label": label,
                "de": primeiro,
                "ate": fim,
            }
        )
    return out


def buckets_ultimas_semanas(hoje: date, n: int = 3) -> list[dict[str, Any]]:
    """n semanas (seg–dom); a mais recente vai até hoje se ainda não fechou."""
    ini_atual, _ = _semana_seg_dom(hoje)
    out: list[dict[str, Any]] = []
    for offset in range(n - 1, -1, -1):
        ini = ini_atual - timedelta(days=7 * offset)
        fim = ini + timedelta(days=6)
        if offset == 0 and fim > hoje:
            fim = hoje
        label = f"{ini.strftime('%d/%m')}–{fim.strftime('%d/%m')}"
        out.append(
            {
                "key": ini.isoformat(),
                "label": label,
                "de": ini,
                "ate": fim,
            }
        )
    return out


def _valor_despesa_titulo(t, por: str) -> Decimal:
    modo = (por or "competencia").strip().lower()
    if modo == "pagamento":
        return _dec2(t.valor_pago)
    if modo == "vencimento":
        return _dec2(t.valor_restante) if _titulo_aberto(t) else Decimal("0")
    return _dec2(t.valor_bruto)


def gastos_por_plano_periodo_pg(
    *,
    data_de: date,
    data_ate: date,
    por: str,
    empresa_nome: str | None = None,
) -> dict[str, Decimal]:
    from produtos.mongo_financeiro_util import _dashboard_plano_excluido_gastos_chart

    modo = (por or "competencia").strip().lower()
    st = "todos" if modo == "pagamento" else "abertos" if modo != "competencia" else "todos"
    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=por, despesa=True, status=st
    )
    por_plano: dict[str, Decimal] = defaultdict(Decimal)
    emp = (empresa_nome or "").strip().lower()
    for t in titulos:
        if emp and str(t.empresa or "").strip().lower() != emp:
            continue
        plano = str(t.plano_conta or "").strip() or "(sem plano)"
        if _dashboard_plano_excluido_gastos_chart(plano):
            continue
        dt = _campo_data_titulo(t, por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        if modo == "pagamento" and _dec2(t.valor_pago) <= 0:
            continue
        val = _valor_despesa_titulo(t, por)
        if val <= 0:
            continue
        por_plano[plano] += val
    return dict(por_plano)


def _fmt_pct(delta: Decimal, base: Decimal) -> float | None:
    if base <= 0:
        return None
    return float((delta / base * Decimal("100")).quantize(Decimal("0.1")))


def _tendencia(delta: Decimal, limiar: Decimal = Decimal("0.01")) -> str:
    if delta > limiar:
        return "up"
    if delta < -limiar:
        return "down"
    return "flat"


def gastos_variacao_pg(
    *,
    empresa_id: int,
    modo: str = "mes",
    por: str = "competencia",
    top_chart: int = 10,
) -> dict[str, Any]:
    """Tabela + dados de gráfico agrupado para variação por plano."""
    empresa = get_object_or_none_empresa(empresa_id)
    if not empresa:
        return {"ok": False, "erro": "Empresa não encontrada", "linhas": [], "buckets": []}
    nome = (empresa.nome_fantasia or "").strip()
    if not nome:
        return {
            "ok": False,
            "erro": "Cadastre o nome fantasia da empresa (filtro Empresa nos títulos PG).",
            "linhas": [],
            "buckets": [],
        }

    hoje = date.today()
    modo = (modo or "mes").strip().lower()
    buckets = (
        buckets_ultimas_semanas(hoje, 3)
        if modo == "semana"
        else buckets_ultimos_meses(hoje, 3)
    )

    acum: dict[str, list[float]] = defaultdict(lambda: [0.0] * len(buckets))
    for i, b in enumerate(buckets):
        totais = gastos_por_plano_periodo_pg(
            data_de=b["de"],
            data_ate=b["ate"],
            por=por,
            empresa_nome=nome,
        )
        for plano, val in totais.items():
            acum[plano][i] = float(val.quantize(Decimal("0.01")))

    linhas: list[dict[str, Any]] = []
    for plano, vals in acum.items():
        v0, v1, v2 = (Decimal(str(x)) for x in vals)
        delta = v2 - v1
        pct = _fmt_pct(delta, v1)
        linhas.append(
            {
                "plano": plano,
                "valores": vals,
                "total": float(sum(vals)),
                "delta_abs": float(delta.quantize(Decimal("0.01"))),
                "delta_pct": pct,
                "tendencia": _tendencia(delta),
            }
        )

    linhas.sort(key=lambda r: (-r["valores"][-1], r["plano"].casefold()))

    top = linhas[:top_chart]
    chart_labels = [r["plano"][:28] for r in top]
    cores = ["#94a3b8", "#f59e0b", "#059669"]
    chart_datasets = []
    for i, b in enumerate(buckets):
        chart_datasets.append(
            {
                "label": b["label"],
                "data": [r["valores"][i] for r in top],
                "backgroundColor": cores[i % len(cores)],
                "borderRadius": 4,
            }
        )

    total_ultimo = sum(r["valores"][-1] for r in linhas)
    return {
        "ok": True,
        "erro": None,
        "modo": modo,
        "por": por,
        "buckets": [
            {
                "key": b["key"],
                "label": b["label"],
                "de": b["de"].isoformat(),
                "ate": b["ate"].isoformat(),
            }
            for b in buckets
        ],
        "linhas": linhas,
        "total_planos": len(linhas),
        "total_ultimo_periodo": round(total_ultimo, 2),
        "chart": {
            "labels": chart_labels,
            "datasets": chart_datasets,
        },
    }
