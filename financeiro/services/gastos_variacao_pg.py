"""Comparativo de despesas por categoria — últimos 3 meses ou 3 semanas (Postgres)."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from financeiro.models import LancamentoFinanceiro as NF
from financeiro.services.plano_despesa_niveis import (
    grupo_negocio_ui,
    lookup_plano_nivel,
    ordem_grupos_negocio,
    tipo_ui,
)
from financeiro.services.resumo_operacional_mongo import (
    classificar_despesa_plano,
    get_object_or_none_empresa,
)
from produtos.lancamentos_financeiro_pg_analytics_util import (
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



_GRUPO_ORDEM = ("fixa", "variavel", "outra")
_GRUPO_LABEL = {
    "fixa": "Despesas fixas",
    "variavel": "Despesas variáveis",
    "outra": "Outras despesas",
}


def _grupo_despesa_ui(nome_plano: str) -> str:
    t = tipo_ui(nome_plano)
    if t:
        return t
    nat = classificar_despesa_plano(nome_plano)
    if nat == NF.NATUREZA_DESPESA_FIXA:
        return "fixa"
    if nat == NF.NATUREZA_DESPESA_VARIAVEL:
        return "variavel"
    return "outra"


def _ordem_grupo_negocio(nome: str) -> int:
    try:
        return ordem_grupos_negocio().index(nome)
    except ValueError:
        return 999


def _subtotais_bucket(rows: list[dict], n: int) -> list[float]:
    sub = [0.0] * n
    for r in rows:
        for i, v in enumerate(r["valores"]):
            sub[i] += v
    return [round(x, 2) for x in sub]


def gastos_por_plano_periodo_pg(
    *,
    data_de: date,
    data_ate: date,
    por: str,
    empresa_nome: str | None = None,
) -> dict[str, Decimal]:
    """Totais por plano — mesma base da lista CP (dedup + bruto em competência)."""
    from produtos.mongo_financeiro_util import _dashboard_plano_excluido_gastos_chart

    del empresa_nome  # CP não filtra empresa; tabela despesas = espelho CP

    modo = (por or "competencia").strip().lower()
    st = "todos" if modo == "pagamento" else "abertos" if modo != "competencia" else "todos"
    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=por, despesa=True, status=st
    )
    por_plano: dict[str, Decimal] = defaultdict(Decimal)
    for t in titulos:
        plano = str(t.plano_conta or "").strip() or "(sem plano)"
        if _dashboard_plano_excluido_gastos_chart(plano):
            continue
        if modo == "competencia":
            por_plano[plano] += _dec2(t.valor_bruto)
        elif modo == "pagamento":
            val = _dec2(t.valor_pago)
            if val > 0:
                por_plano[plano] += val
        else:
            val = _dec2(t.valor_restante) if _titulo_aberto(t) else Decimal("0")
            if val > 0:
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
    grupo_filtro: str = "todas",
) -> dict[str, Any]:
    """Tabela + gráfico agrupado — despesas por categoria (fixa / variável)."""
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
        grupo = _grupo_despesa_ui(plano)
        gneg = grupo_negocio_ui(plano)
        reg = lookup_plano_nivel(plano)
        linhas.append(
            {
                "plano": plano,
                "categoria": plano,
                "grupo": grupo,
                "grupo_negocio": gneg,
                "grupo_negocio_ordem": _ordem_grupo_negocio(gneg),
                "nota_grupo": (reg.observacao[:80] if reg and reg.vale_nao_soma_pessoal else ""),
                "grupo_label": _GRUPO_LABEL[grupo],
                "valores": vals,
                "total": float(sum(vals)),
                "delta_abs": float(delta.quantize(Decimal("0.01"))),
                "delta_pct": pct,
                "tendencia": _tendencia(delta),
            }
        )

    n_buckets = len(buckets)

    gf = (grupo_filtro or "todas").strip().lower()
    if gf in ("fixa", "variavel", "outra"):
        linhas = [r for r in linhas if r["grupo"] == gf]

    linhas.sort(
        key=lambda r: (
            _GRUPO_ORDEM.index(r["grupo"]) if r["grupo"] in _GRUPO_ORDEM else 9,
            r.get("grupo_negocio_ordem", 999),
            r.get("grupo_negocio", "").casefold(),
            -r["valores"][-1],
            r["plano"].casefold(),
        )
    )

    resumo_grupos: list[dict[str, Any]] = []
    for gkey in _GRUPO_ORDEM:
        rows_g = [r for r in linhas if r["grupo"] == gkey]
        sub = [0.0] * n_buckets
        for r in rows_g:
            for i, v in enumerate(r["valores"]):
                sub[i] += v
        resumo_grupos.append(
            {
                "key": gkey,
                "label": _GRUPO_LABEL[gkey],
                "qtd": len(rows_g),
                "subtotais": [round(x, 2) for x in sub],
                "ultimo": round(sub[-1], 2) if sub else 0.0,
            }
        )

    # Blocos Tipo → Grupo (para a tabela Indicadores)
    blocos_tipo: list[dict[str, Any]] = []
    for gkey in _GRUPO_ORDEM:
        rows_tipo = [r for r in linhas if r["grupo"] == gkey]
        if not rows_tipo:
            continue
        res_t = next((x for x in resumo_grupos if x["key"] == gkey), None)
        grupos_neg: list[dict[str, Any]] = []
        gneg_atual = None
        bucket_rows: list[dict] = []
        for row in rows_tipo:
            gn = row["grupo_negocio"]
            if gneg_atual is not None and gn != gneg_atual:
                grupos_neg.append(
                    {
                        "nome": gneg_atual,
                        "qtd": len(bucket_rows),
                        "subtotais": _subtotais_bucket(bucket_rows, n_buckets),
                        "linhas": bucket_rows,
                    }
                )
                bucket_rows = []
            gneg_atual = gn
            bucket_rows.append(row)
        if bucket_rows and gneg_atual is not None:
            grupos_neg.append(
                {
                    "nome": gneg_atual,
                    "qtd": len(bucket_rows),
                    "subtotais": _subtotais_bucket(bucket_rows, n_buckets),
                    "linhas": bucket_rows,
                }
            )
        blocos_tipo.append(
            {
                "key": gkey,
                "label": _GRUPO_LABEL[gkey],
                "qtd": len(rows_tipo),
                "subtotais": res_t["subtotais"] if res_t else _subtotais_bucket(rows_tipo, n_buckets),
                "grupos_negocio": grupos_neg,
            }
        )

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
        "blocos_tipo": blocos_tipo,
        "resumo_grupos": resumo_grupos,
        "grupo_filtro": gf if gf in ("fixa", "variavel", "outra") else "todas",
        "total_categorias": len(linhas),
        "total_planos": len(linhas),
        "total_ultimo_periodo": round(total_ultimo, 2),
        "chart": {
            "labels": chart_labels,
            "datasets": chart_datasets,
        },
    }
