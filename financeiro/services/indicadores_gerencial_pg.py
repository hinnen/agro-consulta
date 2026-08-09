"""
Dashboard financeiro gerencial — 100 % Postgres (TituloFinanceiroAgro).

Mesma classificação DRE do Resumo gerencial; comparativo = média 60 dias projetada.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.utils import timezone

from financeiro.models import LancamentoFinanceiro as NF
from financeiro.services.equilibrio import EquilibrioFinanceiroService
from financeiro.services.gastos_variacao_pg import gastos_variacao_pg
from financeiro.services.resumo_operacional_mongo import natureza_buckets_from_linhas_dre
from financeiro.services.resumo_operacional_pg import consolidar_empresa_pg

REF_DIAS_COMPARACAO = 60


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x or 0))
    except Exception:
        return Decimal("0")


def _norm_filtro_contas(raw: str) -> str:
    fc = (raw or "").strip().lower() or (
        getattr(settings, "DRE_RESULTADO_FILTRO", "resultado") or "resultado"
    )
    if fc not in ("resultado", "resultado_erp", "todas"):
        fc = "resultado"
    return fc


def _indicadores_from_core(
    core: dict[str, Any],
    *,
    caixa_buckets: dict[str, Decimal],
    dias_janela: int,
) -> dict[str, Any]:
    receita_op = _dec(core.get("receita_operacional"))
    receita_nao_op = _dec(core.get("receita_nao_operacional"))
    cmv = _dec(core.get("cmv"))
    df = _dec(core.get("despesas_fixas"))
    dv = _dec(core.get("despesas_variaveis"))
    desp_fin = _dec(core.get("despesas_financeiras"))

    lucro_bruto = receita_op - cmv
    margem_bruta_pct = (
        (lucro_bruto / receita_op * Decimal("100")) if receita_op > 0 else Decimal("0")
    )
    markup_pct = (
        ((receita_op / cmv) - Decimal("1")) * Decimal("100")
        if cmv > 0
        else Decimal("0")
    )
    margem_contrib = receita_op - cmv - dv
    margem_contrib_pct = (
        (margem_contrib / receita_op * Decimal("100")) if receita_op > 0 else Decimal("0")
    )

    dias_u = max(dias_janela, 1)
    eq = EquilibrioFinanceiroService().calcular(receita_op, cmv, df, dv, dias_periodo=dias_u)
    mc_ratio = eq["margem_contribuicao_pct"]
    faturamento_equilibrio = eq["faturamento_equilibrio"]
    pe_diario = eq["faturamento_diario_equilibrio"]
    indice_seguranca_pct = (
        ((receita_op - faturamento_equilibrio) / receita_op * Decimal("100"))
        if receita_op > 0
        else Decimal("0")
    )

    ebitda = margem_contrib - df
    resultado_liquido = _dec(core.get("resultado_liquido_gerencial")) + receita_nao_op
    rec_lanc = core.get("receita_lancamentos")
    receita_lancamentos = _dec(rec_lanc) if rec_lanc is not None else receita_op
    receita_fonte = str(core.get("receita_fonte") or "lancamentos")

    entradas_caixa = (
        caixa_buckets[NF.NATUREZA_RECEITA_OPERACIONAL]
        + caixa_buckets[NF.NATUREZA_RECEITA_NAO_OPERACIONAL]
        + caixa_buckets[NF.NATUREZA_EMPRESTIMO_ENTRADA]
        + caixa_buckets[NF.NATUREZA_APORTE_SOCIO]
    )
    saidas_caixa = (
        caixa_buckets[NF.NATUREZA_CMV]
        + caixa_buckets[NF.NATUREZA_DESPESA_FIXA]
        + caixa_buckets[NF.NATUREZA_DESPESA_VARIAVEL]
        + caixa_buckets[NF.NATUREZA_DESPESA_FINANCEIRA]
        + caixa_buckets[NF.NATUREZA_EMPRESTIMO_AMORTIZACAO]
        + caixa_buckets[NF.NATUREZA_RETIRADA_SOCIO]
    )
    geracao_caixa = entradas_caixa - saidas_caixa

    return {
        "receita_op": receita_op,
        "receita_nao_op": receita_nao_op,
        "cmv": cmv,
        "df": df,
        "dv": dv,
        "desp_fin": desp_fin,
        "lucro_bruto": lucro_bruto,
        "margem_bruta_pct": margem_bruta_pct,
        "markup_pct": markup_pct,
        "margem_contrib": margem_contrib,
        "margem_contrib_pct": margem_contrib_pct,
        "mc_ratio": mc_ratio,
        "ponto_equilibrio": faturamento_equilibrio,
        "pe_diario": pe_diario,
        "indice_seguranca_pct": indice_seguranca_pct,
        "ebitda": ebitda,
        "resultado_liquido": resultado_liquido,
        "entradas_caixa": entradas_caixa,
        "saidas_caixa": saidas_caixa,
        "geracao_caixa": geracao_caixa,
        "aportes": _dec(core.get("aportes_socios")),
        "retiradas": _dec(core.get("retiradas_socios")),
        "receita_fonte": receita_fonte,
        "receita_lancamentos": receita_lancamentos,
    }


_DRE_CMV_JS_FIELDS = (
    "cmv",
    "lucro_bruto",
    "margem_bruta_pct",
    "markup_pct",
    "margem_contrib",
    "margem_contrib_pct",
    "ebitda",
    "resultado_liquido",
    "ponto_equilibrio",
    "indice_seguranca_pct",
)


def recalc_indicadores_cmv(ind: dict[str, Any], cmv_novo, dias_janela: int) -> dict[str, Any]:
    """Recalcula lucro/margem/EBITDA/líquido/PE com outro CMV. Caixa não muda."""
    rec = _dec(ind.get("receita_op"))
    dv = _dec(ind.get("dv"))
    df = _dec(ind.get("df"))
    desp_fin = _dec(ind.get("desp_fin"))
    rec_nao = _dec(ind.get("receita_nao_op"))
    cmv = _dec(cmv_novo)
    lucro_bruto = rec - cmv
    margem_bruta_pct = (
        (lucro_bruto / rec * Decimal("100")) if rec > 0 else Decimal("0")
    )
    markup_pct = (
        ((rec / cmv) - Decimal("1")) * Decimal("100") if cmv > 0 else Decimal("0")
    )
    margem_contrib = rec - cmv - dv
    margem_contrib_pct = (
        (margem_contrib / rec * Decimal("100")) if rec > 0 else Decimal("0")
    )
    dias_u = max(int(dias_janela or 1), 1)
    eq = EquilibrioFinanceiroService().calcular(rec, cmv, df, dv, dias_periodo=dias_u)
    faturamento_equilibrio = eq["faturamento_equilibrio"]
    pe_diario = eq["faturamento_diario_equilibrio"]
    indice_seguranca_pct = (
        ((rec - faturamento_equilibrio) / rec * Decimal("100"))
        if rec > 0
        else Decimal("0")
    )
    ebitda = margem_contrib - df
    resultado_liquido = ebitda - desp_fin + rec_nao
    out = dict(ind)
    out.update(
        {
            "cmv": cmv,
            "lucro_bruto": lucro_bruto,
            "margem_bruta_pct": margem_bruta_pct,
            "markup_pct": markup_pct,
            "margem_contrib": margem_contrib,
            "margem_contrib_pct": margem_contrib_pct,
            "mc_ratio": eq["margem_contribuicao_pct"],
            "ponto_equilibrio": faturamento_equilibrio,
            "pe_diario": pe_diario,
            "indice_seguranca_pct": indice_seguranca_pct,
            "ebitda": ebitda,
            "resultado_liquido": resultado_liquido,
        }
    )
    return out


def _pack_cmv_js(ind: dict[str, Any]) -> dict[str, float]:
    return {k: float(_dec(ind.get(k))) for k in _DRE_CMV_JS_FIELDS}


def _zeros(dias: int) -> dict[str, Any]:
    z = Decimal("0")
    caixa_z = {k: z for k in natureza_buckets_from_linhas_dre([])}
    return _indicadores_from_core(
        {
            "receita_operacional": z,
            "receita_nao_operacional": z,
            "cmv": z,
            "despesas_fixas": z,
            "despesas_variaveis": z,
            "despesas_financeiras": z,
            "resultado_liquido_gerencial": z,
            "aportes_socios": z,
            "retiradas_socios": z,
        },
        caixa_buckets=caixa_z,
        dias_janela=max(dias, 1),
    )


def _benchmark(ref: dict, dias_periodo: int) -> dict:
    k = Decimal(dias_periodo) / Decimal(max(REF_DIAS_COMPARACAO, 1))
    ent = _dec(ref.get("entradas_caixa")) * k
    sai = _dec(ref.get("saidas_caixa")) * k
    caixa_buckets = natureza_buckets_from_linhas_dre([])
    caixa_buckets[NF.NATUREZA_RECEITA_OPERACIONAL] = ent
    caixa_buckets[NF.NATUREZA_CMV] = sai
    return _indicadores_from_core(
        {
            "receita_operacional": _dec(ref.get("receita_op")) * k,
            "receita_nao_operacional": _dec(ref.get("receita_nao_op")) * k,
            "cmv": _dec(ref.get("cmv")) * k,
            "despesas_fixas": _dec(ref.get("df")) * k,
            "despesas_variaveis": _dec(ref.get("dv")) * k,
            "despesas_financeiras": _dec(ref.get("desp_fin")) * k,
            "resultado_liquido_gerencial": (
                _dec(ref.get("resultado_liquido")) - _dec(ref.get("receita_nao_op"))
            )
            * k,
            "aportes_socios": _dec(ref.get("aportes")) * k,
            "retiradas_socios": _dec(ref.get("retiradas")) * k,
            "receita_fonte": ref.get("receita_fonte") or "lancamentos",
            "receita_lancamentos": _dec(ref.get("receita_lancamentos")) * k,
        },
        caixa_buckets=caixa_buckets,
        dias_janela=max(dias_periodo, 1),
    )


def _faturamento_pdv_periodo(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> dict[str, Any]:
    """Mesma série do BI (``_dashboard_mongo_vendas_serie`` — PDV + planilha + fallback)."""
    from financeiro.services.receita_pdv_util import faturamento_pdv_periodo

    return faturamento_pdv_periodo(data_ini, data_fim, deposito=deposito)


def _serie_faturamento_7d(
    data_fim: date,
    por_dia: dict[str, Any],
) -> tuple[list[str], list[float]]:
    start = max(data_fim - timedelta(days=6), data_fim.replace(day=1))
    labels: list[str] = []
    vals: list[float] = []
    d = start
    while d <= data_fim:
        labels.append(d.strftime("%d/%m"))
        vals.append(float(por_dia.get(d.isoformat()) or 0))
        d += timedelta(days=1)
    return labels, vals


def _serie_receita_7d(
    empresa_id: int,
    data_fim: date,
    *,
    por: str,
    valor: str,
    filtro_contas: str,
) -> tuple[list[str], list[float]]:
    start = max(data_fim - timedelta(days=6), data_fim.replace(day=1))
    labels: list[str] = []
    vals: list[float] = []
    d = start
    while d <= data_fim:
        sub = consolidar_empresa_pg(
            empresa_id=empresa_id,
            data_inicio=d,
            data_fim=d,
            por=por,
            valor=valor,
            filtro_contas=filtro_contas,
        )
        v = float(sub.get("receita_operacional") or 0) if not sub.get("erro") else 0.0
        labels.append(d.strftime("%d/%m"))
        vals.append(v)
        d += timedelta(days=1)
    return labels, vals


def _tendencia_linear(valores: list[float]) -> str:
    if len(valores) < 2:
        return "Estável"
    n = len(valores)
    xs = list(range(n))
    mx, my = sum(xs) / n, sum(valores) / n
    num = sum((xs[i] - mx) * (valores[i] - my) for i in range(n))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return "Estável"
    slope = num / den
    if slope > 0.01:
        return "Alta"
    if slope < -0.01:
        return "Queda"
    return "Estável"


def _dicas(ind: dict, previsao_30: Decimal, pe_30: Decimal) -> list[dict]:
    dicas: list[dict] = []
    ro = ind["receita_op"]
    if ro > 0 and ind["df"] / ro > Decimal("0.30"):
        dicas.append(
            {
                "titulo": "Custo fixo alto",
                "msg": "Despesas fixas passam de 30% da receita operacional.",
                "nivel": "danger",
            }
        )
    if ro > 0 and ind["cmv"] / ro > Decimal("0.70"):
        dicas.append(
            {
                "titulo": "CMV elevado",
                "msg": "CMV acima de 70% da receita — confira preços e perdas.",
                "nivel": "warning",
            }
        )
    if ind["geracao_caixa"] < 0:
        dicas.append(
            {
                "titulo": "Caixa negativo",
                "msg": "Entradas abaixo das saídas no período (pagamento realizado).",
                "nivel": "warning",
            }
        )
    if previsao_30 > 0 and pe_30 > 0 and previsao_30 < pe_30:
        dicas.append(
            {
                "titulo": "Projeção x equilíbrio",
                "msg": "Média 60d projetada em 30 dias fica abaixo do ponto de equilíbrio.",
                "nivel": "danger",
            }
        )
    return dicas


def _bloco_periodo(
    empresa_id: int,
    data_ini: date,
    data_fim: date,
    *,
    por: str,
    valor: str,
    filtro_contas: str,
) -> tuple[dict, str | None]:
    core = consolidar_empresa_pg(
        empresa_id=empresa_id,
        data_inicio=data_ini,
        data_fim=data_fim,
        por=por,
        valor=valor,
        filtro_contas=filtro_contas,
    )
    if core.get("erro"):
        dias = max((data_fim - data_ini).days + 1, 1)
        z = _zeros(dias)
        return z, str(core["erro"])

    caixa = consolidar_empresa_pg(
        empresa_id=empresa_id,
        data_inicio=data_ini,
        data_fim=data_fim,
        por="pagamento",
        valor="realizado",
        filtro_contas=filtro_contas,
        usar_receita_pdv=False,
    )
    if caixa.get("erro"):
        buckets_caixa = natureza_buckets_from_linhas_dre([])
    else:
        buckets_caixa = natureza_buckets_from_linhas_dre(caixa.get("linhas_dre") or [])

    dias = max((data_fim - data_ini).days + 1, 1)
    return (
        _indicadores_from_core(core, caixa_buckets=buckets_caixa, dias_janela=dias),
        None,
    )


def get_indicadores_gerencial_pg(
    empresa_id: int,
    data_inicio: date,
    data_fim: date,
    *,
    por: str = "competencia",
    valor: str = "bruto",
    filtro_contas: str = "",
    var_modo: str = "mes",
    var_por: str = "competencia",
    var_grupo: str = "todas",
) -> dict[str, Any]:
    hoje = timezone.localdate()
    dias_periodo = max((data_fim - data_inicio).days + 1, 1)
    fc = _norm_filtro_contas(filtro_contas)
    por = (por or "competencia").strip().lower()
    valor = (valor or "bruto").strip().lower()

    ref_ini = hoje - timedelta(days=REF_DIAS_COMPARACAO - 1)
    avisos: list[str] = []

    atual, e1 = _bloco_periodo(
        empresa_id, data_inicio, data_fim, por=por, valor=valor, filtro_contas=fc
    )
    ref60, e2 = _bloco_periodo(
        empresa_id, ref_ini, hoje, por=por, valor=valor, filtro_contas=fc
    )
    for e in (e1, e2):
        if e:
            avisos.append(e)

    referencia = _benchmark(ref60, dias_periodo)
    media_60 = ref60["receita_op"] / Decimal(str(REF_DIAS_COMPARACAO))
    previsao_30 = media_60 * Decimal("30")

    from financeiro.services.receita_pdv_util import deposito_pdv_por_empresa_id

    dep_pdv = deposito_pdv_por_empresa_id(empresa_id)
    fat_pdv = _faturamento_pdv_periodo(data_inicio, data_fim, deposito=dep_pdv)
    if fat_pdv.get("ok"):
        labels, serie = _serie_faturamento_7d(data_fim, fat_pdv.get("por_dia") or {})
    else:
        labels, serie = _serie_receita_7d(
            empresa_id, data_fim, por=por, valor=valor, filtro_contas=fc
        )

    atual_paga = dict(atual)
    referencia_paga = dict(referencia)
    cmv_v_atual = {"ok": False, "total": Decimal("0"), "skus_com_custo": 0, "skus_sem_custo": 0}
    cmv_v_60 = {"ok": False, "total": Decimal("0"), "skus_com_custo": 0, "skus_sem_custo": 0}
    try:
        from produtos.relatorios_vendas_util import custo_mercadoria_vendida

        cmv_v_atual = custo_mercadoria_vendida(
            data_inicio, data_fim, deposito=dep_pdv
        )
        cmv_v_60 = custo_mercadoria_vendida(ref_ini, hoje, deposito=dep_pdv)
    except Exception as exc:
        avisos.append(f"CMV vendida indisponível: {exc}")

    k_ref = Decimal(str(dias_periodo)) / Decimal(str(REF_DIAS_COMPARACAO))
    cmv_v_ok = bool(cmv_v_atual.get("ok"))
    if cmv_v_ok:
        atual_vendida = recalc_indicadores_cmv(
            atual_paga, _dec(cmv_v_atual.get("total")), dias_periodo
        )
        referencia_vendida = recalc_indicadores_cmv(
            referencia_paga, _dec(cmv_v_60.get("total")) * k_ref, dias_periodo
        )
    else:
        atual_vendida = dict(atual_paga)
        referencia_vendida = dict(referencia_paga)
    skus_sem = int(cmv_v_atual.get("skus_sem_custo") or 0)
    if cmv_v_ok and skus_sem > 0:
        avisos.append(
            f"{skus_sem} produto(s) vendido(s) sem custo no cadastro — CMV vendida ficou menor."
        )

    modo_ssr = "vendida" if cmv_v_ok else "paga"
    atual = dict(atual_vendida if cmv_v_ok else atual_paga)
    atual["cmv_paga"] = _dec(atual_paga.get("cmv"))
    atual["cmv_vendida"] = _dec(cmv_v_atual.get("total")) if cmv_v_ok else _dec(atual_paga.get("cmv"))
    atual["cmv_modo"] = modo_ssr
    atual["cmv_skus_sem_custo"] = skus_sem if cmv_v_ok else 0
    atual["cmv_skus_com_custo"] = int(cmv_v_atual.get("skus_com_custo") or 0) if cmv_v_ok else 0
    referencia = dict(referencia_vendida if cmv_v_ok else referencia_paga)
    referencia["cmv_paga"] = _dec(referencia_paga.get("cmv"))
    referencia["cmv_vendida"] = (
        _dec(cmv_v_60.get("total")) * k_ref if cmv_v_ok else _dec(referencia_paga.get("cmv"))
    )
    referencia["cmv_modo"] = modo_ssr

    pe_30 = (atual["pe_diario"] or Decimal("0")) * Decimal("30")

    variacao = gastos_variacao_pg(
        empresa_id=empresa_id,
        modo=var_modo,
        por=var_por,
        grupo_filtro=var_grupo,
    )
    if not variacao.get("ok") and variacao.get("erro"):
        avisos.append(str(variacao["erro"]))

    return {
        "atual": atual,
        "referencia": referencia,
        "faturamento_pdv": fat_pdv,
        "cmv_modos": {
            "vendida": {
                "atual": _pack_cmv_js(atual_vendida),
                "ref": _pack_cmv_js(referencia_vendida),
            },
            "paga": {
                "atual": _pack_cmv_js(atual_paga),
                "ref": _pack_cmv_js(referencia_paga),
            },
            "skus_sem_custo": skus_sem if cmv_v_ok else 0,
            "skus_com_custo": int(cmv_v_atual.get("skus_com_custo") or 0) if cmv_v_ok else 0,
        },
        "extras": {
            "previsao_30": previsao_30,
            "tendencia": _tendencia_linear(serie),
            "dicas": _dicas(atual, previsao_30, pe_30),
            "grafico_labels": labels,
            "grafico_data": serie,
        },
        "variacao": variacao,
        "dias_periodo": dias_periodo,
        "data_inicio_atual": data_inicio,
        "data_fim_atual": data_fim,
        "ref_inicio": ref_ini,
        "ref_fim": hoje,
        "meta": {
            "fonte": "sisvale",
            "por": por,
            "valor": valor,
            "filtro_contas": fc,
            "var_modo": var_modo,
            "var_por": var_por,
            "var_grupo": var_grupo,
            "avisos": avisos,
            "ref_dias": REF_DIAS_COMPARACAO,
        },
    }
