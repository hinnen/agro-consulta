"""
Dashboard gerencial (Postgres / LancamentoFinanceiro): DRE por competência,
fluxo por data de movimento, comparativo de janelas, insights e série para gráfico.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Sum
from django.utils import timezone

from financeiro.models import LancamentoFinanceiro
from financeiro.services.equilibrio import EquilibrioFinanceiroService


def _zero_map() -> dict[str, Decimal]:
    return {n[0]: Decimal("0") for n in LancamentoFinanceiro.NATUREZAS}


def _totais_por_natureza(qs) -> dict[str, Decimal]:
    totais = _zero_map()
    for row in qs.values("natureza").annotate(total=Sum("valor")):
        totais[row["natureza"]] = row["total"] or Decimal("0")
    return totais


def _serie_receita_operacional_diaria(
    empresa_id: int, data_fim: date, dias: int = 7
) -> tuple[list[str], list[float]]:
    labels: list[str] = []
    valores: list[float] = []
    qs_base = LancamentoFinanceiro.objects.filter(
        empresa_id=empresa_id,
        natureza=LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL,
        eh_interno_grupo=False,
    )
    for i in range(dias - 1, -1, -1):
        d = data_fim - timedelta(days=i)
        s = qs_base.filter(data_competencia=d).aggregate(t=Sum("valor"))["t"]
        labels.append(d.strftime("%d/%m"))
        valores.append(float(s or 0))
    return labels, valores


def _tendencia_linear_simples(valores: list[float]) -> str:
    if len(valores) < 2:
        return "Estável"
    n = len(valores)
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(valores) / n
    num = sum((xs[i] - mean_x) * (valores[i] - mean_y) for i in range(n))
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return "Estável"
    slope = num / den
    if slope > 0.01:
        return "Alta"
    if slope < -0.01:
        return "Queda"
    return "Estável"


def calcular_indicadores_periodo(
    empresa_id: int, data_inicio: date, data_fim: date
) -> dict:
    """DRE (competência) + caixa (movimento) + derivados."""
    base = dict(
        empresa_id=empresa_id,
        eh_interno_grupo=False,
    )
    qs_dre = LancamentoFinanceiro.objects.filter(
        **base,
        data_competencia__range=[data_inicio, data_fim],
    )
    qs_caixa = LancamentoFinanceiro.objects.filter(
        **base,
        data_movimento__range=[data_inicio, data_fim],
    )
    dre = _totais_por_natureza(qs_dre)
    caixa = _totais_por_natureza(qs_caixa)

    receita_op = dre[LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL]
    receita_nao_op = dre[LancamentoFinanceiro.NATUREZA_RECEITA_NAO_OPERACIONAL]
    receita_total = receita_op + receita_nao_op
    cmv = dre[LancamentoFinanceiro.NATUREZA_CMV]
    df = dre[LancamentoFinanceiro.NATUREZA_DESPESA_FIXA]
    dv = dre[LancamentoFinanceiro.NATUREZA_DESPESA_VARIAVEL]
    desp_fin = dre[LancamentoFinanceiro.NATUREZA_DESPESA_FINANCEIRA]

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

    dias_janela = max((data_fim - data_inicio).days + 1, 1)
    eq = EquilibrioFinanceiroService().calcular(
        receita_op, cmv, df, dv, dias_periodo=dias_janela
    )
    mc_ratio = eq["margem_contribuicao_pct"]
    faturamento_equilibrio = eq["faturamento_equilibrio"]
    pe_diario = eq["faturamento_diario_equilibrio"]
    indice_seguranca_pct = (
        ((receita_op - faturamento_equilibrio) / receita_op * Decimal("100"))
        if receita_op > 0
        else Decimal("0")
    )

    ebitda = margem_contrib - df
    margem_ebitda_pct = (
        (ebitda / receita_op * Decimal("100")) if receita_op > 0 else Decimal("0")
    )
    lucro_operacional = ebitda - desp_fin
    margem_operacional_pct = (
        (lucro_operacional / receita_op * Decimal("100"))
        if receita_op > 0
        else Decimal("0")
    )
    resultado_liquido = lucro_operacional + receita_nao_op
    margem_liquida_pct = (
        (resultado_liquido / receita_total * Decimal("100"))
        if receita_total > 0
        else Decimal("0")
    )

    entradas_caixa = (
        caixa[LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL]
        + caixa[LancamentoFinanceiro.NATUREZA_RECEITA_NAO_OPERACIONAL]
        + caixa[LancamentoFinanceiro.NATUREZA_EMPRESTIMO_ENTRADA]
        + caixa[LancamentoFinanceiro.NATUREZA_APORTE_SOCIO]
    )
    saidas_caixa = (
        caixa[LancamentoFinanceiro.NATUREZA_CMV]
        + caixa[LancamentoFinanceiro.NATUREZA_DESPESA_FIXA]
        + caixa[LancamentoFinanceiro.NATUREZA_DESPESA_VARIAVEL]
        + caixa[LancamentoFinanceiro.NATUREZA_DESPESA_FINANCEIRA]
        + caixa[LancamentoFinanceiro.NATUREZA_EMPRESTIMO_AMORTIZACAO]
        + caixa[LancamentoFinanceiro.NATUREZA_RETIRADA_SOCIO]
    )
    geracao_caixa = entradas_caixa - saidas_caixa

    return {
        "receita_op": receita_op,
        "receita_nao_op": receita_nao_op,
        "receita_total": receita_total,
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
        "margem_ebitda_pct": margem_ebitda_pct,
        "lucro_operacional": lucro_operacional,
        "margem_operacional_pct": margem_operacional_pct,
        "resultado_liquido": resultado_liquido,
        "margem_liquida_pct": margem_liquida_pct,
        "entradas_caixa": entradas_caixa,
        "saidas_caixa": saidas_caixa,
        "geracao_caixa": geracao_caixa,
        "aportes": dre[LancamentoFinanceiro.NATUREZA_APORTE_SOCIO],
        "retiradas": dre[LancamentoFinanceiro.NATUREZA_RETIRADA_SOCIO],
    }


def _build_dicas(
    *,
    receita_op: Decimal,
    df: Decimal,
    mc_ratio: Decimal,
    previsao_30: Decimal,
    pe_30: Decimal,
    cmv: Decimal,
    geracao_caixa: Decimal,
    indice_seguranca_pct: Decimal,
) -> list[dict]:
    dicas: list[dict] = []
    if receita_op > 0:
        if (df / receita_op) > Decimal("0.30"):
            dicas.append(
                {
                    "titulo": "Peso alto de custo fixo",
                    "msg": "Despesas fixas passam de 30% da receita operacional no período. Vale revisar contratos e recorrências.",
                    "nivel": "danger",
                }
            )
        if cmv / receita_op > Decimal("0.70"):
            dicas.append(
                {
                    "titulo": "CMV elevado",
                    "msg": "CMV ultrapassa 70% da receita. Confira precificação, perdas e mix de produtos.",
                    "nivel": "warning",
                }
            )
    if mc_ratio < Decimal("0.25") and receita_op > 0:
        dicas.append(
            {
                "titulo": "Margem de contribuição apertada",
                "msg": "Abaixo de 25% sobre a receita. Revise despesas variáveis e markup.",
                "nivel": "warning",
            }
        )
    if previsao_30 > 0 and pe_30 > 0 and previsao_30 < pe_30:
        dicas.append(
            {
                "titulo": "Projeção x ponto de equilíbrio",
                "msg": "Se o ritmo recente se mantiver, o faturamento em 30 dias pode ficar abaixo do necessário para cobrir fixos (pelo MC atual).",
                "nivel": "danger",
            }
        )
    if geracao_caixa < 0:
        dicas.append(
            {
                "titulo": "Fluxo de caixa líquido negativo",
                "msg": "No período, entradas realizadas foram menores que saídas (por data de movimento). Confira caixa e vencimentos.",
                "nivel": "warning",
            }
        )
    if indice_seguranca_pct < 0 and receita_op > 0:
        dicas.append(
            {
                "titulo": "Abaixo do equilíbrio operacional",
                "msg": "A receita do período está abaixo do faturamento de equilíbrio estimado com base no MC atual.",
                "nivel": "danger",
            }
        )
    return dicas


def _extras_periodo_atual(
    empresa_id: int, dias: int, data_fim: date, bloco_periodo: dict
) -> dict:
    """Previsão simples, dicas e série para gráfico (janela atual)."""
    receita_op = bloco_periodo["receita_op"]
    dias_u = max(dias, 1)
    media_diaria = receita_op / Decimal(dias_u)
    previsao_30 = media_diaria * Decimal("30")
    pe_d = bloco_periodo["pe_diario"] or Decimal("0")
    pe_30 = pe_d * Decimal("30")
    labels, serie = _serie_receita_operacional_diaria(empresa_id, data_fim, dias=7)
    tendencia = _tendencia_linear_simples(serie)
    dicas = _build_dicas(
        receita_op=receita_op,
        df=bloco_periodo["df"],
        mc_ratio=bloco_periodo["mc_ratio"],
        previsao_30=previsao_30,
        pe_30=pe_30,
        cmv=bloco_periodo["cmv"],
        geracao_caixa=bloco_periodo["geracao_caixa"],
        indice_seguranca_pct=bloco_periodo["indice_seguranca_pct"],
    )
    return {
        "media_diaria": media_diaria,
        "previsao_30": previsao_30,
        "pe_30": pe_30,
        "tendencia": tendencia,
        "dicas": dicas,
        "grafico_labels": labels,
        "grafico_data": serie,
    }


def get_dashboard_data(empresa_id: int, dias: int = 60) -> dict:
    hoje = timezone.now().date()
    dias = max(int(dias), 1)
    inicio_atual = hoje - timedelta(days=dias - 1)
    inicio_anterior = inicio_atual - timedelta(days=dias)
    fim_anterior = inicio_atual - timedelta(days=1)

    atual = calcular_indicadores_periodo(empresa_id, inicio_atual, hoje)
    anterior = calcular_indicadores_periodo(empresa_id, inicio_anterior, fim_anterior)
    extras = _extras_periodo_atual(empresa_id, dias, hoje, atual)

    return {
        "atual": atual,
        "anterior": anterior,
        "extras": extras,
        "dias": dias,
        "data_inicio_atual": inicio_atual,
        "data_fim_atual": hoje,
        "data_inicio_anterior": inicio_anterior,
        "data_fim_anterior": fim_anterior,
    }
