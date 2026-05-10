"""
Dashboard gerencial: Postgres ou Mongo, período configurável (presets como o BI),
comparativo = média dos últimos 60 dias projetada para o mesmo número de dias do período.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.conf import settings
from django.db.models import Sum
from django.utils import timezone

from financeiro.models import LancamentoFinanceiro
from financeiro.services.equilibrio import EquilibrioFinanceiroService

REF_MESES_COMPARACAO_DIAS = 60


def _zero_map() -> dict[str, Decimal]:
    return {n[0]: Decimal("0") for n in LancamentoFinanceiro.NATUREZAS}


def _totais_por_natureza(qs) -> dict[str, Decimal]:
    totais = _zero_map()
    for row in qs.values("natureza").annotate(total=Sum("valor")):
        totais[row["natureza"]] = row["total"] or Decimal("0")
    return totais


def _indicadores_from_componentes(
    *,
    receita_op: Decimal,
    receita_nao_op: Decimal,
    cmv: Decimal,
    df: Decimal,
    dv: Decimal,
    desp_fin: Decimal,
    entradas_caixa: Decimal,
    saidas_caixa: Decimal,
    aportes: Decimal,
    retiradas: Decimal,
    dias_janela: int,
) -> dict:
    receita_total = receita_op + receita_nao_op
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
    eq = EquilibrioFinanceiroService().calcular(
        receita_op, cmv, df, dv, dias_periodo=dias_u
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
        "aportes": aportes,
        "retiradas": retiradas,
    }


def _zeros_indicadores(dias_janela: int) -> dict:
    z = Decimal("0")
    return _indicadores_from_componentes(
        receita_op=z,
        receita_nao_op=z,
        cmv=z,
        df=z,
        dv=z,
        desp_fin=z,
        entradas_caixa=z,
        saidas_caixa=z,
        aportes=z,
        retiradas=z,
        dias_janela=max(dias_janela, 1),
    )


def calcular_indicadores_periodo(
    empresa_id: int, data_inicio: date, data_fim: date
) -> dict:
    base = dict(empresa_id=empresa_id, eh_interno_grupo=False)
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
    NF = LancamentoFinanceiro
    dias_janela = max((data_fim - data_inicio).days + 1, 1)
    entradas_caixa = (
        caixa[NF.NATUREZA_RECEITA_OPERACIONAL]
        + caixa[NF.NATUREZA_RECEITA_NAO_OPERACIONAL]
        + caixa[NF.NATUREZA_EMPRESTIMO_ENTRADA]
        + caixa[NF.NATUREZA_APORTE_SOCIO]
    )
    saidas_caixa = (
        caixa[NF.NATUREZA_CMV]
        + caixa[NF.NATUREZA_DESPESA_FIXA]
        + caixa[NF.NATUREZA_DESPESA_VARIAVEL]
        + caixa[NF.NATUREZA_DESPESA_FINANCEIRA]
        + caixa[NF.NATUREZA_EMPRESTIMO_AMORTIZACAO]
        + caixa[NF.NATUREZA_RETIRADA_SOCIO]
    )
    return _indicadores_from_componentes(
        receita_op=dre[NF.NATUREZA_RECEITA_OPERACIONAL],
        receita_nao_op=dre[NF.NATUREZA_RECEITA_NAO_OPERACIONAL],
        cmv=dre[NF.NATUREZA_CMV],
        df=dre[NF.NATUREZA_DESPESA_FIXA],
        dv=dre[NF.NATUREZA_DESPESA_VARIAVEL],
        desp_fin=dre[NF.NATUREZA_DESPESA_FINANCEIRA],
        entradas_caixa=entradas_caixa,
        saidas_caixa=saidas_caixa,
        aportes=dre[NF.NATUREZA_APORTE_SOCIO],
        retiradas=dre[NF.NATUREZA_RETIRADA_SOCIO],
        dias_janela=dias_janela,
    )


def calcular_indicadores_periodo_mongo(
    db,
    empresa_id: int,
    data_inicio: date,
    data_fim: date,
    *,
    por: str,
    valor: str,
    filtro_contas: str,
) -> dict:
    from financeiro.services.resumo_operacional_mongo import (
        consolidar_empresa_mongo,
        natureza_buckets_from_linhas_dre,
    )

    NF = LancamentoFinanceiro
    dias_janela = max((data_fim - data_inicio).days + 1, 1)
    core = consolidar_empresa_mongo(
        db,
        empresa_id=empresa_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
        por=por,
        valor=valor,
        filtro_contas=filtro_contas or "",
    )
    if core.get("erro"):
        out = _zeros_indicadores(dias_janela)
        out["_erro"] = str(core["erro"])
        return out

    caixa_core = consolidar_empresa_mongo(
        db,
        empresa_id=empresa_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
        por="pagamento",
        valor="realizado",
        filtro_contas=filtro_contas or "",
    )
    if caixa_core.get("erro"):
        buckets_caixa = natureza_buckets_from_linhas_dre([])
    else:
        buckets_caixa = natureza_buckets_from_linhas_dre(
            caixa_core.get("linhas_dre") or []
        )

    entradas_caixa = (
        buckets_caixa[NF.NATUREZA_RECEITA_OPERACIONAL]
        + buckets_caixa[NF.NATUREZA_RECEITA_NAO_OPERACIONAL]
        + buckets_caixa[NF.NATUREZA_EMPRESTIMO_ENTRADA]
        + buckets_caixa[NF.NATUREZA_APORTE_SOCIO]
    )
    saidas_caixa = (
        buckets_caixa[NF.NATUREZA_CMV]
        + buckets_caixa[NF.NATUREZA_DESPESA_FIXA]
        + buckets_caixa[NF.NATUREZA_DESPESA_VARIAVEL]
        + buckets_caixa[NF.NATUREZA_DESPESA_FINANCEIRA]
        + buckets_caixa[NF.NATUREZA_EMPRESTIMO_AMORTIZACAO]
        + buckets_caixa[NF.NATUREZA_RETIRADA_SOCIO]
    )

    return _indicadores_from_componentes(
        receita_op=Decimal(core["receita_operacional"]),
        receita_nao_op=Decimal(core["receita_nao_operacional"]),
        cmv=Decimal(core["cmv"]),
        df=Decimal(core["despesas_fixas"]),
        dv=Decimal(core["despesas_variaveis"]),
        desp_fin=Decimal(core["despesas_financeiras"]),
        entradas_caixa=entradas_caixa,
        saidas_caixa=saidas_caixa,
        aportes=Decimal(core["aportes_socios"]),
        retiradas=Decimal(core["retiradas_socios"]),
        dias_janela=dias_janela,
    )


def _fetch_indicadores(
    *,
    fonte: str,
    mongo_db,
    empresa_id: int,
    data_inicio: date,
    data_fim: date,
    por: str,
    valor: str,
    fc: str,
) -> dict:
    dias_j = max((data_fim - data_inicio).days + 1, 1)
    if fonte == "mongo":
        if mongo_db is None:
            return _zeros_indicadores(dias_j)
        return calcular_indicadores_periodo_mongo(
            mongo_db,
            empresa_id,
            data_inicio,
            data_fim,
            por=por,
            valor=valor,
            filtro_contas=fc,
        )
    return calcular_indicadores_periodo(empresa_id, data_inicio, data_fim)


def _benchmark_media_60(
    ref60: dict, dias_periodo: int, dias_ref: int = REF_MESES_COMPARACAO_DIAS
) -> dict:
    """Escala os totais dos últimos `dias_ref` dias para `dias_periodo` e recalcula indicadores."""
    k = Decimal(dias_periodo) / Decimal(max(dias_ref, 1))
    return _indicadores_from_componentes(
        receita_op=ref60["receita_op"] * k,
        receita_nao_op=ref60["receita_nao_op"] * k,
        cmv=ref60["cmv"] * k,
        df=ref60["df"] * k,
        dv=ref60["dv"] * k,
        desp_fin=ref60["desp_fin"] * k,
        entradas_caixa=ref60["entradas_caixa"] * k,
        saidas_caixa=ref60["saidas_caixa"] * k,
        aportes=ref60["aportes"] * k,
        retiradas=ref60["retiradas"] * k,
        dias_janela=max(dias_periodo, 1),
    )


def _serie_grafico_receita(
    *,
    fonte: str,
    mongo_db,
    empresa_id: int,
    data_inicio: date,
    data_fim: date,
    por: str,
    valor: str,
    filtro_contas: str,
    max_days: int = 7,
) -> tuple[list[str], list[float]]:
    from financeiro.services.resumo_operacional_mongo import consolidar_empresa_mongo

    start_d = max(data_inicio, data_fim - timedelta(days=max_days - 1))
    labels: list[str] = []
    valores: list[float] = []
    d = start_d
    while d <= data_fim:
        if fonte == "postgres":
            qs = LancamentoFinanceiro.objects.filter(
                empresa_id=empresa_id,
                natureza=LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL,
                eh_interno_grupo=False,
                data_competencia=d,
            )
            agg = qs.aggregate(t=Sum("valor"))["t"]
            v = float(agg or 0)
        elif mongo_db is not None:
            sub = consolidar_empresa_mongo(
                mongo_db,
                empresa_id=empresa_id,
                data_inicio=d,
                data_fim=d,
                por=por,
                valor=valor,
                filtro_contas=filtro_contas or "",
            )
            v = (
                float(sub.get("receita_operacional") or 0)
                if not sub.get("erro")
                else 0.0
            )
        else:
            v = 0.0
        labels.append(d.strftime("%d/%m"))
        valores.append(v)
        d += timedelta(days=1)
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
                "msg": "A média dos últimos 60 dias, projetada em 30 dias, fica abaixo do patamar de equilíbrio estimado para o período exibido.",
                "nivel": "danger",
            }
        )
    if geracao_caixa < 0:
        dicas.append(
            {
                "titulo": "Fluxo de caixa líquido negativo",
                "msg": "No período, as entradas classificadas ficaram abaixo das saídas no recorte de caixa.",
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
    empresa_id: int,
    dias_periodo: int,
    data_inicio: date,
    data_fim: date,
    bloco_periodo: dict,
    ref60_bruto: dict,
    *,
    fonte: str,
    mongo_db,
    por: str,
    valor: str,
    filtro_contas: str,
) -> dict:
    receita_op = bloco_periodo["receita_op"]
    dias_u = max(dias_periodo, 1)
    media_periodo = receita_op / Decimal(dias_u)
    media_60 = ref60_bruto["receita_op"] / Decimal(str(REF_MESES_COMPARACAO_DIAS))
    previsao_30 = media_60 * Decimal("30")
    pe_d = bloco_periodo["pe_diario"] or Decimal("0")
    pe_30 = pe_d * Decimal("30")
    labels, serie = _serie_grafico_receita(
        fonte=fonte,
        mongo_db=mongo_db,
        empresa_id=empresa_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
        por=por,
        valor=valor,
        filtro_contas=filtro_contas,
        max_days=7,
    )
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
        "media_diaria_periodo": media_periodo,
        "media_diaria_60": media_60,
        "previsao_30": previsao_30,
        "pe_30": pe_30,
        "tendencia": tendencia,
        "dicas": dicas,
        "grafico_labels": labels,
        "grafico_data": serie,
    }


def _pop_erro(bloco: dict) -> tuple[dict, str | None]:
    b = dict(bloco)
    err = b.pop("_erro", None)
    return b, err


def _norm_filtro_contas(raw: str) -> str:
    fc = (raw or "").strip().lower() or (
        getattr(settings, "DRE_RESULTADO_FILTRO", "resultado") or "resultado"
    )
    if fc not in ("resultado", "resultado_erp", "todas"):
        fc = "resultado"
    return fc


def get_dashboard_data(
    empresa_id: int,
    data_inicio: date,
    data_fim: date,
    *,
    fonte: str = "mongo",
    por: str = "competencia",
    valor: str = "bruto",
    filtro_contas: str = "",
    mongo_db=None,
) -> dict:
    hoje = timezone.localdate()
    dias_periodo = max((data_fim - data_inicio).days + 1, 1)

    ref_ini = hoje - timedelta(days=REF_MESES_COMPARACAO_DIAS - 1)
    ref_fim = hoje

    fonte = (fonte or "mongo").strip().lower()
    if fonte not in ("mongo", "postgres"):
        fonte = "mongo"
    por = (por or "competencia").strip().lower()
    if por not in ("competencia", "vencimento", "pagamento"):
        por = "competencia"
    valor = (valor or "bruto").strip().lower()
    if valor not in ("bruto", "realizado"):
        valor = "bruto"
    fc = _norm_filtro_contas(filtro_contas)

    avisos: list[str] = []

    if fonte == "mongo" and mongo_db is None:
        avisos.append("Mongo indisponível — não foi possível carregar os dados do ERP.")

    atual = _fetch_indicadores(
        fonte=fonte,
        mongo_db=mongo_db,
        empresa_id=empresa_id,
        data_inicio=data_inicio,
        data_fim=data_fim,
        por=por,
        valor=valor,
        fc=fc,
    )
    ref60 = _fetch_indicadores(
        fonte=fonte,
        mongo_db=mongo_db,
        empresa_id=empresa_id,
        data_inicio=ref_ini,
        data_fim=ref_fim,
        por=por,
        valor=valor,
        fc=fc,
    )

    atual, e1 = _pop_erro(atual)
    ref60, e2 = _pop_erro(ref60)
    for e in (e1, e2):
        if e:
            avisos.append(e)

    referencia = _benchmark_media_60(ref60, dias_periodo)

    extras = _extras_periodo_atual(
        empresa_id,
        dias_periodo,
        data_inicio,
        data_fim,
        atual,
        ref60,
        fonte=fonte,
        mongo_db=mongo_db,
        por=por,
        valor=valor,
        filtro_contas=fc,
    )

    return {
        "atual": atual,
        "referencia": referencia,
        "ref60_bruto": ref60,
        "extras": extras,
        "dias_periodo": dias_periodo,
        "data_inicio_atual": data_inicio,
        "data_fim_atual": data_fim,
        "ref60_inicio": ref_ini,
        "ref60_fim": ref_fim,
        "meta": {
            "fonte": fonte,
            "por": por,
            "valor": valor,
            "filtro_contas": fc,
            "avisos": avisos,
            "ref_dias": REF_MESES_COMPARACAO_DIAS,
        },
    }
