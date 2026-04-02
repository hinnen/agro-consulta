"""
Agrega o DRE simples do Mongo (DtoLancamento, espelho do ERP) nas naturezas do resumo gerencial.

O mapeamento plano → natureza é heurístico (palavras-chave no nome do plano). Ajuste conforme o seu plano de contas.
"""

from __future__ import annotations

import logging
import unicodedata
from collections import defaultdict
from decimal import Decimal
from typing import Any

_log_diag = logging.getLogger("financeiro.resumo_diagnostico")

from django.conf import settings

from base.models import Empresa
from financeiro.models import GrupoEmpresarial, LancamentoFinanceiro as NF


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def _match_any(folded: str, patterns: tuple[str, ...]) -> bool:
    return any(p in folded for p in patterns)


def classificar_receita_plano(nome_plano: str) -> str:
    f = _fold(nome_plano)
    if _match_any(
        f,
        (
            "emprestimo",
            "empréstimo",
            "credito bancario",
            "crédito bancário",
            "capital de giro",
            "antecipacao",
            "antecipação",
        ),
    ) and "juros" not in f:
        return NF.NATUREZA_EMPRESTIMO_ENTRADA
    if _match_any(f, ("aporte", "integralizacao", "integralização", "subscricao", "subscrição")):
        return NF.NATUREZA_APORTE_SOCIO
    if _match_any(
        f,
        (
            "nao operacional",
            "não operacional",
            "outras receitas",
            "receita financeira",
            "rendimento aplicacao",
            "rendimento aplicação",
        ),
    ):
        return NF.NATUREZA_RECEITA_NAO_OPERACIONAL
    return NF.NATUREZA_RECEITA_OPERACIONAL


def classificar_despesa_plano(nome_plano: str) -> str:
    f = _fold(nome_plano)
    if _match_any(
        f,
        (
            "juros",
            "encargo financeiro",
            "tarifa banc",
            "iof",
            "multa banc",
            "taxa banc",
            "desconto obtido",  # às vezes financeiro — conservador: financeira
        ),
    ):
        return NF.NATUREZA_DESPESA_FINANCEIRA
    if _match_any(
        f,
        (
            "amortizacao emprestimo",
            "amortização empréstimo",
            "pagamento emprestimo",
            "pagamento empréstimo",
            "principal emprestimo",
            "principal empréstimo",
            "liquidacao emprestimo",
            "liquidação empréstimo",
        ),
    ):
        return NF.NATUREZA_EMPRESTIMO_AMORTIZACAO
    if _match_any(
        f,
        (
            "retirada",
            "pro-labore extraordin",
            "pró-labore extraordin",
            "distribuicao lucro",
            "distribuição lucro",
        ),
    ):
        return NF.NATUREZA_RETIRADA_SOCIO
    if _match_any(
        f,
        (
            "compra mercador",
            "cmv",
            "custo mercador",
            "custo venda",
            "mercadoria revenda",
            "estoque",
        ),
    ):
        return NF.NATUREZA_CMV
    if _match_any(
        f,
        (
            "comissao",
            "comissão",
            "taxa maquina",
            "taxa máquina",
            "cartao",
            "cartão",
            "embalagem",
            "frete",
            "publicidade",
            "marketing",
        ),
    ):
        return NF.NATUREZA_DESPESA_VARIAVEL
    return NF.NATUREZA_DESPESA_FIXA


def _buckets_vazios() -> dict[str, Decimal]:
    return {
        NF.NATUREZA_RECEITA_OPERACIONAL: Decimal("0"),
        NF.NATUREZA_RECEITA_NAO_OPERACIONAL: Decimal("0"),
        NF.NATUREZA_CMV: Decimal("0"),
        NF.NATUREZA_DESPESA_FIXA: Decimal("0"),
        NF.NATUREZA_DESPESA_VARIAVEL: Decimal("0"),
        NF.NATUREZA_DESPESA_FINANCEIRA: Decimal("0"),
        NF.NATUREZA_EMPRESTIMO_ENTRADA: Decimal("0"),
        NF.NATUREZA_EMPRESTIMO_AMORTIZACAO: Decimal("0"),
        NF.NATUREZA_APORTE_SOCIO: Decimal("0"),
        NF.NATUREZA_RETIRADA_SOCIO: Decimal("0"),
        NF.NATUREZA_TRANSFERENCIA_INTERNA: Decimal("0"),
    }


def agregar_linhas_dre_em_resumo(linhas: list[dict[str, Any]]) -> dict[str, Any]:
    b = _buckets_vazios()
    for linha in linhas:
        plano = str(linha.get("plano") or "")
        rec = _dec(linha.get("receita"))
        des = _dec(linha.get("despesa"))
        if rec > 0:
            nat = classificar_receita_plano(plano)
            b[nat] = b.get(nat, Decimal("0")) + rec
        if des > 0:
            nat = classificar_despesa_plano(plano)
            b[nat] = b.get(nat, Decimal("0")) + des

    receita_operacional = b[NF.NATUREZA_RECEITA_OPERACIONAL]
    receita_nao_operacional = b[NF.NATUREZA_RECEITA_NAO_OPERACIONAL]
    cmv = b[NF.NATUREZA_CMV]
    despesas_fixas = b[NF.NATUREZA_DESPESA_FIXA]
    despesas_variaveis = b[NF.NATUREZA_DESPESA_VARIAVEL]
    despesas_financeiras = b[NF.NATUREZA_DESPESA_FINANCEIRA]
    emprestimos_entrada = b[NF.NATUREZA_EMPRESTIMO_ENTRADA]
    amortizacao_emprestimos = b[NF.NATUREZA_EMPRESTIMO_AMORTIZACAO]
    aportes_socios = b[NF.NATUREZA_APORTE_SOCIO]
    retiradas_socios = b[NF.NATUREZA_RETIRADA_SOCIO]
    transferencias_internas = b[NF.NATUREZA_TRANSFERENCIA_INTERNA]

    lucro_bruto = receita_operacional - cmv
    resultado_operacional = receita_operacional - cmv - despesas_fixas - despesas_variaveis
    resultado_liquido_gerencial = resultado_operacional - despesas_financeiras
    geracao_caixa = (
        resultado_liquido_gerencial
        + emprestimos_entrada
        + aportes_socios
        - amortizacao_emprestimos
        - retiradas_socios
    )

    return {
        "receita_operacional": receita_operacional,
        "receita_nao_operacional": receita_nao_operacional,
        "cmv": cmv,
        "lucro_bruto": lucro_bruto,
        "despesas_fixas": despesas_fixas,
        "despesas_variaveis": despesas_variaveis,
        "despesas_financeiras": despesas_financeiras,
        "resultado_operacional": resultado_operacional,
        "resultado_liquido_gerencial": resultado_liquido_gerencial,
        "emprestimos_entrada": emprestimos_entrada,
        "amortizacao_emprestimos": amortizacao_emprestimos,
        "aportes_socios": aportes_socios,
        "retiradas_socios": retiradas_socios,
        "geracao_caixa": geracao_caixa,
        "ajustes_eliminacao": {
            "receitas_internas_eliminadas": Decimal("0"),
            "transferencias_internas": transferencias_internas,
            "observacao_mongo": (
                "Dados do Mongo (DtoLancamento). Sem eliminação de vendas entre empresas do grupo; "
                "mapeamento por nome do plano de contas."
            ),
        },
    }


def _debug_totais_por_natureza(linhas: list[dict[str, Any]]) -> dict[str, float]:
    """Só para diagnóstico: mesma lógica de classificação de agregar_linhas_dre_em_resumo."""
    b: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for linha in linhas:
        plano = str(linha.get("plano") or "")
        rec = _dec(linha.get("receita"))
        des = _dec(linha.get("despesa"))
        if rec > 0:
            nat = classificar_receita_plano(plano)
            b[nat] = b[nat] + rec
        if des > 0:
            nat = classificar_despesa_plano(plano)
            b[nat] = b[nat] + des
    return {k: float(v.quantize(Decimal("0.01"))) for k, v in sorted(b.items())}


def _dre_mongo(
    db,
    *,
    data_inicio,
    data_fim,
    empresa_nome: str | None,
    empresa_mongo_id: str | None,
    por: str,
    valor: str,
    filtro_contas: str,
    diagnostico: bool = False,
):
    from produtos.mongo_financeiro_util import dre_resumo_simples_mongo

    extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    return dre_resumo_simples_mongo(
        db,
        data_de=data_inicio,
        data_ate=data_fim,
        por=por,
        valor=valor,
        filtro_contas=filtro_contas,
        regex_excluir_extra=extra or None,
        empresa=empresa_nome or None,
        empresa_id=empresa_mongo_id,
        diagnostico=diagnostico,
    )


def consolidar_empresa_mongo(
    db,
    *,
    empresa_id: int,
    data_inicio,
    data_fim,
    por: str = "competencia",
    valor: str = "bruto",
    filtro_contas: str = "",
    diagnostico: bool = False,
) -> dict[str, Any]:
    empresa = get_object_or_none_empresa(empresa_id)
    if not empresa:
        return {"fonte": "mongo", "erro": "Empresa não encontrada", "linhas_dre": []}
    nome = (empresa.nome_fantasia or "").strip()
    if not nome:
        return {
            "fonte": "mongo",
            "erro": "Cadastre o nome fantasia da empresa; ele é usado para bater com o campo Empresa do Mongo.",
            "linhas_dre": [],
        }
    mongo_eid = str(empresa.pk)
    if diagnostico:
        _log_diag.info(
            "[FINANCEIRO_RESUMO_DIAG] empresa_django resolvida: pk=%s nome_fantasia=%r "
            "razao_social=%r ativo=%s mongo_EmpresaID_enviado=%r filtro_nome_tokens=%r",
            empresa.pk,
            nome,
            (empresa.razao_social or "")[:120],
            empresa.ativo,
            mongo_eid,
            nome.split(),
        )
    fc = (filtro_contas or "").strip().lower() or (
        getattr(settings, "DRE_RESULTADO_FILTRO", "resultado") or "resultado"
    )
    if fc not in ("resultado", "resultado_erp", "todas"):
        fc = "resultado"

    raw = _dre_mongo(
        db,
        data_inicio=data_inicio,
        data_fim=data_fim,
        empresa_nome=nome,
        empresa_mongo_id=mongo_eid,
        por=por,
        valor=valor,
        filtro_contas=fc,
        diagnostico=diagnostico,
    )
    if not raw.get("ok"):
        return {
            "fonte": "mongo",
            "erro": raw.get("erro") or "Falha ao ler Mongo",
            "linhas_dre": [],
        }

    core = agregar_linhas_dre_em_resumo(raw.get("linhas") or [])
    if diagnostico:
        linhas = raw.get("linhas") or []
        _log_diag.info(
            "[FINANCEIRO_RESUMO_DIAG] apos_dre linhas_dre=%s totais_dre=%s "
            "totais_por_natureza(classificacao_plano)=%s",
            len(linhas),
            raw.get("totais"),
            _debug_totais_por_natureza(linhas),
        )
        _log_diag.info(
            "[FINANCEIRO_RESUMO_DIAG] payload_resumo_operacional "
            "receita_op=%s cmv=%s desp_fix=%s desp_var=%s resultado_op=%s",
            core.get("receita_operacional"),
            core.get("cmv"),
            core.get("despesas_fixas"),
            core.get("despesas_variaveis"),
            core.get("resultado_operacional"),
        )
    core["fonte"] = "mongo"
    core["empresa_id"] = empresa_id
    core["empresa_nome_filtro"] = nome
    core["periodo_mongo"] = raw.get("periodo")
    core["campo_data_mongo"] = raw.get("campo_data")
    core["valor_modo_mongo"] = raw.get("valor_modo")
    core["filtro_contas_mongo"] = raw.get("filtro_contas")
    core["linhas_dre"] = raw.get("linhas") or []
    return core


def get_object_or_none_empresa(pk: int):
    try:
        return Empresa.objects.get(pk=pk)
    except Empresa.DoesNotExist:
        return None


def consolidar_grupo_mongo(
    db,
    *,
    grupo_id: int,
    data_inicio,
    data_fim,
    por: str = "competencia",
    valor: str = "bruto",
    filtro_contas: str = "",
    diagnostico: bool = False,
) -> dict[str, Any]:
    grupo = GrupoEmpresarial.objects.filter(pk=grupo_id, ativo=True).first()
    if not grupo:
        return {"fonte": "mongo", "erro": "Grupo não encontrado"}

    vinculos = grupo.empresas_vinculadas.filter(ativo=True).select_related("empresa")
    keys_acumular = (
        "receita_operacional",
        "receita_nao_operacional",
        "cmv",
        "lucro_bruto",
        "despesas_fixas",
        "despesas_variaveis",
        "despesas_financeiras",
        "resultado_operacional",
        "resultado_liquido_gerencial",
        "emprestimos_entrada",
        "amortizacao_emprestimos",
        "aportes_socios",
        "retiradas_socios",
        "geracao_caixa",
    )

    por_empresa_limpo: list[dict[str, Any]] = []
    todas_linhas: list[dict[str, Any]] = []

    for v in vinculos:
        sub = consolidar_empresa_mongo(
            db,
            empresa_id=v.empresa_id,
            data_inicio=data_inicio,
            data_fim=data_fim,
            por=por,
            valor=valor,
            filtro_contas=filtro_contas,
            diagnostico=diagnostico,
        )
        if sub.get("erro"):
            por_empresa_limpo.append({"empresa_id": v.empresa_id, "erro": sub["erro"]})
            continue
        todas_linhas.extend(sub.get("linhas_dre") or [])
        por_empresa_limpo.append(
            {
                "empresa_id": v.empresa_id,
                **{k: sub[k] for k in keys_acumular if k in sub},
            }
        )

    consolidado = agregar_linhas_dre_em_resumo(todas_linhas)
    consolidado["ajustes_eliminacao"] = {
        "receitas_internas_eliminadas": Decimal("0"),
        "transferencias_internas": Decimal("0"),
        "observacao_mongo": (
            "Consolidado grupo = soma das linhas DRE de cada empresa (Mongo). "
            "Não há eliminação automática de vendas entre filiais."
        ),
    }

    return {
        "fonte": "mongo",
        "grupo_id": grupo.id,
        "grupo_nome": grupo.nome,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "por_empresa": por_empresa_limpo,
        "consolidado": consolidado,
    }
