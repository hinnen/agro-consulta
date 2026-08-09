"""Resumo gerencial a partir de ``TituloFinanceiroAgro`` (financeiro PG)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from django.conf import settings

from base.models import Empresa
from financeiro.models import GrupoEmpresarial
from financeiro.services.resumo_operacional_mongo import (
    agregar_linhas_dre_em_resumo,
    get_object_or_none_empresa,
)


def _dre_pg(
    *,
    data_inicio,
    data_fim,
    empresa_nome: str | None,
    por: str,
    valor: str,
    filtro_contas: str,
    diagnostico: bool = False,
) -> dict[str, Any]:
    from produtos.lancamentos_financeiro_pg_analytics_util import dre_resumo_simples_pg

    extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    return dre_resumo_simples_pg(
        data_de=data_inicio,
        data_ate=data_fim,
        por=por,
        valor=valor,
        filtro_contas=filtro_contas,
        regex_excluir_extra=extra or None,
        empresa=empresa_nome or None,
        diagnostico=diagnostico,
    )


def consolidar_empresa_pg(
    *,
    empresa_id: int,
    data_inicio,
    data_fim,
    por: str = "competencia",
    valor: str = "bruto",
    filtro_contas: str = "",
    diagnostico: bool = False,
    usar_receita_pdv: bool = True,
) -> dict[str, Any]:
    empresa = get_object_or_none_empresa(empresa_id)
    if not empresa:
        return {"fonte": "postgres", "erro": "Empresa não encontrada", "linhas_dre": []}
    nome = (empresa.nome_fantasia or "").strip()
    if not nome:
        return {
            "fonte": "postgres",
            "erro": "Cadastre o nome fantasia da empresa; ele filtra o campo Empresa dos títulos PG.",
            "linhas_dre": [],
        }

    fc = (filtro_contas or "").strip().lower() or (
        getattr(settings, "DRE_RESULTADO_FILTRO", "resultado") or "resultado"
    )
    if fc not in ("resultado", "resultado_erp", "todas"):
        fc = "resultado"

    raw = _dre_pg(
        data_inicio=data_inicio,
        data_fim=data_fim,
        empresa_nome=nome,
        por=por,
        valor=valor,
        filtro_contas=fc,
        diagnostico=diagnostico,
    )
    if not raw.get("ok"):
        return {
            "fonte": "postgres",
            "erro": raw.get("erro") or "Falha ao ler lançamentos",
            "linhas_dre": [],
        }

    core = agregar_linhas_dre_em_resumo(raw.get("linhas") or [])
    core["fonte"] = "postgres"
    core["empresa_id"] = empresa_id
    core["empresa_nome_filtro"] = nome
    core["periodo_pg"] = {"de": data_inicio, "ate": data_fim}
    core["campo_data_pg"] = raw.get("por")
    core["valor_modo_pg"] = raw.get("valor")
    core["filtro_contas_pg"] = raw.get("filtro_contas")
    core["linhas_dre"] = raw.get("linhas") or []
    core["ajustes_eliminacao"] = {
        "receitas_internas_eliminadas": Decimal("0"),
        "transferencias_internas": Decimal("0"),
    }
    if not usar_receita_pdv:
        core["receita_fonte"] = "lancamentos"
        core["receita_lancamentos"] = core.get("receita_operacional")
        return core

    from financeiro.services.receita_pdv_util import aplicar_receita_pdv_no_resumo

    return aplicar_receita_pdv_no_resumo(
        core, data_inicio, data_fim, empresa_nome=nome
    )


def consolidar_grupo_pg(
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
        return {"fonte": "postgres", "erro": "Grupo não encontrado"}

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
        "receita_lancamentos",
        "receita_fonte",
    )

    por_empresa_limpo: list[dict[str, Any]] = []
    todas_linhas: list[dict[str, Any]] = []

    for v in grupo.empresas_vinculadas.filter(ativo=True).select_related("empresa"):
        sub = consolidar_empresa_pg(
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

    from financeiro.services.receita_pdv_util import somar_resumos_dre_empresas

    consolidado = somar_resumos_dre_empresas(por_empresa_limpo)
    consolidado["ajustes_eliminacao"] = {
        "receitas_internas_eliminadas": Decimal("0"),
        "transferencias_internas": Decimal("0"),
        "observacao": (
            "Consolidado do grupo = soma das empresas (PDV de cada loja). "
            "Sem eliminação automática entre filiais."
        ),
    }
    consolidado["linhas_dre"] = todas_linhas

    return {
        "fonte": "postgres",
        "grupo_id": grupo.id,
        "grupo_nome": grupo.nome,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "por_empresa": por_empresa_limpo,
        "consolidado": consolidado,
    }
