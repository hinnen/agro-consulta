"""DRE gerencial — use ConsolidacaoFinanceiraService como fonte numérica."""

from financeiro.services.consolidacao import ConsolidacaoFinanceiraService


def resumo_dre_empresa(empresa_id, data_inicio, data_fim):
    return ConsolidacaoFinanceiraService().consolidar_empresa(
        empresa_id, data_inicio, data_fim
    )


def resumo_dre_grupo(grupo_id, data_inicio, data_fim):
    return ConsolidacaoFinanceiraService().consolidar_grupo(
        grupo_id, data_inicio, data_fim
    )
