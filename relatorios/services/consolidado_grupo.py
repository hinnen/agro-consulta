from financeiro.services.consolidacao import ConsolidacaoFinanceiraService


def consolidado_grupo(grupo_id, data_inicio, data_fim):
    return ConsolidacaoFinanceiraService().consolidar_grupo(
        grupo_id, data_inicio, data_fim
    )
