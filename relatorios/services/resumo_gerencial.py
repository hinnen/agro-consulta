from financeiro.services.consolidacao import ConsolidacaoFinanceiraService


def resumo_operacional_empresa(empresa_id, data_inicio, data_fim):
    return ConsolidacaoFinanceiraService().consolidar_empresa(
        empresa_id, data_inicio, data_fim
    )
