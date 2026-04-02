"""
Mapeamento plano de contas → natureza (importação / conciliação).
Preencher conforme plano real do grupo.
"""

from financeiro.models import LancamentoFinanceiro

NATUREZA_POR_PREFIXO = {
    # Exemplo: ajustar para o plano do cliente
    "3.1": LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL,
    "4.1": LancamentoFinanceiro.NATUREZA_CMV,
}


def sugerir_natureza(codigo_plano: str):
    if not codigo_plano:
        return None
    for prefix, natureza in sorted(NATUREZA_POR_PREFIXO.items(), key=lambda x: -len(x[0])):
        if codigo_plano.startswith(prefix):
            return natureza
    return None
