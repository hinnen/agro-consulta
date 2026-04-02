from decimal import Decimal


class EquilibrioFinanceiroService:
    def calcular(
        self,
        receita_operacional,
        cmv,
        despesas_fixas,
        despesas_variaveis,
        dias_periodo=30,
    ):
        receita_operacional = Decimal(receita_operacional or 0)
        cmv = Decimal(cmv or 0)
        despesas_fixas = Decimal(despesas_fixas or 0)
        despesas_variaveis = Decimal(despesas_variaveis or 0)

        if receita_operacional <= 0:
            margem_contribuicao_pct = Decimal("0")
        else:
            margem_contribuicao_pct = (
                receita_operacional - cmv - despesas_variaveis
            ) / receita_operacional

        faturamento_equilibrio = Decimal("0")
        faturamento_diario_equilibrio = Decimal("0")

        if margem_contribuicao_pct > 0:
            faturamento_equilibrio = despesas_fixas / margem_contribuicao_pct
            faturamento_diario_equilibrio = faturamento_equilibrio / Decimal(
                dias_periodo
            )

        return {
            "margem_contribuicao_pct": margem_contribuicao_pct,
            "faturamento_equilibrio": faturamento_equilibrio,
            "faturamento_diario_equilibrio": faturamento_diario_equilibrio,
        }
