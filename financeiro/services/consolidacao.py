from decimal import Decimal

from financeiro.models import GrupoEmpresarial, LancamentoFinanceiro


class ConsolidacaoFinanceiraService:
    def consolidar_empresa(self, empresa_id, data_inicio, data_fim):
        lancamentos = LancamentoFinanceiro.objects.filter(
            empresa_id=empresa_id,
            data_competencia__range=[data_inicio, data_fim],
        )
        return self._agrupar_lancamentos(lancamentos, eliminar_internos=False)

    def consolidar_grupo(self, grupo_id, data_inicio, data_fim):
        grupo = GrupoEmpresarial.objects.get(pk=grupo_id, ativo=True)
        empresas_ids = list(
            grupo.empresas_vinculadas.filter(ativo=True).values_list(
                "empresa_id", flat=True
            )
        )

        lancamentos = LancamentoFinanceiro.objects.filter(
            empresa_id__in=empresas_ids,
            data_competencia__range=[data_inicio, data_fim],
        )

        consolidado = self._agrupar_lancamentos(lancamentos, eliminar_internos=True)

        por_empresa = []
        for eid in empresas_ids:
            lanc_empresa = lancamentos.filter(empresa_id=eid)
            por_empresa.append(
                {
                    "empresa_id": eid,
                    **self._agrupar_lancamentos(lanc_empresa, eliminar_internos=False),
                }
            )

        return {
            "grupo_id": grupo.id,
            "grupo_nome": grupo.nome,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "por_empresa": por_empresa,
            "consolidado": consolidado,
        }

    def _agrupar_lancamentos(self, qs, eliminar_internos=True):
        receita_operacional = Decimal("0")
        receita_nao_operacional = Decimal("0")
        cmv = Decimal("0")
        despesas_fixas = Decimal("0")
        despesas_variaveis = Decimal("0")
        despesas_financeiras = Decimal("0")
        emprestimos_entrada = Decimal("0")
        amortizacao_emprestimos = Decimal("0")
        aportes_socios = Decimal("0")
        retiradas_socios = Decimal("0")
        transferencias_internas = Decimal("0")
        receitas_internas_eliminadas = Decimal("0")

        for lanc in qs.iterator():
            if eliminar_internos and lanc.eh_interno_grupo:
                if lanc.natureza == LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL:
                    receitas_internas_eliminadas += lanc.valor
                elif lanc.natureza == LancamentoFinanceiro.NATUREZA_TRANSFERENCIA_INTERNA:
                    transferencias_internas += lanc.valor
                continue

            if lanc.natureza == LancamentoFinanceiro.NATUREZA_RECEITA_OPERACIONAL:
                receita_operacional += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_RECEITA_NAO_OPERACIONAL:
                receita_nao_operacional += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_CMV:
                cmv += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_DESPESA_FIXA:
                despesas_fixas += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_DESPESA_VARIAVEL:
                despesas_variaveis += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_DESPESA_FINANCEIRA:
                despesas_financeiras += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_EMPRESTIMO_ENTRADA:
                emprestimos_entrada += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_EMPRESTIMO_AMORTIZACAO:
                amortizacao_emprestimos += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_APORTE_SOCIO:
                aportes_socios += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_RETIRADA_SOCIO:
                retiradas_socios += lanc.valor
            elif lanc.natureza == LancamentoFinanceiro.NATUREZA_TRANSFERENCIA_INTERNA:
                transferencias_internas += lanc.valor

        lucro_bruto = receita_operacional - cmv
        resultado_operacional = (
            receita_operacional - cmv - despesas_fixas - despesas_variaveis
        )
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
                "receitas_internas_eliminadas": receitas_internas_eliminadas,
                "transferencias_internas": transferencias_internas,
            },
        }
