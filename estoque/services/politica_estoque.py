from decimal import Decimal

from estoque.models import PoliticaEstoque


class PoliticaEstoqueService:
    def obter_ou_criar(self, empresa_id, loja_id, produto_id):
        politica, _ = PoliticaEstoque.objects.get_or_create(
            empresa_id=empresa_id,
            loja_id=loja_id,
            produto_id=produto_id,
            defaults={
                "estoque_seguranca": Decimal("0"),
                "dias_cobertura": Decimal("15"),
                "permite_transferencia": True,
                "permite_compra": True,
                "prioridade_manual": 0,
            },
        )
        return politica

    def calcular_estoque_minimo(self, politica, venda_media_dia):
        if politica.estoque_minimo_manual is not None:
            return politica.estoque_minimo_manual
        return (venda_media_dia * politica.dias_cobertura) + politica.estoque_seguranca

    def calcular_estoque_ideal(self, politica, venda_media_dia):
        if politica.estoque_ideal_manual is not None:
            ideal = politica.estoque_ideal_manual
        else:
            ideal = self.calcular_estoque_minimo(politica, venda_media_dia)

        if politica.capacidade_maxima is not None:
            ideal = min(ideal, politica.capacidade_maxima)

        return ideal
