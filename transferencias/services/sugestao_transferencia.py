from decimal import Decimal

from estoque.services.politica_estoque import PoliticaEstoqueService


class SugestaoTransferenciaService:
    def __init__(self):
        self.politica_service = PoliticaEstoqueService()

    def calcular_para_produto_loja(
        self,
        empresa,
        loja_destino,
        produto,
        saldo_destino,
        venda_media_destino,
        lojas_origem_data,
    ):
        politica_destino = self.politica_service.obter_ou_criar(
            empresa.id, loja_destino.id, produto.id
        )

        estoque_minimo_destino = self.politica_service.calcular_estoque_minimo(
            politica_destino, venda_media_destino
        )
        estoque_ideal_destino = self.politica_service.calcular_estoque_ideal(
            politica_destino, venda_media_destino
        )

        necessidade = max(estoque_ideal_destino - saldo_destino, Decimal("0"))

        if necessidade <= 0:
            return {
                "acao": "OK",
                "estoque_minimo": estoque_minimo_destino,
                "estoque_ideal": estoque_ideal_destino,
                "necessidade": Decimal("0"),
                "qtd_transferir": Decimal("0"),
                "qtd_comprar": Decimal("0"),
                "loja_origem_id": None,
            }

        melhor_origem = None
        melhor_qtd = Decimal("0")

        if politica_destino.permite_transferencia:
            for origem in lojas_origem_data:
                loja_origem = origem["loja"]
                saldo_origem = origem["saldo_atual"]
                venda_media_origem = origem["venda_media_dia"]

                politica_origem = self.politica_service.obter_ou_criar(
                    empresa.id, loja_origem.id, produto.id
                )
                estoque_minimo_origem = self.politica_service.calcular_estoque_minimo(
                    politica_origem, venda_media_origem
                )
                excedente = max(saldo_origem - estoque_minimo_origem, Decimal("0"))

                if excedente > melhor_qtd:
                    melhor_qtd = excedente
                    melhor_origem = loja_origem

        qtd_transferir = (
            min(melhor_qtd, necessidade) if melhor_origem else Decimal("0")
        )
        restante = max(necessidade - qtd_transferir, Decimal("0"))

        if (
            qtd_transferir > 0
            and restante > 0
            and politica_destino.permite_compra
            and venda_media_destino > 0
        ):
            acao = "TRANSFERIR_E_COMPRAR"
        elif qtd_transferir > 0:
            acao = "TRANSFERIR"
        elif restante > 0 and politica_destino.permite_compra and venda_media_destino > 0:
            acao = "COMPRAR"
        else:
            acao = "SEM_ACAO"

        qtd_comprar = (
            restante if acao in ["TRANSFERIR_E_COMPRAR", "COMPRAR"] else Decimal("0")
        )

        return {
            "acao": acao,
            "estoque_minimo": estoque_minimo_destino,
            "estoque_ideal": estoque_ideal_destino,
            "necessidade": necessidade,
            "qtd_transferir": qtd_transferir,
            "qtd_comprar": qtd_comprar,
            "loja_origem_id": getattr(melhor_origem, "id", None),
        }
