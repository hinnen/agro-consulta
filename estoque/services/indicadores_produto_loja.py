from decimal import Decimal

from estoque.models import IndicadorProdutoLoja
from estoque.services.politica_estoque import PoliticaEstoqueService
from transferencias.services.sugestao_transferencia import SugestaoTransferenciaService


class IndicadoresProdutoLojaService:
    def __init__(self):
        self.politica_service = PoliticaEstoqueService()
        self.transferencia_service = SugestaoTransferenciaService()

    def calcular_dias_cobertura(self, saldo_atual, venda_media_dia):
        if venda_media_dia and venda_media_dia > 0:
            return saldo_atual / venda_media_dia
        return Decimal("9999")

    def calcular_margem_bruta_pct(self, preco_venda, custo_medio):
        if not preco_venda or preco_venda <= 0:
            return Decimal("0")
        return ((preco_venda - custo_medio) / preco_venda) * Decimal("100")

    def calcular_score_prioridade(
        self,
        saldo_atual,
        dias_cobertura_atual,
        venda_media_dia,
        margem_bruta_pct,
        dias_sem_venda,
        classe_abc,
        prioridade_manual=0,
        eh_granel=False,
        contexto_reposicao=True,
    ):
        score = Decimal("0")

        if saldo_atual <= 0:
            score += Decimal("100")

        if dias_cobertura_atual < 3:
            score += Decimal("40")
        elif dias_cobertura_atual < 7:
            score += Decimal("20")
        elif dias_cobertura_atual < 15:
            score += Decimal("10")

        score += min(Decimal(venda_media_dia) * Decimal("5"), Decimal("30"))

        if margem_bruta_pct >= 35:
            score += Decimal("15")
        elif margem_bruta_pct >= 25:
            score += Decimal("8")

        if dias_sem_venda > 60:
            score -= Decimal("20")
        elif dias_sem_venda > 30:
            score -= Decimal("10")

        if classe_abc == "A":
            score += Decimal("20")
        elif classe_abc == "B":
            score += Decimal("10")

        if eh_granel and not contexto_reposicao:
            score -= Decimal("40")

        score += Decimal(prioridade_manual or 0)
        return score

    def upsert_snapshot(
        self,
        empresa,
        loja,
        produto,
        data_base,
        saldo_atual,
        venda_media_dia,
        dias_sem_venda,
        custo_medio,
        preco_venda,
        classe_abc,
        eh_granel,
        lojas_origem_data,
    ):
        politica = self.politica_service.obter_ou_criar(empresa.id, loja.id, produto.id)

        estoque_minimo = self.politica_service.calcular_estoque_minimo(
            politica, venda_media_dia
        )
        estoque_ideal = self.politica_service.calcular_estoque_ideal(
            politica, venda_media_dia
        )
        necessidade = max(estoque_ideal - saldo_atual, Decimal("0"))
        dias_cobertura_atual = self.calcular_dias_cobertura(saldo_atual, venda_media_dia)
        margem_bruta_pct = self.calcular_margem_bruta_pct(preco_venda, custo_medio)

        sugestao = self.transferencia_service.calcular_para_produto_loja(
            empresa=empresa,
            loja_destino=loja,
            produto=produto,
            saldo_destino=saldo_atual,
            venda_media_destino=venda_media_dia,
            lojas_origem_data=lojas_origem_data,
        )

        score = self.calcular_score_prioridade(
            saldo_atual=saldo_atual,
            dias_cobertura_atual=dias_cobertura_atual,
            venda_media_dia=venda_media_dia,
            margem_bruta_pct=margem_bruta_pct,
            dias_sem_venda=dias_sem_venda,
            classe_abc=classe_abc,
            prioridade_manual=politica.prioridade_manual,
            eh_granel=eh_granel,
            contexto_reposicao=True,
        )

        obj, _ = IndicadorProdutoLoja.objects.update_or_create(
            empresa=empresa,
            loja=loja,
            produto=produto,
            data_base=data_base,
            defaults={
                "saldo_atual": saldo_atual,
                "venda_media_dia": venda_media_dia,
                "dias_sem_venda": dias_sem_venda,
                "dias_cobertura_atual": dias_cobertura_atual,
                "estoque_minimo": estoque_minimo,
                "estoque_ideal": estoque_ideal,
                "necessidade": necessidade,
                "custo_medio": custo_medio,
                "preco_venda": preco_venda,
                "margem_bruta_pct": margem_bruta_pct,
                "score_prioridade": score,
                "classe_abc": classe_abc,
                "classe_criticidade": self._classificar_criticidade(
                    necessidade, dias_cobertura_atual
                ),
                "sugestao_acao": sugestao["acao"],
                "qtd_transferir": sugestao["qtd_transferir"],
                "qtd_comprar": sugestao["qtd_comprar"],
                "loja_origem_sugerida_id": sugestao["loja_origem_id"],
            },
        )
        return obj

    def _classificar_criticidade(self, necessidade, dias_cobertura_atual):
        if necessidade > 0 and dias_cobertura_atual < 3:
            return "RUPTURA_IMINENTE"
        if necessidade > 0 and dias_cobertura_atual < 7:
            return "ALTA"
        if necessidade > 0:
            return "MEDIA"
        return "OK"
