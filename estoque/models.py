from django.db import models

class Estoque(models.Model):
    empresa = models.ForeignKey('base.Empresa', on_delete=models.CASCADE, related_name='estoques', null=True, blank=True)
    produto = models.ForeignKey('produtos.Produto', on_delete=models.CASCADE, related_name='estoques')
    loja = models.ForeignKey('base.Loja', on_delete=models.CASCADE, related_name='estoques')
    saldo = models.DecimalField(max_digits=10, decimal_places=3, default=0)
    estoque_minimo = models.DecimalField(max_digits=10, decimal_places=3, default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Estoque'
        verbose_name_plural = 'Estoques'
        ordering = ['loja__nome', 'produto__nome']
        constraints = [models.UniqueConstraint(fields=['produto', 'loja'], name='unique_produto_loja_estoque')]

class AjusteRapidoEstoque(models.Model):
    empresa = models.ForeignKey('base.Empresa', on_delete=models.CASCADE, null=True, blank=True)
    loja = models.ForeignKey('base.Loja', on_delete=models.CASCADE, null=True, blank=True)
    produto_externo_id = models.CharField(max_length=100, db_index=True)
    codigo_interno = models.CharField(max_length=100, blank=True, default='')
    nome_produto = models.CharField(max_length=255, blank=True, default='')
    deposito = models.CharField(max_length=20, db_index=True)
    saldo_erp_referencia = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    saldo_informado = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    diferenca_saldo = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-criado_em']

    def save(self, *args, **kwargs):
        self.diferenca_saldo = self.saldo_informado - self.saldo_erp_referencia
        super().save(*args, **kwargs)

class ConfiguracaoTransferencia(models.Model):
    produto_externo_id = models.CharField("ID do Produto", max_length=100, unique=True, db_index=True)
    nome_produto = models.CharField("Nome do Produto", max_length=255, blank=True, default='')
    
    # Parâmetros da Fórmula
    venda_media_diaria = models.DecimalField("Venda Média Diária", max_digits=10, decimal_places=3, default=0)
    capacidade_maxima = models.DecimalField("Capacidade Máxima", max_digits=10, decimal_places=3, default=0)
    dias_cobertura = models.IntegerField("Dias de Cobertura", default=1)
    estoque_seguranca = models.DecimalField("Estoque Segurança", max_digits=10, decimal_places=3, default=0)
    
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Configuração de Transferência'
        verbose_name_plural = 'Configurações de Transferências'

    @property
    def capacidade_minima(self):
        """Ponto de Pedido/Transferência"""
        return (self.venda_media_diaria * self.dias_cobertura) + self.estoque_seguranca
