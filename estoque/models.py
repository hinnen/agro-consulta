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

# NOVO MODELO PARA OS PARÂMETROS DA PLANILHA
class ConfiguracaoProduto(models.Model):
    codigo_interno = models.CharField(max_length=100, unique=True)
    estoque_seguranca = models.DecimalField(max_digits=10, decimal_places=3, default=1)
    estoque_maximo_centro = models.DecimalField(max_digits=10, decimal_places=3, default=15)

    def __str__(self):
        return f"{self.codigo_interno} (Seg: {self.estoque_seguranca} | Max: {self.estoque_maximo_centro})"