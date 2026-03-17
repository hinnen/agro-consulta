from django.db import models


class Estoque(models.Model):
    produto = models.ForeignKey(
        'produtos.Produto',
        on_delete=models.CASCADE,
        related_name='estoques'
    )
    loja = models.ForeignKey(
        'lojas.Loja',
        on_delete=models.CASCADE,
        related_name='estoques'
    )
    saldo = models.DecimalField(max_digits=10, decimal_places=3, default=0)
    estoque_minimo = models.DecimalField(max_digits=10, decimal_places=3, default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Estoque'
        verbose_name_plural = 'Estoques'
        ordering = ['loja__nome', 'produto__nome']
        unique_together = ('produto', 'loja')

    def __str__(self):
        return f'{self.produto.nome} - {self.loja.nome}'


class AjusteRapidoEstoque(models.Model):
    DEPOSITO_CHOICES = [
        ('centro', 'Centro'),
        ('vila', 'Vila Elias'),
    ]

    produto_externo_id = models.CharField(max_length=100, db_index=True)
    codigo_interno = models.CharField(max_length=100, blank=True, default='')
    nome_produto = models.CharField(max_length=255, blank=True, default='')
    deposito = models.CharField(max_length=20, choices=DEPOSITO_CHOICES, db_index=True)

    saldo_erp_referencia = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    saldo_informado = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    diferenca_saldo = models.DecimalField(max_digits=12, decimal_places=3, default=0)

    observacao = models.CharField(max_length=255, blank=True, default='')
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Ajuste Rápido de Estoque'
        verbose_name_plural = 'Ajustes Rápidos de Estoque'
        ordering = ['-criado_em']

    def __str__(self):
        return (
            f'{self.nome_produto} - {self.get_deposito_display()} - '
            f'diferença {self.diferenca_saldo}'
        )