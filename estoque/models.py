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