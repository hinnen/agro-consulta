from django.db import models


class Produto(models.Model):
    empresa = models.ForeignKey(
        'base.Empresa',
        on_delete=models.CASCADE,
        related_name='produtos',
        verbose_name='Empresa',
        null=True,
        blank=True,
    )
    codigo_interno = models.CharField(max_length=50)
    codigo_barras = models.CharField(max_length=50, blank=True, null=True)
    nome = models.CharField(max_length=200)
    categoria = models.CharField(max_length=100, blank=True, null=True)
    marca = models.CharField(max_length=100, blank=True, null=True)
    unidade = models.CharField(max_length=20, default='UN')
    custo = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    preco_venda = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Produto'
        verbose_name_plural = 'Produtos'
        ordering = ['nome']
        constraints = [
            models.UniqueConstraint(
                fields=['empresa', 'codigo_interno'],
                name='unique_codigo_interno_por_empresa',
            )
        ]

    def __str__(self):
        return f'{self.codigo_interno} - {self.nome}'   