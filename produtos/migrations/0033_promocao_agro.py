# Generated manually for PromocaoAgro

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0032_fiado_titulo_baixa_evento"),
    ]

    operations = [
        migrations.CreateModel(
            name="PromocaoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=200, verbose_name="Nome")),
                (
                    "tipo",
                    models.CharField(
                        choices=[
                            ("leve_pague", "Leve X, pague Y"),
                            ("acima_unidades", "Acima de X unidades, pague Y"),
                            ("valor_direto", "Valor direto"),
                        ],
                        db_index=True,
                        max_length=20,
                        verbose_name="Tipo",
                    ),
                ),
                (
                    "qtd_x",
                    models.DecimalField(
                        blank=True,
                        decimal_places=3,
                        help_text="Unidades para Leve X ou limiar Acima de X.",
                        max_digits=12,
                        null=True,
                        verbose_name="Quantidade X",
                    ),
                ),
                (
                    "preco_y",
                    models.DecimalField(
                        blank=True,
                        decimal_places=4,
                        help_text="Preço promocional por unidade quando o critério for atendido.",
                        max_digits=12,
                        null=True,
                        verbose_name="Preço Y (por unidade)",
                    ),
                ),
                ("data_inicio", models.DateField(verbose_name="Início")),
                ("data_fim", models.DateField(verbose_name="Fim")),
                (
                    "telas",
                    models.JSONField(
                        blank=True,
                        default=list,
                        help_text='Ex.: ["pdv", "venda_direta", "catalogo"]',
                        verbose_name="Telas",
                    ),
                ),
                (
                    "empresas",
                    models.JSONField(
                        blank=True,
                        default=list,
                        help_text='Ex.: ["centro", "vila"]',
                        verbose_name="Empresas",
                    ),
                ),
                ("ativo", models.BooleanField(db_index=True, default=True, verbose_name="Ativa")),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Promoção",
                "verbose_name_plural": "Promoções",
                "ordering": ["-data_inicio", "-pk"],
            },
        ),
        migrations.CreateModel(
            name="PromocaoProdutoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("produto_externo_id", models.CharField(db_index=True, max_length=64)),
                ("codigo", models.CharField(blank=True, default="", max_length=80)),
                ("nome_produto", models.CharField(blank=True, default="", max_length=300)),
                ("preco_padrao", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                (
                    "preco_promocional",
                    models.DecimalField(
                        blank=True,
                        decimal_places=4,
                        max_digits=12,
                        null=True,
                        verbose_name="Preço promocional (valor direto)",
                    ),
                ),
                (
                    "promocao",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="produtos",
                        to="produtos.promocaoagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Produto da promoção",
                "verbose_name_plural": "Produtos da promoção",
                "ordering": ["codigo", "nome_produto"],
            },
        ),
        migrations.AddConstraint(
            model_name="promocaoprodutoagro",
            constraint=models.UniqueConstraint(
                fields=("promocao", "produto_externo_id"),
                name="uniq_promocao_produto_externo",
            ),
        ),
    ]
