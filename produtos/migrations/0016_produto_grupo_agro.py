# Generated manually for ProdutoGrupoAgro / ProdutoGrupoVarianteAgro

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models
from django.db.models import Q


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0015_mp_point_order_abandoned_status"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProdutoGrupoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=300, verbose_name="Nome do produto")),
                ("preco_venda", models.DecimalField(decimal_places=2, max_digits=12, verbose_name="Preço de venda")),
                ("ativo", models.BooleanField(default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="produto_grupos_agro",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Grupo de produto (Agro)",
                "verbose_name_plural": "Grupos de produto (Agro)",
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="ProdutoGrupoVarianteAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("marca", models.CharField(max_length=120)),
                ("codigo_barras", models.CharField(max_length=80)),
                (
                    "produto_erp_id",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        default="",
                        max_length=64,
                        verbose_name="ID produto ERP/Mongo",
                    ),
                ),
                (
                    "grupo",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="variantes",
                        to="produtos.produtogrupoagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Variante (marca / código de barras)",
                "verbose_name_plural": "Variantes (marca / código de barras)",
                "ordering": ["id"],
            },
        ),
        migrations.AddConstraint(
            model_name="produtogrupovarianteagro",
            constraint=models.UniqueConstraint(
                fields=("grupo", "marca"),
                name="uniq_prod_grupo_variante_marca_por_grupo",
            ),
        ),
        migrations.AddConstraint(
            model_name="produtogrupovarianteagro",
            constraint=models.UniqueConstraint(
                condition=~Q(codigo_barras=""),
                fields=("codigo_barras",),
                name="uniq_prod_grupo_variante_codigo_barras",
            ),
        ),
    ]
