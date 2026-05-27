# Generated manually for caixa reforço/retirada e conferência por forma

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0023_produtogestaooverlayagro_subcategorias_niveis"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="sessaocaixa",
            name="conferencia_fechamento",
            field=models.JSONField(
                blank=True,
                help_text="Conferência por forma: {forma: {esperado, contado, diferenca}}.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="vendaagro",
            name="pagamentos_json",
            field=models.JSONField(
                blank=True,
                help_text="Parcelas por forma [{forma, valor}] quando a venda tem mais de um pagamento.",
                null=True,
            ),
        ),
        migrations.CreateModel(
            name="MovimentoCaixa",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "tipo",
                    models.CharField(
                        choices=[("reforco", "Reforço"), ("retirada", "Retirada")],
                        db_index=True,
                        max_length=12,
                    ),
                ),
                ("forma_pagamento", models.CharField(max_length=80)),
                ("valor", models.DecimalField(decimal_places=2, max_digits=12)),
                (
                    "observacao",
                    models.CharField(blank=True, default="", max_length=500),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "sessao_caixa",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="movimentos",
                        to="produtos.sessaocaixa",
                    ),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="movimentos_caixa",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Movimento de caixa",
                "verbose_name_plural": "Movimentos de caixa",
                "ordering": ["-criado_em"],
            },
        ),
    ]
