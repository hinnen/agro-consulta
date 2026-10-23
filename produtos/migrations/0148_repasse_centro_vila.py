# Generated manually — repasse manual Centro → Vila (PDV)

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0147_nfe_numeracao_agro"),
    ]

    operations = [
        migrations.CreateModel(
            name="RepasseCentroVilaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("valor_total", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("quem_levou", models.CharField(max_length=120)),
                ("forma_pagamento", models.CharField(default="Dinheiro", max_length=80)),
                ("operador", models.CharField(blank=True, default="", max_length=120)),
                ("status_vila", models.CharField(choices=[("pendente", "Pendente na Vila"), ("aplicado", "Aplicado no caixa Vila")], db_index=True, default="pendente", max_length=16)),
                ("observacao", models.CharField(blank=True, default="", max_length=500)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "movimento_entrada",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasse_centro_vila_entrada",
                        to="produtos.movimentocaixa",
                    ),
                ),
                (
                    "movimento_saida",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasse_centro_vila_saida",
                        to="produtos.movimentocaixa",
                    ),
                ),
                (
                    "sessao_centro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasses_centro_saida",
                        to="produtos.sessaocaixa",
                    ),
                ),
                (
                    "sessao_vila",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasses_vila_entrada_centro",
                        to="produtos.sessaocaixa",
                    ),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasses_centro_vila",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Repasse Centro → Vila",
                "verbose_name_plural": "Repasses Centro → Vila",
                "ordering": ["-criado_em", "-pk"],
            },
        ),
    ]
