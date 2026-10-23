# Generated manually — orçamentos PDV no Postgres

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0047_relacionamento_historico_erp"),
    ]

    operations = [
        migrations.CreateModel(
            name="OrcamentoPdvAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("orc_local_id", models.BigIntegerField(db_index=True, unique=True)),
                ("cliente_nome", models.CharField(blank=True, default="", max_length=300)),
                ("cliente_key", models.CharField(db_index=True, max_length=120)),
                ("cliente_mode", models.CharField(blank=True, default="cliente", max_length=32)),
                ("payload_json", models.JSONField(default=dict)),
                ("total_texto", models.CharField(blank=True, default="", max_length=48)),
                ("entrega", models.BooleanField(default=False)),
                ("forma_pagamento", models.CharField(blank=True, default="", max_length=40)),
                ("usuario_registro", models.CharField(blank=True, default="", max_length=120)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "cliente_agro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="orcamentos_pdv",
                        to="produtos.clienteagro",
                        verbose_name="Cliente (cadastro PDV)",
                    ),
                ),
            ],
            options={
                "verbose_name": "Orçamento PDV",
                "verbose_name_plural": "Orçamentos PDV",
                "ordering": ["-criado_em"],
                "indexes": [
                    models.Index(fields=["cliente_key", "-criado_em"], name="orc_pdv_cli_key_dt_idx"),
                ],
            },
        ),
    ]
