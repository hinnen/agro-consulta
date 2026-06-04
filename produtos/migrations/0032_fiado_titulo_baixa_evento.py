# Generated manually for fiado gestão

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0031_sessaocaixa_ponto_caixa"),
    ]

    operations = [
        migrations.CreateModel(
            name="FiadoTituloAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "chave_unica",
                    models.CharField(
                        db_index=True,
                        help_text="Chave idempotente (pdv:… ou import:…) para evitar duplicata.",
                        max_length=120,
                        unique=True,
                    ),
                ),
                ("cliente_nome", models.CharField(max_length=300)),
                (
                    "cliente_codigo",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        default="",
                        help_text="Código ERP / planilha quando existir.",
                        max_length=32,
                    ),
                ),
                ("numero_documento", models.CharField(blank=True, default="", max_length=80)),
                ("parcela_num", models.PositiveSmallIntegerField(default=1)),
                ("parcela_total", models.PositiveSmallIntegerField(default=1)),
                ("vencimento", models.DateField(db_index=True)),
                ("valor_bruto", models.DecimalField(decimal_places=2, max_digits=12)),
                ("valor_pago", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                (
                    "situacao",
                    models.CharField(
                        choices=[
                            ("aberto", "Em aberto"),
                            ("parcial", "Pago parcialmente"),
                            ("quitado", "Quitado"),
                            ("cancelado", "Cancelado"),
                        ],
                        db_index=True,
                        default="aberto",
                        max_length=12,
                    ),
                ),
                (
                    "origem",
                    models.CharField(
                        choices=[("pdv", "PDV"), ("importacao", "Importação")],
                        db_index=True,
                        default="pdv",
                        max_length=16,
                    ),
                ),
                ("descricao", models.CharField(blank=True, default="", max_length=500)),
                ("dados_snapshot_json", models.JSONField(blank=True, default=dict)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "cliente_agro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="fiado_titulos",
                        to="produtos.clienteagro",
                    ),
                ),
                (
                    "venda_agro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="fiado_titulos",
                        to="produtos.vendaagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Título fiado",
                "verbose_name_plural": "Títulos fiado",
                "ordering": ["vencimento", "pk"],
            },
        ),
        migrations.CreateModel(
            name="FiadoBaixaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("valor", models.DecimalField(decimal_places=2, max_digits=12)),
                ("forma_pagamento", models.CharField(max_length=80)),
                ("usuario", models.CharField(blank=True, default="", max_length=150)),
                ("observacao", models.CharField(blank=True, default="", max_length=500)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "movimento_caixa",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="fiado_baixas",
                        to="produtos.movimentocaixa",
                    ),
                ),
                (
                    "sessao_caixa",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="fiado_baixas",
                        to="produtos.sessaocaixa",
                    ),
                ),
                (
                    "titulo",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="baixas",
                        to="produtos.fiadotituloagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Baixa fiado",
                "verbose_name_plural": "Baixas fiado",
                "ordering": ["-criado_em"],
            },
        ),
        migrations.CreateModel(
            name="FiadoEventoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "tipo",
                    models.CharField(
                        choices=[
                            ("titulo_criado", "Título criado"),
                            ("baixa", "Baixa"),
                            ("limite", "Limite alterado"),
                            ("cancelamento", "Cancelamento"),
                            ("import", "Importação"),
                        ],
                        db_index=True,
                        max_length=24,
                    ),
                ),
                ("payload_json", models.JSONField(blank=True, default=dict)),
                ("usuario", models.CharField(blank=True, default="", max_length=150)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "baixa",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="eventos",
                        to="produtos.fiadobaixaagro",
                    ),
                ),
                (
                    "cliente_agro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="fiado_eventos",
                        to="produtos.clienteagro",
                    ),
                ),
                (
                    "titulo",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="eventos",
                        to="produtos.fiadotituloagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Evento fiado",
                "verbose_name_plural": "Eventos fiado",
                "ordering": ["-criado_em"],
            },
        ),
    ]
