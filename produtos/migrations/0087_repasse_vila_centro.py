# Generated manually for Repasse Vila → Centro
from decimal import Decimal

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0086_estoque_lote_deposito"),
    ]

    operations = [
        migrations.CreateModel(
            name="RepasseVilaConfigAgro",
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
                    "percentual_lucro_padrao",
                    models.DecimalField(
                        decimal_places=2,
                        default=Decimal("50"),
                        help_text="0 a 100. Padrão na tela/PDV ao montar o envio.",
                        max_digits=5,
                    ),
                ),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "atualizado_por",
                    models.CharField(blank=True, default="", max_length=120),
                ),
            ],
            options={
                "verbose_name": "Repasse Vila · config",
                "verbose_name_plural": "Repasse Vila · config",
            },
        ),
        migrations.CreateModel(
            name="RepasseVilaCentroAgro",
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
                    "data_ref",
                    models.DateField(
                        db_index=True,
                        help_text="Dia das vendas/fiados deste cálculo.",
                    ),
                ),
                (
                    "percentual_lucro",
                    models.DecimalField(decimal_places=2, default=50, max_digits=5),
                ),
                (
                    "modo_dia_cheio",
                    models.BooleanField(
                        default=False,
                        help_text="True = mandou o dia cheio de novo (não só o que faltava).",
                    ),
                ),
                ("incluir_cmv", models.BooleanField(default=True)),
                ("incluir_lucro", models.BooleanField(default=True)),
                ("incluir_fiado", models.BooleanField(default=True)),
                (
                    "valor_cmv",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "valor_lucro",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "valor_fiado",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "valor_total",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "receita_dia",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "cmv_dia",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "lucro_bruto_dia",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "fiado_pago_dia",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                ("quem_levou", models.CharField(max_length=120)),
                (
                    "forma_pagamento",
                    models.CharField(default="Dinheiro", max_length=80),
                ),
                (
                    "operador",
                    models.CharField(blank=True, default="", max_length=120),
                ),
                (
                    "status_centro",
                    models.CharField(
                        choices=[
                            ("pendente", "Pendente no Centro"),
                            ("aplicado", "Aplicado no caixa Centro"),
                        ],
                        db_index=True,
                        default="pendente",
                        max_length=16,
                    ),
                ),
                ("aviso_abertura_visto", models.BooleanField(default=False)),
                (
                    "observacao",
                    models.CharField(blank=True, default="", max_length=500),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "movimento_entrada",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasse_vila_entrada",
                        to="produtos.movimentocaixa",
                    ),
                ),
                (
                    "movimento_saida",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasse_vila_saida",
                        to="produtos.movimentocaixa",
                    ),
                ),
                (
                    "sessao_centro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasses_centro_entrada",
                        to="produtos.sessaocaixa",
                    ),
                ),
                (
                    "sessao_vila",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasses_vila_saida",
                        to="produtos.sessaocaixa",
                    ),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="repasses_vila_centro",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Repasse Vila → Centro",
                "verbose_name_plural": "Repasses Vila → Centro",
                "ordering": ["-criado_em", "-pk"],
            },
        ),
    ]
