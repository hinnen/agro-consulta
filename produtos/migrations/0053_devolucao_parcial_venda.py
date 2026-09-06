from decimal import Decimal

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0052_merge_0050_frete_0051_modelo"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="frete_devolvido",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("0"),
                help_text="Soma do frete já devolvido (parcial ou total).",
                max_digits=12,
            ),
        ),
        migrations.AddField(
            model_name="itemvendaagro",
            name="quantidade_devolvida",
            field=models.DecimalField(
                decimal_places=4,
                default=Decimal("0"),
                help_text="Quantidade já devolvida (parcial acumulada).",
                max_digits=14,
            ),
        ),
        migrations.CreateModel(
            name="DevolucaoVendaAgro",
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
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("usuario", models.CharField(blank=True, default="", max_length=150)),
                ("motivo", models.TextField(blank=True, default="")),
                (
                    "total",
                    models.DecimalField(decimal_places=2, default=Decimal("0"), max_digits=12),
                ),
                (
                    "pagamentos_json",
                    models.JSONField(
                        blank=True,
                        help_text="Formas e valores deste evento [{forma, valor}].",
                        null=True,
                    ),
                ),
                (
                    "movimento_caixa_ids",
                    models.JSONField(
                        blank=True,
                        help_text="IDs de MovimentoCaixa (retirada) deste evento.",
                        null=True,
                    ),
                ),
                ("incluiu_frete", models.BooleanField(default=False)),
                (
                    "frete_valor",
                    models.DecimalField(decimal_places=2, default=Decimal("0"), max_digits=12),
                ),
                (
                    "totalizou_venda",
                    models.BooleanField(
                        default=False,
                        help_text="True se este evento zerou o restante da venda.",
                    ),
                ),
                (
                    "venda",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="devolucoes",
                        to="produtos.vendaagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Devolução de venda Agro",
                "verbose_name_plural": "Devoluções de venda Agro",
                "ordering": ["-criado_em"],
            },
        ),
        migrations.CreateModel(
            name="DevolucaoItemVendaAgro",
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
                    "quantidade",
                    models.DecimalField(decimal_places=4, default=Decimal("0"), max_digits=14),
                ),
                (
                    "valor_total",
                    models.DecimalField(decimal_places=2, default=Decimal("0"), max_digits=12),
                ),
                (
                    "devolucao",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="itens",
                        to="produtos.devolucaovendaagro",
                    ),
                ),
                (
                    "item",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="devolucoes_item",
                        to="produtos.itemvendaagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Item devolvido (venda Agro)",
                "verbose_name_plural": "Itens devolvidos (venda Agro)",
            },
        ),
    ]
