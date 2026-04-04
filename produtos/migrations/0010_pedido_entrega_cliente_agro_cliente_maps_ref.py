# Generated manually for entregas ↔ ClienteAgro sync

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0009_opcao_baixa_financeiro_extra"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="maps_url_manual",
            field=models.CharField(
                blank=True,
                default="",
                max_length=600,
                verbose_name="Link do Maps (colado)",
            ),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="referencia_rural",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Porteira, km, cor — texto para o entregador; não compõe o link do Maps.",
                max_length=300,
                verbose_name="Referência (entrega)",
            ),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="cliente_agro",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="pedidos_entrega",
                to="produtos.clienteagro",
                verbose_name="Cliente (cadastro PDV)",
            ),
        ),
    ]
