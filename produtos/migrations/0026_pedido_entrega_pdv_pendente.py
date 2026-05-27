# Generated manually — venda PDV pendente (cobrar na entrega)

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0025_venda_agro_devolucao"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="aguarda_pagamento_pdv",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="Venda do PDV aguardando fechamento após entrega (cobrar na entrega).",
                verbose_name="Aguarda pagamento no PDV",
            ),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="pdv_wizard_state",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text="Snapshot do estado do wizard PDV para retomar no pagamento.",
            ),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="sessao_caixa",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="entregas_pdv_pendentes",
                to="produtos.sessaocaixa",
            ),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="venda_agro",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="pedido_entrega_origem",
                to="produtos.vendaagro",
            ),
        ),
    ]
