from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0130_pedido_entrega_pdv_lista_concluida"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="loja_pagamento",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="Caixa que fecha a venda: centro | vila. Vazio = mesmo que loja_entrega / sessão.",
                max_length=16,
            ),
        ),
    ]
