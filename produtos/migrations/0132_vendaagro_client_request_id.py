from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0131_pedido_entrega_loja_pagamento"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="client_request_id",
            field=models.CharField(
                blank=True,
                help_text="Chave do PDV (UUID) — mesmo Confirmar/Enter de novo não cria outra venda.",
                max_length=96,
                null=True,
                unique=True,
            ),
        ),
    ]
