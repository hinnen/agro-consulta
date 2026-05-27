# Fiado: cronograma de parcelas na venda local

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0026_pedido_entrega_pdv_pendente"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="fiado_cronograma_json",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Parcelas do fiado [{parcela, dias, vencimento, valor}] para envio manual ao ERP.",
            ),
        ),
    ]
