from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0027_venda_agro_fiado_cronograma"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="erp_envio_log_json",
            field=models.JSONField(
                blank=True,
                help_text="Histórico de tentativas/reversões de envio manual ao ERP [{ts, acao, ok, ...}].",
                null=True,
            ),
        ),
    ]
