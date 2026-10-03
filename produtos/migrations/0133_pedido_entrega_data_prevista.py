from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0132_vendaagro_client_request_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="data_prevista",
            field=models.DateField(
                blank=True,
                db_index=True,
                help_text="Dia combinado. Vazio = o dia em que lançou.",
                null=True,
            ),
        ),
    ]
