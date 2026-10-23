from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0060_catalogo_logo_loja"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="loja_entrega",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="Dono da entrega: centro | vila. Vazio = ainda sem loja (as duas veem).",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="loja_assumida_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="pedidoentrega",
            name="loja_assumida_por",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
