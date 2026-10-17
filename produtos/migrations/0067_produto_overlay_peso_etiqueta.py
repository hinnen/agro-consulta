# Generated manually — peso_etiqueta no overlay (etiqueta gôndola)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0066_compras_folha_saldo_filtro_preset"),
    ]

    operations = [
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="peso_etiqueta",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Texto livre para etiqueta gôndola (ex.: 5 KG, 500 g).",
                max_length=40,
                verbose_name="Peso (etiqueta)",
            ),
        ),
    ]
