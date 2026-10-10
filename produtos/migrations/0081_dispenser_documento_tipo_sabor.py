# Generated manually — tipo "sabor" no DispenserDocumentoAgro (só choices; coluna já existe)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0080_etiqueta_lote_agro"),
    ]

    operations = [
        migrations.AlterField(
            model_name="dispenserdocumentoagro",
            name="tipo",
            field=models.CharField(
                choices=[
                    ("folha", "Folha pronta"),
                    ("layout", "Modelo de layout"),
                    ("sabor", "Sabor customizado"),
                ],
                db_index=True,
                max_length=20,
            ),
        ),
    ]
