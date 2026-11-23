from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0158_etiqueta_preco_gondola"),
    ]

    operations = [
        migrations.AddField(
            model_name="etiquetaprecogondolaagro",
            name="ok_por",
            field=models.CharField(blank=True, default="", max_length=150),
        ),
    ]
