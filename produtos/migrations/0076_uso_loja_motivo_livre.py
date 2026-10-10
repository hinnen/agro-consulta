from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0075_uso_loja_item_precos"),
    ]

    operations = [
        migrations.AlterField(
            model_name="usolojaretiradaagro",
            name="motivo",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
