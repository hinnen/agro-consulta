from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0074_bug_report_dispositivo_loja"),
    ]

    operations = [
        migrations.AddField(
            model_name="usolojaretiradaitemagro",
            name="preco_custo",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Custo unitário no momento da saída (snapshot).",
                max_digits=12,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="usolojaretiradaitemagro",
            name="preco_venda",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Preço de venda unitário no momento da saída (snapshot).",
                max_digits=12,
                null=True,
            ),
        ),
    ]
