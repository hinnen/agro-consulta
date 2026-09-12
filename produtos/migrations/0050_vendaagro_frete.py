from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0049_plano_unificacao_lote_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="frete",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Taxa de entrega cobrada na venda (compõe o total).",
                max_digits=12,
            ),
        ),
    ]
