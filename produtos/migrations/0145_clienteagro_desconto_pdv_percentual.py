from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0144_metavendafaixaagro_deposito"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="desconto_pdv_percentual",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                default=None,
                help_text=(
                    "Opcional. Ex.: 5 = 5% sobre o subtotal dos itens no PDV. "
                    "Vazio = nenhum desconto automático."
                ),
                max_digits=5,
                null=True,
                verbose_name="% desconto PDV (fixo)",
            ),
        ),
    ]
