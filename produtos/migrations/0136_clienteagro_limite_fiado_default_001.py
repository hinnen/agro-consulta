from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0135_vendaagro_cartao_maquina_dia"),
    ]

    operations = [
        migrations.AlterField(
            model_name="clienteagro",
            name="limite_fiado_local",
            field=models.DecimalField(
                decimal_places=2,
                default=Decimal("0.01"),
                help_text=(
                    "0,01 = fiado bloqueado no PDV. 0 = usa o padrão da loja (R$ 5.000). "
                    "Outro valor = limite fixo em reais."
                ),
                max_digits=12,
                verbose_name="Limite fiado (local)",
            ),
        ),
    ]
