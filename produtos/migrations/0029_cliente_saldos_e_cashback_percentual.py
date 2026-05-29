from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0028_venda_agro_erp_envio_log"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="limite_fiado_local",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Quando maior que zero, substitui o limite vindo do ERP/Mongo para este cliente.",
                max_digits=12,
                verbose_name="Limite fiado (local)",
            ),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="saldo_cashback",
            field=models.DecimalField(decimal_places=2, default=0, max_digits=12, verbose_name="Saldo cashback"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="saldo_vale_credito",
            field=models.DecimalField(decimal_places=2, default=0, max_digits=12, verbose_name="Saldo vale crédito"),
        ),
        migrations.AddField(
            model_name="produto",
            name="cashback_percentual",
            field=models.DecimalField(
                decimal_places=2,
                default=1,
                help_text="Percentual de cashback gerado na venda deste produto.",
                max_digits=5,
                validators=[MinValueValidator(0), MaxValueValidator(100)],
                verbose_name="Cashback (%)",
            ),
        ),
    ]
