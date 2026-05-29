from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0029_cliente_saldos_e_cashback_percentual"),
    ]

    operations = [
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="cashback_percentual",
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text="Vazio = usar padrão do sistema (ex.: 1%). Zero desliga cashback na venda.",
                max_digits=5,
                null=True,
                validators=[MinValueValidator(0), MaxValueValidator(100)],
                verbose_name="Cashback (%)",
            ),
        ),
    ]
