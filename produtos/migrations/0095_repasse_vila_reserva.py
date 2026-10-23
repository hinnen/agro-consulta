# Repasse Vila — valor fixo que fica na Vila (desconta do envio ao Centro)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0094_repasse_vila_delta_cache"),
    ]

    operations = [
        migrations.AddField(
            model_name="repassevilaconfigagro",
            name="reserva_vila",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Valor fixo que fica na Vila (troco) e desconta do envio ao Centro.",
                max_digits=12,
            ),
        ),
    ]
