# Validade — baixa por loja (Centro conferiu ≠ some na Vila)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0095_repasse_vila_reserva"),
    ]

    operations = [
        migrations.AddField(
            model_name="estoquelote",
            name="baixado_centro_em",
            field=models.DateTimeField(
                blank=True,
                help_text="Quando o Centro conferiu/baixou este lote. A Vila continua vendo até baixar.",
                null=True,
                verbose_name="Baixa validade Centro",
            ),
        ),
        migrations.AddField(
            model_name="estoquelote",
            name="baixado_vila_em",
            field=models.DateTimeField(
                blank=True,
                help_text="Quando a Vila conferiu/baixou este lote. O Centro continua vendo até baixar.",
                null=True,
                verbose_name="Baixa validade Vila",
            ),
        ),
    ]
