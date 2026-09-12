import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0076_uso_loja_motivo_livre"),
    ]

    operations = [
        migrations.AddField(
            model_name="usolojaretiradaagro",
            name="cliente_brinde",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="uso_loja_brindes",
                to="produtos.clienteagro",
                verbose_name="Cliente do brinde",
            ),
        ),
    ]
