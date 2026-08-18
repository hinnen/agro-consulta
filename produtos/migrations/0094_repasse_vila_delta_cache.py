# Repasse Vila — cache delta dia + FK ajuste→repasse

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0093_repasse_vila_acumulado_ajuste"),
    ]

    operations = [
        migrations.CreateModel(
            name="RepasseVilaDeltaDiaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("data_ref", models.DateField(db_index=True, unique=True)),
                ("alvo_fisico", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("enviado", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("delta", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Repasse Vila · delta dia (cache)",
                "verbose_name_plural": "Repasse Vila · deltas dia (cache)",
                "ordering": ["-data_ref"],
            },
        ),
        migrations.AddField(
            model_name="repassevilaacumuladoajusteagro",
            name="repasse",
            field=models.ForeignKey(
                blank=True,
                help_text="Repasse que quitou parte do acumulado (automático).",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="ajustes_acumulado",
                to="produtos.repassevilacentroagro",
            ),
        ),
    ]
