# Generated manually for ComprasFolhaSaldoFiltroPreset

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0063_dispenser_a6_biblioteca_compartilhada"),
    ]

    operations = [
        migrations.CreateModel(
            name="ComprasFolhaSaldoFiltroPreset",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(max_length=80)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("is_padrao", models.BooleanField(db_index=True, default=False)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="compras_folha_saldo_presets",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Filtro Folha de saldo (Compras)",
                "verbose_name_plural": "Filtros Folha de saldo (Compras)",
                "ordering": ["-is_padrao", "nome", "pk"],
            },
        ),
    ]
