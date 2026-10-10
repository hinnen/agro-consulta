from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("financeiro", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="GraficoGastosAtalhoAgro",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("slot", models.PositiveSmallIntegerField(unique=True)),
                ("nome", models.CharField(blank=True, default="", max_length=80)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "atualizado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="grafico_gastos_atalhos",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Atalho gráfico gastos",
                "verbose_name_plural": "Atalhos gráfico gastos",
                "db_table": "grafico_gastos_atalho_agro",
            },
        ),
        migrations.AddConstraint(
            model_name="graficogastosatalhoagro",
            constraint=models.CheckConstraint(
                condition=models.Q(("slot__gte", 1), ("slot__lte", 4)),
                name="grafico_gastos_atalho_slot_1_4",
            ),
        ),
    ]
