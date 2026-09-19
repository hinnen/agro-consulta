# Generated manually for EtiquetaPresetAgro (presets multi-PC)

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0078_ajustecodigopendenteagro"),
    ]

    operations = [
        migrations.CreateModel(
            name="EtiquetaPresetAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("client_key", models.CharField(db_index=True, max_length=64, unique=True)),
                ("nome", models.CharField(max_length=120)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="etiqueta_presets",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Preset etiqueta",
                "verbose_name_plural": "Presets etiquetas",
                "ordering": ["nome", "pk"],
            },
        ),
    ]
