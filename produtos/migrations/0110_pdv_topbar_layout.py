# Layout quente/frio da topbar PDV (Postgres · multi-PC)

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0107_pdv_topbar_clique_dia"),
    ]

    operations = [
        migrations.CreateModel(
            name="PdvTopbarLayoutAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(default="default", max_length=32, unique=True)),
                ("quente", models.JSONField(blank=True, default=list)),
                ("frio", models.JSONField(blank=True, default=list)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "atualizado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="pdv_topbar_layouts",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Layout topbar PDV",
                "verbose_name_plural": "Layouts topbar PDV",
            },
        ),
    ]
