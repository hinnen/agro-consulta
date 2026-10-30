from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0150_pdv_rapido_slot"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="PdvRapidoLayoutAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(default="default", max_length=32, unique=True)),
                ("foto_pct", models.PositiveSmallIntegerField(default=40)),
                ("nome_escala", models.PositiveSmallIntegerField(default=100)),
                ("preco_escala", models.PositiveSmallIntegerField(default=100)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "atualizado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="pdv_rapido_layouts",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Proporção dos rápidos do PDV",
                "verbose_name_plural": "Proporções dos rápidos do PDV",
            },
        ),
    ]
