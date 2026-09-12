# Generated manually — histórico / desfazer importação Excel cadastro

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0036_etiqueta_impressao_historico"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="CadastroPlanilhaImportHistoricoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("nome_arquivo", models.CharField(blank=True, default="", max_length=255)),
                ("n_produtos", models.PositiveIntegerField(default=0)),
                ("n_campos", models.PositiveIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[("aplicado", "Aplicado"), ("revertido", "Revertido")],
                        db_index=True,
                        default="aplicado",
                        max_length=16,
                    ),
                ),
                (
                    "backup",
                    models.JSONField(
                        default=dict,
                        help_text="Snapshot antes de aplicar (por produto).",
                    ),
                ),
                ("revertido_em", models.DateTimeField(blank=True, null=True)),
                (
                    "revertido_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="cadastro_planilha_reversoes",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="cadastro_planilha_imports",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Histórico importação planilha cadastro",
                "verbose_name_plural": "Históricos importação planilha cadastro",
                "ordering": ["-criado_em"],
                "indexes": [
                    models.Index(fields=["-criado_em"], name="cad_plan_imp_criado_idx"),
                ],
            },
        ),
    ]
