from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0048_orcamento_pdv_agro"),
    ]

    operations = [
        migrations.CreateModel(
            name="PlanoUnificacaoLoteAgro",
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
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("n_titulos", models.PositiveIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("aplicado", "Aplicado"),
                            ("revertido", "Revertido"),
                        ],
                        db_index=True,
                        default="aplicado",
                        max_length=16,
                    ),
                ),
                (
                    "alteracoes",
                    models.JSONField(
                        default=list,
                        help_text="Lista {mongo_id, de, para} antes de renomear.",
                    ),
                ),
                ("revertido_em", models.DateTimeField(blank=True, null=True)),
                (
                    "revertido_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="plano_unificacao_reversoes",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="plano_unificacao_lotes",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Lote unificação planos CP",
                "verbose_name_plural": "Lotes unificação planos CP",
                "ordering": ["-criado_em"],
            },
        ),
    ]
