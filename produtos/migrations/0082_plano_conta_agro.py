# Generated manually — só PlanoContaAgro (evita RenameIndex SQLite quebrado).

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0081_dispenser_documento_tipo_sabor"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="PlanoContaAgro",
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
                ("nome", models.CharField(db_index=True, max_length=200, unique=True)),
                (
                    "codigo",
                    models.CharField(
                        blank=True,
                        default="",
                        help_text="Código / hierarquia opcional (ex. 2.1.3).",
                        max_length=40,
                    ),
                ),
                (
                    "natureza",
                    models.CharField(
                        choices=[
                            ("despesa", "Despesa (CP)"),
                            ("receita", "Receita (CR)"),
                            ("ambos", "Despesa e receita"),
                        ],
                        db_index=True,
                        default="despesa",
                        max_length=16,
                    ),
                ),
                ("grupo", models.CharField(blank=True, default="", max_length=120)),
                ("ativo", models.BooleanField(db_index=True, default=True)),
                ("observacao", models.CharField(blank=True, default="", max_length=300)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="planos_conta_criados",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Plano de contas Agro",
                "verbose_name_plural": "Planos de contas Agro",
                "ordering": ["nome"],
            },
        ),
    ]
