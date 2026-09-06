# Generated manually — cadastro oficial planos CP + seed CSV

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def seed_planos(apps, schema_editor):
    from produtos.plano_conta_agro_util import seed_planos_conta_agro

    seed_planos_conta_agro(force=False)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0064_dispenser_a6_biblioteca_compartilhada"),
    ]

    operations = [
        migrations.CreateModel(
            name="PlanoContaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(db_index=True, max_length=200, unique=True)),
                (
                    "tipo",
                    models.CharField(
                        blank=True,
                        choices=[("fixa", "Fixa"), ("variavel", "Variável"), ("outra", "Outra")],
                        default="outra",
                        max_length=16,
                    ),
                ),
                ("grupo", models.CharField(blank=True, default="", max_length=120)),
                ("observacao", models.CharField(blank=True, default="", max_length=400)),
                ("ativo", models.BooleanField(db_index=True, default=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Plano de conta Agro",
                "verbose_name_plural": "Planos de conta Agro",
                "ordering": ["nome"],
            },
        ),
        migrations.CreateModel(
            name="PlanoContaAliasAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("grafia", models.CharField(db_index=True, max_length=200, unique=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="plano_conta_aliases",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "plano",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="aliases",
                        to="produtos.planocontaagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Alias plano de conta",
                "verbose_name_plural": "Aliases plano de conta",
                "ordering": ["grafia"],
            },
        ),
        migrations.RunPython(seed_planos, noop_reverse),
    ]
