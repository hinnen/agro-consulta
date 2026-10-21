from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0148_repasse_centro_vila"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="FornecedorEntradaNfeAliasAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("fornecedor_duplicado_id", models.CharField(db_index=True, max_length=32, unique=True)),
                ("fornecedor_canonico_id", models.CharField(db_index=True, max_length=32)),
                ("canonico_nome", models.CharField(blank=True, default="", max_length=300)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="fornecedor_entrada_nfe_aliases",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Alias fornecedor Entrada NF",
                "verbose_name_plural": "Aliases fornecedor Entrada NF",
                "ordering": ["-criado_em"],
            },
        ),
    ]
