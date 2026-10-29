from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("produtos", "0077_uso_loja_brinde_cliente"),
    ]

    operations = [
        migrations.CreateModel(
            name="AjusteCodigoPendenteAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo_bipado", models.CharField(db_index=True, max_length=64)),
                ("produto_externo_id", models.CharField(db_index=True, max_length=100)),
                ("nome_produto", models.CharField(blank=True, default="", max_length=255)),
                ("operador", models.CharField(blank=True, default="", max_length=120)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pendente", "Pendente"),
                            ("feito", "Feito"),
                            ("descartado", "Descartado"),
                        ],
                        db_index=True,
                        default="pendente",
                        max_length=16,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "usuario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="ajuste_codigos_pendentes",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Código pendente (ajuste)",
                "verbose_name_plural": "Códigos pendentes (ajuste)",
                "ordering": ["-criado_em"],
            },
        ),
    ]
