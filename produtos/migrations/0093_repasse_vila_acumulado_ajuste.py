# Repasse Vila — ajustes manuais no saldo acumulado

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0092_clienteagro_evento"),
    ]

    operations = [
        migrations.CreateModel(
            name="RepasseVilaAcumuladoAjusteAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "valor",
                    models.DecimalField(
                        decimal_places=2,
                        help_text="Positivo = falta levar mais. Negativo = crédito / desconto do acumulado.",
                        max_digits=12,
                    ),
                ),
                ("observacao", models.CharField(max_length=500)),
                ("operador", models.CharField(blank=True, default="", max_length=120)),
                (
                    "data_ref",
                    models.DateField(
                        blank=True,
                        db_index=True,
                        help_text="Dia de referência opcional (só registro).",
                        null=True,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
            ],
            options={
                "verbose_name": "Repasse Vila · ajuste acumulado",
                "verbose_name_plural": "Repasse Vila · ajustes acumulado",
                "ordering": ["-criado_em", "-pk"],
            },
        ),
    ]
