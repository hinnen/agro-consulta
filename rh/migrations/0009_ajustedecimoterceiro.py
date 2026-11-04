import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("rh", "0008_decimoterceirofuncionario"),
    ]

    operations = [
        migrations.CreateModel(
            name="AjusteDecimoTerceiro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "tipo",
                    models.CharField(
                        choices=[("DESCONTO", "Desconto"), ("ACRESCIMO", "Acréscimo")],
                        max_length=20,
                    ),
                ),
                ("descricao", models.CharField(max_length=200)),
                ("valor", models.DecimalField(decimal_places=2, max_digits=12)),
                (
                    "decimo",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ajustes",
                        to="rh.decimoterceirofuncionario",
                    ),
                ),
            ],
            options={
                "verbose_name": "Ajuste do 13º",
                "verbose_name_plural": "Ajustes do 13º",
                "ordering": ["id"],
            },
        ),
    ]
