from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0062_sessaocaixa_diferenca_abertura_usuario_fechamento"),
    ]

    operations = [
        migrations.CreateModel(
            name="AgroNfeDistDfeCursor",
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
                ("cnpj", models.CharField(db_index=True, max_length=14, unique=True)),
                (
                    "ult_nsu",
                    models.CharField(default="000000000000000", max_length=15),
                ),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Cursor Dist DF-e",
                "verbose_name_plural": "Cursores Dist DF-e",
            },
        ),
    ]
