from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0139_fiadotituloagro_deposito"),
    ]

    operations = [
        migrations.CreateModel(
            name="ContaBancariaLojaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome", models.CharField(db_index=True, max_length=300, unique=True)),
                (
                    "codigo",
                    models.CharField(
                        db_index=True,
                        help_text="ID estável usado como banco_id nos títulos/baixas.",
                        max_length=80,
                        unique=True,
                    ),
                ),
                ("ativo", models.BooleanField(db_index=True, default=True)),
                ("ordem", models.PositiveIntegerField(db_index=True, default=0)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Conta bancária (loja)",
                "verbose_name_plural": "Contas bancárias (loja)",
                "ordering": ["ordem", "nome"],
            },
        ),
    ]
