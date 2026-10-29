from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0146_nfe_saida_rascunho_agro"),
    ]

    operations = [
        migrations.CreateModel(
            name="NfeNumeracaoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "emitente_cnpj",
                    models.CharField(
                        blank=True,
                        db_index=True,
                        default="",
                        help_text="CNPJ do emitente. Numeração independente por CNPJ.",
                        max_length=14,
                    ),
                ),
                ("serie", models.PositiveSmallIntegerField(default=1)),
                ("proximo_numero", models.PositiveIntegerField(default=1)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Numeração NF-e saída",
                "verbose_name_plural": "Numerações NF-e saída",
            },
        ),
        migrations.AddConstraint(
            model_name="nfenumeracaoagro",
            constraint=models.UniqueConstraint(
                fields=("emitente_cnpj", "serie"),
                name="nfe_numeracao_cnpj_serie_uniq",
            ),
        ),
    ]
