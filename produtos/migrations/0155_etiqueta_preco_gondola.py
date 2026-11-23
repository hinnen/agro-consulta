from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0154_clienteagro_aniversario"),
    ]

    operations = [
        migrations.CreateModel(
            name="EtiquetaPrecoGondolaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("produto_externo_id", models.CharField(db_index=True, max_length=64)),
                ("deposito", models.CharField(db_index=True, max_length=16)),
                ("nome", models.CharField(blank=True, default="", max_length=300)),
                ("codigo", models.CharField(blank=True, default="", max_length=80)),
                ("preco_ok", models.DecimalField(decimal_places=2, max_digits=12)),
                ("preco_atual", models.DecimalField(decimal_places=2, max_digits=12)),
                ("pendente", models.BooleanField(db_index=True, default=False)),
                ("ok_em", models.DateTimeField(blank=True, null=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={"verbose_name": "Etiqueta de preço pendente"},
        ),
        migrations.AddConstraint(
            model_name="etiquetaprecogondolaagro",
            constraint=models.UniqueConstraint(
                fields=("produto_externo_id", "deposito"),
                name="uniq_etq_preco_gondola_prod_loja",
            ),
        ),
        migrations.CreateModel(
            name="EtiquetaPrecoPromoBaseAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("deposito", models.CharField(max_length=16, unique=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
            ],
        ),
    ]
