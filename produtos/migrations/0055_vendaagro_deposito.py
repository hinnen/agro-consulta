# Generated manually for Vila Elias PDV depósito

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0054_produto_cadastro_alteracao_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="deposito",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="centro",
                help_text="Depósito da baixa de estoque nesta venda: centro | vila (Vila Elias).",
                max_length=16,
            ),
        ),
    ]
