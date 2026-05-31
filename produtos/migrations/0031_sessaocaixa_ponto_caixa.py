# Ponto de operação do caixa (gaveta / notebook / teste)

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0030_produtogestaooverlayagro_cashback_percentual"),
    ]

    operations = [
        migrations.AddField(
            model_name="sessaocaixa",
            name="ponto_caixa",
            field=models.CharField(
                choices=[
                    ("gaveta", "Caixa Gaveta"),
                    ("notebook", "Caixa Notebook"),
                    ("teste", "Caixa Teste"),
                ],
                db_index=True,
                default="gaveta",
                help_text="Ponto físico do turno: gaveta (principal), notebook (satélite) ou teste.",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="sessaocaixa",
            name="sessao_principal",
            field=models.ForeignKey(
                blank=True,
                help_text="Turno principal (Caixa Gaveta) quando este registro for satélite.",
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="pontos_vinculados",
                to="produtos.sessaocaixa",
            ),
        ),
    ]
