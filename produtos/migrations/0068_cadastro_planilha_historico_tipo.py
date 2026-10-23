# Generated manually — tipo cadastro|estoque no histórico de planilha

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0067_produto_overlay_peso_etiqueta"),
    ]

    operations = [
        migrations.AddField(
            model_name="cadastroplanilhaimporthistoricoagro",
            name="tipo",
            field=models.CharField(
                choices=[("cadastro", "Cadastro (preços/dados)"), ("estoque", "Estoque (saldos)")],
                db_index=True,
                default="cadastro",
                max_length=16,
            ),
        ),
        migrations.AddIndex(
            model_name="cadastroplanilhaimporthistoricoagro",
            index=models.Index(fields=["tipo", "-criado_em"], name="cad_plan_imp_tipo_idx"),
        ),
    ]
