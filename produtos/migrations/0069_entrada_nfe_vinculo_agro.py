from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0068_cadastro_planilha_historico_tipo"),
    ]

    operations = [
        migrations.CreateModel(
            name="EntradaNfeVinculoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(choices=[("c_prod", "Código fornecedor (cProd)"), ("x_prod", "Descrição (xProd)")], db_index=True, max_length=16)),
                ("chave", models.CharField(db_index=True, max_length=120)),
                ("emit_cnpj", models.CharField(blank=True, db_index=True, default="", max_length=14)),
                ("produto_externo_id", models.CharField(db_index=True, max_length=64)),
                ("nome_catalogo", models.CharField(blank=True, default="", max_length=300)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Vínculo Entrada NF Agro",
                "verbose_name_plural": "Vínculos Entrada NF Agro",
            },
        ),
        migrations.AddConstraint(
            model_name="entradanfevinculoagro",
            constraint=models.UniqueConstraint(
                fields=("tipo", "chave", "emit_cnpj"),
                name="entrada_nfe_vinculo_tipo_chave_cnpj_uq",
            ),
        ),
        migrations.AddIndex(
            model_name="entradanfevinculoagro",
            index=models.Index(fields=["tipo", "chave"], name="entrada_nfe_vinculo_tipo_chave"),
        ),
    ]
