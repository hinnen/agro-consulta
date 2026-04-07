from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("rh", "0002_funcionario_clienteagro_perfil"),
    ]

    operations = [
        migrations.AddField(
            model_name="fechamentofolhasimplificado",
            name="data_vencimento_pagamento",
            field=models.DateField(
                blank=True,
                help_text="Vencimento do título de salário no financeiro (Mongo / contas a pagar).",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="fechamentofolhasimplificado",
            name="mongo_lancamento_salario_id",
            field=models.CharField(
                blank=True,
                default="",
                help_text="ObjectId do DtoLancamento único de salário (despesa) no Mongo.",
                max_length=32,
            ),
        ),
    ]
