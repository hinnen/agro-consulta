# Generated manually — devolução de venda Agro

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0024_caixa_movimento_conferencia"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="devolvida_em",
            field=models.DateTimeField(
                blank=True,
                db_index=True,
                help_text="Quando preenchido, a venda foi devolvida (estoque e saída no caixa).",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="vendaagro",
            name="devolucao_motivo",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="vendaagro",
            name="devolucao_pagamentos_json",
            field=models.JSONField(
                blank=True,
                help_text="Formas e valores devolvidos ao cliente [{forma, valor}].",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="vendaagro",
            name="devolucao_movimento_caixa_ids",
            field=models.JSONField(
                blank=True,
                help_text="IDs de MovimentoCaixa (retirada) gerados na devolução.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="vendaagro",
            name="devolucao_usuario",
            field=models.CharField(blank=True, default="", max_length=150),
        ),
    ]
