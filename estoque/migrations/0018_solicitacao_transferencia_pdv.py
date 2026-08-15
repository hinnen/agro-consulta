# Generated for SolicitacaoTransferenciaPdv + tipos de histórico PDV

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("estoque", "0017_contagem_ciclica_dias_mov"),
    ]

    operations = [
        migrations.CreateModel(
            name="SolicitacaoTransferenciaPdv",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("produto_externo_id", models.CharField(db_index=True, max_length=100)),
                ("nome_produto", models.CharField(blank=True, default="", max_length=255)),
                ("codigo_interno", models.CharField(blank=True, default="", max_length=100)),
                ("quantidade", models.DecimalField(decimal_places=3, max_digits=10)),
                ("loja_origem", models.CharField(db_index=True, max_length=20)),
                ("loja_destino", models.CharField(db_index=True, max_length=20)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("PENDENTE", "Pendente"),
                            ("ACEITO", "Aceito"),
                            ("RECUSADO", "Recusado"),
                            ("TRANSFERIDO", "Transferido"),
                            ("CANCELADO", "Cancelado"),
                        ],
                        db_index=True,
                        default="PENDENTE",
                        max_length=20,
                    ),
                ),
                ("grupo_uuid", models.UUIDField(blank=True, db_index=True, null=True)),
                ("usuario_solicitante", models.CharField(blank=True, default="", max_length=200)),
                ("usuario_resposta", models.CharField(blank=True, default="", max_length=200)),
                ("observacao", models.TextField(blank=True, default="")),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Solicitação de transferência (PDV)",
                "verbose_name_plural": "Solicitações de transferência (PDV)",
                "ordering": ["-criado_em", "-id"],
            },
        ),
        migrations.AddIndex(
            model_name="solicitacaotransferenciapdv",
            index=models.Index(fields=["status", "loja_origem"], name="estoque_sol_status_7e2b1a_idx"),
        ),
        migrations.AddIndex(
            model_name="solicitacaotransferenciapdv",
            index=models.Index(fields=["status", "loja_destino"], name="estoque_sol_status_3c91d4_idx"),
        ),
    ]
