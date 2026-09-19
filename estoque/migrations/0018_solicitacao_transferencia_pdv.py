from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("estoque", "0017_contagem_ciclica_dias_mov"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="SolicitacaoTransferenciaPdv",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("loja_origem", models.CharField(db_index=True, max_length=20)),
                ("loja_destino", models.CharField(db_index=True, max_length=20)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pendente", "Pendente"),
                            ("aceito", "Aceito"),
                            ("pronto", "Pronto"),
                            ("concluido", "Concluído"),
                            ("cancelado", "Cancelado"),
                        ],
                        db_index=True,
                        default="pendente",
                        max_length=20,
                    ),
                ),
                ("observacao", models.CharField(blank=True, default="", max_length=400)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
                ("criado_por_label", models.CharField(blank=True, default="", max_length=150)),
                ("aceito_em", models.DateTimeField(blank=True, null=True)),
                ("aceito_por_label", models.CharField(blank=True, default="", max_length=150)),
                ("pronto_em", models.DateTimeField(blank=True, null=True)),
                ("pronto_por_label", models.CharField(blank=True, default="", max_length=150)),
                ("concluido_em", models.DateTimeField(blank=True, null=True)),
                ("concluido_por_label", models.CharField(blank=True, default="", max_length=150)),
                ("cancelado_em", models.DateTimeField(blank=True, null=True)),
                ("cancelado_por_label", models.CharField(blank=True, default="", max_length=150)),
                ("cancelado_motivo", models.CharField(blank=True, default="", max_length=300)),
                (
                    "aceito_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="solicitacoes_transf_pdv_aceitas",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "cancelado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="solicitacoes_transf_pdv_canceladas",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "concluido_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="solicitacoes_transf_pdv_concluidas",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "criado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="solicitacoes_transf_pdv_criadas",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "pronto_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="solicitacoes_transf_pdv_prontas",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "verbose_name": "Solicitação de transferência PDV",
                "verbose_name_plural": "Solicitações de transferência PDV",
                "ordering": ["-criado_em", "-id"],
            },
        ),
        migrations.CreateModel(
            name="SolicitacaoTransferenciaPdvItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("produto_externo_id", models.CharField(db_index=True, max_length=100)),
                ("nome_produto", models.CharField(max_length=255)),
                ("codigo_interno", models.CharField(blank=True, default="", max_length=100)),
                ("quantidade", models.DecimalField(decimal_places=3, max_digits=10)),
                (
                    "solicitacao",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="itens",
                        to="estoque.solicitacaotransferenciapdv",
                    ),
                ),
            ],
            options={
                "verbose_name": "Item de solicitação PDV",
                "verbose_name_plural": "Itens de solicitação PDV",
                "ordering": ["id"],
            },
        ),
        migrations.CreateModel(
            name="SolicitacaoTransferenciaPdvEvento",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("acao", models.CharField(db_index=True, max_length=30)),
                ("status_de", models.CharField(blank=True, default="", max_length=20)),
                ("status_para", models.CharField(blank=True, default="", max_length=20)),
                ("operador_label", models.CharField(blank=True, default="", max_length=150)),
                ("observacao", models.CharField(blank=True, default="", max_length=400)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "operador",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="eventos_transf_pdv",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "solicitacao",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="eventos",
                        to="estoque.solicitacaotransferenciapdv",
                    ),
                ),
            ],
            options={
                "verbose_name": "Evento de solicitação PDV",
                "verbose_name_plural": "Eventos de solicitação PDV",
                "ordering": ["-criado_em", "-id"],
            },
        ),
        migrations.AddIndex(
            model_name="solicitacaotransferenciapdv",
            index=models.Index(fields=["loja_origem", "status"], name="estoque_sol_loja_or_8c1a2b_idx"),
        ),
        migrations.AddIndex(
            model_name="solicitacaotransferenciapdv",
            index=models.Index(fields=["loja_destino", "status"], name="estoque_sol_loja_de_9d2c3a_idx"),
        ),
    ]
