# Generated — log de operações no cadastro do cliente (PIN + histórico)

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0091_repasse_planos_desconto_centro"),
    ]

    operations = [
        migrations.CreateModel(
            name="ClienteAgroEventoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "tipo",
                    models.CharField(
                        choices=[
                            ("limpar_whatsapp", "Limpar telefone"),
                            ("transferir_saldos", "Transferir cashback/vale"),
                            ("excluir", "Excluir cadastro"),
                            ("vale_manual", "Vale crédito manual"),
                            ("vale_pago", "Vale crédito pago (caixa)"),
                        ],
                        db_index=True,
                        max_length=24,
                    ),
                ),
                ("cliente_pk_snap", models.PositiveIntegerField(blank=True, db_index=True, null=True)),
                ("cliente_nome_snap", models.CharField(blank=True, default="", max_length=200)),
                ("destino_pk_snap", models.PositiveIntegerField(blank=True, null=True)),
                ("destino_nome_snap", models.CharField(blank=True, default="", max_length=200)),
                ("payload_json", models.JSONField(blank=True, default=dict)),
                ("usuario", models.CharField(blank=True, default="", max_length=150)),
                ("pin_operador", models.CharField(blank=True, default="", max_length=150)),
                ("origem_tela", models.CharField(blank=True, default="", max_length=32)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                (
                    "cliente_agro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="eventos_cadastro",
                        to="produtos.clienteagro",
                    ),
                ),
                (
                    "destino_agro",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="eventos_cadastro_destino",
                        to="produtos.clienteagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Evento cadastro cliente",
                "verbose_name_plural": "Eventos cadastro cliente",
                "ordering": ["-criado_em"],
            },
        ),
        migrations.AddIndex(
            model_name="clienteagroeventoagro",
            index=models.Index(fields=["cliente_pk_snap", "-criado_em"], name="cli_evt_pk_dt_idx"),
        ),
    ]
