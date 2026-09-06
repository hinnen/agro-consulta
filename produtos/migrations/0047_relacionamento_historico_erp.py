# Histórico ERP importado para F8 (FL-042)

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0046_clienteagro_relacionamento_extras"),
    ]

    operations = [
        migrations.CreateModel(
            name="RelacionamentoHistoricoImportLoteAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("lote_id", models.CharField(db_index=True, max_length=64, unique=True, verbose_name="ID do lote")),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("erp_ate", models.DateField(verbose_name="ERP até (inclusivo)")),
                ("pdv_desde", models.DateField(verbose_name="PDV SisVale desde")),
                ("dry_run", models.BooleanField(default=False)),
                ("stats_json", models.JSONField(blank=True, default=dict)),
                ("observacao", models.CharField(blank=True, default="", max_length=300)),
            ],
            options={
                "verbose_name": "Lote import histórico ERP (F8)",
                "verbose_name_plural": "Lotes import histórico ERP (F8)",
                "ordering": ["-criado_em"],
            },
        ),
        migrations.CreateModel(
            name="RelacionamentoVendaHistoricoErpAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("venda_id_erp", models.CharField(db_index=True, max_length=64)),
                ("cliente_id_erp", models.CharField(blank=True, default="", max_length=64)),
                ("cliente_nome_snapshot", models.CharField(blank=True, default="", max_length=300)),
                ("data_venda", models.DateTimeField(db_index=True)),
                ("total", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("forma_pagamento", models.CharField(blank=True, default="", max_length=120)),
                (
                    "cliente_agro",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="vendas_historico_erp",
                        to="produtos.clienteagro",
                    ),
                ),
                (
                    "lote",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="vendas",
                        to="produtos.relacionamentohistoricoimportloteagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Venda histórico ERP (F8)",
                "verbose_name_plural": "Vendas histórico ERP (F8)",
                "ordering": ["-data_venda"],
            },
        ),
        migrations.CreateModel(
            name="RelacionamentoItemHistoricoErpAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("produto_id_erp", models.CharField(blank=True, db_index=True, default="", max_length=64)),
                ("codigo_gm", models.CharField(blank=True, db_index=True, default="", max_length=64)),
                ("descricao", models.CharField(blank=True, default="", max_length=300)),
                ("quantidade", models.DecimalField(decimal_places=3, default=0, max_digits=12)),
                ("valor_unitario", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("valor_total", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                (
                    "venda",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="itens",
                        to="produtos.relacionamentovendahistoricoerpagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Item histórico ERP (F8)",
                "verbose_name_plural": "Itens histórico ERP (F8)",
                "ordering": ["id"],
            },
        ),
        migrations.AddIndex(
            model_name="relacionamentovendahistoricoerpagro",
            index=models.Index(fields=["cliente_agro", "-data_venda"], name="rel_hist_erp_cli_dt_idx"),
        ),
        migrations.AddConstraint(
            model_name="relacionamentovendahistoricoerpagro",
            constraint=models.UniqueConstraint(
                fields=("lote", "venda_id_erp"),
                name="rel_hist_erp_venda_lote_uid",
            ),
        ),
    ]
