# Generated manually — CreateModel only (shadow credit analysis). No AlterField / RunPython.

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0136_clienteagro_limite_fiado_default_001"),
    ]

    operations = [
        migrations.CreateModel(
            name="ClienteAnaliseCreditoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("calculado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("regra_versao", models.CharField(db_index=True, default="shadow_v1", max_length=32)),
                (
                    "score",
                    models.PositiveSmallIntegerField(
                        blank=True,
                        help_text="0–100; nulo = sem histórico suficiente.",
                        null=True,
                    ),
                ),
                (
                    "classificacao",
                    models.CharField(
                        choices=[
                            ("ALTO_RISCO", "Alto risco"),
                            ("REGULAR", "Regular"),
                            ("BOM", "Bom"),
                            ("MUITO_BOM", "Muito bom"),
                            ("EXCELENTE", "Excelente"),
                            ("SEM_HISTORICO", "Sem histórico"),
                        ],
                        db_index=True,
                        default="SEM_HISTORICO",
                        max_length=20,
                    ),
                ),
                (
                    "confianca",
                    models.CharField(
                        choices=[
                            ("SEM_DADOS", "Sem dados"),
                            ("BAIXA", "Baixa"),
                            ("MEDIA", "Média"),
                            ("ALTA", "Alta"),
                        ],
                        db_index=True,
                        default="SEM_DADOS",
                        max_length=16,
                    ),
                ),
                ("limite_cadastrado_snapshot", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("limite_efetivo_snapshot", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("saldo_aberto_snapshot", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("saldo_vencido_snapshot", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("media_fiado_3m", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("limite_sugerido", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("tem_vencido_snapshot", models.BooleanField(db_index=True, default=False)),
                ("maior_atraso_dias", models.PositiveIntegerField(default=0)),
                ("indicadores_json", models.JSONField(blank=True, default=dict)),
                ("alertas_json", models.JSONField(blank=True, default=list)),
                (
                    "cliente",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="analises_credito_shadow",
                        to="produtos.clienteagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Análise crédito (shadow)",
                "verbose_name_plural": "Análises crédito (shadow)",
                "ordering": ["-calculado_em", "-pk"],
            },
        ),
        migrations.AddIndex(
            model_name="clienteanalisecreditoagro",
            index=models.Index(fields=["cliente", "-calculado_em"], name="cli_analise_cli_dt_idx"),
        ),
    ]
