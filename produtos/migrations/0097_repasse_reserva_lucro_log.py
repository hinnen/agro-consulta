# Repasse Vila — reserva no lucro antes do % + data início + log

import datetime

from django.db import migrations, models
import django.db.models.deletion


def _seed_desde(apps, schema_editor):
    """Campo criado em 18/08/2026 — marca início do diário nas configs existentes."""
    Config = apps.get_model("produtos", "RepasseVilaConfigAgro")
    Log = apps.get_model("produtos", "RepasseVilaReservaLogAgro")
    Delta = apps.get_model("produtos", "RepasseVilaDeltaDiaAgro")
    desde = datetime.date(2026, 8, 18)
    for cfg in Config.objects.all():
        if cfg.reserva_vila_desde is None:
            cfg.reserva_vila_desde = desde
            cfg.save(update_fields=["reserva_vila_desde"])
            Log.objects.create(
                tipo="desde",
                operador="migrate-0097",
                data_ref=desde,
                valor_antes=cfg.reserva_vila or 0,
                valor_depois=cfg.reserva_vila or 0,
                mensagem=(
                    "Início do valor manual diário: 18/08/2026 "
                    "(data de criação do campo reserva_vila)."
                ),
                detalhe={"origem": "migrate_0097", "reserva_vila_desde": "2026-08-18"},
            )
    # Recalcula acumulado com a nova regra (reserva no lucro antes do %).
    Delta.objects.filter(data_ref__gte=desde).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0096_estoque_lote_baixa_por_loja"),
    ]

    operations = [
        migrations.AddField(
            model_name="repassevilaconfigagro",
            name="reserva_vila_desde",
            field=models.DateField(
                blank=True,
                help_text="A partir desta data o valor manual entra todo dia (criação do campo: 18/08/2026).",
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="repassevilaconfigagro",
            name="reserva_vila",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text=(
                    "Valor manual diário que fica na Vila: desconta do lucro bruto "
                    "antes de aplicar o % enviado ao Centro."
                ),
                max_digits=12,
            ),
        ),
        migrations.AddField(
            model_name="repassevilacentroagro",
            name="reserva_aplicada",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Valor manual descontado do lucro bruto antes do % neste envio.",
                max_digits=12,
            ),
        ),
        migrations.AddField(
            model_name="repassevilacentroagro",
            name="lucro_penultimo_dia",
            field=models.DecimalField(
                decimal_places=2,
                default=0,
                help_text="Lucro bruto − reserva (base do %).",
                max_digits=12,
            ),
        ),
        migrations.CreateModel(
            name="RepasseVilaReservaLogAgro",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "tipo",
                    models.CharField(
                        choices=[
                            ("config", "Alteração do valor"),
                            ("aplicado", "Aplicado no envio"),
                            ("desde", "Data início diário"),
                        ],
                        db_index=True,
                        max_length=16,
                    ),
                ),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("operador", models.CharField(blank=True, default="", max_length=120)),
                ("data_ref", models.DateField(blank=True, db_index=True, null=True)),
                (
                    "valor_antes",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "valor_depois",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                ("mensagem", models.CharField(blank=True, default="", max_length=500)),
                (
                    "detalhe",
                    models.JSONField(
                        blank=True,
                        default=dict,
                        help_text="Snapshot: lucro bruto, penúltimo, %, alvos, totais, etc.",
                    ),
                ),
                (
                    "repasse",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="logs_reserva",
                        to="produtos.repassevilacentroagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "Repasse Vila · log reserva",
                "verbose_name_plural": "Repasse Vila · logs reserva",
                "ordering": ["-criado_em", "-pk"],
            },
        ),
        migrations.RunPython(_seed_desde, migrations.RunPython.noop),
    ]
