# Generated manually — MetaVendaFaixaAgro (mostruário META).

from decimal import Decimal

from django.db import migrations, models
from django.utils import timezone


def seed_faixas_mes_atual(apps, schema_editor):
    MetaVendaFaixaAgro = apps.get_model("produtos", "MetaVendaFaixaAgro")
    hoje = timezone.localdate()
    comp = f"{hoje.year:04d}-{hoje.month:02d}"
    if MetaVendaFaixaAgro.objects.filter(competencia=comp).exists():
        return
    padrao = [
        (Decimal("105000.00"), Decimal("100.00"), ""),
        (Decimal("110000.00"), Decimal("150.00"), ""),
        (Decimal("115000.00"), Decimal("100.00"), "moleton"),
        (Decimal("120000.00"), Decimal("250.00"), ""),
        (Decimal("130000.00"), Decimal("500.00"), ""),
    ]
    MetaVendaFaixaAgro.objects.bulk_create(
        [
            MetaVendaFaixaAgro(
                competencia=comp,
                valor_meta=vm,
                bonus_valor=bv,
                bonus_extra=be,
                ordem=i,
            )
            for i, (vm, bv, be) in enumerate(padrao)
        ]
    )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0137_clienteanalisecreditoagro"),
    ]

    operations = [
        migrations.CreateModel(
            name="MetaVendaFaixaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("competencia", models.CharField(db_index=True, help_text="YYYY-MM", max_length=7, verbose_name="Competência")),
                ("valor_meta", models.DecimalField(decimal_places=2, max_digits=14, verbose_name="Meta de venda (R$)")),
                ("bonus_valor", models.DecimalField(decimal_places=2, default=0, max_digits=12, verbose_name="Bônus (R$)")),
                (
                    "bonus_extra",
                    models.CharField(
                        blank=True,
                        default="",
                        help_text="Ex.: moleton — texto livre além do R$.",
                        max_length=120,
                        verbose_name="Bônus extra",
                    ),
                ),
                ("ordem", models.PositiveSmallIntegerField(db_index=True, default=0, verbose_name="Ordem")),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Faixa de meta de venda",
                "verbose_name_plural": "Faixas de meta de venda",
                "ordering": ["competencia", "ordem", "valor_meta", "id"],
            },
        ),
        migrations.AddIndex(
            model_name="metavendafaixaagro",
            index=models.Index(fields=["competencia", "ordem"], name="meta_venda_comp_ord_idx"),
        ),
        migrations.RunPython(seed_faixas_mes_atual, noop_reverse),
    ]
