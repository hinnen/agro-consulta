# Meta por loja — deposito centro | vila em MetaVendaFaixaAgro.

from django.db import migrations, models


def atribuir_centro_e_clonar_vila(apps, schema_editor):
    MetaVendaFaixaAgro = apps.get_model("produtos", "MetaVendaFaixaAgro")
    # Linhas antigas (sem loja) viram Centro; Vila recebe cópia para o usuário ajustar.
    for row in MetaVendaFaixaAgro.objects.all().order_by("id"):
        if (row.deposito or "").strip():
            continue
        row.deposito = "centro"
        row.save(update_fields=["deposito"])
        if MetaVendaFaixaAgro.objects.filter(
            competencia=row.competencia, deposito="vila", ordem=row.ordem
        ).exists():
            continue
        MetaVendaFaixaAgro.objects.create(
            competencia=row.competencia,
            deposito="vila",
            valor_meta=row.valor_meta,
            bonus_valor=row.bonus_valor,
            bonus_extra=row.bonus_extra,
            ordem=row.ordem,
        )


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0143_gm_lojas_app_log_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="metavendafaixaagro",
            name="deposito",
            field=models.CharField(
                choices=[("centro", "Centro"), ("vila", "Vila")],
                db_index=True,
                default="centro",
                max_length=8,
                verbose_name="Loja",
            ),
        ),
        migrations.RunPython(atribuir_centro_e_clonar_vila, migrations.RunPython.noop),
        migrations.RemoveIndex(
            model_name="metavendafaixaagro",
            name="meta_venda_comp_ord_idx",
        ),
        migrations.AddIndex(
            model_name="metavendafaixaagro",
            index=models.Index(
                fields=["competencia", "deposito", "ordem"],
                name="meta_venda_comp_dep_ord_idx",
            ),
        ),
    ]
