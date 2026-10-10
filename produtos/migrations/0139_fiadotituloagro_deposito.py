from django.db import migrations, models


def backfill_deposito_fiado(apps, schema_editor):
    FiadoTituloAgro = apps.get_model("produtos", "FiadoTituloAgro")
    VendaAgro = apps.get_model("produtos", "VendaAgro")
    qs = FiadoTituloAgro.objects.filter(deposito="").exclude(venda_agro_id__isnull=True)
    venda_ids = list(qs.values_list("venda_agro_id", flat=True).distinct())
    if not venda_ids:
        return
    dep_map = {
        v.pk: str(v.deposito or "").strip().lower()
        for v in VendaAgro.objects.filter(pk__in=venda_ids).only("pk", "deposito")
    }
    for t in qs.iterator(chunk_size=500):
        dep = dep_map.get(t.venda_agro_id) or ""
        if dep not in ("centro", "vila"):
            continue
        t.deposito = dep
        t.save(update_fields=["deposito"])


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0138_metavendafaixaagro"),
    ]

    operations = [
        migrations.AddField(
            model_name="fiadotituloagro",
            name="deposito",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="Loja da compra: centro | vila. Vazio = ainda sem loja (legado/importação).",
                max_length=16,
            ),
        ),
        migrations.RunPython(backfill_deposito_fiado, noop_reverse),
    ]
