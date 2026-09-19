# NFC-e por loja: CNPJ emitente na numeração e no documento (Centro × Vila).

from django.db import migrations, models


def _backfill_emitente(apps, schema_editor):
    NfceNumeracaoAgro = apps.get_model("produtos", "NfceNumeracaoAgro")
    NfceDocumentoAgro = apps.get_model("produtos", "NfceDocumentoAgro")
    centro = "48900774000103"
    for num in NfceNumeracaoAgro.objects.all():
        if not (num.emitente_cnpj or "").strip():
            num.emitente_cnpj = centro
            num.save(update_fields=["emitente_cnpj"])
    for doc in NfceDocumentoAgro.objects.all().iterator():
        cnpj = (doc.emitente_cnpj or "").strip()
        if cnpj:
            continue
        ch = "".join(c for c in (doc.chave or "") if c.isdigit())
        if len(ch) >= 20:
            cnpj = ch[6:20]
        else:
            cnpj = centro
        doc.emitente_cnpj = cnpj
        doc.save(update_fields=["emitente_cnpj"])


def _noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0087_repasse_vila_centro"),
    ]

    operations = [
        migrations.AddField(
            model_name="nfcenumeracaoagro",
            name="emitente_cnpj",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="CNPJ do emitente (Centro ou Vila). Numeração independente por CNPJ.",
                max_length=14,
            ),
        ),
        migrations.AddField(
            model_name="nfcedocumentoagro",
            name="emitente_cnpj",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="CNPJ no XML emit (Centro /0001 ou Vila /0002).",
                max_length=14,
            ),
        ),
        migrations.RunPython(_backfill_emitente, _noop_reverse),
        migrations.AddConstraint(
            model_name="nfcenumeracaoagro",
            constraint=models.UniqueConstraint(
                fields=("emitente_cnpj", "serie"),
                name="nfce_numeracao_cnpj_serie_uniq",
            ),
        ),
    ]
