# Generated manually — Pedir loja: qtd pedida vs enviada

from django.db import migrations, models
from django.db.models import F


def backfill_quantidade_pedida(apps, schema_editor):
    Item = apps.get_model("estoque", "SolicitacaoTransferenciaPdvItem")
    Item.objects.filter(quantidade_pedida=0).update(quantidade_pedida=F("quantidade"))


class Migration(migrations.Migration):

    dependencies = [
        ("estoque", "0019_rename_estoque_sol_loja_or_8c1a2b_idx_estoque_sol_loja_or_d79150_idx_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="solicitacaotransferenciapdvitem",
            name="quantidade_pedida",
            field=models.DecimalField(decimal_places=3, default=0, max_digits=10),
        ),
        migrations.RunPython(backfill_quantidade_pedida, migrations.RunPython.noop),
    ]
