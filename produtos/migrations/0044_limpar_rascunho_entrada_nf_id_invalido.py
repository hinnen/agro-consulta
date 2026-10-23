"""Remove rascunhos Entrada NF com PK inválida (bug str(None) → 'None' / NULL)."""
from __future__ import annotations

from django.db import migrations


def _limpar_ids_invalidos(apps, schema_editor):
    EntradaNotaRascunhoAgro = apps.get_model("produtos", "EntradaNotaRascunhoAgro")
    EntradaNotaRascunhoAgro.objects.filter(rascunho_id__isnull=True).delete()
    EntradaNotaRascunhoAgro.objects.filter(rascunho_id="").delete()
    EntradaNotaRascunhoAgro.objects.filter(rascunho_id__iexact="none").delete()
    EntradaNotaRascunhoAgro.objects.filter(rascunho_id__iexact="null").delete()


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0043_entrada_nota_rascunho_agro"),
    ]

    operations = [
        migrations.RunPython(_limpar_ids_invalidos, migrations.RunPython.noop),
    ]
