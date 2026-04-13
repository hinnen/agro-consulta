# Generated manually for transferência pedido / impresso

import uuid

from django.db import migrations, models
from django.utils import timezone


def backfill_pedidos(apps, schema_editor):
    Pedido = apps.get_model("estoque", "PedidoTransferencia")
    for p in Pedido.objects.all():
        changed = False
        if p.lote_uuid is None:
            p.lote_uuid = uuid.uuid4()
            changed = True
        if p.impresso_em is None:
            p.impresso_em = p.criado_em or timezone.now()
            changed = True
        if not (p.status or "").strip():
            p.status = "IMPRESSO"
            changed = True
        if changed:
            p.save(update_fields=["lote_uuid", "impresso_em", "status"])


class Migration(migrations.Migration):

    dependencies = [
        ("estoque", "0011_estoque_sync_health_ajuste_origem"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidotransferencia",
            name="lote_uuid",
            field=models.UUIDField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name="pedidotransferencia",
            name="status",
            field=models.CharField(db_index=True, default="IMPRESSO", max_length=20),
        ),
        migrations.AddField(
            model_name="pedidotransferencia",
            name="impresso_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.RunPython(backfill_pedidos, migrations.RunPython.noop),
    ]
