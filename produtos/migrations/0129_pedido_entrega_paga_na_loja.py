from django.db import migrations, models


def backfill_paga_na_loja(apps, schema_editor):
    PedidoEntrega = apps.get_model("produtos", "PedidoEntrega")
    PedidoEntrega.objects.filter(
        forma_pagamento="Pago na loja",
        venda_agro_id__isnull=False,
        aguarda_pagamento_pdv=False,
    ).exclude(status="cancelado").update(paga_na_loja=True)


class Migration(migrations.Migration):
    dependencies = [
        ("produtos", "0128_pedido_entrega_caixa_adiada"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="paga_na_loja",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="Venda já cobrada no caixa ao lançar a entrega. Overlay PDV 24h; não trava fechar caixa.",
                verbose_name="Paga na loja",
            ),
        ),
        migrations.RunPython(backfill_paga_na_loja, migrations.RunPython.noop),
    ]
