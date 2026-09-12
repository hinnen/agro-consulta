# Código de ligação WhatsApp (sem QR) — Renan 01/09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0114_whatsapp_midia"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappponteestadoagro",
            name="pairing_code",
            field=models.CharField(blank=True, default="", max_length=16),
        ),
        migrations.AlterField(
            model_name="whatsapppontepedidoagro",
            name="tipo",
            field=models.CharField(
                choices=[
                    ("contatos", "Agenda"),
                    ("historico", "Histórico"),
                    ("pairing", "Código de ligação"),
                ],
                db_index=True,
                max_length=16,
            ),
        ),
    ]
