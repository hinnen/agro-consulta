# Pedido «logout» — trocar número do WhatsApp na ponte.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0118_whatsapp_aguardando_loja"),
    ]

    operations = [
        migrations.AlterField(
            model_name="whatsapppontepedidoagro",
            name="tipo",
            field=models.CharField(
                choices=[
                    ("contatos", "Agenda"),
                    ("historico", "Histórico"),
                    ("pairing", "Código de ligação"),
                    ("logout", "Trocar WhatsApp"),
                ],
                db_index=True,
                max_length=16,
            ),
        ),
    ]
