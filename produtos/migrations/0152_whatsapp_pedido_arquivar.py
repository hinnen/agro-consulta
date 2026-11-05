# Pedido «arquivar» — sync arquivo Zap ↔ Agro (celular / Web / zap.sistvale).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0151_pdv_rapido_layout"),
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
                    ("apagar", "Apagar mensagem"),
                    ("arquivar", "Arquivar conversa"),
                ],
                db_index=True,
                max_length=16,
            ),
        ),
    ]
