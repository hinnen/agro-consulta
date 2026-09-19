# Apagar mensagem no Zap (pra todos) + flag local.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0121_whatsapp_foto_perfil"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="apagada",
            field=models.BooleanField(db_index=True, default=False),
        ),
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
                ],
                db_index=True,
                max_length=16,
            ),
        ),
    ]
