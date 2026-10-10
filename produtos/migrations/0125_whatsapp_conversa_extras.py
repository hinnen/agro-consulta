# Extras da conversa WhatsApp (VIP, nota, lista espera) — recursos desligados por padrão.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0124_whatsapp_msg_wa_id_uniq"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="extras",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
