# Junta chat @lid com o telefone do mesmo cliente.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0115_whatsapp_pairing_code"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="jid_lid",
            field=models.CharField(blank=True, max_length=80, null=True, unique=True),
        ),
    ]
