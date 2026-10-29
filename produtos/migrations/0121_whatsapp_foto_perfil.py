# Foto de perfil do contato no WhatsApp (lista estilo Zap Web).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0120_whatsapp_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="foto_perfil",
            field=models.FileField(blank=True, upload_to="whatsapp/perfil/%Y/%m/"),
        ),
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="foto_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
