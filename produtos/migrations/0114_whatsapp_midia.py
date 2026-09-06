# Foto/áudio no chat WhatsApp (Renan 01/09)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0113_whatsapp_bot_cfg_renan"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="tipo_midia",
            field=models.CharField(blank=True, default="", max_length=16),
        ),
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="arquivo",
            field=models.FileField(blank=True, upload_to="whatsapp/%Y/%m/"),
        ),
    ]
