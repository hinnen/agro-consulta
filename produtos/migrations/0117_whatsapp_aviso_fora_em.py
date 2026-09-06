# Guarda quando o bot avisou «fora do horário», para não repetir a cada mensagem.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0116_whatsapp_jid_lid"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="aviso_fora_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
