# Generated manually for WA-ARQUIVO

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0125_whatsapp_conversa_extras"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="arquivada",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="arquivada_em",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="arquivada_por",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
        migrations.AddIndex(
            model_name="whatsappconversaagro",
            index=models.Index(
                fields=["arquivada", "loja", "ultima_em"],
                name="wa_conv_arq_loja_ult_idx",
            ),
        ),
    ]
