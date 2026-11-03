from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0154_whatsapp_msg_reacao_quote_ack"),
    ]

    operations = [
        migrations.CreateModel(
            name="WhatsAppPublicOriginAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(default="default", max_length=32, unique=True)),
                ("origin", models.CharField(blank=True, default="", max_length=512)),
                ("fonte", models.CharField(blank=True, default="pc", max_length=32)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "WhatsApp — URL pública (PC)",
                "verbose_name_plural": "WhatsApp — URL pública (PC)",
            },
        ),
    ]
