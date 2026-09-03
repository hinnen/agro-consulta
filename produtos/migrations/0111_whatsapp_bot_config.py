# Bot WhatsApp — config Postgres + atraso de envio

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0109_alter_whatsapp_choices"),
    ]

    operations = [
        migrations.CreateModel(
            name="WhatsAppBotConfigAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(default="default", max_length=32, unique=True)),
                ("dados", models.JSONField(blank=True, default=dict)),
                ("atualizado_por", models.CharField(blank=True, default="", max_length=120)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Config bot WhatsApp",
                "verbose_name_plural": "Configs bot WhatsApp",
            },
        ),
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="liberar_envio_em",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
    ]
