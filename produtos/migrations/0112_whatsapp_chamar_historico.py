# WhatsApp — chamar contato + histórico curto + agenda sob demanda

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0111_whatsapp_bot_config"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappconversaagro",
            name="origem_abertura",
            field=models.CharField(db_index=True, default="in", max_length=8),
        ),
        migrations.AlterField(
            model_name="whatsappmensagemagro",
            name="criado_em",
            field=models.DateTimeField(db_index=True, default=django.utils.timezone.now),
        ),
        migrations.CreateModel(
            name="WhatsAppAgendaContatoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("jid", models.CharField(max_length=80, unique=True)),
                ("telefone", models.CharField(blank=True, db_index=True, default="", max_length=32)),
                ("nome", models.CharField(blank=True, default="", max_length=120)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Contato agenda WhatsApp",
                "verbose_name_plural": "Contatos agenda WhatsApp",
            },
        ),
        migrations.CreateModel(
            name="WhatsAppPontePedidoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(db_index=True, max_length=16)),
                ("jid", models.CharField(blank=True, db_index=True, default="", max_length=80)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("status", models.CharField(db_index=True, default="pendente", max_length=12)),
                ("erro", models.CharField(blank=True, default="", max_length=200)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
            ],
            options={
                "verbose_name": "Pedido ponte WhatsApp",
                "verbose_name_plural": "Pedidos ponte WhatsApp",
            },
        ),
    ]
