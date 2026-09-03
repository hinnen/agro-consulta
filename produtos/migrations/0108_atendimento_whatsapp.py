# Atendimento WhatsApp (QR + filas Centro/Vila)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0110_pdv_topbar_layout"),
    ]

    operations = [
        migrations.CreateModel(
            name="WhatsAppPonteEstadoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(default="default", max_length=32, unique=True)),
                ("status", models.CharField(db_index=True, default="desconectado", max_length=20)),
                ("qr_data_url", models.TextField(blank=True, default="")),
                ("numero", models.CharField(blank=True, default="", max_length=32)),
                ("aviso", models.CharField(blank=True, default="", max_length=240)),
                ("heartbeat_em", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Ponte WhatsApp (estado)",
                "verbose_name_plural": "Ponte WhatsApp (estado)",
            },
        ),
        migrations.CreateModel(
            name="WhatsAppConversaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("jid", models.CharField(max_length=80, unique=True)),
                ("telefone", models.CharField(blank=True, db_index=True, default="", max_length=32)),
                ("nome", models.CharField(blank=True, default="", max_length=120)),
                ("loja", models.CharField(db_index=True, default="pendente", max_length=16)),
                ("menu_enviado", models.BooleanField(default=False)),
                ("nao_lidas", models.PositiveIntegerField(default=0)),
                ("ultima_preview", models.CharField(blank=True, default="", max_length=160)),
                ("ultima_em", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "verbose_name": "Conversa WhatsApp",
                "verbose_name_plural": "Conversas WhatsApp",
                "ordering": ["-ultima_em", "-id"],
            },
        ),
        migrations.CreateModel(
            name="WhatsAppMensagemAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "conversa",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="mensagens",
                        to="produtos.whatsappconversaagro",
                    ),
                ),
                ("direcao", models.CharField(db_index=True, max_length=8)),
                ("texto", models.TextField()),
                ("wa_id", models.CharField(blank=True, db_index=True, default="", max_length=80)),
                ("pendente_envio", models.BooleanField(db_index=True, default=False)),
                ("enviado_em", models.DateTimeField(blank=True, null=True)),
                ("erro_envio", models.CharField(blank=True, default="", max_length=200)),
                ("autor_nome", models.CharField(blank=True, default="", max_length=120)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
            ],
            options={
                "verbose_name": "Mensagem WhatsApp",
                "verbose_name_plural": "Mensagens WhatsApp",
                "ordering": ["id"],
            },
        ),
        migrations.AddIndex(
            model_name="whatsappconversaagro",
            index=models.Index(fields=["loja", "ultima_em"], name="wa_conv_loja_ult_idx"),
        ),
        migrations.AddIndex(
            model_name="whatsappmensagemagro",
            index=models.Index(fields=["conversa", "id"], name="wa_msg_conv_id_idx"),
        ),
    ]
