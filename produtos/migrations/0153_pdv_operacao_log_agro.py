from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0152_whatsapp_pedido_arquivar"),
    ]

    operations = [
        migrations.CreateModel(
            name="PdvOperacaoLogAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("operador_nome", models.CharField(blank=True, default="", max_length=120)),
                ("usuario_login", models.CharField(blank=True, default="", max_length=150)),
                ("acao", models.CharField(db_index=True, max_length=80)),
                ("metodo", models.CharField(blank=True, default="", max_length=12)),
                ("path", models.CharField(blank=True, default="", max_length=300)),
                ("status_http", models.PositiveSmallIntegerField(default=0)),
                ("detalhe_json", models.JSONField(blank=True, default=dict)),
                ("django_session_key", models.CharField(blank=True, default="", max_length=50)),
            ],
            options={
                "verbose_name": "Log PDV",
                "verbose_name_plural": "Logs PDV",
                "ordering": ["-criado_em"],
            },
        ),
        migrations.AddIndex(
            model_name="pdvoperacaologagro",
            index=models.Index(fields=["acao", "-criado_em"], name="pdv_op_log_acao_idx"),
        ),
    ]
