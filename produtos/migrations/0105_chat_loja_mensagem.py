# Generated manually for chat loja PDV

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0104_tabela_preco_forma"),
    ]

    operations = [
        migrations.CreateModel(
            name="ChatLojaMensagemAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("canal", models.CharField(db_index=True, default="geral", max_length=32)),
                ("texto", models.CharField(max_length=500)),
                ("autor_nome", models.CharField(blank=True, default="", max_length=120)),
                ("deposito", models.CharField(blank=True, db_index=True, default="", max_length=16)),
                ("ponto", models.CharField(blank=True, default="", max_length=32)),
                ("origem_rotulo", models.CharField(blank=True, default="", max_length=80)),
                ("device_id", models.CharField(blank=True, db_index=True, default="", max_length=64)),
                ("criado_em", models.DateTimeField(auto_now_add=True, db_index=True)),
            ],
            options={
                "verbose_name": "Mensagem chat loja",
                "verbose_name_plural": "Mensagens chat loja",
                "ordering": ["id"],
            },
        ),
        migrations.AddIndex(
            model_name="chatlojamensagemagro",
            index=models.Index(fields=["canal", "id"], name="chatloja_canal_id_idx"),
        ),
    ]
