# Generated manually for Catalogo Delivery GM Agro

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0056_sessaocaixa_ponto_vila"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="origem",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                help_text="Ex.: pdv, catalogo — vazio = legado PDV.",
                max_length=24,
            ),
        ),
        migrations.CreateModel(
            name="CatalogoDeliveryConfig",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nome_loja", models.CharField(default="GM Agro", max_length=100)),
                (
                    "whatsapp_contato",
                    models.CharField(
                        blank=True,
                        default="",
                        help_text="DDI+DDD+número, só dígitos",
                        max_length=20,
                    ),
                ),
                ("mensagem_boas_vindas", models.TextField(blank=True, default="")),
                ("area_entrega", models.CharField(blank=True, default="", max_length=300)),
                ("endereco_loja", models.CharField(blank=True, default="", max_length=320)),
                ("cor_primaria", models.CharField(default="#059669", max_length=7)),
                ("cor_secundaria", models.CharField(default="#fff7ed", max_length=7)),
                (
                    "publicado",
                    models.BooleanField(
                        default=False,
                        help_text="Se desligado, o link público mostra «em breve» (staff ainda vê).",
                    ),
                ),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Configuração catálogo delivery",
                "verbose_name_plural": "Configuração catálogo delivery",
            },
        ),
    ]
