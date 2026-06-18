from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0038_cadastro_planilha_import_historico"),
    ]

    operations = [
        migrations.CreateModel(
            name="NfceNumeracaoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("serie", models.PositiveSmallIntegerField(default=1)),
                ("proximo_numero", models.PositiveIntegerField(default=1)),
                ("atualizado_em", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Numeração NFC-e",
                "verbose_name_plural": "Numerações NFC-e",
            },
        ),
        migrations.CreateModel(
            name="NfceDocumentoAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("autorizada", "Autorizada"),
                            ("rejeitada", "Rejeitada"),
                            ("erro", "Erro técnico"),
                        ],
                        db_index=True,
                        max_length=16,
                    ),
                ),
                ("chave", models.CharField(blank=True, db_index=True, default="", max_length=44)),
                ("numero", models.PositiveIntegerField(default=0)),
                ("serie", models.PositiveSmallIntegerField(default=1)),
                ("protocolo", models.CharField(blank=True, default="", max_length=20)),
                ("dest_cpf", models.CharField(blank=True, default="", max_length=11)),
                ("consumidor_sem_identificacao", models.BooleanField(default=False)),
                ("xml_autorizado", models.TextField(blank=True, default="")),
                ("qr_code_url", models.TextField(blank=True, default="")),
                ("mensagem_sefaz", models.TextField(blank=True, default="")),
                ("tp_amb", models.PositiveSmallIntegerField(default=2, help_text="1 produção · 2 homologação")),
                ("criado_em", models.DateTimeField(auto_now_add=True)),
                (
                    "venda",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="nfce",
                        to="produtos.vendaagro",
                    ),
                ),
            ],
            options={
                "verbose_name": "NFC-e Agro",
                "verbose_name_plural": "NFC-e Agro",
                "ordering": ["-criado_em"],
            },
        ),
    ]
