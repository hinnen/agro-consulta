from django.db import migrations, models
import django.db.models.deletion


def _criar_nfce_se_faltando(apps, schema_editor):
    """Só no banco: pula se as tabelas já existirem (staging com drift)."""
    existing = set(schema_editor.connection.introspection.table_names())
    if "produtos_nfcenumeracaoagro" in existing and "produtos_nfcedocumentoagro" in existing:
        return

    vendor = schema_editor.connection.vendor
    if vendor == "postgresql":
        if "produtos_nfcenumeracaoagro" not in existing:
            schema_editor.execute(
                """
                CREATE TABLE IF NOT EXISTS produtos_nfcenumeracaoagro (
                    id BIGSERIAL PRIMARY KEY,
                    serie SMALLINT NOT NULL CHECK (serie >= 0),
                    proximo_numero INTEGER NOT NULL CHECK (proximo_numero >= 0),
                    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        if "produtos_nfcedocumentoagro" not in existing:
            schema_editor.execute(
                """
                CREATE TABLE IF NOT EXISTS produtos_nfcedocumentoagro (
                    id BIGSERIAL PRIMARY KEY,
                    status VARCHAR(16) NOT NULL,
                    chave VARCHAR(44) NOT NULL DEFAULT '',
                    numero INTEGER NOT NULL CHECK (numero >= 0),
                    serie SMALLINT NOT NULL CHECK (serie >= 0),
                    protocolo VARCHAR(20) NOT NULL DEFAULT '',
                    dest_cpf VARCHAR(11) NOT NULL DEFAULT '',
                    consumidor_sem_identificacao BOOLEAN NOT NULL DEFAULT FALSE,
                    xml_autorizado TEXT NOT NULL DEFAULT '',
                    qr_code_url TEXT NOT NULL DEFAULT '',
                    mensagem_sefaz TEXT NOT NULL DEFAULT '',
                    tp_amb SMALLINT NOT NULL CHECK (tp_amb >= 0),
                    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    venda_id BIGINT NOT NULL UNIQUE REFERENCES produtos_vendaagro(id)
                        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
                );
                """
            )
            schema_editor.execute(
                "CREATE INDEX IF NOT EXISTS produtos_nfcedocumentoagro_status_idx "
                "ON produtos_nfcedocumentoagro (status);"
            )
            schema_editor.execute(
                "CREATE INDEX IF NOT EXISTS produtos_nfcedocumentoagro_chave_idx "
                "ON produtos_nfcedocumentoagro (chave);"
            )
        return

    # SQLite / dev local
    numeracao = apps.get_model("produtos", "NfceNumeracaoAgro")
    documento = apps.get_model("produtos", "NfceDocumentoAgro")
    if "produtos_nfcenumeracaoagro" not in existing:
        schema_editor.create_model(numeracao)
    if "produtos_nfcedocumentoagro" not in existing:
        schema_editor.create_model(documento)


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0038_cadastro_planilha_import_historico"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name="NfceNumeracaoAgro",
                    fields=[
                        (
                            "id",
                            models.BigAutoField(
                                auto_created=True,
                                primary_key=True,
                                serialize=False,
                                verbose_name="ID",
                            ),
                        ),
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
                        (
                            "id",
                            models.BigAutoField(
                                auto_created=True,
                                primary_key=True,
                                serialize=False,
                                verbose_name="ID",
                            ),
                        ),
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
                        (
                            "chave",
                            models.CharField(blank=True, db_index=True, default="", max_length=44),
                        ),
                        ("numero", models.PositiveIntegerField(default=0)),
                        ("serie", models.PositiveSmallIntegerField(default=1)),
                        ("protocolo", models.CharField(blank=True, default="", max_length=20)),
                        ("dest_cpf", models.CharField(blank=True, default="", max_length=11)),
                        ("consumidor_sem_identificacao", models.BooleanField(default=False)),
                        ("xml_autorizado", models.TextField(blank=True, default="")),
                        ("qr_code_url", models.TextField(blank=True, default="")),
                        ("mensagem_sefaz", models.TextField(blank=True, default="")),
                        (
                            "tp_amb",
                            models.PositiveSmallIntegerField(
                                default=2, help_text="1 produção · 2 homologação"
                            ),
                        ),
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
            ],
            database_operations=[
                migrations.RunPython(_criar_nfce_se_faltando, migrations.RunPython.noop),
            ],
        ),
    ]
