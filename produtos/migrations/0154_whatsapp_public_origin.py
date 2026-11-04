from django.db import migrations, models

_TABELA = "produtos_whatsapppublicoriginagro"


def criar_tabela_se_faltar(apps, schema_editor):
    """A tabela já está no Postgres da loja. Criar de novo derruba o build do Render."""
    nomes = set(schema_editor.connection.introspection.table_names())
    if _TABELA in nomes:
        return
    model = apps.get_model("produtos", "WhatsAppPublicOriginAgro")
    schema_editor.create_model(model)


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0153_nfe_fiscal_contador"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name="WhatsAppPublicOriginAgro",
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
            ],
            database_operations=[
                migrations.RunPython(criar_tabela_se_faltar, migrations.RunPython.noop),
            ],
        ),
    ]
