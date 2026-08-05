# Alinha PlanoContaAgro do teste ao cadastro que já roda na loja (0065):
# ganha `tipo` + PlanoContaAliasAgro e perde codigo/natureza/criado_por.
# DB em RunPython condicional (Postgres e SQLite): na loja já está pronto → no-op.

import django.db.models.deletion
from django.conf import settings
from django.core.exceptions import FieldDoesNotExist
from django.db import migrations, models


TABELA = "produtos_planocontaagro"
TABELA_ALIAS = "produtos_planocontaaliasagro"


def _colunas(schema_editor) -> set[str]:
    conn = schema_editor.connection
    with conn.cursor() as cur:
        return {c.name for c in conn.introspection.get_table_description(cur, TABELA)}


def alinhar(apps, schema_editor):
    """Deixa o banco no formato da loja. Onde já estiver assim, não faz nada."""
    from produtos.models import PlanoContaAgro as PlanoFinal
    from produtos.models import PlanoContaAliasAgro

    conn = schema_editor.connection
    tabelas = set(conn.introspection.table_names())
    if TABELA not in tabelas:
        return

    # Colunas antigas saem pelo modelo histórico (SQLite refaz a tabela com o formato certo).
    antigo = apps.get_model("produtos", "PlanoContaAgro")
    cols = _colunas(schema_editor)
    for nome, coluna in (("codigo", "codigo"), ("natureza", "natureza"), ("criado_por", "criado_por_id")):
        if coluna not in cols:
            continue
        try:
            campo = antigo._meta.get_field(nome)
        except FieldDoesNotExist:
            continue
        schema_editor.remove_field(antigo, campo)

    if "tipo" not in _colunas(schema_editor):
        schema_editor.add_field(PlanoFinal, PlanoFinal._meta.get_field("tipo"))

    if TABELA_ALIAS not in tabelas:
        schema_editor.create_model(PlanoContaAliasAgro)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0083_dfe_manifestacao_ciencia"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[migrations.RunPython(alinhar, noop_reverse)],
            state_operations=[
                migrations.RemoveField(model_name="planocontaagro", name="codigo"),
                migrations.RemoveField(model_name="planocontaagro", name="natureza"),
                migrations.RemoveField(model_name="planocontaagro", name="criado_por"),
                migrations.AddField(
                    model_name="planocontaagro",
                    name="tipo",
                    field=models.CharField(
                        blank=True,
                        choices=[
                            ("fixa", "Fixa"),
                            ("variavel", "Variável"),
                            ("outra", "Outra"),
                        ],
                        default="outra",
                        max_length=16,
                    ),
                ),
                migrations.AlterField(
                    model_name="planocontaagro",
                    name="observacao",
                    field=models.CharField(blank=True, default="", max_length=400),
                ),
                migrations.AlterModelOptions(
                    name="planocontaagro",
                    options={
                        "ordering": ["nome"],
                        "verbose_name": "Plano de conta Agro",
                        "verbose_name_plural": "Planos de conta Agro",
                    },
                ),
                migrations.CreateModel(
                    name="PlanoContaAliasAgro",
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
                        ("grafia", models.CharField(db_index=True, max_length=200, unique=True)),
                        ("criado_em", models.DateTimeField(auto_now_add=True)),
                        (
                            "criado_por",
                            models.ForeignKey(
                                blank=True,
                                null=True,
                                on_delete=django.db.models.deletion.SET_NULL,
                                related_name="plano_conta_aliases",
                                to=settings.AUTH_USER_MODEL,
                            ),
                        ),
                        (
                            "plano",
                            models.ForeignKey(
                                on_delete=django.db.models.deletion.CASCADE,
                                related_name="aliases",
                                to="produtos.planocontaagro",
                            ),
                        ),
                    ],
                    options={
                        "verbose_name": "Alias plano de conta",
                        "verbose_name_plural": "Aliases plano de conta",
                        "ordering": ["grafia"],
                    },
                ),
            ],
        ),
    ]
