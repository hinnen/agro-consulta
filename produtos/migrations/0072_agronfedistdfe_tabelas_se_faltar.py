# Garante tabelas DF-e em ambientes que ainda não as têm (Render/Postgres novo).

from django.db import migrations


def _criar_se_faltar(apps, schema_editor):
    connection = schema_editor.connection
    existing = set(connection.introspection.table_names())
    AgroNfeDistDfeCursor = apps.get_model("produtos", "AgroNfeDistDfeCursor")
    AgroNfeDistDfeDocumento = apps.get_model("produtos", "AgroNfeDistDfeDocumento")
    if "produtos_agronfedistdfecursor" not in existing:
        schema_editor.create_model(AgroNfeDistDfeCursor)
    if "produtos_agronfedistdfedocumento" not in existing:
        schema_editor.create_model(AgroNfeDistDfeDocumento)


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0071_agronfedistdfe_models_state"),
    ]

    operations = [
        migrations.RunPython(_criar_se_faltar, migrations.RunPython.noop),
    ]
