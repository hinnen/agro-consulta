from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("financeiro", "0002_grafico_gastos_atalho_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="graficogastosatalhoagro",
            name="eh_padrao",
            field=models.BooleanField(
                default=False,
                help_text="Ao abrir o gráfico, aplica este atalho automaticamente (só um por vez).",
            ),
        ),
    ]
