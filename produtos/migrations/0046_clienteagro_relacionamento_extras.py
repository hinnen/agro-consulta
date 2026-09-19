# Generated manually — relacionamento F8 extras no Postgres

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0045_dashboard_venda_dia_historico"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="relacionamento_extras_json",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text="Pets, lembretes de saúde e anotações do F8 — fonte Agro (Postgres).",
                verbose_name="Relacionamento (pets, saúde, anotações)",
            ),
        ),
    ]
