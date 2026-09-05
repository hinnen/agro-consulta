# Generated manually — planos que descontam do lucro enviado ao Centro

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0090_caixa_conferencia_rascunho_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="repassevilaconfigagro",
            name="planos_desconto_centro",
            field=models.JSONField(
                blank=True,
                default=list,
                help_text="Nomes de plano de conta que descontam do lucro enviado ao Centro. "
                "Os demais descontam do que ficou na Vila.",
            ),
        ),
    ]
