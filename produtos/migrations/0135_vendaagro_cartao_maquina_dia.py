from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0134_vendaagro_cartao_maquina_dia_anterior"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="cartao_maquina_dia",
            field=models.DateField(
                blank=True,
                db_index=True,
                help_text="Dia em que o cartão passou na máquina, quando não foi hoje.",
                null=True,
            ),
        ),
    ]
