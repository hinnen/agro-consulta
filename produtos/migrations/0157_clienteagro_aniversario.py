from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0156_cliente_cartao_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="clienteagro",
            name="aniversario_dia",
            field=models.PositiveSmallIntegerField(blank=True, null=True, verbose_name="Dia do aniversário"),
        ),
        migrations.AddField(
            model_name="clienteagro",
            name="aniversario_mes",
            field=models.PositiveSmallIntegerField(blank=True, null=True, verbose_name="Mês do aniversário"),
        ),
    ]
