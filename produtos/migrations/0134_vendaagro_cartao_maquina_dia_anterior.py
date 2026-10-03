from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0133_pedido_entrega_data_prevista"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="cartao_maquina_dia_anterior",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="Entrega fechada no dia seguinte: o cartão passou na máquina no dia anterior e não soma no esperado do relatório de hoje.",
            ),
        ),
    ]
