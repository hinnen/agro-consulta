from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("produtos", "0129_pedido_entrega_paga_na_loja"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidoentrega",
            name="pdv_lista_concluida",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="Some da lista Pagas na loja. Se não marcar, some sozinha em 24 h.",
                verbose_name="Concluída no overlay PDV",
            ),
        ),
    ]
