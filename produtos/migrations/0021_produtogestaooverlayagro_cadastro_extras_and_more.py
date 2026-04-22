# Generated manually for Agro cadastro extras + variação código interno

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0020_remove_produtomarcavariacaoagro_pmva_codigo_barras_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="cadastro_extras",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text="JSON livre: fiscal (NCM, CFOP…), kit (baixa_componentes, deposito), etc.",
                verbose_name="Extras cadastro (fiscal local, kit PDV, etc.)",
            ),
        ),
        migrations.AddField(
            model_name="produtomarcavariacaoagro",
            name="codigo_interno",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                max_length=80,
                verbose_name="Código interno (variação)",
            ),
        ),
    ]
