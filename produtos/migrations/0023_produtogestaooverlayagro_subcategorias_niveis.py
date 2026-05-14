# Generated manually for Agro overlay — quatro níveis de subcategoria.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0022_estoque_lote"),
    ]

    operations = [
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="subcategoria_2",
            field=models.CharField(
                blank=True,
                default="",
                max_length=200,
                verbose_name="Subcategoria 2",
            ),
        ),
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="subcategoria_3",
            field=models.CharField(
                blank=True,
                default="",
                max_length=200,
                verbose_name="Subcategoria 3",
            ),
        ),
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="subcategoria_4",
            field=models.CharField(
                blank=True,
                default="",
                max_length=200,
                verbose_name="Subcategoria 4",
            ),
        ),
    ]
