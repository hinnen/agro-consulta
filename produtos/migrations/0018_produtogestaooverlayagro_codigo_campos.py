from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0017_produto_gestao_overlay_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="codigo_barras",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                max_length=80,
                verbose_name="Código de barras (override)",
            ),
        ),
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="codigo_nfe",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="",
                max_length=64,
                verbose_name="Código / NFe GM (override)",
            ),
        ),
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="subcategoria",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="produtogestaooverlayagro",
            name="descricao",
            field=models.TextField(blank=True, default="", verbose_name="Descrição (override)"),
        ),
    ]
