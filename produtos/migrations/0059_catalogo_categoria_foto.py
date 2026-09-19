# Generated manually — foto no card da categoria (home do catálogo)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0058_catalogo_categorias_enderecos"),
    ]

    operations = [
        migrations.AddField(
            model_name="catalogodeliverycategoria",
            name="imagem_base64",
            field=models.TextField(
                blank=True,
                default="",
                help_text="Foto do card na home do catálogo (só categorias principais).",
            ),
        ),
        migrations.AddField(
            model_name="catalogodeliverycategoria",
            name="imagem_mime",
            field=models.CharField(blank=True, default="image/jpeg", max_length=40),
        ),
    ]
