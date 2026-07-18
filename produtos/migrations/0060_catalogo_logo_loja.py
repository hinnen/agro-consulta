# Generated manually — logotipo da loja no catálogo delivery

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0059_catalogo_categoria_foto"),
    ]

    operations = [
        migrations.AddField(
            model_name="catalogodeliveryconfig",
            name="logo_base64",
            field=models.TextField(
                blank=True,
                default="",
                help_text="Logotipo da loja no topo do catálogo (antes do nome).",
            ),
        ),
        migrations.AddField(
            model_name="catalogodeliveryconfig",
            name="logo_mime",
            field=models.CharField(blank=True, default="image/png", max_length=40),
        ),
    ]
