# Generated manually — cor do card em qualquer nível da categoria.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0097_repasse_reserva_lucro_log"),
    ]

    operations = [
        migrations.AddField(
            model_name="catalogodeliverycategoria",
            name="cor",
            field=models.CharField(
                blank=True,
                default="",
                help_text="Cor do card (#059669). Vazio = verde padrão. Vale em qualquer nível.",
                max_length=7,
            ),
        ),
        migrations.AlterField(
            model_name="catalogodeliverycategoria",
            name="imagem_base64",
            field=models.TextField(
                blank=True,
                default="",
                help_text="Foto de capa do card no catálogo (qualquer nível).",
            ),
        ),
    ]
