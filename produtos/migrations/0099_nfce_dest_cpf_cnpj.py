# NFC-e destinatário: dest_cpf passa a caber CNPJ (14 dígitos).

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0098_catalogo_categoria_cor"),
    ]

    operations = [
        migrations.AlterField(
            model_name="nfcedocumentoagro",
            name="dest_cpf",
            field=models.CharField(
                blank=True,
                default="",
                help_text="CPF (11) ou CNPJ (14) do destinatário na NFC-e.",
                max_length=14,
            ),
        ),
    ]
