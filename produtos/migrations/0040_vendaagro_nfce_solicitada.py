from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0039_nfce_documento_agro"),
    ]

    operations = [
        migrations.AddField(
            model_name="vendaagro",
            name="nfce_solicitada",
            field=models.BooleanField(
                db_index=True,
                default=False,
                help_text="PDV pediu cupom fiscal (NFC-e) nesta venda.",
            ),
        ),
    ]
