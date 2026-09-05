from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0088_nfce_emitente_por_loja"),
    ]

    operations = [
        migrations.AddField(
            model_name="itemvendaagro",
            name="unidade",
            field=models.CharField(
                blank=True,
                default="UN",
                help_text="UN, KG, etc. — usado na NFC-e (uCom/uTrib).",
                max_length=12,
            ),
        ),
    ]
