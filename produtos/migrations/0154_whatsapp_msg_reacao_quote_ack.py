# WA-SKIN-V2 passos 3–4: reações, citar, ✓✓, digitando (extras)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0153_nfe_fiscal_contador"),
    ]

    operations = [
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="ack",
            field=models.PositiveSmallIntegerField(db_index=True, default=0),
        ),
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="reply_wa_id",
            field=models.CharField(blank=True, db_index=True, default="", max_length=80),
        ),
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="reply_texto",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
        migrations.AddField(
            model_name="whatsappmensagemagro",
            name="reacoes",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
