# Status/stories WhatsApp — separado do chat.

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0119_whatsapp_pedido_logout"),
    ]

    operations = [
        migrations.CreateModel(
            name="WhatsAppStatusAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("autor_jid", models.CharField(db_index=True, max_length=80)),
                ("telefone", models.CharField(blank=True, db_index=True, default="", max_length=32)),
                ("nome", models.CharField(blank=True, default="", max_length=120)),
                ("jid_lid", models.CharField(blank=True, default="", max_length=80)),
                ("wa_id", models.CharField(blank=True, db_index=True, default="", max_length=80)),
                ("texto", models.TextField(blank=True, default="")),
                ("tipo_midia", models.CharField(blank=True, default="", max_length=16)),
                ("arquivo", models.FileField(blank=True, upload_to="whatsapp/status/%Y/%m/")),
                ("criado_em", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("expira_em", models.DateTimeField(db_index=True)),
            ],
            options={
                "verbose_name": "Status WhatsApp",
                "verbose_name_plural": "Status WhatsApp",
                "ordering": ["criado_em", "id"],
                "indexes": [
                    models.Index(fields=["autor_jid", "criado_em"], name="wa_st_autor_cri_idx"),
                ],
            },
        ),
        migrations.AddConstraint(
            model_name="whatsappstatusagro",
            constraint=models.UniqueConstraint(
                condition=models.Q(("wa_id", ""), _negated=True),
                fields=("wa_id",),
                name="wa_st_wa_id_uniq",
            ),
        ),
    ]
