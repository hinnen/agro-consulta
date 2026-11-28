from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("produtos", "0156_whatsapp_pedido_arquivar"),
    ]

    operations = [
        migrations.CreateModel(
            name="WhatsAppFigurinhaAgro",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("chave", models.CharField(max_length=64, unique=True)),
                ("arquivo", models.FileField(upload_to="whatsapp/figurinhas/")),
                ("mime", models.CharField(blank=True, default="image/webp", max_length=40)),
                ("criado_em", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
            ],
            options={
                "verbose_name": "Figurinha WhatsApp",
                "verbose_name_plural": "Figurinhas WhatsApp",
                "ordering": ["-id"],
            },
        ),
    ]
